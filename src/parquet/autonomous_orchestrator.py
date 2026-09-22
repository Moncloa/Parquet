from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from parquet.execution.autonomous import (
    AutonomousExecutionCoordinator,
    ExecutionAttempt,
    ExecutionAttemptState,
)
from parquet.execution.demo import DemoExecutionAdapter
from parquet.execution.etoro import EtoroExecutionClient
from parquet.execution.net_edge import evaluate_net_edge
from parquet.execution.net_exit import evaluate_net_exit
from parquet.execution.sizing import choose_autonomous_real_terms
from parquet.execution.supervised import RealSmallExecutionAdapter
from parquet.models import (
    Bias,
    MarketAnalysis,
    MarketObservation,
    ReviewRequest,
    Side,
    Trigger,
    TriggerAction,
    TriggerType,
    WatchItem,
)
from parquet.orchestrator import Orchestrator
from parquet.portfolio import PositionManager
from parquet.reconciliation import ReconciliationService
from parquet.scheduler import ScheduledReview

_WIDE_MIN_SAMPLES = 10
_WIDE_MIN_SPAN_SECONDS = 120.0


class AutonomousOrchestrator(Orchestrator):
    """Orchestrator variant that routes EXECUTE watches through the durable ledger.

    Autonomous execution supports shadow, demo, and explicitly enabled real mode.
    Real writes remain fail-closed behind reconciliation, risk gates, broker preflight,
    durable idempotency, and unresolved-outcome blocking.
    """

    def __init__(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
        super().__init__(*args, **kwargs)
        self.position_manager = PositionManager(self.storage)
        self.autonomous_execution = AutonomousExecutionCoordinator(
            self.storage,
            self.position_manager,
        )
        self.demo_execution: DemoExecutionAdapter | None = None
        if (
            self.settings.execution.autonomous_mode == "demo"
            and self.settings.execution.autonomous_demo_enabled
        ):
            try:
                demo_client = EtoroExecutionClient(
                    api_key=_read_secret(
                        self.settings.etoro.api_key_file,
                        "eToro API key",
                    ),
                    user_key=_read_secret(
                        self.settings.etoro.demo_user_key_file,
                        "eToro demo User key",
                    ),
                    base_url=self.settings.etoro.execution_base_url,
                    identity_base_url=self.settings.etoro.base_url,
                    environment="demo",
                )
            except Exception as exc:
                self.storage.set("demo_execution_ready", "0")
                self.storage.set("demo_execution_error", str(exc))
            else:
                self.demo_execution = DemoExecutionAdapter(
                    settings=self.settings,
                    storage=self.storage,
                    client=demo_client,
                )
                self.storage.set("demo_execution_ready", "1")
                self.storage.set("demo_execution_error", "")

        self.real_execution: RealSmallExecutionAdapter | None = None
        self.real_reconciliation: ReconciliationService | None = None
        self.real_client: EtoroExecutionClient | None = None
        if (
            self.settings.execution.autonomous_mode == "real"
            and self.settings.execution.autonomous_real_enabled
        ):
            try:
                self.real_client = EtoroExecutionClient(
                    api_key=_read_secret(
                        self.settings.etoro.api_key_file,
                        "eToro API key",
                    ),
                    user_key=_read_secret(
                        self.settings.etoro.user_key_file,
                        "eToro User key",
                    ),
                    base_url=self.settings.etoro.execution_base_url,
                    identity_base_url=self.settings.etoro.base_url,
                    environment="real",
                )
                self.real_reconciliation = ReconciliationService(
                    self.settings,
                    self.storage,
                    self.market_client,
                )
                self.real_execution = RealSmallExecutionAdapter(
                    settings=self.settings,
                    storage=self.storage,
                    position_manager=self.real_reconciliation.position_manager,
                    reconciliation=self.real_reconciliation,
                    client=self.real_client,
                )
            except Exception as exc:
                self.real_execution = None
                self.real_reconciliation = None
                self.real_client = None
                self.storage.set("real_execution_ready", "0")
                self.storage.set("real_execution_error", str(exc))
            else:
                self.storage.set("real_execution_ready", "1")
                self.storage.set("real_execution_error", "")

    def process_analysis(self, analysis: MarketAnalysis) -> None:
        super().process_analysis(analysis)
        if not self.settings.execution.autonomous_enabled:
            return
        if self.settings.execution.autonomous_mode not in {"shadow", "demo", "real"}:
            return

        for proposal in analysis.trade_proposals:
            symbol = proposal.symbol.upper()
            if symbol in {"AIR", "AIR.PA"}:
                self.storage.add_event(
                    "autonomous_proposal_blocked",
                    json.dumps(
                        {
                            "proposal_id": proposal.proposal_id,
                            "symbol": proposal.symbol,
                            "reason": "excluded_symbol",
                        }
                    ),
                )
                continue
            if proposal.stop_loss is None:
                self.storage.add_event(
                    "autonomous_proposal_blocked",
                    json.dumps(
                        {
                            "proposal_id": proposal.proposal_id,
                            "symbol": proposal.symbol,
                            "reason": "stop_loss_missing",
                        }
                    ),
                )
                continue

            is_buy = proposal.side == Side.BUY
            watch = WatchItem(
                watch_id=f"auto-exec-{proposal.proposal_id}",
                symbol=proposal.symbol,
                bias=Bias.LONG if is_buy else Bias.SHORT,
                trigger=Trigger(
                    type=(
                        TriggerType.PRICE_ABOVE
                        if is_buy
                        else TriggerType.PRICE_BELOW
                    ),
                    price=proposal.stop_loss,
                    timeframe=None,
                ),
                invalidation=proposal.stop_loss,
                expires_at=proposal.expires_at,
                on_trigger=TriggerAction.EXECUTE,
                proposal_id=proposal.proposal_id,
                execution_mode=self.settings.execution.autonomous_mode,
                rationale="deterministic bridge from fresh proposal to execution gate",
            )
            self.storage.save_watch(analysis.analysis_id, watch)
            self.storage.add_event(
                "autonomous_proposal_armed",
                json.dumps(
                    {
                        "analysis_id": analysis.analysis_id,
                        "proposal_id": proposal.proposal_id,
                        "watch_id": watch.watch_id,
                        "symbol": proposal.symbol,
                    }
                ),
            )

    def _persist_stream_state(self, current: datetime) -> None:
        super()._persist_stream_state(current)
        scanner = self.stream_scanner
        if scanner is None:
            return
        candidate_pool = scanner.shortlist(
            limit=min(100, max(self.settings.etoro.websocket_shortlist_size * 5, 20)),
            history_points=min(20, self.settings.etoro.history_context_points),
        )
        candidates = _filter_stream_candidates(
            candidate_pool,
            limit=self.settings.etoro.websocket_shortlist_size,
            max_spread_bps=self.settings.risk.max_spread_bps,
        )
        # Do not destroy the last good cross-process shortlist while a freshly
        # restarted/rotated scanner is still warming up to the quality threshold.
        # The stored timestamp remains unchanged and the normal freshness guard will
        # naturally expire it if no new good shortlist appears.
        if candidates:
            self.storage.set("websocket_shortlist_raw", json.dumps(candidates))
            self.storage.set("websocket_shortlist_at", current.astimezone(UTC).isoformat())
        self.storage.set("websocket_streamed_instruments", str(len(scanner.series)))
        self.storage.set("websocket_incoming_message_count", str(scanner.incoming_message_count))
        self.storage.set("websocket_parsed_tick_count", str(scanner.parsed_tick_count))
        self.storage.set(
            "websocket_last_tick_at",
            "" if scanner.last_tick_at is None else scanner.last_tick_at.isoformat(),
        )

    async def _stream_context(self, current: datetime) -> tuple[dict[str, object], list[str]]:
        scanner = self.stream_scanner
        live_warming_up = False
        if scanner is not None and scanner.connected and scanner.series:
            candidate_pool = scanner.shortlist(
                limit=min(100, max(self.settings.etoro.websocket_shortlist_size * 5, 20)),
                history_points=min(20, self.settings.etoro.history_context_points),
            )
            candidates = _filter_stream_candidates(
                candidate_pool,
                limit=self.settings.etoro.websocket_shortlist_size,
                max_spread_bps=self.settings.risk.max_spread_bps,
            )
            if candidates:
                enriched, symbols = await self._enrich_stream_candidates(candidates, source="live")
                return (
                    {
                        "enabled": True,
                        "source": "live_service_scanner",
                        "connected": True,
                        "url": self.settings.etoro.websocket_url,
                        "subscribed_instruments": len(scanner.instrument_ids),
                        "streamed_instruments": len(scanner.series),
                        "last_message_at": (
                            None if scanner.last_message_at is None else scanner.last_message_at.isoformat()
                        ),
                        "last_tick_at": (
                            None if scanner.last_tick_at is None else scanner.last_tick_at.isoformat()
                        ),
                        "last_error": scanner.last_error,
                        "ranking": "abs_stream_change_plus_step_volatility",
                        "filters": {
                            "min_samples": _WIDE_MIN_SAMPLES,
                            "min_span_seconds": _WIDE_MIN_SPAN_SECONDS,
                            "max_known_spread_bps": self.settings.risk.max_spread_bps,
                        },
                        "candidates": enriched,
                    },
                    symbols,
                )
            live_warming_up = True

        candidates = self._stored_stream_candidates(current)
        enriched, symbols = await self._enrich_stream_candidates(
            candidates,
            source="persisted_service_shortlist",
        )
        stored_connected = self.storage.get("websocket_connected") == "1"
        source = "persisted_service_shortlist" if candidates else (
            "scanner_warming_up" if live_warming_up or stored_connected else "no_fresh_shortlist"
        )
        return (
            {
                "enabled": True,
                "source": source,
                "connected": True if live_warming_up else stored_connected,
                "url": self.settings.etoro.websocket_url,
                "subscribed_instruments": (
                    len(scanner.instrument_ids)
                    if live_warming_up and scanner is not None
                    else _optional_int(self.storage.get("websocket_universe_subscribed_count"))
                ),
                "streamed_instruments": (
                    len(scanner.series)
                    if live_warming_up and scanner is not None
                    else _optional_int(self.storage.get("websocket_streamed_instruments"))
                ),
                "last_message_at": (
                    scanner.last_message_at.isoformat()
                    if live_warming_up and scanner is not None and scanner.last_message_at is not None
                    else self.storage.get("websocket_last_message_at") or None
                ),
                "last_tick_at": (
                    scanner.last_tick_at.isoformat()
                    if live_warming_up and scanner is not None and scanner.last_tick_at is not None
                    else self.storage.get("websocket_last_tick_at") or None
                ),
                "last_error": (
                    scanner.last_error
                    if live_warming_up and scanner is not None
                    else self.storage.get("websocket_last_error") or None
                ),
                "shortlist_at": self.storage.get("websocket_shortlist_at") or None,
                "ranking": "abs_stream_change_plus_step_volatility",
                "filters": {
                    "min_samples": _WIDE_MIN_SAMPLES,
                    "min_span_seconds": _WIDE_MIN_SPAN_SECONDS,
                    "max_known_spread_bps": self.settings.risk.max_spread_bps,
                },
                "candidates": enriched,
            },
            symbols,
        )

    async def _enrich_stream_candidates(
        self,
        candidates: list[dict[str, Any]],
        *,
        source: str,
    ) -> tuple[list[dict[str, object]], list[str]]:
        if not candidates:
            return [], []

        ids = [int(item["instrument_id"]) for item in candidates]
        metadata = {}
        if self.universe_client is not None:
            try:
                metadata = await self.universe_client.metadata(ids)
            except Exception as exc:
                self.storage.add_event(
                    "websocket_metadata_error",
                    json.dumps({"error": repr(exc), "source": source}),
                )

        rates_by_id = {}
        missing_symbol_ids = [
            instrument_id
            for instrument_id in ids
            if metadata.get(instrument_id) is None
            or metadata[instrument_id].symbol is None
        ]
        if missing_symbol_ids and self.market_client is not None:
            try:
                rates = await self.market_client.rates(missing_symbol_ids)
                rates_by_id = {rate.instrument_id: rate for rate in rates}
            except Exception as exc:
                self.storage.add_event(
                    "websocket_symbol_fallback_error",
                    json.dumps({"error": repr(exc), "source": source}),
                )

        enriched: list[dict[str, object]] = []
        symbols: list[str] = []
        for item in candidates:
            instrument_id = int(item["instrument_id"])
            meta = metadata.get(instrument_id)
            rate = rates_by_id.get(instrument_id)
            raw_symbol = item.get("symbol")
            symbol = (
                None if raw_symbol in (None, "") else str(raw_symbol)
            ) or (None if meta is None else meta.symbol) or (
                None if rate is None else rate.symbol
            )
            if symbol:
                self._instrument_ids[symbol.upper()] = instrument_id
                symbols.append(symbol)
            enriched.append(
                {
                    **item,
                    "symbol": symbol,
                    "name": None if meta is None else meta.name,
                    "instrument_type_id": None if meta is None else meta.instrument_type_id,
                    "exchange_id": None if meta is None else meta.exchange_id,
                }
            )
        return enriched, sorted(set(symbols))

    def _stored_stream_candidates(self, current: datetime) -> list[dict[str, Any]]:
        shortlist_at_raw = self.storage.get("websocket_shortlist_at")
        if not shortlist_at_raw:
            return []
        try:
            shortlist_at = datetime.fromisoformat(
                shortlist_at_raw.replace("Z", "+00:00")
            ).astimezone(UTC)
        except ValueError:
            return []
        freshness_seconds = max(120, self.settings.poll_seconds * 5)
        age = (current.astimezone(UTC) - shortlist_at).total_seconds()
        if age < 0 or age > freshness_seconds:
            return []

        raw = self.storage.get("websocket_shortlist_raw")
        if not raw:
            return []
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            return []
        if not isinstance(payload, list):
            return []
        candidates = [
            {str(key): value for key, value in raw_item.items()}
            for raw_item in payload
            if isinstance(raw_item, dict) and raw_item.get("instrument_id") is not None
        ]
        return _filter_stream_candidates(
            candidates,
            limit=self.settings.etoro.websocket_shortlist_size,
            max_spread_bps=self.settings.risk.max_spread_bps,
        )

    async def post_due_reviews(self, now: datetime | None = None) -> int:
        if self.bridge is None:
            return 0
        current = (now or datetime.now(UTC)).astimezone(UTC)
        due = self.reviews.due(current)
        if not due:
            return 0

        active_symbols = {watch.symbol for watch in self.storage.active_watches(current)}
        review_symbols = sorted(active_symbols | set(self.settings.etoro.review_symbols))
        stream_context, dynamic_symbols = await self._stream_context(current)
        review_symbols = sorted(set(review_symbols) | set(dynamic_symbols))

        context: dict[str, object] = {}
        if self.market_client is not None and review_symbols:
            try:
                context = await self._market_context(review_symbols, current)
            except Exception as exc:
                self.storage.add_event(
                    "market_snapshot_error",
                    json.dumps({"error": repr(exc)}),
                )
                context = {
                    "market_data": {
                        "provider": "etoro",
                        "error": repr(exc),
                    }
                }

        gate_reassessments: dict[str, object] = {}
        for symbol in review_symbols:
            raw_gate = self.storage.get(f"gate_reassessment:{symbol.upper()}")
            if not raw_gate:
                continue
            try:
                parsed_gate = json.loads(raw_gate)
            except json.JSONDecodeError:
                continue
            if isinstance(parsed_gate, dict):
                gate_reassessments[symbol.upper()] = parsed_gate
        if gate_reassessments:
            context["gate_reassessments"] = gate_reassessments

        market_data = context.get("market_data")
        if isinstance(market_data, dict):
            market_data["wide_scanner"] = stream_context
        else:
            context["market_data"] = {
                "provider": "etoro",
                "wide_scanner": stream_context,
            }

        count = 0
        for review in due:
            request = ReviewRequest(
                request_id=str(uuid4()),
                requested_at=current,
                reason=review.reason,
                symbols=review_symbols,
                context=context,
            )
            await self.bridge.post_review_request(request)
            self.storage.add_event("review_request", request.model_dump_json())
            self.storage.delete_review(review)
            count += 1
        self.ensure_structural_reviews(current)
        return count

    def _handle_execute_watch(
        self,
        watch: WatchItem,
        observation: MarketObservation,
    ) -> None:
        payload: dict[str, object] = {
            "watch_id": watch.watch_id,
            "symbol": watch.symbol,
            "observed_at": observation.observed_at.isoformat(),
        }
        if watch.on_trigger != TriggerAction.EXECUTE:
            return
        current_mode = self.settings.execution.autonomous_mode
        if watch.execution_mode != current_mode:
            payload["reasons"] = [
                f"execution_mode_mismatch:{watch.execution_mode or 'unset'}->{current_mode}"
            ]
            self.storage.add_event("execution_blocked", json.dumps(payload))
            return
        if watch.symbol.upper() in {"AIR", "AIR.PA"}:
            payload["reasons"] = ["excluded_symbol"]
            self.storage.add_event("execution_blocked", json.dumps(payload))
            return
        if watch.proposal_id is None:
            payload["reasons"] = ["proposal_id_missing"]
            self.storage.add_event("execution_rejected", json.dumps(payload))
            return

        payload["proposal_id"] = watch.proposal_id
        proposal = self.storage.get_proposal(watch.proposal_id)
        if proposal is None:
            payload["reasons"] = ["proposal_not_found"]
            self.storage.add_event("execution_rejected", json.dumps(payload))
            return
        if proposal.symbol.upper() != watch.symbol.upper():
            payload["reasons"] = ["proposal_watch_symbol_mismatch"]
            self.storage.add_event("execution_rejected", json.dumps(payload))
            return

        snapshot = self.storage.get_risk_snapshot()
        if snapshot is None:
            payload["reasons"] = ["risk_snapshot_unavailable"]
            self.storage.add_event("execution_rejected", json.dumps(payload))
            return

        history_context = self.history.context(
            proposal.symbol,
            now=observation.observed_at,
            retention_minutes=self.settings.etoro.history_retention_minutes,
            max_points=self.settings.etoro.history_context_points,
        )
        decision = self.execution_gate.evaluate(
            proposal,
            snapshot,
            observation,
            now=observation.observed_at,
            history_context=history_context,
        )
        payload["gate"] = decision.as_dict()
        if not decision.approved:
            self.storage.add_event("execution_rejected", json.dumps(payload))
            if "stop_too_tight_for_market" in decision.reasons:
                # This proposal's structural stop is incompatible with the current
                # noise regime. Do not keep retrying the same immutable proposal.
                self.storage.set_watch_status(watch.watch_id, "REASSESS_REQUIRED")
                floor = decision.minimum_stop_distance_bps
                stop = decision.stop_distance_bps
                components = decision.stop_floor_components or {}
                dominant = (
                    max(components.items(), key=lambda item: item[1])
                    if components
                    else None
                )
                reason_payload = {
                    "proposal_id": proposal.proposal_id,
                    "symbol": proposal.symbol,
                    "reason": "stop_too_tight_for_market",
                    "stop_distance_bps": stop,
                    "minimum_stop_distance_bps": floor,
                    "stop_floor_components": components,
                    "dominant_stop_floor": (
                        None
                        if dominant is None
                        else {"component": dominant[0], "bps": dominant[1]}
                    ),
                    "instruction": (
                        "Reassess current market regime. Treat the stop floor as a "
                        "noise lower bound, not a target. Test whether the recent "
                        "tradable range/oscillation can support a structural stop, "
                        "round-trip costs and sufficient net reward/risk. Generate a "
                        "new proposal only if the net edge survives; otherwise NO TRADE."
                    ),
                }
                self.storage.set(
                    f"gate_reassessment:{proposal.symbol.upper()}",
                    json.dumps(reason_payload),
                )
                self.storage.schedule_review(
                    ScheduledReview(
                        at=observation.observed_at.astimezone(UTC),
                        reason=f"gate_reassess:{proposal.symbol}:stop_too_tight_for_market",
                        source="gate",
                    )
                )
                self.storage.add_event(
                    "gate_reassessment_requested",
                    json.dumps(reason_payload),
                )
            return

        if not self.settings.execution.autonomous_enabled:
            payload["reasons"] = ["autonomous_execution_disabled"]
            self.storage.add_event("execution_blocked", json.dumps(payload))
            return

        mode = self.settings.execution.autonomous_mode
        if mode == "real" and not self.settings.execution.autonomous_real_enabled:
            payload["reasons"] = ["autonomous_real_execution_disabled"]
            self.storage.add_event("execution_blocked", json.dumps(payload))
            return
        if mode == "real" and self.real_execution is None:
            payload["reasons"] = [
                self.storage.get("real_execution_error")
                or "real_execution_adapter_unavailable"
            ]
            self.storage.add_event("execution_blocked", json.dumps(payload))
            return
        if mode == "demo" and not self.settings.execution.autonomous_demo_enabled:
            payload["reasons"] = ["autonomous_demo_execution_disabled"]
            self.storage.add_event("execution_blocked", json.dumps(payload))
            return
        if mode == "demo" and self.demo_execution is None:
            payload["reasons"] = [
                self.storage.get("demo_execution_error")
                or "demo_execution_adapter_unavailable"
            ]
            self.storage.add_event("execution_blocked", json.dumps(payload))
            return

        try:
            attempt = self.autonomous_execution.prepare(
                proposal=proposal,
                watch_id=watch.watch_id,
                observation=observation,
                decision=decision,
                now=observation.observed_at,
            )
        except RuntimeError as exc:
            payload["reasons"] = [str(exc)]
            self.storage.add_event("execution_blocked", json.dumps(payload))
            return

        if mode == "shadow":
            if attempt.state == ExecutionAttemptState.PREPARED:
                attempt = self.autonomous_execution.execute_shadow(
                    attempt,
                    now=observation.observed_at,
                )
            payload["attempt"] = attempt.model_dump(mode="json")
            self.storage.add_event(
                "execution_shadow",
                json.dumps(payload, default=str),
            )
            return

        if mode == "demo":
            if attempt.state == ExecutionAttemptState.PREPARED:
                attempt = self.autonomous_execution.mark_demo_pending(
                    attempt,
                    now=observation.observed_at,
                )
            payload["attempt"] = attempt.model_dump(mode="json")
            self.storage.add_event(
                "execution_demo_queued",
                json.dumps(payload, default=str),
            )
            return

        if attempt.state == ExecutionAttemptState.PREPARED:
            attempt = self.autonomous_execution.mark_real_pending(
                attempt,
                now=observation.observed_at,
            )
        payload["attempt"] = attempt.model_dump(mode="json")
        self.storage.add_event(
            "execution_real_queued",
            json.dumps(payload, default=str),
        )

    async def poll_market_once(self, now: datetime | None = None) -> int:
        processed = await super().poll_market_once(now)
        await self._process_demo_pending(now)
        await self._process_real_pending(now)
        await self._process_real_exits(now)
        return processed

    async def _process_demo_pending(self, now: datetime | None = None) -> int:
        if (
            not self.settings.execution.autonomous_enabled
            or self.settings.execution.autonomous_mode != "demo"
            or not self.settings.execution.autonomous_demo_enabled
            or self.demo_execution is None
        ):
            return 0

        pending = self.storage.demo_pending_execution_attempts(limit=1)
        if not pending:
            return 0

        attempt = pending[0]
        proposal = self.storage.get_proposal(attempt.proposal_id)
        snapshot = self.storage.get_risk_snapshot()
        if proposal is None or snapshot is None or self.market_client is None:
            return self._block_demo_attempt(
                attempt,
                "proposal_or_risk_or_market_context_unavailable",
            )

        try:
            rates = await self.market_client.rates([attempt.instrument_id])
        except Exception as exc:
            self.storage.add_event(
                "demo_execution_preflight_error",
                json.dumps(
                    {
                        "attempt_id": attempt.attempt_id,
                        "error": repr(exc),
                    }
                ),
            )
            return 0

        rate = next(
            (
                item
                for item in rates
                if item.instrument_id == attempt.instrument_id
            ),
            None,
        )
        if rate is None or rate.bid is None or rate.ask is None:
            return self._block_demo_attempt(
                attempt,
                "fresh_bid_ask_unavailable",
            )

        price = (
            rate.last_price
            if rate.last_price is not None
            else (rate.bid + rate.ask) / 2
        )
        observation = MarketObservation(
            symbol=proposal.symbol,
            price=price,
            observed_at=rate.timestamp,
            instrument_id=attempt.instrument_id,
            bid=rate.bid,
            ask=rate.ask,
        )
        gate_now = datetime.now(UTC)
        history_context = self.history.context(
            proposal.symbol,
            now=gate_now,
            retention_minutes=self.settings.etoro.history_retention_minutes,
            max_points=self.settings.etoro.history_context_points,
        )
        decision = self.execution_gate.evaluate(
            proposal,
            snapshot,
            observation,
            now=gate_now,
            history_context=history_context,
        )
        if not decision.approved or decision.amount_usd is None:
            reasons = ",".join(decision.reasons) or "gate_rejected"
            return self._block_demo_attempt(attempt, reasons)

        refreshed = attempt.model_copy(
            update={
                "amount_usd": decision.amount_usd,
                "updated_at": datetime.now(UTC),
            }
        )
        self.storage.save_execution_attempt(refreshed)
        try:
            await self.demo_execution.execute(refreshed)
        except RuntimeError as exc:
            latest = (
                self.storage.get_execution_attempt(refreshed.attempt_id)
                or refreshed
            )
            if latest.state == ExecutionAttemptState.DEMO_PENDING:
                return self._block_demo_attempt(latest, str(exc))
            self.storage.add_event(
                "demo_execution_runtime_error",
                json.dumps(
                    {
                        "attempt_id": latest.attempt_id,
                        "state": latest.state.value,
                        "error": str(exc),
                    }
                ),
            )
            return 0
        return 1

    async def _process_real_pending(self, now: datetime | None = None) -> int:
        if (
            not self.settings.execution.autonomous_enabled
            or self.settings.execution.autonomous_mode != "real"
            or not self.settings.execution.autonomous_real_enabled
            or self.real_execution is None
            or self.real_reconciliation is None
            or self.real_client is None
        ):
            return 0

        pending = self.storage.real_pending_execution_attempts(limit=1)
        if not pending:
            return 0

        attempt = pending[0]
        proposal = self.storage.get_proposal(attempt.proposal_id)
        if proposal is None or self.market_client is None:
            return self._block_real_attempt(
                attempt,
                "proposal_or_market_context_unavailable",
            )

        await self.real_reconciliation.poll_once(force=True)
        report = self.storage.get_reconciliation_report()
        snapshot = self.storage.get_risk_snapshot()
        if (
            report is None
            or not report.trading_enabled
            or snapshot is None
            or snapshot.equity_usd is None
        ):
            return self._block_real_attempt(
                attempt,
                "real_reconciliation_or_risk_not_ready",
            )

        try:
            rates = await self.market_client.rates([attempt.instrument_id])
        except Exception as exc:
            return self._block_real_attempt(
                attempt,
                f"fresh_quote_error:{type(exc).__name__}",
            )

        rate = next(
            (
                item
                for item in rates
                if item.instrument_id == attempt.instrument_id
            ),
            None,
        )
        if rate is None or rate.bid is None or rate.ask is None:
            return self._block_real_attempt(
                attempt,
                "fresh_bid_ask_unavailable",
            )

        price = (
            rate.last_price
            if rate.last_price is not None
            else (rate.bid + rate.ask) / 2
        )
        observation = MarketObservation(
            symbol=proposal.symbol,
            price=price,
            observed_at=rate.timestamp,
            instrument_id=attempt.instrument_id,
            bid=rate.bid,
            ask=rate.ask,
        )
        gate_now = datetime.now(UTC)
        history_context = self.history.context(
            proposal.symbol,
            now=gate_now,
            retention_minutes=self.settings.etoro.history_retention_minutes,
            max_points=self.settings.etoro.history_context_points,
        )
        decision = self.execution_gate.evaluate(
            proposal,
            snapshot,
            observation,
            now=gate_now,
            history_context=history_context,
        )
        if not decision.approved or decision.amount_usd is None:
            reasons = ",".join(decision.reasons) or "gate_rejected"
            return self._block_real_attempt(attempt, reasons)

        try:
            eligibility = await self.real_client.instrument_eligibility(
                instrument_id=attempt.instrument_id
            )
            if not eligibility.allow_open_position:
                raise RuntimeError("broker_disallows_open_position")
            direction = "LONG" if proposal.side == Side.BUY else "SHORT"
            minimum_capital = (
                snapshot.equity_usd
                * self.settings.execution.autonomous_real_min_position_pct
                / 100.0
            )
            maximum_capital = (
                snapshot.equity_usd
                * self.settings.execution.autonomous_real_max_position_pct
                / 100.0
            )
            (
                leverage,
                settlement_type,
                _broker_minimum,
                capital,
                _maximum_safe,
            ) = choose_autonomous_real_terms(
                eligibility,
                direction=direction,
                gate_maximum_notional_usd=decision.amount_usd,
                minimum_capital_usd=minimum_capital,
                maximum_capital_usd=maximum_capital,
                max_leverage=self.settings.execution.autonomous_real_max_leverage,
            )
        except Exception as exc:
            return self._block_real_attempt(
                attempt,
                f"real_broker_preflight:{exc}",
            )

        if self.settings.risk.net_edge_enabled:
            if decision.execution_price is None:
                return self._block_real_attempt(attempt, "net_edge_execution_price_unavailable")
            try:
                costs = await self.real_client.what_if_open_costs(
                    transaction="buy" if proposal.side == Side.BUY else "sellShort",
                    instrument_id=attempt.instrument_id,
                    settlement_type=settlement_type,
                    amount_usd=capital,
                    stop_loss_rate=proposal.stop_loss,
                    take_profit_rate=proposal.take_profit,
                    leverage=leverage,
                )
                edge = evaluate_net_edge(
                    proposal,
                    execution_price=decision.execution_price,
                    exposure_usd=capital * leverage,
                    open_cost_usd=costs.total_usd,
                    round_trip_cost_multiplier=(
                        self.settings.risk.estimated_round_trip_cost_multiplier
                    ),
                    min_net_reward_risk=self.settings.risk.min_net_reward_risk,
                    min_gross_reward_to_cost=(
                        self.settings.risk.min_gross_reward_to_cost
                    ),
                )
            except Exception as exc:
                return self._block_real_attempt(
                    attempt,
                    f"net_edge_preflight:{type(exc).__name__}:{exc}",
                )
            self.storage.add_event(
                "net_edge_preflight",
                json.dumps(
                    {
                        "attempt_id": attempt.attempt_id,
                        "proposal_id": proposal.proposal_id,
                        "symbol": proposal.symbol,
                        "capital_usd": capital,
                        "leverage": leverage,
                        "exposure_usd": capital * leverage,
                        "open_cost_usd": costs.total_usd,
                        "edge": edge.as_dict(),
                    },
                    default=str,
                ),
            )
            if not edge.approved:
                return self._block_real_attempt(
                    attempt,
                    edge.reason or "net_edge_rejected",
                )

        initial_net_risk_usd = None
        estimated_open_cost_usd = 0.0
        if self.settings.risk.net_edge_enabled:
            estimated_open_cost_usd = max(0.0, costs.total_usd)
            if proposal.stop_loss is not None and decision.execution_price is not None:
                stop_fraction = abs(decision.execution_price - proposal.stop_loss) / decision.execution_price
                initial_net_risk_usd = (
                    capital * leverage * stop_fraction
                    + estimated_open_cost_usd
                )

        refreshed = attempt.model_copy(
            update={
                "amount_usd": capital,
                "leverage": leverage,
                "settlement_type": settlement_type,
                "estimated_open_cost_usd": estimated_open_cost_usd,
                "initial_net_risk_usd": initial_net_risk_usd,
                "updated_at": datetime.now(UTC),
            }
        )
        self.storage.save_execution_attempt(refreshed)

        try:
            await self.real_execution.execute_autonomous(refreshed)
        except RuntimeError as exc:
            latest = (
                self.storage.get_execution_attempt(refreshed.attempt_id)
                or refreshed
            )
            if latest.state == ExecutionAttemptState.REAL_PENDING:
                return self._block_real_attempt(latest, str(exc))
            self.storage.add_event(
                "real_execution_runtime_error",
                json.dumps(
                    {
                        "attempt_id": latest.attempt_id,
                        "state": latest.state.value,
                        "error": str(exc),
                    }
                ),
            )
            return 0
        return 1

    async def _process_real_exits(self, now: datetime | None = None) -> int:
        """Evaluate managed real positions on net P&L and close only when enabled.

        Every close is preceded by forced reconciliation. A transport failure is
        never retried here because the broker outcome is ambiguous; reconciliation
        on the next poll is the source of truth.
        """
        if (
            not self.settings.risk.net_exit_enabled
            or self.settings.execution.autonomous_mode != "real"
            or self.real_client is None
            or self.real_reconciliation is None
        ):
            return 0

        await self.real_reconciliation.poll_once(force=True)
        report = self.storage.get_reconciliation_report()
        if report is None or not report.trading_enabled:
            return 0

        positions = self.storage.active_managed_positions()
        actions = 0
        for position in positions:
            initial_risk = position.initial_net_risk_usd
            if initial_risk is None and position.open_rate and position.stop_loss_rate:
                exposure = position.amount_usd * (position.leverage or 1.0)
                stop_fraction = abs(position.open_rate - position.stop_loss_rate) / position.open_rate
                initial_risk = exposure * stop_fraction + position.estimated_open_cost_usd

            # Until eToro exposes a dedicated close-cost what-if in this client,
            # use the observed opening cost as a conservative symmetric estimate.
            close_cost = max(0.0, position.estimated_open_cost_usd)
            decision = evaluate_net_exit(
                gross_pnl_usd=position.last_unrealized_pnl_usd,
                estimated_open_cost_usd=max(0.0, position.estimated_open_cost_usd),
                estimated_close_cost_usd=close_cost,
                initial_net_risk_usd=initial_risk,
                take_profit_net_r=self.settings.risk.net_exit_take_profit_r,
                protect_profit_net_r=self.settings.risk.net_exit_protect_profit_r,
            )
            self.storage.add_event(
                "net_exit_evaluation",
                json.dumps(
                    {
                        "local_id": position.local_id,
                        "broker_position_id": position.broker_position_id,
                        "symbol": position.symbol,
                        "decision": decision.as_dict(),
                    },
                    default=str,
                ),
            )

            if decision.action != "CLOSE":
                continue
            if not self.settings.risk.net_exit_real_close_enabled:
                self.storage.add_event(
                    "net_exit_close_shadow",
                    json.dumps(
                        {
                            "local_id": position.local_id,
                            "broker_position_id": position.broker_position_id,
                            "symbol": position.symbol,
                            "reason": decision.reason,
                        }
                    ),
                )
                continue

            # Reconcile immediately before each broker write.
            await self.real_reconciliation.poll_once(force=True)
            fresh_report = self.storage.get_reconciliation_report()
            if fresh_report is None or not fresh_report.trading_enabled:
                break
            try:
                result = await self.real_client.close_position(
                    position_id=position.broker_position_id
                )
            except Exception as exc:
                self.storage.add_event(
                    "net_exit_close_uncertain",
                    json.dumps(
                        {
                            "local_id": position.local_id,
                            "broker_position_id": position.broker_position_id,
                            "symbol": position.symbol,
                            "error": repr(exc),
                        }
                    ),
                )
                # Fail closed: do not issue any further broker writes this cycle.
                break
            self.storage.add_event(
                "net_exit_close_submitted",
                json.dumps(
                    {
                        "local_id": position.local_id,
                        "broker_position_id": position.broker_position_id,
                        "symbol": position.symbol,
                        "request_id": result.request_id,
                        "reason": decision.reason,
                    }
                ),
            )
            actions += 1
            # Confirm through reconciliation rather than trusting the write response.
            await self.real_reconciliation.poll_once(force=True)
        return actions

    def _block_real_attempt(
        self,
        attempt: ExecutionAttempt,
        reason: str,
    ) -> int:
        blocked = attempt.model_copy(
            update={
                "state": ExecutionAttemptState.BLOCKED,
                "reason": reason,
                "updated_at": datetime.now(UTC),
            }
        )
        self.storage.save_execution_attempt(blocked)
        self.storage.add_event(
            "real_execution_blocked",
            blocked.model_dump_json(),
        )
        return 1

    def _block_demo_attempt(
        self,
        attempt: ExecutionAttempt,
        reason: str,
    ) -> int:
        blocked = attempt.model_copy(
            update={
                "state": ExecutionAttemptState.BLOCKED,
                "reason": reason,
                "updated_at": datetime.now(UTC),
            }
        )
        self.storage.save_execution_attempt(blocked)
        self.storage.add_event(
            "demo_execution_blocked",
            blocked.model_dump_json(),
        )
        return 1


def _filter_stream_candidates(
    candidates: list[dict[str, Any]],
    *,
    limit: int,
    max_spread_bps: float,
) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for item in candidates:
        try:
            samples = int(item.get("sample_count") or 0)
        except (TypeError, ValueError):
            continue
        if samples < _WIDE_MIN_SAMPLES:
            continue

        first_at = _parse_utc(item.get("first_at"))
        last_at = _parse_utc(item.get("last_at"))
        if first_at is None or last_at is None:
            continue
        if (last_at - first_at).total_seconds() < _WIDE_MIN_SPAN_SECONDS:
            continue

        spread_raw = item.get("spread_bps")
        if spread_raw is not None:
            try:
                if float(spread_raw) > max_spread_bps:
                    continue
            except (TypeError, ValueError):
                continue
        result.append(item)
        if len(result) >= limit:
            break
    return result


def _parse_utc(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
    except ValueError:
        return None


def _optional_int(value: str | None) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _read_secret(path: Path, label: str) -> str:
    try:
        value = path.read_text(encoding="utf-8").strip()
    except FileNotFoundError as exc:
        raise RuntimeError(f"Missing {label} file: {path}") from exc
    if not value:
        raise RuntimeError(f"Empty {label} file: {path}")
    return value
