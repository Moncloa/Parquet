from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from parquet.execution.autonomous import AutonomousExecutionCoordinator
from parquet.models import MarketObservation, ReviewRequest, TriggerAction, WatchItem
from parquet.orchestrator import Orchestrator
from parquet.portfolio import PositionManager

_WIDE_MIN_SAMPLES = 10
_WIDE_MIN_SPAN_SECONDS = 120.0


class AutonomousOrchestrator(Orchestrator):
    """Orchestrator variant that routes EXECUTE watches through the durable ledger.

    Autonomous execution is limited to shadow/demo validation. Real broker writes
    remain unavailable; the coordinator still enforces fresh reconciliation and
    unresolved-outcome blocking before preparing an attempt.
    """

    def __init__(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
        super().__init__(*args, **kwargs)
        self.position_manager = PositionManager(self.storage)
        self.autonomous_execution = AutonomousExecutionCoordinator(
            self.storage,
            self.position_manager,
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

        candidates = self._stored_stream_candidates(current)
        enriched, symbols = await self._enrich_stream_candidates(
            candidates,
            source="persisted_service_shortlist",
        )
        return (
            {
                "enabled": True,
                "source": "persisted_service_shortlist" if candidates else "no_fresh_shortlist",
                "connected": self.storage.get("websocket_connected") == "1",
                "url": self.settings.etoro.websocket_url,
                "subscribed_instruments": _optional_int(
                    self.storage.get("websocket_universe_subscribed_count")
                ),
                "streamed_instruments": _optional_int(
                    self.storage.get("websocket_streamed_instruments")
                ),
                "last_message_at": self.storage.get("websocket_last_message_at") or None,
                "last_tick_at": self.storage.get("websocket_last_tick_at") or None,
                "last_error": self.storage.get("websocket_last_error") or None,
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

        decision = self.execution_gate.evaluate(
            proposal,
            snapshot,
            observation,
            now=observation.observed_at,
        )
        payload["gate"] = decision.as_dict()
        if not decision.approved:
            self.storage.add_event("execution_rejected", json.dumps(payload))
            return

        if not self.settings.execution.autonomous_enabled:
            payload["reasons"] = ["autonomous_execution_disabled"]
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

        mode = self.settings.execution.autonomous_mode
        if mode == "shadow":
            attempt = self.autonomous_execution.execute_shadow(
                attempt,
                now=observation.observed_at,
            )
            payload["attempt"] = attempt.model_dump(mode="json")
            self.storage.add_event("execution_shadow", json.dumps(payload, default=str))
            return

        attempt = self.autonomous_execution.mark_demo_pending(
            attempt,
            now=observation.observed_at,
        )
        payload["attempt"] = attempt.model_dump(mode="json")
        payload["reasons"] = ["demo_broker_adapter_not_implemented"]
        self.storage.add_event("execution_demo_pending", json.dumps(payload, default=str))


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
