from __future__ import annotations

import asyncio
import json
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from parquet.bridge.github import GitHubBridge
from parquet.config import Settings
from parquet.execution import ExecutionGate
from parquet.market.etoro import EtoroMarketDataClient, InstrumentRate
from parquet.market.history import MarketHistoryStore
from parquet.market.stream import EtoroWebSocketScanner
from parquet.market.universe import EtoroUniverseClient, rotate_universe
from parquet.models import (
    MarketAnalysis,
    MarketObservation,
    ReviewRequest,
    RiskSnapshot,
    TriggerAction,
    WatchEvent,
    WatchEventType,
    WatchItem,
)
from parquet.scheduler import ReviewQueue, ScheduledReview, ensure_structural_reviews
from parquet.storage import Storage
from parquet.watch import WatchEngine


class Orchestrator:
    def __init__(
        self,
        settings: Settings,
        market_client: EtoroMarketDataClient | None = None,
    ) -> None:
        self.settings = settings
        self.storage = Storage(settings.state_db)
        self.history = MarketHistoryStore(self.storage)
        self.watch_engine = WatchEngine()
        self.execution_gate = ExecutionGate(settings.risk, settings.etoro)
        self.reviews = ReviewQueue()
        for review in self.storage.pending_reviews():
            self.reviews.add(review)
        self.bridge = (
            GitHubBridge(
                settings.github.repository,
                settings.github.runtime_pr,
                settings.github.token_file,
            )
            if settings.github.enabled
            else None
        )
        self.market_client = market_client or self._build_market_client()
        self._instrument_ids = {
            symbol.upper(): instrument_id
            for symbol, instrument_id in settings.etoro.instrument_ids.items()
        }
        self._last_account_poll_at: datetime | None = None
        self._last_history_poll_at: datetime | None = None
        self._last_stream_rotation_at: datetime | None = None
        self._stream_task: asyncio.Task[None] | None = None
        try:
            self._stream_offset = int(self.storage.get("websocket_universe_offset") or 0)
        except ValueError:
            self._stream_offset = 0
        self.universe_client: EtoroUniverseClient | None = None
        self.stream_scanner: EtoroWebSocketScanner | None = None
        if settings.etoro.enabled and settings.etoro.websocket_enabled:
            api_key = _read_secret(settings.etoro.api_key_file, "eToro API key")
            user_key = _read_secret(settings.etoro.user_key_file, "eToro User key")
            self.universe_client = EtoroUniverseClient(
                api_key=api_key,
                user_key=user_key,
                base_url=settings.etoro.base_url,
            )
            self.stream_scanner = EtoroWebSocketScanner(
                api_key=api_key,
                user_key=user_key,
                url=settings.etoro.websocket_url,
                max_points_per_instrument=settings.etoro.websocket_points_per_instrument,
            )
        self.ensure_structural_reviews()

    def _build_market_client(self) -> EtoroMarketDataClient | None:
        if not self.settings.etoro.enabled:
            return None
        api_key = _read_secret(self.settings.etoro.api_key_file, "eToro API key")
        user_key = _read_secret(self.settings.etoro.user_key_file, "eToro User key")
        return EtoroMarketDataClient(
            api_key=api_key,
            user_key=user_key,
            base_url=self.settings.etoro.base_url,
        )

    def add_review(self, review: ScheduledReview) -> None:
        self.reviews.add(review)
        self.storage.schedule_review(review)

    def ensure_structural_reviews(self, now: datetime | None = None) -> None:
        before = {review.key: review for review in self.reviews.pending()}
        ensure_structural_reviews(
            self.reviews,
            self.settings.schedule.structural_reviews,
            now,
        )
        after = {review.key: review for review in self.reviews.pending()}
        for key, review in before.items():
            if key not in after:
                self.storage.delete_review(review)
        for key, review in after.items():
            if key not in before:
                self.storage.schedule_review(review)

    def process_analysis(self, analysis: MarketAnalysis) -> None:
        self.storage.save_analysis(
            analysis.analysis_id,
            analysis.generated_at.isoformat(),
            analysis.model_dump_json(),
        )
        for proposal in analysis.trade_proposals:
            self.storage.save_proposal(analysis.analysis_id, proposal)
        for watch in analysis.watch:
            self.storage.save_watch(analysis.analysis_id, watch)
        if analysis.next_review is not None:
            self.add_review(
                ScheduledReview(
                    at=analysis.next_review.at.astimezone(UTC),
                    reason=analysis.next_review.reason,
                    source="chatgpt",
                )
            )
        self.storage.set("latest_analysis_id", analysis.analysis_id)

    def process_observation(self, observation: MarketObservation) -> list[WatchEvent]:
        events: list[WatchEvent] = []
        for watch in self.storage.active_watches(observation.observed_at):
            event = self.watch_engine.evaluate(watch, observation)
            if event is None:
                continue
            events.append(event)
            self.storage.add_event("watch_event", event.model_dump_json())
            self.storage.set_watch_status(watch.watch_id, event.event.value)
            if event.event == WatchEventType.TRIGGERED and event.action == TriggerAction.REASSESS:
                self.add_review(
                    ScheduledReview(
                        at=observation.observed_at.astimezone(UTC),
                        reason=f"watch_trigger:{watch.watch_id}",
                        source="watch",
                    )
                )
            elif event.event == WatchEventType.TRIGGERED:
                self._handle_execute_watch(watch, observation)
        return events

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
        elif self.settings.mode.lower() == "shadow":
            self.storage.add_event("execution_shadow_approved", json.dumps(payload))
        else:
            payload["reasons"] = ["broker_execution_not_implemented"]
            self.storage.add_event("execution_blocked", json.dumps(payload))

    async def poll_github_once(self) -> int:
        if self.bridge is None:
            return 0
        last_id = int(self.storage.get("github_last_comment_id") or 0)
        processed = 0
        comments = await self.bridge.comments()
        for comment in comments:
            comment_id = int(comment["id"])
            if comment_id <= last_id:
                continue
            body = str(comment.get("body", ""))
            analysis = self.bridge.parse_analysis_comment(body)
            if analysis is not None:
                self.process_analysis(analysis)
                processed += 1
            last_id = max(last_id, comment_id)
        self.storage.set("github_last_comment_id", str(last_id))
        return processed

    async def poll_account_once(
        self,
        now: datetime | None = None,
        *,
        force: bool = False,
    ) -> int:
        if self.market_client is None:
            return 0
        current = (now or datetime.now(UTC)).astimezone(UTC)
        if (
            not force
            and self._last_account_poll_at is not None
            and (current - self._last_account_poll_at).total_seconds()
            < self.settings.etoro.account_poll_seconds
        ):
            return 0

        self._last_account_poll_at = current
        try:
            account = await self.market_client.account_snapshot(now=current)
            if account.equity_usd <= 0:
                raise ValueError("eToro equity must be positive for execution sizing")
        except Exception as exc:
            self.storage.add_event("account_snapshot_error", json.dumps({"error": repr(exc)}))
            return 0

        reverse_ids = {
            instrument_id: symbol
            for symbol, instrument_id in self._instrument_ids.items()
        }
        open_symbols = set(account.open_symbols)
        for instrument_id in account.open_instrument_ids:
            symbol = reverse_ids.get(instrument_id)
            if symbol is not None:
                open_symbols.add(symbol)

        previous = self.storage.get_risk_snapshot()
        snapshot = RiskSnapshot(
            as_of=account.captured_at,
            equity_usd=account.equity_usd,
            open_positions=account.open_positions,
            trades_today=0 if previous is None else previous.trades_today,
            daily_pnl_pct=0.0 if previous is None else previous.daily_pnl_pct,
            weekly_pnl_pct=0.0 if previous is None else previous.weekly_pnl_pct,
            open_symbols=sorted(open_symbols),
            open_instrument_ids=account.open_instrument_ids,
        )
        self.storage.set_risk_snapshot(snapshot)
        self.storage.set(
            "account_snapshot_components",
            json.dumps(
                {
                    "as_of": account.captured_at.isoformat(),
                    "equity_usd": account.equity_usd,
                    "available_cash_usd": account.available_cash_usd,
                    "invested_usd": account.invested_usd,
                    "unrealized_pnl_usd": account.unrealized_pnl_usd,
                    "open_positions": account.open_positions,
                }
            ),
        )
        return 1

    async def _resolve_instrument_ids(self, symbols: list[str]) -> dict[str, int]:
        if self.market_client is None:
            return {}
        resolved: dict[str, int] = {}
        for symbol in symbols:
            key = symbol.upper()
            instrument_id = self._instrument_ids.get(key)
            if instrument_id is None:
                hits = await self.market_client.search(symbol)
                exact = next(
                    (
                        hit
                        for hit in hits
                        if hit.symbol is not None and hit.symbol.upper() == key
                    ),
                    None,
                )
                if exact is None:
                    self.storage.add_event(
                        "market_symbol_unresolved",
                        json.dumps({"symbol": symbol}),
                    )
                    continue
                instrument_id = exact.instrument_id
                self._instrument_ids[key] = instrument_id
            resolved[symbol] = instrument_id
        return resolved

    async def _rates_for_symbols(self, symbols: list[str]) -> dict[str, InstrumentRate]:
        if self.market_client is None or not symbols:
            return {}
        resolved = await self._resolve_instrument_ids(symbols)
        if not resolved:
            return {}
        rates = await self.market_client.rates(sorted(set(resolved.values())))
        by_id = {rate.instrument_id: rate for rate in rates}
        return {
            symbol: by_id[instrument_id]
            for symbol, instrument_id in resolved.items()
            if instrument_id in by_id
        }

    def _record_rate(self, symbol: str, rate: InstrumentRate) -> bool:
        price = _usable_price(rate)
        if price is None:
            return False
        self.history.record(
            MarketObservation(
                symbol=symbol,
                price=price,
                observed_at=rate.timestamp,
                instrument_id=rate.instrument_id,
                bid=rate.bid,
                ask=rate.ask,
            ),
            retention_minutes=self.settings.etoro.history_retention_minutes,
        )
        return True

    async def poll_market_history_once(self, now: datetime | None = None) -> int:
        if self.market_client is None:
            return 0
        current = (now or datetime.now(UTC)).astimezone(UTC)
        if (
            self._last_history_poll_at is not None
            and (current - self._last_history_poll_at).total_seconds()
            < self.settings.etoro.history_sample_seconds
        ):
            return 0
        self._last_history_poll_at = current
        active_symbols = {watch.symbol for watch in self.storage.active_watches(current)}
        symbols = sorted(active_symbols | set(self.settings.etoro.review_symbols))
        if not symbols:
            return 0
        try:
            rates = await self._rates_for_symbols(symbols)
        except Exception as exc:
            self.storage.add_event("market_history_poll_error", json.dumps({"error": repr(exc)}))
            return 0
        processed = 0
        for symbol, rate in rates.items():
            if self._record_rate(symbol, rate):
                processed += 1
        return processed

    async def poll_wide_market_once(self, now: datetime | None = None) -> int:
        if self.stream_scanner is None or self.universe_client is None:
            return 0
        current = (now or datetime.now(UTC)).astimezone(UTC)
        rotation_seconds = self.settings.etoro.websocket_rotation_minutes * 60
        task_alive = self._stream_task is not None and not self._stream_task.done()
        if (
            task_alive
            and self._last_stream_rotation_at is not None
            and (current - self._last_stream_rotation_at).total_seconds() < rotation_seconds
        ):
            self._persist_stream_state(current)
            return 0

        try:
            open_ids = await self.universe_client.open_instrument_ids()
            pinned = list(
                (
                    await self._resolve_instrument_ids(
                        sorted(set(self.settings.etoro.review_symbols))
                    )
                ).values()
            )
            selected, next_offset = rotate_universe(
                open_ids,
                offset=self._stream_offset,
                limit=self.settings.etoro.websocket_universe_size,
                pinned=pinned,
            )
            if not selected:
                raise RuntimeError("eToro wide-market universe is empty")
        except Exception as exc:
            self.storage.set("websocket_last_error", repr(exc))
            self.storage.set("websocket_last_error_at", current.isoformat())
            return 0

        if self._stream_task is not None:
            self._stream_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._stream_task
        self.stream_scanner.set_universe(selected)
        self._stream_task = asyncio.create_task(
            self.stream_scanner.run_forever(),
            name="parquet-etoro-websocket",
        )
        self._stream_offset = next_offset
        self._last_stream_rotation_at = current
        self.storage.set("websocket_universe_offset", str(next_offset))
        self.storage.set("websocket_universe_open_count", str(len(open_ids)))
        self.storage.set("websocket_universe_subscribed_count", str(len(selected)))
        self.storage.set("websocket_last_rotation_at", current.isoformat())
        self.storage.set("websocket_last_error", "")
        self._persist_stream_state(current)
        return len(selected)

    def _persist_stream_state(self, current: datetime) -> None:
        scanner = self.stream_scanner
        if scanner is None:
            return
        self.storage.set("websocket_connected", "1" if scanner.connected else "0")
        self.storage.set(
            "websocket_last_message_at",
            "" if scanner.last_message_at is None else scanner.last_message_at.isoformat(),
        )
        if scanner.last_error:
            self.storage.set("websocket_last_error", scanner.last_error)
            self.storage.set("websocket_last_error_at", current.isoformat())

    async def _stream_context(self, current: datetime) -> tuple[dict[str, object], list[str]]:
        scanner = self.stream_scanner
        universe_client = self.universe_client
        if scanner is None or universe_client is None:
            return {"enabled": False}, []

        candidates = scanner.shortlist(
            limit=self.settings.etoro.websocket_shortlist_size,
            history_points=min(20, self.settings.etoro.history_context_points),
        )
        ids = [int(item["instrument_id"]) for item in candidates]
        metadata = {}
        if ids:
            try:
                metadata = await universe_client.metadata(ids)
            except Exception as exc:
                self.storage.add_event(
                    "websocket_metadata_error",
                    json.dumps({"error": repr(exc)}),
                )

        enriched: list[dict[str, object]] = []
        symbols: list[str] = []
        for item in candidates:
            instrument_id = int(item["instrument_id"])
            meta = metadata.get(instrument_id)
            symbol = None if meta is None else meta.symbol
            name = None if meta is None else meta.name
            if symbol:
                self._instrument_ids[symbol.upper()] = instrument_id
                symbols.append(symbol)
            enriched.append(
                {
                    **item,
                    "symbol": symbol,
                    "name": name,
                    "instrument_type_id": None if meta is None else meta.instrument_type_id,
                    "exchange_id": None if meta is None else meta.exchange_id,
                }
            )

        self._persist_stream_state(current)
        return (
            {
                "enabled": True,
                "connected": scanner.connected,
                "url": self.settings.etoro.websocket_url,
                "subscribed_instruments": len(scanner.instrument_ids),
                "streamed_instruments": len(scanner.series),
                "last_message_at": (
                    None if scanner.last_message_at is None else scanner.last_message_at.isoformat()
                ),
                "last_error": scanner.last_error,
                "ranking": "abs_stream_change_plus_step_volatility",
                "candidates": enriched,
            },
            symbols,
        )

    async def _market_context(
        self,
        symbols: list[str],
        current: datetime,
    ) -> dict[str, object]:
        rates = await self._rates_for_symbols(symbols)
        quotes: dict[str, object] = {}
        history: dict[str, object] = {}
        for symbol, rate in rates.items():
            age_seconds = max(0.0, (current.astimezone(UTC) - rate.timestamp).total_seconds())
            quotes[symbol] = {
                "instrument_id": rate.instrument_id,
                "bid": rate.bid,
                "ask": rate.ask,
                "last_price": rate.last_price,
                "change": rate.change,
                "timestamp": rate.timestamp.isoformat(),
                "age_seconds": round(age_seconds, 3),
                "stale": age_seconds > self.settings.etoro.max_quote_age_seconds,
            }
            self._record_rate(symbol, rate)
            history[symbol] = self.history.context(
                symbol,
                now=current,
                retention_minutes=self.settings.etoro.history_retention_minutes,
                max_points=self.settings.etoro.history_context_points,
            )
        unresolved = sorted(set(symbols) - set(rates))
        context: dict[str, object] = {
            "market_data": {
                "provider": "etoro",
                "captured_at": current.astimezone(UTC).isoformat(),
                "quotes": quotes,
                "history": history,
                "unresolved_symbols": unresolved,
            }
        }
        risk_snapshot = self.storage.get_risk_snapshot()
        if risk_snapshot is not None:
            context["risk_snapshot"] = risk_snapshot.model_dump(mode="json")
        return context

    async def poll_market_once(self, now: datetime | None = None) -> int:
        current = (now or datetime.now(UTC)).astimezone(UTC)
        await self.poll_market_history_once(current)
        await self.poll_wide_market_once(current)
        if self.market_client is None:
            return 0
        symbols = sorted({watch.symbol for watch in self.storage.active_watches(current)})
        if not symbols:
            return 0
        try:
            rates = await self._rates_for_symbols(symbols)
        except Exception as exc:
            self.storage.add_event("market_poll_error", json.dumps({"error": repr(exc)}))
            return 0
        processed = 0
        for symbol, rate in rates.items():
            price = _usable_price(rate)
            if price is None:
                self.storage.add_event(
                    "market_rate_unusable",
                    json.dumps({"symbol": symbol, "instrument_id": rate.instrument_id}),
                )
                continue
            observation = MarketObservation(
                symbol=symbol,
                price=price,
                observed_at=rate.timestamp,
                instrument_id=rate.instrument_id,
                bid=rate.bid,
                ask=rate.ask,
            )
            self.process_observation(observation)
            processed += 1
        return processed

    async def post_due_reviews(self, now: datetime | None = None) -> int:
        if self.bridge is None:
            return 0
        current = (now or datetime.now(UTC)).astimezone(UTC)
        due = self.reviews.due(current)
        if not due:
            return 0

        active_symbols = {watch.symbol for watch in self.storage.active_watches(current)}
        review_symbols = sorted(active_symbols | set(self.settings.etoro.review_symbols))
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

        stream_context, dynamic_symbols = await self._stream_context(current)
        market_data = context.get("market_data")
        if isinstance(market_data, dict):
            market_data["wide_scanner"] = stream_context
        else:
            context["market_data"] = {
                "provider": "etoro",
                "wide_scanner": stream_context,
            }
        review_symbols = sorted(set(review_symbols) | set(dynamic_symbols))

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

    async def run_forever(self) -> None:
        while True:
            try:
                await self.poll_github_once()
                self.ensure_structural_reviews()
                await self.poll_account_once()
                await self.poll_market_once()
                await self.post_due_reviews()
            except Exception as exc:
                self.storage.add_event("orchestrator_error", json.dumps({"error": repr(exc)}))
            await asyncio.sleep(self.settings.poll_seconds)


def _read_secret(path: Path, label: str) -> str:
    try:
        value = path.read_text(encoding="utf-8").strip()
    except FileNotFoundError as exc:
        raise RuntimeError(f"Missing {label} file: {path}") from exc
    if not value:
        raise RuntimeError(f"Empty {label} file: {path}")
    return value


def _usable_price(rate: InstrumentRate) -> float | None:
    if rate.last_price is not None:
        return rate.last_price
    if rate.bid is not None and rate.ask is not None:
        return (rate.bid + rate.ask) / 2
    return None
