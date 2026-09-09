from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from parquet.autonomous_orchestrator import (
    AutonomousOrchestrator as BaseAutonomousOrchestrator,
)
from parquet.autonomous_orchestrator import _filter_stream_candidates, _optional_int
from parquet.market.candles import EtoroCandleClient
from parquet.market.ranking import rank_stream_candidates
from parquet.models import MarketObservation, ReviewRequest


class AutonomousOrchestrator(BaseAutonomousOrchestrator):
    """Autonomous orchestrator with wide-market history bootstrap and robust ranking."""

    def __init__(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
        super().__init__(*args, **kwargs)
        self.candle_client = None
        if self.market_client is not None:
            self.candle_client = EtoroCandleClient(
                api_key=self.market_client.api_key,
                user_key=self.market_client.user_key,
                base_url=self.market_client.base_url,
            )

    def _ranked_stream_candidates(self) -> list[dict[str, Any]]:
        scanner = self.stream_scanner
        if scanner is None:
            return []
        pool_size = min(100, max(self.settings.etoro.websocket_shortlist_size * 5, 20))
        candidate_pool = scanner.shortlist(
            limit=pool_size,
            history_points=min(60, max(20, self.settings.etoro.history_context_points * 2)),
        )
        ranked = rank_stream_candidates(
            candidate_pool,
            max_spread_bps=self.settings.risk.max_spread_bps,
        )
        return _filter_stream_candidates(
            ranked,
            limit=self.settings.etoro.websocket_shortlist_size,
            max_spread_bps=self.settings.risk.max_spread_bps,
        )

    def _persist_stream_state(self, current: datetime) -> None:
        # Persist base transport health first, then the enhanced shortlist.
        from parquet.orchestrator import Orchestrator

        Orchestrator._persist_stream_state(self, current)
        scanner = self.stream_scanner
        if scanner is None:
            return
        candidates = self._ranked_stream_candidates()
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
            candidates = self._ranked_stream_candidates()
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
                        "ranking": "time_window_momentum_quality_v2",
                        "filters": {
                            "min_samples": 10,
                            "min_span_seconds": 120.0,
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
                "ranking": "time_window_momentum_quality_v2",
                "filters": {
                    "min_samples": 10,
                    "min_span_seconds": 120.0,
                    "max_known_spread_bps": self.settings.risk.max_spread_bps,
                },
                "candidates": enriched,
            },
            symbols,
        )

    async def _bootstrap_candidate_history(
        self,
        symbols: list[str],
        current: datetime,
    ) -> int:
        if self.candle_client is None or not symbols:
            return 0

        retention = self.settings.etoro.history_retention_minutes
        candle_count = min(240, max(60, retention))
        sufficient_span_minutes = min(45.0, max(15.0, retention * 0.375))
        bootstrapped = 0

        for symbol in symbols[: self.settings.etoro.websocket_shortlist_size]:
            context = self.history.context(
                symbol,
                now=current,
                retention_minutes=retention,
                max_points=10,
            )
            try:
                span_minutes = float(context.get("span_minutes") or 0.0)
            except (TypeError, ValueError):
                span_minutes = 0.0
            if span_minutes >= sufficient_span_minutes:
                continue

            instrument_id = self._instrument_ids.get(symbol.upper())
            if instrument_id is None:
                continue

            cooldown_key = f"wide_candle_bootstrap_at:{instrument_id}"
            previous_raw = self.storage.get(cooldown_key)
            if previous_raw:
                try:
                    previous = datetime.fromisoformat(
                        previous_raw.replace("Z", "+00:00")
                    ).astimezone(UTC)
                    if (current - previous).total_seconds() < 300:
                        continue
                except ValueError:
                    pass
            self.storage.set(cooldown_key, current.astimezone(UTC).isoformat())

            try:
                candles = await self.candle_client.candles(
                    instrument_id,
                    interval="OneMinute",
                    count=candle_count,
                    direction="asc",
                )
            except Exception as exc:
                self.storage.add_event(
                    "candidate_candle_bootstrap_error",
                    json.dumps(
                        {
                            "symbol": symbol,
                            "instrument_id": instrument_id,
                            "error": repr(exc),
                        }
                    ),
                )
                continue

            recorded = 0
            for candle in candles:
                if candle.from_date > current:
                    continue
                self.history.record(
                    MarketObservation(
                        symbol=symbol,
                        price=candle.close,
                        observed_at=candle.from_date,
                        instrument_id=instrument_id,
                    ),
                    retention_minutes=retention,
                )
                recorded += 1
            if recorded:
                bootstrapped += 1
                self.storage.add_event(
                    "candidate_candle_bootstrap",
                    json.dumps(
                        {
                            "symbol": symbol,
                            "instrument_id": instrument_id,
                            "candles": recorded,
                        }
                    ),
                )
        return bootstrapped

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
        await self._bootstrap_candidate_history(dynamic_symbols, current)
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
            market_data["candidate_history_bootstrap"] = {
                "enabled": True,
                "interval": "OneMinute",
                "max_candles": min(240, max(60, self.settings.etoro.history_retention_minutes)),
            }
        else:
            context["market_data"] = {
                "provider": "etoro",
                "wide_scanner": stream_context,
                "candidate_history_bootstrap": {
                    "enabled": True,
                    "interval": "OneMinute",
                },
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
