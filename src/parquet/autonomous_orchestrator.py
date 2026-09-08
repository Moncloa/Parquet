from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from parquet.execution.autonomous import AutonomousExecutionCoordinator
from parquet.models import MarketObservation, TriggerAction, WatchItem
from parquet.orchestrator import Orchestrator
from parquet.portfolio import PositionManager


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
        candidates = scanner.shortlist(
            limit=self.settings.etoro.websocket_shortlist_size,
            history_points=min(20, self.settings.etoro.history_context_points),
        )
        self.storage.set("websocket_shortlist_raw", json.dumps(candidates))
        self.storage.set("websocket_shortlist_at", current.astimezone(UTC).isoformat())

    async def _stream_context(self, current: datetime) -> tuple[dict[str, object], list[str]]:
        context, symbols = await super()._stream_context(current)
        live_candidates = context.get("candidates")
        if isinstance(live_candidates, list) and live_candidates:
            return context, symbols

        candidates = self._stored_stream_candidates(current)
        if not candidates or self.universe_client is None:
            return context, symbols

        ids = [int(item["instrument_id"]) for item in candidates]
        try:
            metadata = await self.universe_client.metadata(ids)
        except Exception as exc:
            self.storage.add_event(
                "websocket_metadata_error",
                json.dumps({"error": repr(exc), "source": "stored_shortlist"}),
            )
            return context, symbols

        enriched: list[dict[str, object]] = []
        stored_symbols: list[str] = []
        for item in candidates:
            instrument_id = int(item["instrument_id"])
            meta = metadata.get(instrument_id)
            symbol = None if meta is None else meta.symbol
            name = None if meta is None else meta.name
            if symbol:
                self._instrument_ids[symbol.upper()] = instrument_id
                stored_symbols.append(symbol)
            enriched.append(
                {
                    **item,
                    "symbol": symbol,
                    "name": name,
                    "instrument_type_id": None if meta is None else meta.instrument_type_id,
                    "exchange_id": None if meta is None else meta.exchange_id,
                }
            )

        restored = dict(context)
        restored["source"] = "persisted_service_shortlist"
        restored["candidates"] = enriched
        restored["shortlist_at"] = self.storage.get("websocket_shortlist_at")
        return restored, sorted(set(symbols) | set(stored_symbols))

    def _stored_stream_candidates(self, current: datetime) -> list[dict[str, Any]]:
        raw = self.storage.get("websocket_shortlist_raw")
        if not raw:
            return []
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            return []
        if not isinstance(payload, list):
            return []

        result: list[dict[str, Any]] = []
        for raw_item in payload:
            if not isinstance(raw_item, dict):
                continue
            item = {str(key): value for key, value in raw_item.items()}
            if item.get("instrument_id") is None or not isinstance(item.get("last_at"), str):
                continue
            try:
                last_at = datetime.fromisoformat(
                    str(item["last_at"]).replace("Z", "+00:00")
                ).astimezone(UTC)
            except ValueError:
                continue
            age = (current.astimezone(UTC) - last_at).total_seconds()
            if 0 <= age <= self.settings.etoro.max_quote_age_seconds:
                result.append(item)
        return result[: self.settings.etoro.websocket_shortlist_size]

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
