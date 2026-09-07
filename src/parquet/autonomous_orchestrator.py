from __future__ import annotations

import json

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
