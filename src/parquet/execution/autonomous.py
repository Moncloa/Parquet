from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, cast
from uuid import uuid4

from pydantic import BaseModel, Field

from parquet.execution.gate import ExecutionDecision
from parquet.models import MarketObservation, TradeProposal
from parquet.portfolio import PositionManager


class ExecutionAttemptState(StrEnum):
    PREPARED = "PREPARED"
    SUBMITTING = "SUBMITTING"
    ACKNOWLEDGED = "ACKNOWLEDGED"
    SHADOW_EXECUTED = "SHADOW_EXECUTED"
    DEMO_PENDING = "DEMO_PENDING"
    REJECTED = "REJECTED"
    BLOCKED = "BLOCKED"
    OUTCOME_UNKNOWN = "OUTCOME_UNKNOWN"
    RECONCILING = "RECONCILING"
    RECONCILED = "RECONCILED"


class ExecutionAttempt(BaseModel):
    attempt_id: str = Field(min_length=1)
    proposal_id: str = Field(min_length=1)
    watch_id: str = Field(min_length=1)
    symbol: str = Field(min_length=1)
    instrument_id: int = Field(gt=0)
    side: str = Field(min_length=1)
    amount_usd: float = Field(gt=0)
    stop_loss: float = Field(gt=0)
    take_profit: float | None = Field(default=None, gt=0)
    created_at: datetime
    updated_at: datetime
    state: ExecutionAttemptState
    broker_request_id: str | None = None
    broker_order_id: str | None = None
    broker_position_id: str | None = None
    reason: str | None = None


class AutonomousExecutionCoordinator:
    """Transactional execution coordinator shared by shadow and supervised flows."""

    def __init__(self, storage: Any, position_manager: PositionManager) -> None:
        self.storage = storage
        self.position_manager = position_manager

    def prepare(
        self,
        *,
        proposal: TradeProposal,
        watch_id: str,
        observation: MarketObservation,
        decision: ExecutionDecision,
        now: datetime | None = None,
    ) -> ExecutionAttempt:
        current = (now or datetime.now(UTC)).astimezone(UTC)
        self.assert_no_uncertain_execution()
        if not decision.approved:
            raise RuntimeError("Cannot prepare rejected execution decision")
        if decision.amount_usd is None or decision.amount_usd <= 0:
            raise RuntimeError("Cannot prepare execution without positive amount")
        if observation.instrument_id is None:
            raise RuntimeError("Cannot prepare execution without broker instrument id")
        if proposal.stop_loss is None:
            raise RuntimeError("Cannot prepare execution without stop loss")

        self.position_manager.assert_trading_enabled(now=current)
        existing = self.storage.get_active_execution_attempt_for_proposal(proposal.proposal_id)
        if existing is not None:
            return cast(ExecutionAttempt, existing)

        attempt = ExecutionAttempt(
            attempt_id=str(uuid4()),
            proposal_id=proposal.proposal_id,
            watch_id=watch_id,
            symbol=proposal.symbol,
            instrument_id=observation.instrument_id,
            side=proposal.side.value,
            amount_usd=decision.amount_usd,
            stop_loss=proposal.stop_loss,
            take_profit=proposal.take_profit,
            created_at=current,
            updated_at=current,
            state=ExecutionAttemptState.PREPARED,
        )
        self.storage.save_execution_attempt(attempt)
        return attempt

    def execute_shadow(
        self, attempt: ExecutionAttempt, *, now: datetime | None = None
    ) -> ExecutionAttempt:
        current = (now or datetime.now(UTC)).astimezone(UTC)
        if attempt.state != ExecutionAttemptState.PREPARED:
            raise RuntimeError(f"Execution attempt is not PREPARED: {attempt.state}")
        updated = attempt.model_copy(
            update={"state": ExecutionAttemptState.SHADOW_EXECUTED, "updated_at": current}
        )
        self.storage.save_execution_attempt(updated)
        return updated

    def mark_demo_pending(
        self, attempt: ExecutionAttempt, *, now: datetime | None = None
    ) -> ExecutionAttempt:
        current = (now or datetime.now(UTC)).astimezone(UTC)
        if attempt.state != ExecutionAttemptState.PREPARED:
            raise RuntimeError(f"Execution attempt is not PREPARED: {attempt.state}")
        updated = attempt.model_copy(
            update={"state": ExecutionAttemptState.DEMO_PENDING, "updated_at": current}
        )
        self.storage.save_execution_attempt(updated)
        return updated

    def mark_outcome_unknown(
        self,
        attempt: ExecutionAttempt,
        *,
        reason: str,
        broker_request_id: str | None = None,
        now: datetime | None = None,
    ) -> ExecutionAttempt:
        current = (now or datetime.now(UTC)).astimezone(UTC)
        updated = attempt.model_copy(
            update={
                "state": ExecutionAttemptState.OUTCOME_UNKNOWN,
                "reason": reason,
                "broker_request_id": broker_request_id,
                "updated_at": current,
            }
        )
        self.storage.save_execution_attempt(updated)
        self.storage.set("execution_uncertain", "1")
        return updated

    def resolve_uncertain_execution(self) -> None:
        self.storage.set("execution_uncertain", "0")

    def assert_no_uncertain_execution(self) -> None:
        if self.storage.get("execution_uncertain") == "1":
            raise RuntimeError("Execution blocked: unresolved broker outcome")
