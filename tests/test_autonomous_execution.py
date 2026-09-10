from datetime import UTC, datetime, timedelta

import pytest

from parquet.config import ExecutionConfig
from parquet.execution.autonomous import (
    AutonomousExecutionCoordinator,
    ExecutionAttemptState,
)
from parquet.execution.gate import ExecutionDecision
from parquet.models import MarketObservation, Side, TradeProposal
from parquet.portfolio import PositionManager, ReconciliationReport, ReconciliationState
from parquet.storage import Storage


def _proposal(now: datetime) -> TradeProposal:
    return TradeProposal(
        proposal_id="p1",
        symbol="NSDQ100",
        side=Side.BUY,
        entry=100.0,
        stop_loss=99.0,
        take_profit=102.0,
        confidence=0.8,
        generated_at=now,
        expires_at=now + timedelta(minutes=5),
    )


def _observation(now: datetime) -> MarketObservation:
    return MarketObservation(
        symbol="NSDQ100",
        price=100.0,
        observed_at=now,
        instrument_id=32,
        bid=99.99,
        ask=100.01,
    )


def _decision() -> ExecutionDecision:
    return ExecutionDecision(approved=True, reasons=(), amount_usd=100.0)


def _synced(storage: Storage, now: datetime) -> None:
    storage.set_reconciliation_report(
        ReconciliationReport(
            as_of=now,
            state=ReconciliationState.SYNCED,
            trading_enabled=True,
        )
    )


def test_autonomous_config_accepts_real_mode_but_keeps_real_disabled() -> None:
    config = ExecutionConfig(autonomous_enabled=True, autonomous_mode="real")
    assert config.autonomous_real_enabled is False


def test_prepare_is_idempotent_for_same_proposal(tmp_path) -> None:
    now = datetime(2026, 9, 7, 13, 45, tzinfo=UTC)
    storage = Storage(tmp_path / "state.db")
    _synced(storage, now)
    coordinator = AutonomousExecutionCoordinator(storage, PositionManager(storage))

    first = coordinator.prepare(
        proposal=_proposal(now),
        watch_id="w1",
        observation=_observation(now),
        decision=_decision(),
        now=now,
    )
    second = coordinator.prepare(
        proposal=_proposal(now),
        watch_id="w1",
        observation=_observation(now),
        decision=_decision(),
        now=now,
    )

    assert first.attempt_id == second.attempt_id
    assert len(storage.latest_execution_attempts()) == 1


def test_uncertain_outcome_blocks_new_attempts(tmp_path) -> None:
    now = datetime(2026, 9, 7, 13, 45, tzinfo=UTC)
    storage = Storage(tmp_path / "state.db")
    _synced(storage, now)
    coordinator = AutonomousExecutionCoordinator(storage, PositionManager(storage))
    attempt = coordinator.prepare(
        proposal=_proposal(now),
        watch_id="w1",
        observation=_observation(now),
        decision=_decision(),
        now=now,
    )
    coordinator.mark_outcome_unknown(attempt, reason="timeout", now=now)

    with pytest.raises(RuntimeError, match="unresolved broker outcome"):
        coordinator.prepare(
            proposal=_proposal(now).model_copy(update={"proposal_id": "p2"}),
            watch_id="w2",
            observation=_observation(now),
            decision=_decision(),
            now=now,
        )


def test_shadow_execution_is_durable(tmp_path) -> None:
    now = datetime(2026, 9, 7, 13, 45, tzinfo=UTC)
    storage = Storage(tmp_path / "state.db")
    _synced(storage, now)
    coordinator = AutonomousExecutionCoordinator(storage, PositionManager(storage))
    attempt = coordinator.prepare(
        proposal=_proposal(now),
        watch_id="w1",
        observation=_observation(now),
        decision=_decision(),
        now=now,
    )
    completed = coordinator.execute_shadow(attempt, now=now)

    assert completed.state == ExecutionAttemptState.SHADOW_EXECUTED
    assert storage.latest_execution_attempts()[0].state == ExecutionAttemptState.SHADOW_EXECUTED
