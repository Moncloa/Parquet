from datetime import UTC, datetime, timedelta

import pytest

from parquet.config import EtoroConfig, ExecutionConfig, Settings
from parquet.execution.autonomous import ExecutionAttempt, ExecutionAttemptState
from parquet.execution.autonomous_real import AutonomousRealExecutionAdapter
from parquet.portfolio import (
    BrokerPortfolioSnapshot,
    BrokerPosition,
    ManagedPosition,
    PositionManager,
    ReconciliationState,
)
from parquet.storage import Storage


def _snapshot(at: datetime, positions: list[BrokerPosition] | None = None) -> BrokerPortfolioSnapshot:
    return BrokerPortfolioSnapshot(
        captured_at=at,
        equity_usd=10_000.0,
        available_cash_usd=10_000.0,
        invested_usd=0.0,
        unrealized_pnl_usd=0.0,
        credit_usd=10_000.0,
        positions=positions or [],
    )


def _attempt(state: ExecutionAttemptState) -> ExecutionAttempt:
    now = datetime(2026, 9, 14, 12, 0, tzinfo=UTC)
    return ExecutionAttempt(
        attempt_id="attempt-1",
        proposal_id="proposal-1",
        watch_id="direct-proposal:proposal-1",
        symbol="CRV",
        instrument_id=100068,
        side="BUY",
        amount_usd=10.0,
        leverage=1,
        settlement_type="real",
        stop_loss=0.351,
        take_profit=0.3597,
        created_at=now,
        updated_at=now,
        state=state,
        broker_request_id="request-1",
        broker_order_id="order-1",
        broker_position_id="position-1",
        reason=(
            "filled_position_not_visible_after_reconciliation"
            if state == ExecutionAttemptState.OUTCOME_UNKNOWN
            else None
        ),
    )


def test_rejected_attempt_consumes_proposal_id(tmp_path) -> None:
    storage = Storage(tmp_path / "state.db")
    rejected = _attempt(ExecutionAttemptState.REJECTED)
    storage.save_execution_attempt(rejected)

    found = storage.get_active_execution_attempt_for_proposal(rejected.proposal_id)

    assert found is not None
    assert found.attempt_id == rejected.attempt_id
    assert found.state == ExecutionAttemptState.REJECTED


def test_fresh_managed_position_gets_broker_visibility_grace(tmp_path) -> None:
    storage = Storage(tmp_path / "state.db")
    manager = PositionManager(storage)
    opened_at = datetime(2026, 9, 14, 12, 0, tzinfo=UTC)
    storage.save_managed_position(
        ManagedPosition(
            local_id="attempt-1",
            broker_position_id="position-1",
            proposal_id="proposal-1",
            instrument_id=100068,
            symbol="CRV",
            side="BUY",
            opened_at=opened_at,
            amount_usd=10.0,
            leverage=1.0,
        )
    )

    early_report = manager.reconcile(_snapshot(opened_at + timedelta(seconds=5)))

    assert early_report.state == ReconciliationState.SYNCED
    assert len(storage.active_managed_positions()) == 1

    late_report = manager.reconcile(_snapshot(opened_at + timedelta(seconds=25)))

    assert late_report.state == ReconciliationState.SYNCED
    assert storage.active_managed_positions() == []
    assert storage.managed_positions()[0].status == "CLOSED_AT_BROKER"


@pytest.mark.asyncio
async def test_autonomous_real_recovers_when_filled_position_appears_late(tmp_path) -> None:
    settings = Settings(
        state_db=tmp_path / "state.db",
        etoro=EtoroConfig(expected_gcid=49462743),
        execution=ExecutionConfig(
            autonomous_enabled=True,
            autonomous_mode="real",
            autonomous_real_enabled=True,
            broker_lookup_attempts=3,
            broker_lookup_interval_seconds=0.0,
        ),
    )
    storage = Storage(settings.state_db)
    manager = PositionManager(storage)
    attempt = _attempt(ExecutionAttemptState.OUTCOME_UNKNOWN)
    storage.save_execution_attempt(attempt)
    storage.set("execution_uncertain", "1")
    storage.save_managed_position(
        ManagedPosition(
            local_id=attempt.attempt_id,
            broker_position_id=attempt.broker_position_id or "position-1",
            proposal_id=attempt.proposal_id,
            instrument_id=attempt.instrument_id,
            symbol=attempt.symbol,
            side=attempt.side,
            opened_at=attempt.created_at,
            amount_usd=attempt.amount_usd,
            leverage=float(attempt.leverage),
        )
    )

    class FakeReconciliation:
        def __init__(self) -> None:
            self.calls = 0

        async def poll_once(self, *, force: bool = False) -> int:
            assert force is True
            self.calls += 1
            at = attempt.created_at + timedelta(seconds=self.calls)
            positions = []
            if self.calls >= 2:
                positions = [
                    BrokerPosition(
                        position_id=attempt.broker_position_id or "position-1",
                        instrument_id=attempt.instrument_id,
                        symbol=attempt.symbol,
                        side=attempt.side,
                        amount_usd=attempt.amount_usd,
                        leverage=float(attempt.leverage),
                        stop_loss_rate=attempt.stop_loss,
                        take_profit_rate=attempt.take_profit,
                    )
                ]
            manager.reconcile(_snapshot(at, positions))
            return 1

    reconciliation = FakeReconciliation()
    adapter = AutonomousRealExecutionAdapter(
        settings=settings,
        storage=storage,
        position_manager=manager,
        reconciliation=reconciliation,  # type: ignore[arg-type]
        client=object(),  # type: ignore[arg-type]
    )

    recovered = await adapter._recover_delayed_position_visibility(attempt)

    assert reconciliation.calls == 2
    assert recovered.state == ExecutionAttemptState.RECONCILED
    assert recovered.reason is None
    assert storage.get("execution_uncertain") == "0"
    persisted = storage.get_execution_attempt(attempt.attempt_id)
    assert persisted is not None
    assert persisted.state == ExecutionAttemptState.RECONCILED
