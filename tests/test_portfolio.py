from datetime import UTC, datetime, timedelta

import pytest

from parquet.execution.autonomous import ExecutionAttempt, ExecutionAttemptState
from parquet.portfolio import (
    BrokerPortfolioSnapshot,
    BrokerPosition,
    ManagedPosition,
    PositionManager,
    ReconciliationState,
)
from parquet.storage import Storage


def snapshot(*positions: BrokerPosition, at: datetime | None = None) -> BrokerPortfolioSnapshot:
    return BrokerPortfolioSnapshot(
        captured_at=at or datetime.now(UTC),
        equity_usd=1000.0,
        available_cash_usd=900.0,
        invested_usd=100.0,
        unrealized_pnl_usd=0.0,
        credit_usd=900.0,
        positions=list(positions),
    )


def test_empty_dedicated_portfolio_reconciles_and_enables_trading(tmp_path) -> None:
    storage = Storage(tmp_path / "state.db")
    manager = PositionManager(storage)

    report = manager.reconcile(snapshot())

    assert report.state == ReconciliationState.SYNCED
    assert report.trading_enabled is True
    manager.assert_trading_enabled(max_age_seconds=60)


def test_unknown_broker_position_hard_blocks_autonomous_trading(tmp_path) -> None:
    storage = Storage(tmp_path / "state.db")
    manager = PositionManager(storage)
    broker_position = BrokerPosition(
        position_id="broker-42",
        instrument_id=1001,
        symbol="NSDQ100",
        side="BUY",
        amount_usd=100.0,
    )

    report = manager.reconcile(snapshot(broker_position))

    assert report.state == ReconciliationState.BLOCKED
    assert report.trading_enabled is False
    assert [issue.code for issue in report.issues] == ["UNMANAGED_BROKER_POSITION"]
    with pytest.raises(RuntimeError, match="UNMANAGED_BROKER_POSITION"):
        manager.assert_trading_enabled(max_age_seconds=60)


def test_managed_position_disappearing_at_broker_is_closed_locally(tmp_path) -> None:
    storage = Storage(tmp_path / "state.db")
    manager = PositionManager(storage)
    opened = datetime.now(UTC)
    managed = ManagedPosition(
        local_id="local-1",
        broker_position_id="broker-1",
        proposal_id="proposal-1",
        instrument_id=1001,
        symbol="NSDQ100",
        side="BUY",
        opened_at=opened,
    )
    manager.record_execution_position(managed)

    first = manager.reconcile(
        snapshot(
            BrokerPosition(
                position_id="broker-1",
                instrument_id=1001,
                symbol="NSDQ100",
                side="BUY",
                amount_usd=100.0,
            ),
            at=opened,
        )
    )
    assert first.state == ReconciliationState.SYNCED
    assert len(storage.active_managed_positions()) == 1

    second = manager.reconcile(snapshot(at=opened + timedelta(seconds=5)))
    assert second.state == ReconciliationState.SYNCED
    assert storage.active_managed_positions() == []


def test_stale_reconciliation_blocks_trading(tmp_path) -> None:
    storage = Storage(tmp_path / "state.db")
    manager = PositionManager(storage)
    old = datetime.now(UTC) - timedelta(minutes=5)
    manager.reconcile(snapshot(at=old))

    with pytest.raises(RuntimeError, match="reconciliation is stale"):
        manager.assert_trading_enabled(max_age_seconds=30)


def test_reconciliation_error_blocks_trading(tmp_path) -> None:
    storage = Storage(tmp_path / "state.db")
    manager = PositionManager(storage)

    report = manager.record_error(ValueError("broker unavailable"))

    assert report.state == ReconciliationState.ERROR
    assert report.trading_enabled is False
    with pytest.raises(RuntimeError, match="BROKER_RECONCILIATION_ERROR"):
        manager.assert_trading_enabled(max_age_seconds=60)


def test_broker_stop_loss_worse_than_attempt_blocks_reconciliation(tmp_path) -> None:
    storage = Storage(tmp_path / "state.db")
    manager = PositionManager(storage)
    opened = datetime.now(UTC)

    attempt = ExecutionAttempt(
        attempt_id="attempt-sedg",
        proposal_id="proposal-sedg",
        watch_id="watch-sedg",
        symbol="SEDG",
        instrument_id=1969,
        side="BUY",
        amount_usd=10.0,
        leverage=1,
        settlement_type="real",
        stop_loss=33.51,
        take_profit=34.21,
        created_at=opened,
        updated_at=opened,
        state=ExecutionAttemptState.RECONCILED,
        broker_position_id="3581638192",
    )
    storage.save_execution_attempt(attempt)
    manager.record_execution_position(
        ManagedPosition(
            local_id=attempt.attempt_id,
            broker_position_id="3581638192",
            proposal_id=attempt.proposal_id,
            instrument_id=1969,
            symbol="SEDG",
            side="BUY",
            opened_at=opened,
            amount_usd=10.0,
            leverage=1.0,
            stop_loss_rate=27.10,
            take_profit_rate=34.21,
        )
    )

    report = manager.reconcile(
        snapshot(
            BrokerPosition(
                position_id="3581638192",
                instrument_id=1969,
                symbol="SEDG",
                side="BUY",
                amount_usd=10.0,
                leverage=1.0,
                open_rate=33.87,
                stop_loss_rate=27.10,
                take_profit_rate=34.21,
            ),
            at=opened + timedelta(seconds=5),
        )
    )

    assert report.state == ReconciliationState.BLOCKED
    assert report.trading_enabled is False
    assert [issue.code for issue in report.issues] == [
        "BROKER_STOP_LOSS_MISMATCH"
    ]
    assert "33.51" in report.issues[0].detail
    assert "27.1" in report.issues[0].detail


def test_broker_more_protective_stop_remains_synced(tmp_path) -> None:
    storage = Storage(tmp_path / "state.db")
    manager = PositionManager(storage)
    opened = datetime.now(UTC)

    attempt = ExecutionAttempt(
        attempt_id="attempt-long",
        proposal_id="proposal-long",
        watch_id="watch-long",
        symbol="TEST",
        instrument_id=100,
        side="BUY",
        amount_usd=10.0,
        leverage=1,
        settlement_type="real",
        stop_loss=90.0,
        take_profit=110.0,
        created_at=opened,
        updated_at=opened,
        state=ExecutionAttemptState.RECONCILED,
        broker_position_id="broker-long",
    )
    storage.save_execution_attempt(attempt)
    manager.record_execution_position(
        ManagedPosition(
            local_id=attempt.attempt_id,
            broker_position_id="broker-long",
            proposal_id=attempt.proposal_id,
            instrument_id=100,
            symbol="TEST",
            side="BUY",
            opened_at=opened,
            stop_loss_rate=90.0,
        )
    )

    report = manager.reconcile(
        snapshot(
            BrokerPosition(
                position_id="broker-long",
                instrument_id=100,
                symbol="TEST",
                side="BUY",
                amount_usd=10.0,
                leverage=1.0,
                open_rate=100.0,
                stop_loss_rate=91.0,
                take_profit_rate=110.0,
            ),
            at=opened + timedelta(seconds=5),
        )
    )

    assert report.state == ReconciliationState.SYNCED
    assert report.trading_enabled is True
