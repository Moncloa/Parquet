from __future__ import annotations

from datetime import UTC, datetime

import pytest

from parquet.config import ExecutionConfig, Settings
from parquet.execution.autonomous import ExecutionAttempt, ExecutionAttemptState
from parquet.execution.etoro import (
    EtoroExecutionError,
    EtoroExecutionTransportError,
    EtoroOrderResult,
)
from parquet.execution.supervised import RealSmallExecutionAdapter
from parquet.portfolio import (
    BrokerPortfolioSnapshot,
    BrokerPosition,
    PositionManager,
)
from parquet.storage import Storage


class FakeReconciliation:
    def __init__(
        self,
        storage: Storage,
        position_manager: PositionManager,
        snapshots: list[BrokerPortfolioSnapshot],
    ) -> None:
        self.storage = storage
        self.position_manager = position_manager
        self.snapshots = snapshots
        self.calls = 0

    async def poll_once(self, now=None, *, force: bool = False) -> int:
        index = min(self.calls, len(self.snapshots) - 1)
        snapshot = self.snapshots[index]
        self.position_manager.reconcile(snapshot)
        self.calls += 1
        return 1


class SuccessClient:
    async def open_market_order(self, **kwargs):
        return EtoroOrderResult(
            request_id="req-1",
            payload=dict(kwargs),
            response={"positionId": "pos-1"},
        )


class RejectClient:
    async def open_market_order(self, **kwargs):
        raise EtoroExecutionError(400, "rejected", "req-reject")


class TimeoutClient:
    async def open_market_order(self, **kwargs):
        raise EtoroExecutionTransportError("timeout", "req-timeout")


class OrderOnlyClient:
    async def open_market_order(self, **kwargs):
        return EtoroOrderResult(
            request_id="req-order",
            payload=dict(kwargs),
            response={"orderId": "order-1"},
        )


def _snapshot(*, positions: list[BrokerPosition] | None = None) -> BrokerPortfolioSnapshot:
    return BrokerPortfolioSnapshot(
        captured_at=datetime.now(UTC),
        equity_usd=1_000.0,
        available_cash_usd=1_000.0,
        invested_usd=0.0,
        unrealized_pnl_usd=0.0,
        credit_usd=1_000.0,
        positions=positions or [],
    )


def _attempt(*, amount: float = 10.0) -> ExecutionAttempt:
    now = datetime.now(UTC)
    return ExecutionAttempt(
        attempt_id="attempt-1",
        proposal_id="proposal-1",
        watch_id="watch-1",
        symbol="NSDQ100",
        instrument_id=100,
        side="buy",
        amount_usd=amount,
        stop_loss=29_400.0,
        take_profit=29_600.0,
        created_at=now,
        updated_at=now,
        state=ExecutionAttemptState.PREPARED,
    )


def _adapter(tmp_path, client, snapshots):
    storage = Storage(tmp_path / "state.db")
    manager = PositionManager(storage)
    reconciliation = FakeReconciliation(storage, manager, snapshots)
    settings = Settings(
        execution=ExecutionConfig(
            supervised_real_enabled=True,
            supervised_real_max_amount_usd=25.0,
        )
    )
    adapter = RealSmallExecutionAdapter(
        settings=settings,
        storage=storage,
        position_manager=manager,
        reconciliation=reconciliation,  # type: ignore[arg-type]
        client=client,  # type: ignore[arg-type]
    )
    return adapter, storage, reconciliation


@pytest.mark.asyncio
async def test_supervised_real_success_reconciles_position(tmp_path) -> None:
    second = _snapshot(
        positions=[
            BrokerPosition(
                position_id="pos-1",
                instrument_id=100,
                symbol="NSDQ100",
                side="buy",
                amount_usd=10.0,
            )
        ]
    )
    adapter, storage, reconciliation = _adapter(
        tmp_path,
        SuccessClient(),
        [_snapshot(), second],
    )

    result = await adapter.execute(
        _attempt(),
        confirmation="REAL attempt-1",
    )

    assert result.state == ExecutionAttemptState.RECONCILED
    assert result.broker_position_id == "pos-1"
    assert reconciliation.calls == 2
    assert storage.get("execution_uncertain") == "0"
    managed = storage.active_managed_positions()
    assert len(managed) == 1
    assert managed[0].broker_position_id == "pos-1"


@pytest.mark.asyncio
async def test_supervised_real_rejection_is_terminal_not_uncertain(tmp_path) -> None:
    adapter, storage, _ = _adapter(tmp_path, RejectClient(), [_snapshot()])

    result = await adapter.execute(
        _attempt(),
        confirmation="REAL attempt-1",
    )

    assert result.state == ExecutionAttemptState.REJECTED
    assert result.broker_request_id == "req-reject"
    assert storage.get("execution_uncertain") != "1"


@pytest.mark.asyncio
async def test_supervised_real_transport_timeout_blocks_future_execution(tmp_path) -> None:
    adapter, storage, _ = _adapter(tmp_path, TimeoutClient(), [_snapshot()])

    result = await adapter.execute(
        _attempt(),
        confirmation="REAL attempt-1",
    )

    assert result.state == ExecutionAttemptState.OUTCOME_UNKNOWN
    assert result.broker_request_id == "req-timeout"
    assert storage.get("execution_uncertain") == "1"


@pytest.mark.asyncio
async def test_supervised_real_requires_attempt_bound_confirmation(tmp_path) -> None:
    adapter, _, reconciliation = _adapter(tmp_path, SuccessClient(), [_snapshot()])

    with pytest.raises(RuntimeError, match="Explicit confirmation required"):
        await adapter.execute(_attempt(), confirmation="REAL")

    assert reconciliation.calls == 0


@pytest.mark.asyncio
async def test_supervised_real_enforces_small_amount_cap(tmp_path) -> None:
    adapter, _, reconciliation = _adapter(tmp_path, SuccessClient(), [_snapshot()])

    with pytest.raises(RuntimeError, match="exceeds supervised real cap"):
        await adapter.execute(
            _attempt(amount=25.01),
            confirmation="REAL attempt-1",
        )

    assert reconciliation.calls == 0


@pytest.mark.asyncio
async def test_acknowledged_order_missing_after_reconciliation_becomes_unknown(tmp_path) -> None:
    adapter, storage, reconciliation = _adapter(
        tmp_path,
        OrderOnlyClient(),
        [_snapshot(), _snapshot()],
    )

    result = await adapter.execute(
        _attempt(),
        confirmation="REAL attempt-1",
    )

    assert reconciliation.calls == 2
    assert result.state == ExecutionAttemptState.OUTCOME_UNKNOWN
    assert result.reason == "acknowledged_identity_not_visible_after_reconciliation"
    assert storage.get("execution_uncertain") == "1"
