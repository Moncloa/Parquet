from __future__ import annotations

from datetime import UTC, datetime

import pytest

from parquet.config import EtoroConfig, ExecutionConfig, Settings
from parquet.execution.autonomous import ExecutionAttempt, ExecutionAttemptState
from parquet.execution.etoro import (
    EtoroExecutionError,
    EtoroExecutionTransportError,
    EtoroIdentity,
    EtoroOrderLookupResult,
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


class BaseClient:
    async def identity(self) -> EtoroIdentity:
        return EtoroIdentity(
            gcid=123,
            real_cid=456,
            demo_cid=789,
            scopes=frozenset(
                {
                    "etoro-public:real:read",
                    "etoro-public:real:write",
                    "etoro-public:trade.real:read",
                    "etoro-public:trade.real:write",
                }
            ),
        )


class SuccessClient(BaseClient):
    async def open_market_order(self, **kwargs):
        return EtoroOrderResult(
            request_id="req-1",
            payload=dict(kwargs),
            response={"orderId": "order-1", "referenceId": "req-1"},
        )

    async def lookup_order(self, **kwargs):
        return EtoroOrderLookupResult(
            request_id="lookup-1",
            response={
                "orderId": "order-1",
                "status": {"id": 3, "name": "Filled", "errorCode": 0},
                "positionExecutions": [{"positionId": "pos-1"}],
            },
        )


class RejectClient(BaseClient):
    async def open_market_order(self, **kwargs):
        raise EtoroExecutionError(400, "rejected", "req-reject")


class TimeoutClient(BaseClient):
    async def open_market_order(self, **kwargs):
        raise EtoroExecutionTransportError("timeout", "req-timeout")

    async def lookup_order(self, **kwargs):
        raise EtoroExecutionError(404, "not found", "lookup-timeout")


class InFlightClient(BaseClient):
    async def open_market_order(self, **kwargs):
        return EtoroOrderResult(
            request_id="req-order",
            payload=dict(kwargs),
            response={"orderId": "order-1", "referenceId": "req-order"},
        )

    async def lookup_order(self, **kwargs):
        return EtoroOrderLookupResult(
            request_id="lookup-order",
            response={
                "orderId": "order-1",
                "status": {"id": 2, "name": "Placed", "errorCode": 0},
                "positionExecutions": [],
            },
        )


class WrongIdentityClient(SuccessClient):
    async def identity(self) -> EtoroIdentity:
        identity = await super().identity()
        return EtoroIdentity(
            gcid=999,
            real_cid=identity.real_cid,
            demo_cid=identity.demo_cid,
            scopes=identity.scopes,
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
        etoro=EtoroConfig(expected_gcid=123),
        execution=ExecutionConfig(
            supervised_real_enabled=True,
            supervised_real_max_amount_usd=25.0,
            broker_lookup_attempts=2,
            broker_lookup_interval_seconds=0,
        ),
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
    assert result.broker_order_id == "order-1"
    assert result.broker_position_id == "pos-1"
    assert reconciliation.calls == 2
    assert storage.get("execution_uncertain") == "0"
    assert storage.get("etoro_authenticated_gcid") == "123"
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
async def test_supervised_real_transport_timeout_uses_lookup_then_blocks_if_unresolved(tmp_path) -> None:
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
async def test_filled_position_missing_after_reconciliation_becomes_unknown(tmp_path) -> None:
    adapter, storage, reconciliation = _adapter(
        tmp_path,
        SuccessClient(),
        [_snapshot(), _snapshot()],
    )

    result = await adapter.execute(
        _attempt(),
        confirmation="REAL attempt-1",
    )

    assert reconciliation.calls == 2
    assert result.state == ExecutionAttemptState.OUTCOME_UNKNOWN
    assert result.reason == "filled_position_not_visible_after_reconciliation"
    assert storage.get("execution_uncertain") == "1"


@pytest.mark.asyncio
async def test_in_flight_order_exhausting_lookup_window_becomes_unknown(tmp_path) -> None:
    adapter, storage, reconciliation = _adapter(
        tmp_path,
        InFlightClient(),
        [_snapshot()],
    )

    result = await adapter.execute(
        _attempt(),
        confirmation="REAL attempt-1",
    )

    assert reconciliation.calls == 1
    assert result.state == ExecutionAttemptState.OUTCOME_UNKNOWN
    assert result.reason == "broker_order_still_in_flight:Placed"
    assert storage.get("execution_uncertain") == "1"


@pytest.mark.asyncio
async def test_wrong_agent_portfolio_gcid_blocks_before_post(tmp_path) -> None:
    adapter, storage, reconciliation = _adapter(
        tmp_path,
        WrongIdentityClient(),
        [_snapshot()],
    )

    with pytest.raises(RuntimeError, match="does not match pinned Agent Portfolio GCID"):
        await adapter.execute(
            _attempt(),
            confirmation="REAL attempt-1",
        )

    assert reconciliation.calls == 1
    assert storage.latest_execution_attempts() == []
