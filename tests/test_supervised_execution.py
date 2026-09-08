from __future__ import annotations

from datetime import UTC, datetime

import pytest

from parquet.config import EtoroConfig, ExecutionConfig, Settings
from parquet.execution.autonomous import ExecutionAttempt, ExecutionAttemptState
from parquet.execution.etoro import (
    EtoroCostComponent,
    EtoroCostResult,
    EtoroEligibilityResult,
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

    async def instrument_eligibility(self, *, instrument_id: int) -> EtoroEligibilityResult:
        assert instrument_id == 100
        return EtoroEligibilityResult(
            request_id="eligibility",
            instrument_id=100,
            symbol="NSDQ100",
            min_position_exposure=20.0,
            allow_open_position=True,
            leverage_configs=(
                {
                    "direction": "LONG",
                    "settlementType": "cfd",
                    "leverageValues": [2],
                    "minPositionAmount": 10.0,
                },
            ),
            response={},
        )

    async def what_if_open_costs(self, **kwargs) -> EtoroCostResult:
        assert kwargs["instrument_id"] == 100
        assert kwargs["settlement_type"] == "cfd"
        assert kwargs["leverage"] == 2
        assert kwargs["amount_usd"] == 10.0
        return EtoroCostResult(
            request_id="costs",
            instrument_id=100,
            symbol="NSDQ100",
            costs=(EtoroCostComponent("marketSpread", 0.05, "USD"),),
            last_updated=datetime.now(UTC),
            response={},
        )


class SuccessClient(BaseClient):
    def __init__(self) -> None:
        self.submitted: dict[str, object] | None = None

    async def open_market_order(self, **kwargs):
        self.submitted = dict(kwargs)
        request_id = str(kwargs["request_id"])
        return EtoroOrderResult(
            request_id=request_id,
            payload=dict(kwargs),
            response={"orderId": "order-1", "referenceId": request_id},
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
        request_id = str(kwargs["request_id"])
        raise EtoroExecutionError(400, "rejected", request_id)


class TimeoutClient(BaseClient):
    async def open_market_order(self, **kwargs):
        request_id = str(kwargs["request_id"])
        raise EtoroExecutionTransportError("timeout", request_id)

    async def lookup_order(self, **kwargs):
        raise EtoroExecutionError(404, "not found", "lookup-timeout")


class ServerErrorRecoveredClient(SuccessClient):
    async def open_market_order(self, **kwargs):
        request_id = str(kwargs["request_id"])
        raise EtoroExecutionError(503, "upstream response lost", request_id)


class InFlightClient(BaseClient):
    async def open_market_order(self, **kwargs):
        request_id = str(kwargs["request_id"])
        return EtoroOrderResult(
            request_id=request_id,
            payload=dict(kwargs),
            response={"orderId": "order-1", "referenceId": request_id},
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
        leverage=2,
        settlement_type="cfd",
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
            supervised_real_max_leverage=20,
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


def _filled_snapshot() -> BrokerPortfolioSnapshot:
    return _snapshot(
        positions=[
            BrokerPosition(
                position_id="pos-1",
                instrument_id=100,
                symbol="NSDQ100",
                side="buy",
                amount_usd=10.0,
                leverage=2,
            )
        ]
    )


@pytest.mark.asyncio
async def test_supervised_real_success_reconciles_position(tmp_path) -> None:
    client = SuccessClient()
    adapter, storage, reconciliation = _adapter(
        tmp_path,
        client,
        [_snapshot(), _filled_snapshot()],
    )

    result = await adapter.execute(
        _attempt(),
        confirmation="REAL attempt-1",
    )

    assert result.state == ExecutionAttemptState.RECONCILED
    assert result.broker_request_id
    assert result.broker_order_id == "order-1"
    assert result.broker_position_id == "pos-1"
    assert reconciliation.calls == 2
    assert storage.get("execution_uncertain") == "0"
    assert storage.get("etoro_authenticated_gcid") == "123"
    assert client.submitted is not None
    assert client.submitted["leverage"] == 2
    assert client.submitted["settlement_type"] == "cfd"
    managed = storage.active_managed_positions()
    assert len(managed) == 1
    assert managed[0].broker_position_id == "pos-1"
    assert managed[0].leverage == 2


@pytest.mark.asyncio
async def test_supervised_real_rejection_is_terminal_not_uncertain(tmp_path) -> None:
    adapter, storage, _ = _adapter(tmp_path, RejectClient(), [_snapshot()])

    result = await adapter.execute(
        _attempt(),
        confirmation="REAL attempt-1",
    )

    assert result.state == ExecutionAttemptState.REJECTED
    assert result.broker_request_id
    assert storage.get("execution_uncertain") != "1"


@pytest.mark.asyncio
async def test_supervised_real_transport_timeout_uses_lookup_then_blocks_if_unresolved(tmp_path) -> None:
    adapter, storage, _ = _adapter(tmp_path, TimeoutClient(), [_snapshot()])

    result = await adapter.execute(
        _attempt(),
        confirmation="REAL attempt-1",
    )

    assert result.state == ExecutionAttemptState.OUTCOME_UNKNOWN
    assert result.broker_request_id
    assert storage.get("execution_uncertain") == "1"


@pytest.mark.asyncio
async def test_server_error_recovers_by_reference_id_without_second_post(tmp_path) -> None:
    adapter, storage, reconciliation = _adapter(
        tmp_path,
        ServerErrorRecoveredClient(),
        [_snapshot(), _filled_snapshot()],
    )

    result = await adapter.execute(
        _attempt(),
        confirmation="REAL attempt-1",
    )

    assert result.state == ExecutionAttemptState.RECONCILED
    assert result.broker_request_id
    assert result.broker_order_id == "order-1"
    assert result.broker_position_id == "pos-1"
    assert reconciliation.calls == 2
    assert storage.get("execution_uncertain") == "0"


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
async def test_supervised_real_blocks_ticket_without_persisted_settlement(tmp_path) -> None:
    adapter, _, reconciliation = _adapter(tmp_path, SuccessClient(), [_snapshot()])
    attempt = _attempt().model_copy(update={"settlement_type": None})

    with pytest.raises(RuntimeError, match="settlement_type"):
        await adapter.execute(attempt, confirmation="REAL attempt-1")

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
    assert result.broker_order_id == "order-1"
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
    assert result.broker_order_id == "order-1"
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
