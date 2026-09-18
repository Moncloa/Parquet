from __future__ import annotations

from datetime import UTC, datetime

import httpx
import pytest

from parquet.config import EtoroConfig, ExecutionConfig, Settings
from parquet.execution.autonomous import ExecutionAttempt, ExecutionAttemptState
from parquet.execution.demo import DemoExecutionAdapter
from parquet.execution.etoro import (
    EtoroEligibilityResult,
    EtoroExecutionClient,
    EtoroExecutionTransportError,
    EtoroIdentity,
    EtoroOrderLookupResult,
    EtoroOrderResult,
)
from parquet.storage import Storage


def _settings() -> Settings:
    return Settings(
        etoro=EtoroConfig(expected_gcid=123),
        execution=ExecutionConfig(
            autonomous_enabled=True,
            autonomous_mode="demo",
            autonomous_demo_enabled=True,
            autonomous_demo_max_amount_usd=25.0,
            autonomous_demo_max_leverage=2,
            broker_lookup_attempts=2,
            broker_lookup_interval_seconds=0,
        ),
    )


def _attempt() -> ExecutionAttempt:
    now = datetime.now(UTC)
    return ExecutionAttempt(
        attempt_id="demo-attempt-1",
        proposal_id="proposal-1",
        watch_id="watch-1",
        symbol="NSDQ100",
        instrument_id=100,
        side="BUY",
        amount_usd=100.0,
        leverage=1,
        settlement_type=None,
        stop_loss=29_400.0,
        take_profit=29_600.0,
        created_at=now,
        updated_at=now,
        state=ExecutionAttemptState.DEMO_PENDING,
    )


class DemoSuccessClient:
    def __init__(self) -> None:
        self.pnl_calls = 0
        self.submitted: dict[str, object] | None = None

    async def identity(self) -> EtoroIdentity:
        return EtoroIdentity(
            gcid=123,
            real_cid=456,
            demo_cid=789,
            scopes=frozenset(_settings().etoro.required_demo_scopes),
        )

    async def account_pnl(self):
        self.pnl_calls += 1
        positions = (
            []
            if self.pnl_calls == 1
            else [{"positionId": "demo-pos-1", "instrumentId": 100}]
        )
        return {"clientPortfolio": {"positions": positions}}

    async def instrument_eligibility(self, *, instrument_id: int):
        assert instrument_id == 100
        return EtoroEligibilityResult(
            request_id="eligibility",
            instrument_id=100,
            symbol="NSDQ100",
            min_position_exposure=10.0,
            allow_open_position=True,
            leverage_configs=(
                {
                    "direction": "LONG",
                    "settlementType": "cfd",
                    "leverageValues": [1, 2],
                    "minPositionAmount": 10.0,
                },
            ),
            response={},
        )

    async def open_market_order(self, **kwargs):
        self.submitted = dict(kwargs)
        request_id = str(kwargs["request_id"])
        return EtoroOrderResult(
            request_id=request_id,
            payload=dict(kwargs),
            response={"orderId": "demo-order-1", "referenceId": request_id},
        )

    async def lookup_order(self, **kwargs):
        return EtoroOrderLookupResult(
            request_id="lookup",
            response={
                "orderId": "demo-order-1",
                "status": {"id": 3, "name": "Filled", "errorCode": 0},
                "positionExecutions": [{"positionId": "demo-pos-1"}],
            },
        )


class DemoTimeoutClient(DemoSuccessClient):
    async def open_market_order(self, **kwargs):
        request_id = str(kwargs["request_id"])
        raise EtoroExecutionTransportError("timeout", request_id)

    async def lookup_order(self, **kwargs):
        raise EtoroExecutionTransportError("timeout", "lookup")


@pytest.mark.asyncio
async def test_demo_execution_success_is_reconciled_without_real_write(tmp_path) -> None:
    storage = Storage(tmp_path / "state.db")
    client = DemoSuccessClient()
    adapter = DemoExecutionAdapter(
        settings=_settings(),
        storage=storage,
        client=client,  # type: ignore[arg-type]
    )
    storage.save_execution_attempt(_attempt())

    result = await adapter.execute(_attempt())

    assert result.state == ExecutionAttemptState.RECONCILED
    assert result.broker_order_id == "demo-order-1"
    assert result.broker_position_id == "demo-pos-1"
    assert result.amount_usd == 10.0
    assert result.leverage == 1
    assert result.settlement_type == "cfd"
    assert storage.get("demo_execution_uncertain") == "0"
    assert client.submitted is not None
    assert client.submitted["amount_usd"] == 10.0
    assert client.submitted["leverage"] == 1


@pytest.mark.asyncio
async def test_demo_execution_timeout_never_reposts_and_marks_unknown(tmp_path) -> None:
    storage = Storage(tmp_path / "state.db")
    client = DemoTimeoutClient()
    adapter = DemoExecutionAdapter(
        settings=_settings(),
        storage=storage,
        client=client,  # type: ignore[arg-type]
    )
    storage.save_execution_attempt(_attempt())

    result = await adapter.execute(_attempt())

    assert result.state == ExecutionAttemptState.OUTCOME_UNKNOWN
    assert storage.get("demo_execution_uncertain") == "1"


@pytest.mark.asyncio
async def test_execution_client_uses_demo_paths() -> None:
    seen: list[tuple[str, str]] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.method, request.url.path))
        if request.url.path.endswith("/eligibility"):
            return httpx.Response(
                200,
                json={
                    "eligibilities": [
                        {
                            "instrumentId": 100,
                            "symbol": "NSDQ100",
                            "minPositionExposure": 10,
                            "allowOpenPosition": True,
                            "leverageConfigs": [
                                {
                                    "direction": "LONG",
                                    "settlementType": "cfd",
                                    "leverageValues": [1],
                                }
                            ],
                        }
                    ]
                },
            )
        if request.url.path.endswith("/orders:lookup"):
            return httpx.Response(
                200,
                json={
                    "orderId": "1",
                    "status": {"id": 3, "name": "Filled", "errorCode": 0},
                    "positionExecutions": [{"positionId": "2"}],
                },
            )
        if request.url.path.endswith("/pnl"):
            return httpx.Response(200, json={"clientPortfolio": {"positions": []}})
        return httpx.Response(200, json={"orderId": "1", "referenceId": "ref"})

    client = EtoroExecutionClient(
        api_key="api",
        user_key="demo",
        environment="demo",
        transport=httpx.MockTransport(handler),
    )
    await client.instrument_eligibility(instrument_id=100)
    await client.open_market_order(
        transaction="BUY",
        instrument_id=100,
        amount_usd=10.0,
        stop_loss_rate=9.0,
        settlement_type="cfd",
    )
    await client.lookup_order(reference_id="ref")
    await client.account_pnl()

    assert ("POST", "/api/v2/trading/info/demo/eligibility") in seen
    assert ("POST", "/api/v2/trading/execution/demo/orders") in seen
    assert ("GET", "/api/v2/trading/info/demo/orders:lookup") in seen
    assert ("GET", "/api/v1/trading/info/demo/pnl") in seen
