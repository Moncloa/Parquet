import json

import httpx
import pytest

from parquet.config import EtoroConfig, ExecutionConfig, Settings
from parquet.execution.etoro import EtoroExecutionClient
from parquet.live_test import run_live_test


@pytest.mark.asyncio
async def test_etoro_live_order_uses_flat_unified_order_payload() -> None:
    captured: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["path"] = request.url.path
        captured["body"] = json.loads(request.content)
        captured["request_id"] = request.headers["x-request-id"]
        return httpx.Response(200, json={"orderId": 123, "referenceId": "ref-1"})

    client = EtoroExecutionClient(
        api_key="api",
        user_key="user",
        transport=httpx.MockTransport(handler),
    )
    result = await client.open_market_buy(
        instrument_id=100000,
        amount_usd=10.0,
        stop_loss_rate=77_000.0,
        take_profit_rate=82_000.0,
    )

    assert captured["path"] == "/api/v2/trading/execution/orders"
    assert captured["body"] == {
        "action": "open",
        "transaction": "buy",
        "instrumentId": 100000,
        "orderType": "mkt",
        "amount": 10.0,
        "orderCurrency": "usd",
        "leverage": 1,
        "stopLossRate": 77_000.0,
        "stopLossType": "fixed",
        "takeProfitRate": 82_000.0,
    }
    assert result.request_id == captured["request_id"]
    assert result.response["orderId"] == 123


@pytest.mark.asyncio
async def test_live_test_requires_explicit_real_money_confirmation() -> None:
    settings = Settings(
        etoro=EtoroConfig(enabled=True),
        execution=ExecutionConfig(live_test_enabled=True),
    )

    with pytest.raises(RuntimeError, match="REAL-MONEY"):
        await run_live_test(
            settings,
            symbol="BTC",
            amount_usd=10.0,
            stop_loss_rate=77_000.0,
            take_profit_rate=None,
            confirmation="no",
        )


@pytest.mark.asyncio
async def test_live_test_enforces_hard_amount_cap_before_network() -> None:
    settings = Settings(
        etoro=EtoroConfig(enabled=True),
        execution=ExecutionConfig(live_test_enabled=True, live_test_max_amount_usd=25.0),
    )

    with pytest.raises(RuntimeError, match="exceeds live-test cap"):
        await run_live_test(
            settings,
            symbol="BTC",
            amount_usd=25.01,
            stop_loss_rate=77_000.0,
            take_profit_rate=None,
            confirmation="REAL-MONEY",
        )
