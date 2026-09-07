import json
from datetime import UTC, datetime
from types import SimpleNamespace

import httpx
import pytest

from parquet.config import EtoroConfig, ExecutionConfig, Settings
from parquet.execution.etoro import EtoroExecutionClient, market_buy_payload
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


def test_market_buy_payload_is_reusable_for_preview() -> None:
    assert market_buy_payload(
        instrument_id=100000,
        amount_usd=10.0,
        stop_loss_rate=77_000.0,
        take_profit_rate=82_000.0,
    ) == {
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


@pytest.mark.asyncio
async def test_live_test_dry_run_stops_before_execution_post(tmp_path, monkeypatch) -> None:
    api_key_file = tmp_path / "api"
    user_key_file = tmp_path / "user"
    api_key_file.write_text("api")
    user_key_file.write_text("user")

    class FakeMarketClient:
        def __init__(self, **kwargs) -> None:
            pass

        async def search(self, symbol: str):
            return [SimpleNamespace(symbol="BTC", instrument_id=100000)]

        async def rates(self, instrument_ids: list[int]):
            return [
                SimpleNamespace(
                    instrument_id=100000,
                    bid=79_999.0,
                    ask=80_000.0,
                    timestamp=datetime.now(UTC),
                )
            ]

        async def account_snapshot(self, now=None):
            return SimpleNamespace(equity_usd=10_000.0, open_positions=0)

    class ForbiddenExecutionClient:
        def __init__(self, **kwargs) -> None:
            raise AssertionError("dry-run must not construct the execution client")

    monkeypatch.setattr("parquet.live_test.EtoroMarketDataClient", FakeMarketClient)
    monkeypatch.setattr("parquet.live_test.EtoroExecutionClient", ForbiddenExecutionClient)

    settings = Settings(
        etoro=EtoroConfig(
            enabled=True,
            api_key_file=api_key_file,
            user_key_file=user_key_file,
        ),
        execution=ExecutionConfig(live_test_enabled=True, live_test_max_amount_usd=10.0),
    )
    result = await run_live_test(
        settings,
        symbol="BTC",
        amount_usd=10.0,
        stop_loss_rate=78_000.0,
        take_profit_rate=82_000.0,
        confirmation="",
        dry_run=True,
    )

    assert result["dry_run"] is True
    assert result["execution_post_sent"] is False
    assert result["payload"] == market_buy_payload(
        instrument_id=100000,
        amount_usd=10.0,
        stop_loss_rate=78_000.0,
        take_profit_rate=82_000.0,
    )
