from datetime import UTC, datetime, timedelta

import httpx
import pytest

from parquet.config import EtoroConfig, Settings
from parquet.market.etoro import EtoroMarketDataClient
from parquet.orchestrator import Orchestrator


@pytest.mark.asyncio
async def test_account_poll_persists_risk_snapshot(tmp_path) -> None:
    now = datetime(2026, 9, 7, 11, 42, tzinfo=UTC)

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path.endswith("/trading/info/real/pnl")
        return httpx.Response(
            200,
            json={
                "clientPortfolio": {
                    "positions": [
                        {
                            "instrumentID": 32,
                            "amount": 1000.0,
                            "unrealizedPnL": {"pnL": 25.0},
                        }
                    ],
                    "unrealizedPnL": 25.0,
                    "mirrors": [],
                    "accountCurrencyId": 1,
                    "credit": 9000.0,
                    "orders": [],
                    "ordersForOpen": [],
                }
            },
        )

    settings = Settings(
        state_db=tmp_path / "parquet.db",
        etoro=EtoroConfig(
            instrument_ids={"GER40": 32},
            account_poll_seconds=60,
        ),
    )
    client = EtoroMarketDataClient(
        api_key="api",
        user_key="user",
        transport=httpx.MockTransport(handler),
    )
    orchestrator = Orchestrator(settings, market_client=client)

    refreshed = await orchestrator.poll_account_once(now)

    assert refreshed == 1
    snapshot = orchestrator.storage.get_risk_snapshot()
    assert snapshot is not None
    assert snapshot.as_of == now
    assert snapshot.equity_usd == 10025.0
    assert snapshot.open_positions == 1
    assert snapshot.open_instrument_ids == [32]
    assert snapshot.open_symbols == ["GER40"]

    skipped = await orchestrator.poll_account_once(now + timedelta(seconds=30))
    assert skipped == 0
    refreshed_again = await orchestrator.poll_account_once(
        now + timedelta(seconds=30), force=True
    )
    assert refreshed_again == 1
