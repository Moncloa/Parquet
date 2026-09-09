from datetime import UTC, datetime

import httpx
import pytest

from parquet.market.candles import EtoroCandleClient


@pytest.mark.asyncio
async def test_candles_parse_grouped_etoro_shape() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path.endswith(
            "/market-data/instruments/6473/history/candles/asc/OneMinute/120"
        )
        assert request.headers["x-api-key"] == "api"
        assert request.headers["x-user-key"] == "user"
        assert request.headers["x-request-id"]
        return httpx.Response(
            200,
            json={
                "interval": "OneMinute",
                "candles": [
                    {
                        "instrumentId": 6473,
                        "candles": [
                            {
                                "instrumentID": 6473,
                                "fromDate": "2026-09-09T09:00:00Z",
                                "open": 138.8,
                                "high": 139.2,
                                "low": 138.7,
                                "close": 139.0,
                                "volume": 1234,
                            },
                            {
                                "instrumentID": 6473,
                                "fromDate": "2026-09-09T09:01:00Z",
                                "open": 139.0,
                                "high": 139.4,
                                "low": 138.9,
                                "close": 139.3,
                                "volume": 1500,
                            },
                        ],
                    }
                ],
            },
        )

    client = EtoroCandleClient(
        api_key="api",
        user_key="user",
        transport=httpx.MockTransport(handler),
    )
    candles = await client.candles(6473, interval="OneMinute", count=120)
    assert len(candles) == 2
    assert candles[0].instrument_id == 6473
    assert candles[0].from_date == datetime(2026, 9, 9, 9, 0, tzinfo=UTC)
    assert candles[1].close == 139.3


def test_candles_validate_direction_and_count() -> None:
    client = EtoroCandleClient(api_key="api", user_key="user")
    with pytest.raises(ValueError, match="direction"):
        import asyncio

        asyncio.run(client.candles(1, direction="sideways"))
    with pytest.raises(ValueError, match="count"):
        import asyncio

        asyncio.run(client.candles(1, count=1001))
