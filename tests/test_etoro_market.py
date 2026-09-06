from datetime import UTC, datetime

import httpx
import pytest

from parquet.market.etoro import EtoroMarketDataClient, EtoroRateLimitError


@pytest.mark.asyncio
async def test_rates_parse_official_shape_and_send_auth_headers() -> None:
    seen_headers: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen_headers.update(dict(request.headers))
        assert request.url.params["instrumentIds"] == "1001,1002"
        return httpx.Response(
            200,
            json={
                "rates": [
                    {
                        "instrumentId": 1001,
                        "symbol": "AAPL",
                        "bid": 198.42,
                        "ask": 198.56,
                        "lastPrice": 198.49,
                        "change": 1.23,
                        "timestamp": "2026-04-04T12:00:01Z",
                    }
                ]
            },
        )

    client = EtoroMarketDataClient(
        api_key="api",
        user_key="user",
        transport=httpx.MockTransport(handler),
    )
    rates = await client.rates([1001, 1002])
    assert rates[0].symbol == "AAPL"
    assert rates[0].timestamp == datetime(2026, 4, 4, 12, 0, 1, tzinfo=UTC)
    assert seen_headers["x-api-key"] == "api"
    assert seen_headers["x-user-key"] == "user"
    assert seen_headers["x-request-id"]


@pytest.mark.asyncio
async def test_search_parses_instrument_id() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.params["search"] == "TSLA"
        return httpx.Response(
            200,
            json={"data": [{"instrumentId": 1002, "symbol": "TSLA", "name": "Tesla"}]},
        )

    client = EtoroMarketDataClient(api_key="api", transport=httpx.MockTransport(handler))
    hits = await client.search("TSLA")
    assert hits[0].instrument_id == 1002
    assert hits[0].symbol == "TSLA"


@pytest.mark.asyncio
async def test_429_exposes_retry_after() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(429, headers={"Retry-After": "7"}, text="slow down")

    client = EtoroMarketDataClient(api_key="api", transport=httpx.MockTransport(handler))
    with pytest.raises(EtoroRateLimitError) as exc:
        await client.rates([1001])
    assert exc.value.retry_after_seconds == 7
