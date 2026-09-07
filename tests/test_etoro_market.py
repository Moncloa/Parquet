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
async def test_rates_parse_current_etoro_shape() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "rates": [
                    {
                        "instrumentID": 32,
                        "ask": 25977.05,
                        "bid": 25976.35,
                        "lastExecution": 25976.35,
                        "date": "2026-09-07T09:28:45.8547207Z",
                    }
                ]
            },
        )

    client = EtoroMarketDataClient(api_key="api", transport=httpx.MockTransport(handler))
    rates = await client.rates([32])
    assert len(rates) == 1
    assert rates[0].instrument_id == 32
    assert rates[0].bid == 25976.35
    assert rates[0].ask == 25977.05
    assert rates[0].last_price == 25976.35
    assert rates[0].timestamp == datetime(2026, 9, 7, 9, 28, 45, 854720, tzinfo=UTC)


@pytest.mark.asyncio
async def test_search_uses_exact_symbol_without_fields_projection() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.params["internalSymbolFull"] == "TSLA"
        assert "fields" not in request.url.params
        return httpx.Response(
            200,
            json={
                "items": [
                    {
                        "instrumentId": 1002,
                        "internalSymbolFull": "TSLA",
                        "displayname": "Tesla",
                    }
                ]
            },
        )

    client = EtoroMarketDataClient(api_key="api", transport=httpx.MockTransport(handler))
    hits = await client.search("TSLA")
    assert hits[0].instrument_id == 1002
    assert hits[0].symbol == "TSLA"
    assert hits[0].name == "Tesla"


@pytest.mark.asyncio
async def test_search_keeps_legacy_data_shape_compatible() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"data": [{"instrumentId": 1002, "symbol": "TSLA", "name": "Tesla"}]},
        )

    client = EtoroMarketDataClient(api_key="api", transport=httpx.MockTransport(handler))
    hits = await client.search("TSLA")
    assert hits[0].instrument_id == 1002
    assert hits[0].symbol == "TSLA"


@pytest.mark.asyncio
async def test_account_snapshot_matches_empty_real_pnl_shape() -> None:
    now = datetime(2026, 9, 7, 11, 42, tzinfo=UTC)

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path.endswith("/trading/info/real/pnl")
        return httpx.Response(
            200,
            json={
                "clientPortfolio": {
                    "positions": [],
                    "unrealizedPnL": 0.0,
                    "mirrors": [],
                    "accountCurrencyId": 1,
                    "credit": 10000.0,
                    "orders": [],
                    "stockOrders": [],
                    "entryOrders": [],
                    "exitOrders": [],
                    "ordersForOpen": [],
                    "ordersForClose": [],
                    "ordersForCloseMultiple": [],
                    "bonusCredit": 0.0,
                }
            },
        )

    client = EtoroMarketDataClient(
        api_key="api",
        user_key="user",
        transport=httpx.MockTransport(handler),
    )
    snapshot = await client.account_snapshot(now=now)

    assert snapshot.equity_usd == 10000.0
    assert snapshot.available_cash_usd == 10000.0
    assert snapshot.invested_usd == 0.0
    assert snapshot.unrealized_pnl_usd == 0.0
    assert snapshot.open_positions == 0
    assert snapshot.open_instrument_ids == []


@pytest.mark.asyncio
async def test_account_snapshot_uses_official_equity_components() -> None:
    now = datetime(2026, 9, 7, 11, 42, tzinfo=UTC)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "clientPortfolio": {
                    "positions": [
                        {
                            "instrumentID": 32,
                            "symbol": "GER40",
                            "amount": 1000.0,
                            "unrealizedPnL": {"pnL": 50.0},
                        }
                    ],
                    "unrealizedPnL": 50.0,
                    "mirrors": [],
                    "accountCurrencyId": 1,
                    "credit": 10000.0,
                    "orders": [{"amount": 100.0}],
                    "ordersForOpen": [
                        {"mirrorID": 0, "amount": 500.0, "totalExternalCosts": 2.0}
                    ],
                }
            },
        )

    client = EtoroMarketDataClient(api_key="api", transport=httpx.MockTransport(handler))
    snapshot = await client.account_snapshot(now=now)

    assert snapshot.available_cash_usd == 9400.0
    assert snapshot.invested_usd == 1602.0
    assert snapshot.unrealized_pnl_usd == 50.0
    assert snapshot.equity_usd == 11052.0
    assert snapshot.open_positions == 1
    assert snapshot.open_instrument_ids == [32]
    assert snapshot.open_symbols == ["GER40"]


@pytest.mark.asyncio
async def test_429_exposes_retry_after() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(429, headers={"Retry-After": "7"}, text="slow down")

    client = EtoroMarketDataClient(api_key="api", transport=httpx.MockTransport(handler))
    with pytest.raises(EtoroRateLimitError) as exc:
        await client.rates([1001])
    assert exc.value.retry_after_seconds == 7
