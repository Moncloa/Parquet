from __future__ import annotations

from datetime import UTC, datetime

import httpx
import pytest

from parquet.market.etoro import EtoroMarketDataClient
from parquet.portfolio import BrokerPortfolioSnapshot, BrokerPosition
from parquet.risk_etoro import EtoroRiskReader


def _snapshot(*positions: BrokerPosition, equity: float = 10015.0) -> BrokerPortfolioSnapshot:
    return BrokerPortfolioSnapshot(
        captured_at=datetime(2026, 9, 16, 6, 45, tzinfo=UTC),
        equity_usd=equity,
        available_cash_usd=equity,
        invested_usd=0.0,
        unrealized_pnl_usd=sum(position.unrealized_pnl_usd for position in positions),
        credit_usd=equity,
        positions=list(positions),
    )


def _trade(
    position_id: str,
    opened: str,
    closed: str,
    pnl: float,
) -> dict[str, object]:
    return {
        "positionId": position_id,
        "openTimestamp": opened,
        "closeTimestamp": closed,
        "netProfit": pnl,
    }


def _open_position(
    position_id: str,
    opened: str,
    pnl: float,
) -> dict[str, object]:
    return {
        "positionId": position_id,
        "instrumentId": 1,
        "openDateTime": opened,
        "unrealizedPnL": {"pnL": pnl},
    }


@pytest.mark.asyncio
async def test_rebuilds_daily_and_weekly_risk_from_broker_data() -> None:
    queries: list[dict[str, str]] = []
    history = [
        _trade("1", "2026-09-16T06:00:00Z", "2026-09-16T06:30:00Z", 20.0),
        _trade("2", "2026-09-14T08:00:00Z", "2026-09-14T09:00:00Z", -10.0),
        _trade("3", "2026-09-16T06:10:00Z", "2026-09-16T06:20:00Z", 4.0),
        _trade("3", "2026-09-16T06:10:00Z", "2026-09-16T06:25:00Z", 6.0),
    ]
    open_positions = [_open_position("4", "2026-09-16T06:20:00Z", -5.0)]

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v1/trading/info/trade/history":
            queries.append(dict(request.url.params))
            return httpx.Response(200, json=history)
        if request.url.path == "/api/v1/trading/info/real/pnl":
            return httpx.Response(
                200,
                json={
                    "clientPortfolio": {
                        "positions": open_positions,
                        "mirrors": [],
                    }
                },
            )
        raise AssertionError(f"unexpected path {request.url.path}")

    client = EtoroMarketDataClient(
        api_key="api",
        user_key="user",
        base_url="https://example.test/api/v1",
        transport=httpx.MockTransport(handler),
    )
    reader = EtoroRiskReader(client)
    broker_position = BrokerPosition(
        position_id="4",
        instrument_id=1,
        unrealized_pnl_usd=-5.0,
    )

    result = await reader.snapshot(
        _snapshot(broker_position),
        now=datetime(2026, 9, 16, 6, 45, tzinfo=UTC),
        timezone="Europe/Madrid",
    )

    assert queries == [
        {
            "minDate": "2026-09-13",
            "page": "1",
            "pageSize": "100",
        }
    ]
    assert result.day_start == datetime(2026, 9, 15, 22, 0, tzinfo=UTC)
    assert result.week_start == datetime(2026, 9, 13, 22, 0, tzinfo=UTC)
    assert result.trades_today == 3
    assert result.daily_realized_pnl_usd == 30.0
    assert result.weekly_realized_pnl_usd == 20.0
    assert result.open_unrealized_pnl_usd == -5.0
    assert result.daily_pnl_usd == 25.0
    assert result.weekly_pnl_usd == 15.0
    assert result.daily_start_equity_usd == 9990.0
    assert result.weekly_start_equity_usd == 10000.0
    assert result.daily_pnl_pct == pytest.approx(0.25025025)
    assert result.weekly_pnl_pct == pytest.approx(0.15)


@pytest.mark.asyncio
async def test_open_position_carried_over_day_fails_closed() -> None:
    open_positions = [_open_position("7", "2026-09-15T12:00:00Z", -25.0)]

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v1/trading/info/trade/history":
            return httpx.Response(200, json=[])
        if request.url.path == "/api/v1/trading/info/real/pnl":
            return httpx.Response(
                200,
                json={"clientPortfolio": {"positions": open_positions, "mirrors": []}},
            )
        raise AssertionError(f"unexpected path {request.url.path}")

    client = EtoroMarketDataClient(
        api_key="api",
        user_key="user",
        base_url="https://example.test/api/v1",
        transport=httpx.MockTransport(handler),
    )
    reader = EtoroRiskReader(client)
    broker_position = BrokerPosition(position_id="7", instrument_id=1)

    with pytest.raises(ValueError, match="predates day start"):
        await reader.snapshot(
            _snapshot(broker_position),
            now=datetime(2026, 9, 16, 6, 45, tzinfo=UTC),
            timezone="Europe/Madrid",
        )


@pytest.mark.asyncio
async def test_closed_trade_spanning_day_fails_closed() -> None:
    history = [
        _trade("8", "2026-09-15T12:00:00Z", "2026-09-16T06:00:00Z", -30.0)
    ]

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v1/trading/info/trade/history":
            return httpx.Response(200, json=history)
        if request.url.path == "/api/v1/trading/info/real/pnl":
            return httpx.Response(
                200,
                json={"clientPortfolio": {"positions": [], "mirrors": []}},
            )
        raise AssertionError(f"unexpected path {request.url.path}")

    client = EtoroMarketDataClient(
        api_key="api",
        user_key="user",
        base_url="https://example.test/api/v1",
        transport=httpx.MockTransport(handler),
    )
    reader = EtoroRiskReader(client)

    with pytest.raises(ValueError, match="spans day start"):
        await reader.snapshot(
            _snapshot(),
            now=datetime(2026, 9, 16, 6, 45, tzinfo=UTC),
            timezone="Europe/Madrid",
        )


@pytest.mark.asyncio
async def test_malformed_trade_history_fails_closed() -> None:
    malformed = [
        {
            "positionId": "9",
            "openTimestamp": "2026-09-16T05:00:00Z",
            "closeTimestamp": "2026-09-16T06:00:00Z",
        }
    ]

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v1/trading/info/trade/history":
            return httpx.Response(200, json=malformed)
        raise AssertionError(f"unexpected path {request.url.path}")

    client = EtoroMarketDataClient(
        api_key="api",
        user_key="user",
        base_url="https://example.test/api/v1",
        transport=httpx.MockTransport(handler),
    )
    reader = EtoroRiskReader(client)

    with pytest.raises(ValueError, match="netProfit"):
        await reader.snapshot(
            _snapshot(),
            now=datetime(2026, 9, 16, 6, 45, tzinfo=UTC),
            timezone="Europe/Madrid",
        )
