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


def _trade(position_id: str, opened: str) -> dict[str, object]:
    return {
        "positionId": position_id,
        "openTimestamp": opened,
        "closeTimestamp": "2026-09-16T06:30:00Z",
        "netProfit": 123.0,
    }


def _open_position(position_id: str, opened: str) -> dict[str, object]:
    return {
        "positionId": position_id,
        "instrumentId": 1,
        "openDateTime": opened,
        "unrealizedPnL": {"pnL": -999.0},
    }


def _balance_history(
    *,
    daily: float = 10010.0,
    weekly: float = 10000.0,
    include_daily: bool = True,
    include_weekly: bool = True,
) -> dict[str, object]:
    snapshots: list[dict[str, object]] = []
    if include_weekly:
        snapshots.append(
            {
                "date": "2026-09-13",
                "displayTotalBalance": weekly,
                "accountSnapshots": [
                    {
                        "accountType": "trading",
                        "displayTotal": weekly,
                    }
                ],
            }
        )
    if include_daily:
        snapshots.append(
            {
                "date": "2026-09-15",
                "displayTotalBalance": daily,
                "accountSnapshots": [
                    {
                        "accountType": "trading",
                        "displayTotal": daily,
                    }
                ],
            }
        )
    return {
        "displayCurrency": "USD",
        "fromDate": "2026-09-13",
        "toDate": "2026-09-15",
        "snapshots": snapshots,
    }


@pytest.mark.asyncio
async def test_rebuilds_daily_and_weekly_risk_from_eod_balances() -> None:
    queries: dict[str, dict[str, str]] = {}
    history = [
        _trade("1", "2026-09-16T06:00:00Z"),
        _trade("2", "2026-09-14T08:00:00Z"),
        _trade("3", "2026-09-16T06:10:00Z"),
        _trade("3", "2026-09-16T06:10:00Z"),
    ]
    open_positions = [_open_position("4", "2026-09-16T06:20:00Z")]

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v1/trading/info/trade/history":
            queries["history"] = dict(request.url.params)
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
        if request.url.path == "/api/v1/balances/history":
            queries["balances"] = dict(request.url.params)
            return httpx.Response(200, json=_balance_history())
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
    )

    assert queries["history"] == {
        "minDate": "2026-09-16",
        "page": "1",
        "pageSize": "100",
    }
    assert queries["balances"] == {
        "displayCurrency": "USD",
        "fromDate": "2026-09-13",
        "toDate": "2026-09-15",
        "accountTypes": "Trading",
    }
    assert result.timezone == "UTC"
    assert result.day_start == datetime(2026, 9, 16, 0, 0, tzinfo=UTC)
    assert result.week_start == datetime(2026, 9, 14, 0, 0, tzinfo=UTC)
    assert result.trades_today == 3
    assert result.current_equity_usd == 10015.0
    assert result.daily_start_equity_usd == 10010.0
    assert result.weekly_start_equity_usd == 10000.0
    assert result.daily_pnl_usd == 5.0
    assert result.weekly_pnl_usd == 15.0
    assert result.daily_pnl_pct == pytest.approx(0.04995005)
    assert result.weekly_pnl_pct == pytest.approx(0.15)
    assert result.history_rows == 3
    assert result.open_position_rows == 1


@pytest.mark.asyncio
async def test_open_position_carried_over_day_uses_balance_baseline() -> None:
    open_positions = [_open_position("7", "2026-09-15T12:00:00Z")]

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v1/trading/info/trade/history":
            return httpx.Response(200, json=[])
        if request.url.path == "/api/v1/trading/info/real/pnl":
            return httpx.Response(
                200,
                json={"clientPortfolio": {"positions": open_positions, "mirrors": []}},
            )
        if request.url.path == "/api/v1/balances/history":
            return httpx.Response(200, json=_balance_history(daily=10000.0, weekly=10050.0))
        raise AssertionError(f"unexpected path {request.url.path}")

    client = EtoroMarketDataClient(
        api_key="api",
        user_key="user",
        base_url="https://example.test/api/v1",
        transport=httpx.MockTransport(handler),
    )
    reader = EtoroRiskReader(client)
    broker_position = BrokerPosition(position_id="7", instrument_id=1)

    result = await reader.snapshot(
        _snapshot(broker_position, equity=9975.0),
        now=datetime(2026, 9, 16, 6, 45, tzinfo=UTC),
    )

    assert result.trades_today == 0
    assert result.daily_pnl_usd == -25.0
    assert result.daily_pnl_pct == pytest.approx(-0.25)
    assert result.weekly_pnl_usd == -75.0
    assert result.weekly_pnl_pct == pytest.approx((-75.0 / 10050.0) * 100.0)


@pytest.mark.asyncio
async def test_closed_trade_spanning_day_no_longer_corrupts_period_pnl() -> None:
    history = [_trade("8", "2026-09-15T12:00:00Z")]

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v1/trading/info/trade/history":
            return httpx.Response(200, json=history)
        if request.url.path == "/api/v1/trading/info/real/pnl":
            return httpx.Response(
                200,
                json={"clientPortfolio": {"positions": [], "mirrors": []}},
            )
        if request.url.path == "/api/v1/balances/history":
            return httpx.Response(200, json=_balance_history(daily=10000.0, weekly=10020.0))
        raise AssertionError(f"unexpected path {request.url.path}")

    client = EtoroMarketDataClient(
        api_key="api",
        user_key="user",
        base_url="https://example.test/api/v1",
        transport=httpx.MockTransport(handler),
    )
    reader = EtoroRiskReader(client)

    result = await reader.snapshot(
        _snapshot(equity=9970.0),
        now=datetime(2026, 9, 16, 6, 45, tzinfo=UTC),
    )

    assert result.trades_today == 0
    assert result.daily_pnl_usd == -30.0
    assert result.weekly_pnl_usd == -50.0


@pytest.mark.asyncio
async def test_malformed_trade_history_fails_closed() -> None:
    malformed = [{"positionId": "9", "closeTimestamp": "2026-09-16T06:00:00Z"}]

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

    with pytest.raises(ValueError, match="open timestamp"):
        await reader.snapshot(
            _snapshot(),
            now=datetime(2026, 9, 16, 6, 45, tzinfo=UTC),
        )


@pytest.mark.asyncio
async def test_missing_exact_daily_balance_fails_closed() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v1/trading/info/trade/history":
            return httpx.Response(200, json=[])
        if request.url.path == "/api/v1/trading/info/real/pnl":
            return httpx.Response(
                200,
                json={"clientPortfolio": {"positions": [], "mirrors": []}},
            )
        if request.url.path == "/api/v1/balances/history":
            return httpx.Response(200, json=_balance_history(include_daily=False))
        raise AssertionError(f"unexpected path {request.url.path}")

    client = EtoroMarketDataClient(
        api_key="api",
        user_key="user",
        base_url="https://example.test/api/v1",
        transport=httpx.MockTransport(handler),
    )
    reader = EtoroRiskReader(client)

    with pytest.raises(ValueError, match="missing exact daily EOD balance"):
        await reader.snapshot(
            _snapshot(),
            now=datetime(2026, 9, 16, 6, 45, tzinfo=UTC),
        )


@pytest.mark.asyncio
async def test_balance_scope_failure_is_actionable() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v1/trading/info/trade/history":
            return httpx.Response(200, json=[])
        if request.url.path == "/api/v1/trading/info/real/pnl":
            return httpx.Response(
                200,
                json={"clientPortfolio": {"positions": [], "mirrors": []}},
            )
        if request.url.path == "/api/v1/balances/history":
            return httpx.Response(403, text="Forbidden")
        raise AssertionError(f"unexpected path {request.url.path}")

    client = EtoroMarketDataClient(
        api_key="api",
        user_key="user",
        base_url="https://example.test/api/v1",
        transport=httpx.MockTransport(handler),
    )
    reader = EtoroRiskReader(client)

    with pytest.raises(ValueError, match="etoro-public:money.balance:read"):
        await reader.snapshot(
            _snapshot(),
            now=datetime(2026, 9, 16, 6, 45, tzinfo=UTC),
        )
