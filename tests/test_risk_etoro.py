from __future__ import annotations

from datetime import UTC, datetime, timedelta

import httpx
import pytest

from parquet.market.etoro import EtoroMarketDataClient
from parquet.portfolio import BrokerPortfolioSnapshot, BrokerPosition
from parquet.risk_etoro import EtoroRiskReader
from parquet.risk_ledger import LocalEquityRiskLedger
from parquet.storage import Storage


NOW = datetime(2026, 9, 16, 6, 45, tzinfo=UTC)
DAY_START = datetime(2026, 9, 16, 0, 0, tzinfo=UTC)
WEEK_START = datetime(2026, 9, 14, 0, 0, tzinfo=UTC)


def _snapshot(
    *positions: BrokerPosition,
    equity: float = 10015.0,
    captured_at: datetime = NOW,
) -> BrokerPortfolioSnapshot:
    return BrokerPortfolioSnapshot(
        captured_at=captured_at,
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


def _seed_flat_boundary(
    ledger: LocalEquityRiskLedger,
    boundary: datetime,
    equity: float,
) -> None:
    ledger.record(_snapshot(equity=equity, captured_at=boundary - timedelta(seconds=30)))
    ledger.record(_snapshot(equity=equity, captured_at=boundary + timedelta(seconds=30)))


@pytest.mark.asyncio
async def test_rebuilds_daily_and_weekly_risk_from_local_equity_ledger(tmp_path) -> None:
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
                json={"clientPortfolio": {"positions": open_positions, "mirrors": []}},
            )
        raise AssertionError(f"unexpected path {request.url.path}")

    storage = Storage(tmp_path / "state.db")
    client = EtoroMarketDataClient(
        api_key="api",
        user_key="user",
        base_url="https://example.test/api/v1",
        transport=httpx.MockTransport(handler),
    )
    reader = EtoroRiskReader(client, storage)
    _seed_flat_boundary(reader.ledger, WEEK_START, 10000.0)
    _seed_flat_boundary(reader.ledger, DAY_START, 10010.0)

    broker_position = BrokerPosition(
        position_id="4",
        instrument_id=1,
        unrealized_pnl_usd=-5.0,
    )
    current = _snapshot(broker_position)
    reader.record_equity(current)

    result = await reader.snapshot(current, now=NOW)

    assert queries["history"] == {
        "minDate": "2026-09-16",
        "page": "1",
        "pageSize": "100",
    }
    assert result.timezone == "UTC"
    assert result.day_start == DAY_START
    assert result.week_start == WEEK_START
    assert result.trades_today == 3
    assert result.current_equity_usd == 10015.0
    assert result.daily_start_equity_usd == 10010.0
    assert result.weekly_start_equity_usd == 10000.0
    assert result.daily_pnl_usd == 5.0
    assert result.weekly_pnl_usd == 15.0
    assert result.daily_pnl_pct == pytest.approx(0.04995005)
    assert result.weekly_pnl_pct == pytest.approx(0.15)
    assert result.source == "etoro_trade_history+local_equity_ledger+real_pnl"


@pytest.mark.asyncio
async def test_missing_local_boundary_baseline_fails_closed(tmp_path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v1/trading/info/trade/history":
            return httpx.Response(200, json=[])
        if request.url.path == "/api/v1/trading/info/real/pnl":
            return httpx.Response(200, json={"clientPortfolio": {"positions": [], "mirrors": []}})
        raise AssertionError(f"unexpected path {request.url.path}")

    storage = Storage(tmp_path / "state.db")
    client = EtoroMarketDataClient(
        api_key="api",
        user_key="user",
        base_url="https://example.test/api/v1",
        transport=httpx.MockTransport(handler),
    )
    reader = EtoroRiskReader(client, storage)
    reader.record_equity(_snapshot())

    with pytest.raises(ValueError, match="missing local daily equity baseline"):
        await reader.snapshot(_snapshot(), now=NOW)


@pytest.mark.asyncio
async def test_open_position_across_boundary_fails_closed(tmp_path) -> None:
    position = BrokerPosition(position_id="7", instrument_id=1)
    open_positions = [_open_position("7", "2026-09-15T12:00:00Z")]

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v1/trading/info/trade/history":
            return httpx.Response(200, json=[])
        if request.url.path == "/api/v1/trading/info/real/pnl":
            return httpx.Response(200, json={"clientPortfolio": {"positions": open_positions, "mirrors": []}})
        raise AssertionError(f"unexpected path {request.url.path}")

    storage = Storage(tmp_path / "state.db")
    client = EtoroMarketDataClient(
        api_key="api",
        user_key="user",
        base_url="https://example.test/api/v1",
        transport=httpx.MockTransport(handler),
    )
    reader = EtoroRiskReader(client, storage)
    _seed_flat_boundary(reader.ledger, WEEK_START, 10000.0)
    reader.ledger.record(
        _snapshot(position, equity=10000.0, captured_at=DAY_START - timedelta(seconds=30))
    )
    reader.ledger.record(
        _snapshot(position, equity=9999.0, captured_at=DAY_START + timedelta(seconds=30))
    )
    current = _snapshot(position, equity=9975.0)
    reader.record_equity(current)

    with pytest.raises(ValueError, match="portfolio was not flat across boundary"):
        await reader.snapshot(current, now=NOW)


def test_boundary_equity_change_fails_closed(tmp_path) -> None:
    storage = Storage(tmp_path / "state.db")
    ledger = LocalEquityRiskLedger(storage)
    ledger.record(_snapshot(equity=10000.0, captured_at=DAY_START - timedelta(seconds=30)))
    ledger.record(_snapshot(equity=9999.0, captured_at=DAY_START + timedelta(seconds=30)))

    with pytest.raises(ValueError, match="equity changed across boundary"):
        ledger.baseline(DAY_START, label="daily")


def test_boundary_snapshots_too_far_away_fail_closed(tmp_path) -> None:
    storage = Storage(tmp_path / "state.db")
    ledger = LocalEquityRiskLedger(storage, boundary_tolerance_seconds=120.0)
    ledger.record(_snapshot(equity=10000.0, captured_at=DAY_START - timedelta(minutes=5)))
    ledger.record(_snapshot(equity=10000.0, captured_at=DAY_START + timedelta(minutes=5)))

    with pytest.raises(ValueError, match="too far from boundary"):
        ledger.baseline(DAY_START, label="daily")


@pytest.mark.asyncio
async def test_malformed_trade_history_fails_closed(tmp_path) -> None:
    malformed = [{"positionId": "9", "closeTimestamp": "2026-09-16T06:00:00Z"}]

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v1/trading/info/trade/history":
            return httpx.Response(200, json=malformed)
        raise AssertionError(f"unexpected path {request.url.path}")

    storage = Storage(tmp_path / "state.db")
    client = EtoroMarketDataClient(
        api_key="api",
        user_key="user",
        base_url="https://example.test/api/v1",
        transport=httpx.MockTransport(handler),
    )
    reader = EtoroRiskReader(client, storage)
    _seed_flat_boundary(reader.ledger, WEEK_START, 10000.0)
    _seed_flat_boundary(reader.ledger, DAY_START, 10000.0)

    with pytest.raises(ValueError, match="open timestamp"):
        await reader.snapshot(_snapshot(), now=NOW)
