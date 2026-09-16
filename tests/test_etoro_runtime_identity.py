from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import httpx
import pytest

from parquet.config import EtoroConfig, Settings
from parquet.market.etoro import EtoroMarketDataClient
from parquet.portfolio import BrokerPortfolioSnapshot, ReconciliationState
from parquet.reconciliation import ReconciliationService
from parquet.storage import Storage


def _portfolio_response() -> dict[str, object]:
    return {
        "clientPortfolio": {
            "credit": 1000.0,
            "positions": [],
            "mirrors": [],
            "orders": [],
            "ordersForOpen": [],
            "unrealizedPnL": 0.0,
        }
    }


def _identity_response(gcid: int, *, scopes: list[str] | None = None) -> dict[str, object]:
    return {
        "gcid": gcid,
        "realCid": 456,
        "demoCid": 789,
        "scopes": scopes
        or [
            "etoro-public:real:read",
            "etoro-public:real:write",
            "etoro-public:trade.real:read",
            "etoro-public:trade.real:write",
        ],
    }


def _service(tmp_path, handler, *, expected_gcid: int = 123):
    storage = Storage(tmp_path / "state.db")
    client = EtoroMarketDataClient(
        api_key="api",
        user_key="user",
        transport=httpx.MockTransport(handler),
    )
    settings = Settings(
        etoro=EtoroConfig(
            enabled=True,
            expected_gcid=expected_gcid,
        )
    )
    return ReconciliationService(settings, storage, client), storage


def _flat_snapshot(at: datetime, *, equity: float = 1000.0) -> BrokerPortfolioSnapshot:
    return BrokerPortfolioSnapshot(
        captured_at=at,
        equity_usd=equity,
        available_cash_usd=equity,
        invested_usd=0.0,
        unrealized_pnl_usd=0.0,
        credit_usd=equity,
        positions=[],
    )


def _seed_boundaries(service: ReconciliationService, now: datetime) -> None:
    assert service.risk_reader is not None
    day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    week_start = day_start - timedelta(days=day_start.weekday())
    for boundary in {day_start, week_start}:
        service.risk_reader.record_equity(_flat_snapshot(boundary - timedelta(seconds=30)))
        service.risk_reader.record_equity(_flat_snapshot(boundary + timedelta(seconds=30)))


@pytest.mark.asyncio
async def test_reconciliation_persists_verified_agent_identity(tmp_path) -> None:
    paths: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        paths.append(request.url.path)
        if request.url.path == "/api/v1/me":
            return httpx.Response(200, json=_identity_response(123))
        if request.url.path == "/api/v1/trading/info/real/pnl":
            return httpx.Response(200, json=_portfolio_response())
        if request.url.path == "/api/v1/trading/info/trade/history":
            return httpx.Response(200, json=[])
        raise AssertionError(f"Unexpected path: {request.url.path}")

    service, storage = _service(tmp_path, handler)
    now = datetime.now(UTC)
    _seed_boundaries(service, now)

    assert await service.poll_once(now=now, force=True) == 1
    assert paths == [
        "/api/v1/me",
        "/api/v1/trading/info/real/pnl",
        "/api/v1/trading/info/trade/history",
        "/api/v1/trading/info/real/pnl",
    ]
    assert storage.get("etoro_authenticated_gcid") == "123"
    assert storage.get("etoro_authenticated_real_cid") == "456"
    assert storage.get("etoro_authenticated_demo_cid") == "789"
    assert storage.get("etoro_identity_verified") == "1"
    assert storage.get("etoro_identity_checked_at")
    assert storage.get("etoro_identity_error") == ""
    assert json.loads(storage.get("etoro_authenticated_scopes") or "[]") == [
        "etoro-public:real:read",
        "etoro-public:real:write",
        "etoro-public:trade.real:read",
        "etoro-public:trade.real:write",
    ]
    risk = storage.get_risk_snapshot()
    assert risk is not None
    assert risk.trades_today == 0
    assert risk.daily_pnl_pct == 0.0
    assert risk.weekly_pnl_pct == 0.0
    components = json.loads(storage.get("account_snapshot_components") or "{}")
    assert components["risk_source"] == "etoro_trade_history+local_equity_ledger+real_pnl"
    assert components["risk_timezone"] == "UTC"
    report = storage.get_reconciliation_report()
    assert report is not None
    assert report.state == ReconciliationState.SYNCED
    assert report.trading_enabled is True


@pytest.mark.asyncio
async def test_risk_reconstruction_failure_blocks_trading(tmp_path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v1/me":
            return httpx.Response(200, json=_identity_response(123))
        if request.url.path == "/api/v1/trading/info/real/pnl":
            return httpx.Response(200, json=_portfolio_response())
        if request.url.path == "/api/v1/trading/info/trade/history":
            return httpx.Response(200, json={"unexpected": "shape"})
        raise AssertionError(f"Unexpected path: {request.url.path}")

    service, storage = _service(tmp_path, handler)

    assert await service.poll_once(force=True) == 0
    report = storage.get_reconciliation_report()
    assert report is not None
    assert report.state == ReconciliationState.ERROR
    assert report.trading_enabled is False
    assert "broker risk reconstruction failed" in (storage.get("broker_risk_last_error") or "")


@pytest.mark.asyncio
async def test_wrong_agent_gcid_blocks_reconciliation_before_portfolio_read(tmp_path) -> None:
    paths: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        paths.append(request.url.path)
        if request.url.path == "/api/v1/me":
            return httpx.Response(200, json=_identity_response(999))
        raise AssertionError("Portfolio must not be read after identity mismatch")

    service, storage = _service(tmp_path, handler)

    assert await service.poll_once(force=True) == 0
    assert paths == ["/api/v1/me"]
    assert storage.get("etoro_authenticated_gcid") == "999"
    assert storage.get("etoro_identity_verified") == "0"
    assert "does not match pinned Agent Portfolio GCID 123" in (
        storage.get("etoro_identity_error") or ""
    )
    report = storage.get_reconciliation_report()
    assert report is not None
    assert report.state == ReconciliationState.ERROR
    assert report.trading_enabled is False


@pytest.mark.asyncio
async def test_missing_real_scope_blocks_reconciliation(tmp_path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v1/me":
            return httpx.Response(
                200,
                json=_identity_response(
                    123,
                    scopes=[
                        "etoro-public:real:read",
                        "etoro-public:real:write",
                        "etoro-public:trade.real:read",
                    ],
                ),
            )
        raise AssertionError("Portfolio must not be read after scope validation failure")

    service, storage = _service(tmp_path, handler)

    assert await service.poll_once(force=True) == 0
    assert storage.get("etoro_identity_verified") == "0"
    assert "etoro-public:trade.real:write" in (storage.get("etoro_identity_error") or "")
    report = storage.get_reconciliation_report()
    assert report is not None
    assert report.state == ReconciliationState.ERROR
    assert report.trading_enabled is False
