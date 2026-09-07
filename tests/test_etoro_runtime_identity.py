from __future__ import annotations

import json

import httpx
import pytest

from parquet.config import EtoroConfig, Settings
from parquet.market.etoro import EtoroMarketDataClient
from parquet.portfolio import ReconciliationState
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


@pytest.mark.asyncio
async def test_reconciliation_persists_verified_agent_identity(tmp_path) -> None:
    paths: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        paths.append(request.url.path)
        if request.url.path == "/api/v1/me":
            return httpx.Response(200, json=_identity_response(123))
        if request.url.path == "/api/v1/trading/info/real/pnl":
            return httpx.Response(200, json=_portfolio_response())
        raise AssertionError(f"Unexpected path: {request.url.path}")

    service, storage = _service(tmp_path, handler)

    assert await service.poll_once(force=True) == 1
    assert paths[:2] == ["/api/v1/me", "/api/v1/trading/info/real/pnl"]
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
    report = storage.get_reconciliation_report()
    assert report is not None
    assert report.state == ReconciliationState.SYNCED
    assert report.trading_enabled is True


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
