import json
from datetime import UTC, datetime, timedelta

import httpx
import pytest

from parquet.execution.etoro import (
    EtoroCostComponent,
    EtoroCostResult,
    EtoroExecutionClient,
)
from parquet.tickets import validate_what_if_costs


@pytest.mark.asyncio
async def test_what_if_open_costs_uses_v2_cost_endpoint() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/v2/trading/info/costs"
        payload = json.loads(request.content)
        assert payload["action"] == "open"
        assert payload["transaction"] == "sellShort"
        assert payload["instrumentId"] == 27
        assert payload["settlementType"] == "cfd"
        assert payload["amount"] == 25
        assert payload["leverage"] == 1
        assert payload["stopLossRate"] == 7800
        return httpx.Response(
            200,
            json={
                "instrumentId": 27,
                "symbol": "SPX500",
                "costs": [
                    {"costType": "marketSpread", "value": 0.08, "currency": "USD"},
                    {"costType": "overnightFee", "value": 0.02, "currency": "USD"},
                ],
                "lastUpdated": "2026-09-07T20:00:00Z",
            },
        )

    client = EtoroExecutionClient(
        api_key="a",
        user_key="u",
        transport=httpx.MockTransport(handler),
    )
    result = await client.what_if_open_costs(
        transaction="short",
        instrument_id=27,
        settlement_type="cfd",
        amount_usd=25,
        stop_loss_rate=7800,
        leverage=1,
    )
    assert result.total_usd == pytest.approx(0.10)


@pytest.mark.asyncio
async def test_what_if_open_costs_accepts_live_ger40_shape_and_mixed_currency() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/v2/trading/info/costs"
        return httpx.Response(
            200,
            json={
                "instrumentId": 32,
                "symbol": "GER40",
                "costs": [
                    {"costType": "transactionFee", "currency": "USD", "value": 0.08},
                    {"costType": "markup", "currency": "USD", "value": 0.0},
                    {"costType": "marketSpread", "currency": "USD", "value": 0.1},
                    {"costType": "overnightFee", "currency": "EUR", "value": 5.7052293},
                ],
                "lastUpdated": "2026-09-07T19:59:59.0982637Z",
            },
        )

    client = EtoroExecutionClient(
        api_key="a",
        user_key="u",
        transport=httpx.MockTransport(handler),
    )
    result = await client.what_if_open_costs(
        transaction="buy",
        instrument_id=32,
        settlement_type="cfd",
        amount_usd=1000,
        leverage=1,
    )

    assert [cost.cost_type for cost in result.costs] == [
        "transactionFee",
        "markup",
        "marketSpread",
        "overnightFee",
    ]
    assert result.total_usd == pytest.approx(0.18)
    assert result.totals_by_currency == pytest.approx({"USD": 0.18, "EUR": 5.7052293})

    validate_what_if_costs(
        result,
        instrument_id=32,
        amount_usd=1000,
        now=datetime(2026, 9, 7, 20, 0, 30, tzinfo=UTC),
    )


@pytest.mark.asyncio
async def test_what_if_open_costs_accepts_legacy_amount_field() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "instrumentId": 27,
                "symbol": "SPX500",
                "costs": [
                    {"costType": "marketSpread", "amount": 0.08, "currency": "USD"},
                ],
                "lastUpdated": "2026-09-07T20:00:00Z",
            },
        )

    client = EtoroExecutionClient(
        api_key="a",
        user_key="u",
        transport=httpx.MockTransport(handler),
    )
    result = await client.what_if_open_costs(
        transaction="short",
        instrument_id=27,
        settlement_type="cfd",
        amount_usd=25,
        leverage=1,
    )
    assert result.total_usd == pytest.approx(0.08)


@pytest.mark.asyncio
async def test_what_if_open_costs_can_omit_optional_stop_for_long_x1() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        payload = json.loads(request.content)
        assert payload["transaction"] == "buy"
        assert "stopLossRate" not in payload
        assert "stopLossType" not in payload
        return httpx.Response(
            200,
            json={
                "instrumentId": 101,
                "symbol": "AAPL",
                "costs": [],
                "lastUpdated": "2026-09-07T20:00:00Z",
            },
        )

    client = EtoroExecutionClient(
        api_key="a",
        user_key="u",
        transport=httpx.MockTransport(handler),
    )
    result = await client.what_if_open_costs(
        transaction="buy",
        instrument_id=101,
        settlement_type="real",
        amount_usd=50,
        leverage=1,
    )
    assert result.costs == ()


def test_validate_what_if_costs_rejects_stale_response() -> None:
    now = datetime(2026, 9, 7, 20, 5, tzinfo=UTC)
    costs = EtoroCostResult(
        request_id="r",
        instrument_id=27,
        symbol="SPX500",
        costs=(EtoroCostComponent("marketSpread", 0.1, "USD"),),
        last_updated=now - timedelta(minutes=3),
        response={},
    )
    with pytest.raises(RuntimeError, match="stale"):
        validate_what_if_costs(costs, instrument_id=27, amount_usd=25, now=now)


def test_validate_what_if_costs_keeps_non_usd_components_separate() -> None:
    now = datetime(2026, 9, 7, 20, 5, tzinfo=UTC)
    costs = EtoroCostResult(
        request_id="r",
        instrument_id=27,
        symbol="SPX500",
        costs=(
            EtoroCostComponent("marketSpread", 0.1, "USD"),
            EtoroCostComponent("overnightFee", 0.2, "EUR"),
        ),
        last_updated=now,
        response={},
    )

    validate_what_if_costs(costs, instrument_id=27, amount_usd=25, now=now)
    assert costs.total_usd == pytest.approx(0.1)
    assert costs.totals_by_currency == pytest.approx({"USD": 0.1, "EUR": 0.2})