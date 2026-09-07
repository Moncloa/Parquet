from __future__ import annotations

from datetime import UTC, datetime, timedelta

import httpx
import pytest

from parquet.config import EtoroConfig, ExecutionConfig, Settings
from parquet.execution.autonomous import ExecutionAttemptState
from parquet.execution.etoro import EtoroEligibilityResult, EtoroExecutionClient
from parquet.live_test import run_live_test
from parquet.market.etoro import InstrumentRate, InstrumentSearchHit
from parquet.models import Side, TradeProposal
from parquet.storage import Storage
from parquet.tickets import choose_real_small_amount, prepare_real_small_ticket


def _eligibility(*, minimum: float = 10.0) -> EtoroEligibilityResult:
    return EtoroEligibilityResult(
        request_id="eligibility-1",
        instrument_id=100,
        symbol="TEST",
        min_position_exposure=minimum,
        allow_open_position=True,
        leverage_configs=(
            {
                "settlementType": "CFD",
                "direction": "LONG",
                "leverageValues": [1, 2],
                "minPositionAmount": minimum,
            },
        ),
        response={},
    )


def test_choose_real_small_amount_defaults_to_broker_minimum() -> None:
    amount, maximum = choose_real_small_amount(
        gate_maximum_usd=2_000.0,
        supervised_cap_usd=25.0,
        broker_minimum_usd=10.0,
    )
    assert amount == 10.0
    assert maximum == 25.0


def test_choose_real_small_amount_rejects_broker_minimum_above_cap() -> None:
    with pytest.raises(RuntimeError, match="eToro minimum 50.00 USD exceeds"):
        choose_real_small_amount(
            gate_maximum_usd=2_000.0,
            supervised_cap_usd=25.0,
            broker_minimum_usd=50.0,
        )


def test_eligibility_requires_direction_and_leverage() -> None:
    eligibility = _eligibility(minimum=10.0)
    assert eligibility.minimum_amount(direction="LONG", leverage=1) == 10.0
    with pytest.raises(RuntimeError, match="no SHORT configuration"):
        eligibility.minimum_amount(direction="SHORT", leverage=1)


@pytest.mark.asyncio
async def test_eligibility_client_uses_v2_info_endpoint() -> None:
    captured: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["path"] = request.url.path
        captured["body"] = request.read().decode()
        return httpx.Response(
            200,
            json={
                "currency": "USD",
                "eligibilities": [
                    {
                        "instrumentId": 100,
                        "symbol": "TEST",
                        "minPositionExposure": 10,
                        "allowOpenPosition": True,
                        "leverageConfigs": [
                            {
                                "direction": "LONG",
                                "leverageValues": [1],
                                "minPositionAmount": 10,
                            }
                        ],
                    }
                ],
                "notFoundInstrumentIds": [],
            },
        )

    client = EtoroExecutionClient(
        api_key="api",
        user_key="user",
        transport=httpx.MockTransport(handler),
    )
    result = await client.instrument_eligibility(instrument_id=100)

    assert captured["path"] == "/api/v2/trading/info/eligibility"
    assert '"instrumentIds":[100]' in str(captured["body"]).replace(" ", "")
    assert result.allow_open_position is True
    assert result.minimum_amount(direction="LONG", leverage=1) == 10.0


@pytest.mark.asyncio
async def test_pinned_agent_portfolio_disables_legacy_live_test_write() -> None:
    settings = Settings(
        etoro=EtoroConfig(enabled=True, expected_gcid=123),
        execution=ExecutionConfig(live_test_enabled=True),
    )

    with pytest.raises(RuntimeError, match="Legacy live-test real submission is disabled"):
        await run_live_test(
            settings,
            symbol="BTC",
            amount_usd=10.0,
            stop_loss_rate=77_000.0,
            take_profit_rate=None,
            confirmation="REAL-MONEY",
            dry_run=False,
        )


@pytest.mark.asyncio
async def test_prepare_real_small_uses_fresh_gate_and_broker_minimum(
    tmp_path,
    monkeypatch,
) -> None:
    api_key_file = tmp_path / "api"
    user_key_file = tmp_path / "user"
    api_key_file.write_text("api")
    user_key_file.write_text("user")

    class FakeMarketClient:
        def __init__(self, **kwargs) -> None:
            pass

        async def _get(self, path: str, *, params: dict[str, str]):
            if path == "/me":
                return {
                    "gcid": 123,
                    "realCid": 456,
                    "demoCid": 789,
                    "scopes": [
                        "etoro-public:real:read",
                        "etoro-public:real:write",
                        "etoro-public:trade.real:read",
                        "etoro-public:trade.real:write",
                    ],
                }
            if path == "/trading/info/real/pnl":
                return {
                    "clientPortfolio": {
                        "credit": 10_000,
                        "positions": [],
                        "mirrors": [],
                        "orders": [],
                        "ordersForOpen": [],
                    }
                }
            raise AssertionError(path)

        async def search(self, query: str):
            return [InstrumentSearchHit(instrument_id=100, symbol="TEST", name="Test")]

        async def rates(self, instrument_ids: list[int]):
            return [
                InstrumentRate(
                    instrument_id=100,
                    symbol="TEST",
                    bid=99.95,
                    ask=100.05,
                    last_price=100.0,
                    timestamp=datetime.now(UTC),
                )
            ]

    class FakeExecutionClient:
        def __init__(self, **kwargs) -> None:
            pass

        async def instrument_eligibility(self, *, instrument_id: int):
            assert instrument_id == 100
            return _eligibility(minimum=10.0)

    monkeypatch.setattr("parquet.orchestrator.EtoroMarketDataClient", FakeMarketClient)
    monkeypatch.setattr("parquet.tickets.EtoroExecutionClient", FakeExecutionClient)

    settings = Settings(
        state_db=tmp_path / "state.db",
        etoro=EtoroConfig(
            enabled=True,
            expected_gcid=123,
            api_key_file=api_key_file,
            user_key_file=user_key_file,
        ),
        execution=ExecutionConfig(supervised_real_max_amount_usd=25.0),
    )
    now = datetime.now(UTC)
    proposal = TradeProposal(
        proposal_id="proposal-1",
        symbol="TEST",
        side=Side.BUY,
        entry=100.0,
        stop_loss=95.0,
        take_profit=110.0,
        confidence=0.8,
        generated_at=now,
        expires_at=now + timedelta(minutes=10),
    )
    Storage(settings.state_db).save_proposal("analysis-1", proposal)

    ticket = await prepare_real_small_ticket(settings, proposal_id="proposal-1")

    assert ticket.attempt.state == ExecutionAttemptState.PREPARED
    assert ticket.attempt.amount_usd == 10.0
    assert ticket.broker_minimum_usd == 10.0
    assert ticket.maximum_safe_amount_usd == 25.0
    assert ticket.attempt.broker_request_id is None
