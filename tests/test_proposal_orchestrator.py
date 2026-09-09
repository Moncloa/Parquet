from datetime import UTC, datetime, timedelta

import pytest

from parquet.config import EtoroConfig, ExecutionConfig, Settings
from parquet.execution.autonomous import ExecutionAttemptState
from parquet.market.etoro import InstrumentRate
from parquet.models import RiskSnapshot, Side, TradeProposal
from parquet.portfolio import ReconciliationReport, ReconciliationState
from parquet.proposal_orchestrator import AutonomousOrchestrator, _latest_active_proposals


class FakeMarketClient:
    def __init__(self, rate: InstrumentRate) -> None:
        self.rate = rate

    async def rates(self, instrument_ids: list[int]) -> list[InstrumentRate]:
        assert instrument_ids == [self.rate.instrument_id]
        return [self.rate]

    async def search(self, query: str):  # type: ignore[no-untyped-def]
        raise AssertionError(f"configured instrument id should avoid search: {query}")


def _settings(tmp_path) -> Settings:  # type: ignore[no-untyped-def]
    return Settings(
        state_db=tmp_path / "state.db",
        etoro=EtoroConfig(
            enabled=False,
            instrument_ids={"NSDQ100": 32},
        ),
        execution=ExecutionConfig(
            autonomous_enabled=True,
            autonomous_mode="shadow",
        ),
    )


def _proposal(now: datetime, *, expires_delta: timedelta = timedelta(minutes=5)) -> TradeProposal:
    return TradeProposal(
        proposal_id="direct-p1",
        symbol="NSDQ100",
        side=Side.BUY,
        entry=100.01,
        stop_loss=99.0,
        take_profit=102.0,
        confidence=0.8,
        generated_at=now,
        expires_at=now + expires_delta,
    )


def _prime_runtime(orchestrator: AutonomousOrchestrator, now: datetime) -> None:
    orchestrator.storage.set_reconciliation_report(
        ReconciliationReport(
            as_of=now,
            state=ReconciliationState.SYNCED,
            trading_enabled=True,
        )
    )
    orchestrator.storage.set_risk_snapshot(
        RiskSnapshot(
            as_of=now,
            equity_usd=10_000.0,
            open_positions=0,
            trades_today=0,
            daily_pnl_pct=0.0,
            weekly_pnl_pct=0.0,
        )
    )


@pytest.mark.asyncio
async def test_immediate_proposal_is_shadow_executed_automatically(tmp_path) -> None:
    now = datetime(2026, 9, 9, 14, 22, tzinfo=UTC)
    market = FakeMarketClient(
        InstrumentRate(
            instrument_id=32,
            symbol="NSDQ100",
            bid=100.00,
            ask=100.01,
            last_price=100.00,
            timestamp=now,
        )
    )
    orchestrator = AutonomousOrchestrator(
        _settings(tmp_path),
        market_client=market,  # type: ignore[arg-type]
    )
    _prime_runtime(orchestrator, now)
    proposal = _proposal(now)
    orchestrator.storage.save_proposal("analysis-latest", proposal)
    orchestrator.storage.set("latest_analysis_id", "analysis-latest")

    assert await orchestrator.poll_active_proposals_once(now) == 1

    attempts = orchestrator.storage.latest_execution_attempts()
    assert len(attempts) == 1
    assert attempts[0].proposal_id == proposal.proposal_id
    assert attempts[0].state == ExecutionAttemptState.SHADOW_EXECUTED

    # The durable attempt makes repeated polling idempotent.
    assert await orchestrator.poll_active_proposals_once(now + timedelta(seconds=5)) == 0
    assert len(orchestrator.storage.latest_execution_attempts()) == 1


def test_only_latest_nonexpired_analysis_proposals_are_consumed(tmp_path) -> None:
    now = datetime(2026, 9, 9, 14, 22, tzinfo=UTC)
    orchestrator = AutonomousOrchestrator(_settings(tmp_path), market_client=None)
    old = _proposal(now).model_copy(update={"proposal_id": "old"})
    expired = _proposal(now, expires_delta=timedelta(seconds=-1)).model_copy(
        update={"proposal_id": "expired"}
    )
    current = _proposal(now).model_copy(update={"proposal_id": "current"})
    orchestrator.storage.save_proposal("analysis-old", old)
    orchestrator.storage.save_proposal("analysis-latest", expired)
    orchestrator.storage.save_proposal("analysis-latest", current)
    orchestrator.storage.set("latest_analysis_id", "analysis-latest")

    proposals = _latest_active_proposals(orchestrator.storage, now)
    assert [item.proposal_id for item in proposals] == ["current"]
