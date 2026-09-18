from datetime import UTC, datetime, timedelta

from parquet.autonomous_orchestrator import AutonomousOrchestrator
from parquet.config import EtoroConfig, ExecutionConfig, Settings
from parquet.execution.autonomous import ExecutionAttemptState
from parquet.models import (
    MarketAnalysis,
    MarketObservation,
    RiskSnapshot,
    Side,
    TradeProposal,
    TriggerAction,
)
from parquet.portfolio import ReconciliationReport, ReconciliationState


def _proposal(now: datetime, *, symbol: str = "NSDQ100") -> TradeProposal:
    return TradeProposal(
        proposal_id=f"proposal-{symbol}",
        symbol=symbol,
        side=Side.BUY,
        entry=100.0,
        stop_loss=99.0,
        take_profit=102.0,
        confidence=0.8,
        generated_at=now,
        expires_at=now + timedelta(minutes=5),
    )


def _analysis(now: datetime, proposal: TradeProposal) -> MarketAnalysis:
    return MarketAnalysis(
        analysis_id="analysis-auto-1",
        review_request_id="request-auto-1",
        generated_at=now,
        market_regime="test",
        summary="test proposal",
        trade_proposals=[proposal],
        watch=[],
        sources=[],
        next_review=None,
    )


def _orchestrator(tmp_path) -> AutonomousOrchestrator:
    settings = Settings(
        state_db=tmp_path / "state.db",
        etoro=EtoroConfig(enabled=False),
        execution=ExecutionConfig(
            autonomous_enabled=True,
            autonomous_mode="shadow",
        ),
    )
    return AutonomousOrchestrator(settings)


def test_direct_proposal_is_armed_and_executes_in_shadow(tmp_path) -> None:
    now = datetime(2026, 9, 18, 12, 0, tzinfo=UTC)
    orchestrator = _orchestrator(tmp_path)
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
            equity_usd=1_000.0,
            open_positions=0,
            trades_today=0,
            daily_pnl_pct=0.0,
            weekly_pnl_pct=0.0,
        )
    )

    proposal = _proposal(now)
    orchestrator.process_analysis(_analysis(now, proposal))

    watches = orchestrator.storage.active_watches(now)
    assert len(watches) == 1
    assert watches[0].proposal_id == proposal.proposal_id
    assert watches[0].on_trigger == TriggerAction.EXECUTE

    orchestrator.process_observation(
        MarketObservation(
            symbol="NSDQ100",
            price=100.0,
            observed_at=now + timedelta(seconds=1),
            instrument_id=32,
            bid=99.99,
            ask=100.01,
        )
    )

    attempts = orchestrator.storage.latest_execution_attempts()
    assert len(attempts) == 1
    assert attempts[0].state == ExecutionAttemptState.SHADOW_EXECUTED


def test_airbus_proposal_is_never_armed(tmp_path) -> None:
    now = datetime(2026, 9, 18, 12, 0, tzinfo=UTC)
    orchestrator = _orchestrator(tmp_path)

    orchestrator.process_analysis(_analysis(now, _proposal(now, symbol="AIR.PA")))

    assert orchestrator.storage.active_watches(now) == []
