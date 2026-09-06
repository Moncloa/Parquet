from datetime import UTC, datetime, timedelta

from parquet.config import RiskConfig
from parquet.models import RiskSnapshot, Side, TradeProposal
from parquet.risk import RiskEngine


def proposal(*, stop: float | None = 99, generated_minutes_ago: int = 0) -> TradeProposal:
    now = datetime.now(UTC)
    return TradeProposal(
        proposal_id="p-1",
        symbol="TEST",
        side=Side.BUY,
        entry=100,
        stop_loss=stop,
        take_profit=102,
        confidence=0.8,
        generated_at=now - timedelta(minutes=generated_minutes_ago),
        expires_at=now + timedelta(minutes=5),
    )


def test_rejects_missing_stop_loss() -> None:
    decision = RiskEngine(RiskConfig()).evaluate(proposal(stop=None), RiskSnapshot())
    assert not decision.accepted
    assert "stop_loss_required" in decision.reasons


def test_rejects_daily_loss_limit() -> None:
    decision = RiskEngine(RiskConfig()).evaluate(
        proposal(), RiskSnapshot(daily_pnl_pct=-3.0)
    )
    assert not decision.accepted
    assert "max_daily_loss" in decision.reasons


def test_rejects_stale_signal() -> None:
    decision = RiskEngine(RiskConfig(max_signal_age_minutes=15)).evaluate(
        proposal(generated_minutes_ago=16),
        RiskSnapshot(),
    )
    assert not decision.accepted
    assert "signal_too_old" in decision.reasons


def test_accepts_valid_proposal() -> None:
    decision = RiskEngine(RiskConfig()).evaluate(proposal(), RiskSnapshot())
    assert decision.accepted
