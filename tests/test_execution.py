from datetime import UTC, datetime, timedelta

from parquet.config import EtoroConfig, RiskConfig
from parquet.execution import ExecutionGate
from parquet.models import MarketObservation, RiskSnapshot, Side, TradeProposal


def _proposal(now: datetime, *, side: Side = Side.BUY) -> TradeProposal:
    return TradeProposal(
        proposal_id="p-1",
        symbol="GER40",
        side=side,
        entry=100.0,
        stop_loss=99.0 if side == Side.BUY else 101.0,
        take_profit=102.0 if side == Side.BUY else 98.0,
        confidence=0.8,
        generated_at=now,
        expires_at=now + timedelta(minutes=10),
    )


def _snapshot(now: datetime) -> RiskSnapshot:
    return RiskSnapshot(
        as_of=now,
        equity_usd=10_000.0,
        open_positions=0,
        trades_today=0,
        daily_pnl_pct=0.0,
        weekly_pnl_pct=0.0,
    )


def _observation(now: datetime, *, bid: float = 100.0, ask: float = 100.01) -> MarketObservation:
    return MarketObservation(
        symbol="GER40",
        price=bid,
        bid=bid,
        ask=ask,
        observed_at=now,
    )


def test_execution_gate_approves_fresh_quote_and_sizes_from_stop() -> None:
    now = datetime(2026, 9, 7, 10, 0, tzinfo=UTC)
    gate = ExecutionGate(
        RiskConfig(max_position_notional_pct=100.0),
        EtoroConfig(max_quote_age_seconds=120),
    )

    decision = gate.evaluate(_proposal(now), _snapshot(now), _observation(now), now=now)

    assert decision.approved
    assert decision.risk_budget_usd == 100.0
    assert decision.amount_usd == 10_000.0


def test_execution_gate_rejects_stale_risk_snapshot() -> None:
    now = datetime(2026, 9, 7, 10, 0, tzinfo=UTC)
    snapshot = _snapshot(now - timedelta(minutes=3))
    gate = ExecutionGate(
        RiskConfig(max_risk_snapshot_age_seconds=120),
        EtoroConfig(),
    )

    decision = gate.evaluate(_proposal(now), snapshot, _observation(now), now=now)

    assert not decision.approved
    assert "risk_snapshot_stale" in decision.reasons


def test_execution_gate_rejects_duplicate_symbol_position() -> None:
    now = datetime(2026, 9, 7, 10, 0, tzinfo=UTC)
    snapshot = _snapshot(now).model_copy(update={"open_symbols": ["GER40"]})
    gate = ExecutionGate(RiskConfig(), EtoroConfig())

    decision = gate.evaluate(_proposal(now), snapshot, _observation(now), now=now)

    assert not decision.approved
    assert "duplicate_symbol_position" in decision.reasons


def test_execution_gate_rejects_wide_spread() -> None:
    now = datetime(2026, 9, 7, 10, 0, tzinfo=UTC)
    gate = ExecutionGate(RiskConfig(max_spread_bps=10.0), EtoroConfig())

    decision = gate.evaluate(
        _proposal(now),
        _snapshot(now),
        _observation(now, bid=100.0, ask=100.2),
        now=now,
    )

    assert not decision.approved
    assert "spread_too_wide" in decision.reasons


def test_execution_gate_rejects_adverse_entry_slippage() -> None:
    now = datetime(2026, 9, 7, 10, 0, tzinfo=UTC)
    gate = ExecutionGate(
        RiskConfig(max_spread_bps=100.0, max_entry_slippage_bps=20.0),
        EtoroConfig(),
    )

    decision = gate.evaluate(
        _proposal(now),
        _snapshot(now),
        _observation(now, bid=100.29, ask=100.31),
        now=now,
    )

    assert not decision.approved
    assert "entry_slippage_too_high" in decision.reasons
