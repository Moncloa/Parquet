from datetime import UTC, datetime, timedelta

import pytest

from parquet.execution.net_edge import evaluate_net_edge
from parquet.models import Side, TradeProposal


def _proposal(*, side: Side = Side.BUY, target: float | None = 102.0) -> TradeProposal:
    now = datetime(2026, 9, 22, 10, 0, tzinfo=UTC)
    return TradeProposal(
        proposal_id="edge-1",
        symbol="TEST",
        side=side,
        entry=100.0,
        stop_loss=99.0 if side == Side.BUY else 101.0,
        take_profit=target,
        confidence=0.8,
        generated_at=now,
        expires_at=now + timedelta(minutes=10),
    )


def test_net_edge_accepts_trade_with_cost_headroom() -> None:
    result = evaluate_net_edge(
        _proposal(),
        execution_price=100.0,
        exposure_usd=5000.0,
        open_cost_usd=5.0,
        round_trip_cost_multiplier=2.0,
        min_net_reward_risk=1.2,
        min_gross_reward_to_cost=3.0,
    )
    assert result.approved
    assert result.gross_reward_usd == pytest.approx(100.0)
    assert result.estimated_round_trip_cost_usd == pytest.approx(10.0)
    assert result.net_reward_usd == pytest.approx(90.0)
    assert result.net_risk_usd == pytest.approx(60.0)
    assert result.net_reward_risk == pytest.approx(1.5)
    assert result.gross_reward_to_cost == pytest.approx(10.0)


def test_net_edge_rejects_when_costs_consume_too_much_reward() -> None:
    result = evaluate_net_edge(
        _proposal(target=100.5),
        execution_price=100.0,
        exposure_usd=5000.0,
        open_cost_usd=5.0,
        round_trip_cost_multiplier=2.0,
        min_net_reward_risk=1.2,
        min_gross_reward_to_cost=3.0,
    )
    assert not result.approved
    assert result.reason == "cost_hurdle_too_low"
    assert result.gross_reward_usd == pytest.approx(25.0)
    assert result.gross_reward_to_cost == pytest.approx(2.5)


def test_net_edge_uses_remaining_reward_after_late_entry() -> None:
    result = evaluate_net_edge(
        _proposal(target=102.0),
        execution_price=101.7,
        exposure_usd=5000.0,
        open_cost_usd=2.0,
        round_trip_cost_multiplier=2.0,
        min_net_reward_risk=1.2,
        min_gross_reward_to_cost=3.0,
    )
    assert not result.approved
    assert result.reason in {"cost_hurdle_too_low", "net_reward_risk_too_low"}


def test_net_edge_requires_target_for_real_cost_gate() -> None:
    result = evaluate_net_edge(
        _proposal(target=None),
        execution_price=100.0,
        exposure_usd=5000.0,
        open_cost_usd=1.0,
        round_trip_cost_multiplier=2.0,
        min_net_reward_risk=1.2,
        min_gross_reward_to_cost=3.0,
    )
    assert not result.approved
    assert result.reason == "net_edge_take_profit_missing"


def test_net_edge_handles_short_direction() -> None:
    result = evaluate_net_edge(
        _proposal(side=Side.SELL, target=98.0),
        execution_price=100.0,
        exposure_usd=5000.0,
        open_cost_usd=5.0,
        round_trip_cost_multiplier=2.0,
        min_net_reward_risk=1.2,
        min_gross_reward_to_cost=3.0,
    )
    assert result.approved
    assert result.gross_reward_usd == pytest.approx(100.0)
