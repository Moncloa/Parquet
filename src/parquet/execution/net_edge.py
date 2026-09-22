from __future__ import annotations

from dataclasses import dataclass

from parquet.models import Side, TradeProposal


@dataclass(frozen=True)
class NetEdgeDecision:
    approved: bool
    reason: str | None
    gross_reward_usd: float | None
    gross_risk_usd: float
    estimated_round_trip_cost_usd: float
    net_reward_usd: float | None
    net_risk_usd: float
    net_reward_risk: float | None
    gross_reward_to_cost: float | None

    def as_dict(self) -> dict[str, object]:
        return {
            "approved": self.approved,
            "reason": self.reason,
            "gross_reward_usd": self.gross_reward_usd,
            "gross_risk_usd": self.gross_risk_usd,
            "estimated_round_trip_cost_usd": self.estimated_round_trip_cost_usd,
            "net_reward_usd": self.net_reward_usd,
            "net_risk_usd": self.net_risk_usd,
            "net_reward_risk": self.net_reward_risk,
            "gross_reward_to_cost": self.gross_reward_to_cost,
        }


def evaluate_net_edge(
    proposal: TradeProposal,
    *,
    execution_price: float,
    exposure_usd: float,
    open_cost_usd: float,
    round_trip_cost_multiplier: float,
    min_net_reward_risk: float,
    min_gross_reward_to_cost: float,
) -> NetEdgeDecision:
    """Reject trades whose remaining target edge is too small after estimated costs.

    eToro's preflight endpoint gives opening costs. Until exact close costs are
    available before entry, use a configurable multiple of opening costs as a
    conservative round-trip estimate. Spread already embedded in executable
    entry/exit prices is not added a second time here.
    """
    if execution_price <= 0 or exposure_usd <= 0:
        raise ValueError("execution_price and exposure_usd must be positive")
    if open_cost_usd < 0:
        raise ValueError("open_cost_usd cannot be negative")

    cost = open_cost_usd * round_trip_cost_multiplier
    stop = proposal.stop_loss
    if stop is None:
        raise ValueError("net-edge evaluation requires a stop loss")

    stop_fraction = abs(execution_price - stop) / execution_price
    gross_risk = exposure_usd * stop_fraction
    net_risk = gross_risk + cost

    target = proposal.take_profit
    if target is None:
        return NetEdgeDecision(
            approved=False,
            reason="net_edge_take_profit_missing",
            gross_reward_usd=None,
            gross_risk_usd=gross_risk,
            estimated_round_trip_cost_usd=cost,
            net_reward_usd=None,
            net_risk_usd=net_risk,
            net_reward_risk=None,
            gross_reward_to_cost=None,
        )

    if proposal.side == Side.BUY:
        remaining_fraction = (target - execution_price) / execution_price
    else:
        remaining_fraction = (execution_price - target) / execution_price

    gross_reward = exposure_usd * remaining_fraction
    net_reward = gross_reward - cost
    reward_to_cost = None if cost <= 0 else gross_reward / cost
    net_rr = net_reward / net_risk if net_risk > 0 else None

    reason = None
    if gross_reward <= 0 or net_reward <= 0:
        reason = "net_edge_consumed"
    elif reward_to_cost is not None and reward_to_cost < min_gross_reward_to_cost:
        reason = "cost_hurdle_too_low"
    elif net_rr is None or net_rr < min_net_reward_risk:
        reason = "net_reward_risk_too_low"

    return NetEdgeDecision(
        approved=reason is None,
        reason=reason,
        gross_reward_usd=gross_reward,
        gross_risk_usd=gross_risk,
        estimated_round_trip_cost_usd=cost,
        net_reward_usd=net_reward,
        net_risk_usd=net_risk,
        net_reward_risk=net_rr,
        gross_reward_to_cost=reward_to_cost,
    )
