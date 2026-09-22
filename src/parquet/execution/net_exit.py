from __future__ import annotations

from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class NetExitDecision:
    action: str
    reason: str
    gross_pnl_usd: float
    estimated_total_cost_usd: float
    net_pnl_usd: float
    initial_net_risk_usd: float | None
    net_r_multiple: float | None

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


def evaluate_net_exit(
    *,
    gross_pnl_usd: float,
    estimated_open_cost_usd: float,
    estimated_close_cost_usd: float,
    initial_net_risk_usd: float | None,
    take_profit_net_r: float,
    protect_profit_net_r: float,
) -> NetExitDecision:
    """Evaluate an open position in net dollars, never gross P&L.

    CLOSE is intentionally limited to a strong deterministic profit condition.
    PROTECT means the position has earned enough net profit that the caller should
    seek a non-worsening protective-stop update when broker support is available.
    Until that write path is implemented, PROTECT is advisory and the broker SL
    remains the hard safety control.
    """
    if estimated_open_cost_usd < 0 or estimated_close_cost_usd < 0:
        raise ValueError("estimated costs must be non-negative")
    if take_profit_net_r <= 0 or protect_profit_net_r <= 0:
        raise ValueError("net R thresholds must be positive")
    if protect_profit_net_r > take_profit_net_r:
        raise ValueError("protect threshold cannot exceed take-profit threshold")

    total_cost = estimated_open_cost_usd + estimated_close_cost_usd
    net_pnl = gross_pnl_usd - total_cost
    net_r = None
    if initial_net_risk_usd is not None and initial_net_risk_usd > 0:
        net_r = net_pnl / initial_net_risk_usd

    if net_r is not None and net_r >= take_profit_net_r:
        action, reason = "CLOSE", "net_take_profit_reached"
    elif net_r is not None and net_r >= protect_profit_net_r:
        action, reason = "PROTECT", "net_profit_protection_reached"
    else:
        action, reason = "HOLD", "net_exit_threshold_not_reached"

    return NetExitDecision(
        action=action,
        reason=reason,
        gross_pnl_usd=gross_pnl_usd,
        estimated_total_cost_usd=total_cost,
        net_pnl_usd=net_pnl,
        initial_net_risk_usd=initial_net_risk_usd,
        net_r_multiple=net_r,
    )
