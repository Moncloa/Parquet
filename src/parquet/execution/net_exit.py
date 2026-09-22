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


@dataclass(frozen=True)
class ProtectiveStopDecision:
    approved: bool
    reason: str
    stop_rate: float | None
    locked_net_profit_usd: float
    required_gross_pnl_usd: float

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


def calculate_protective_stop(
    *,
    side: str,
    open_rate: float,
    current_executable_price: float,
    current_stop_rate: float | None,
    exposure_usd: float,
    estimated_open_cost_usd: float,
    estimated_close_cost_usd: float,
    initial_net_risk_usd: float,
    lock_net_r: float = 0.0,
) -> ProtectiveStopDecision:
    """Return a cost-aware, non-worsening stop that locks a minimum net R.

    The returned level is advisory only: this function performs no broker write.
    For a long, executable P&L is measured against bid; for a short, against ask,
    so callers must pass the corresponding executable price.
    """
    if open_rate <= 0 or current_executable_price <= 0 or exposure_usd <= 0:
        raise ValueError("prices and exposure must be positive")
    if estimated_open_cost_usd < 0 or estimated_close_cost_usd < 0:
        raise ValueError("estimated costs must be non-negative")
    if initial_net_risk_usd <= 0 or lock_net_r < 0:
        raise ValueError("initial net risk must be positive and lock_net_r non-negative")

    normalized = side.upper()
    if normalized not in {"BUY", "SELL"}:
        raise ValueError("side must be BUY or SELL")

    locked_net_profit = initial_net_risk_usd * lock_net_r
    required_gross_pnl = (
        locked_net_profit + estimated_open_cost_usd + estimated_close_cost_usd
    )
    move_fraction = required_gross_pnl / exposure_usd
    if normalized == "BUY":
        candidate = open_rate * (1.0 + move_fraction)
        if candidate >= current_executable_price:
            return ProtectiveStopDecision(
                False, "protective_stop_not_below_executable_price", None,
                locked_net_profit, required_gross_pnl,
            )
        if current_stop_rate is not None and candidate <= current_stop_rate:
            return ProtectiveStopDecision(
                False, "protective_stop_would_not_improve", None,
                locked_net_profit, required_gross_pnl,
            )
    else:
        candidate = open_rate * (1.0 - move_fraction)
        if candidate <= 0:
            return ProtectiveStopDecision(
                False, "protective_stop_invalid", None,
                locked_net_profit, required_gross_pnl,
            )
        if candidate <= current_executable_price:
            return ProtectiveStopDecision(
                False, "protective_stop_not_above_executable_price", None,
                locked_net_profit, required_gross_pnl,
            )
        if current_stop_rate is not None and candidate >= current_stop_rate:
            return ProtectiveStopDecision(
                False, "protective_stop_would_not_improve", None,
                locked_net_profit, required_gross_pnl,
            )

    return ProtectiveStopDecision(
        True, "protective_stop_available", candidate,
        locked_net_profit, required_gross_pnl,
    )
