from __future__ import annotations

from parquet.execution.etoro import EtoroEligibilityResult


def choose_autonomous_real_terms(
    eligibility: EtoroEligibilityResult,
    *,
    direction: str,
    gate_maximum_notional_usd: float,
    minimum_capital_usd: float,
    maximum_capital_usd: float,
    max_leverage: int,
) -> tuple[int, str, float | None, float, float]:
    """Choose autonomous real capital without inflating risk-derived sizing.

    The execution gate amount is a notional ceiling. For each broker-supported
    leverage, convert that ceiling to capital, cap it at the configured maximum,
    and reject the leverage if the resulting risk-safe capital is below the
    configured autonomous minimum. The minimum is therefore a rejection
    threshold, never a target amount.
    """
    if gate_maximum_notional_usd <= 0:
        raise RuntimeError("No positive notional remains after the risk gate")
    if minimum_capital_usd <= 0 or maximum_capital_usd <= 0:
        raise RuntimeError("Autonomous real capital bounds must be positive")
    if minimum_capital_usd > maximum_capital_usd:
        raise RuntimeError("Autonomous real minimum capital exceeds maximum capital")
    if max_leverage < 1:
        raise RuntimeError("Maximum autonomous real leverage must be positive")

    allowed = [
        leverage
        for leverage in eligibility.allowed_leverages(direction=direction)
        if leverage <= max_leverage
    ]
    if not allowed:
        raise RuntimeError(
            f"eToro offers no {direction.upper()} leverage at or below x{max_leverage}"
        )

    diagnostics: list[str] = []
    for leverage in allowed:
        maximum_safe = min(
            maximum_capital_usd,
            gate_maximum_notional_usd / leverage,
        )
        if maximum_safe + 1e-9 < minimum_capital_usd:
            diagnostics.append(
                f"x{leverage}: risk-safe capital {maximum_safe:.2f} USD is below "
                f"autonomous minimum {minimum_capital_usd:.2f} USD"
            )
            continue

        broker_minimum = eligibility.minimum_amount(
            direction=direction,
            leverage=leverage,
        )
        if broker_minimum is not None and broker_minimum > maximum_safe + 1e-9:
            diagnostics.append(
                f"x{leverage}: broker minimum {broker_minimum:.2f} USD exceeds "
                f"risk-safe capital {maximum_safe:.2f} USD"
            )
            continue

        settlement_type = eligibility.settlement_type(
            direction=direction,
            leverage=leverage,
        )
        chosen_amount = maximum_safe
        return (
            leverage,
            settlement_type,
            broker_minimum,
            chosen_amount,
            maximum_safe,
        )

    detail = "; ".join(diagnostics) or "no viable broker configuration"
    raise RuntimeError(
        "No leverage satisfies autonomous real position bounds and risk limits: "
        + detail
    )
