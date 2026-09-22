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

    # The risk gate defines the desired/maximum exposure. Capital allocation and
    # leverage are implementation details: choose the *lowest* supported leverage
    # that can express as much of that risk-approved exposure as possible without
    # exceeding the configured capital cap. This prevents leverage from becoming a
    # hidden risk multiplier while avoiding needless under-exposure when x1 is
    # capital-constrained.
    diagnostics: list[str] = []
    candidates: list[tuple[float, int, str, float | None, float, float]] = []
    for leverage in allowed:
        broker_minimum = eligibility.minimum_amount(
            direction=direction,
            leverage=leverage,
        )
        settlement_type = eligibility.settlement_type(
            direction=direction,
            leverage=leverage,
        )
        capital_needed = gate_maximum_notional_usd / leverage
        chosen_capital = min(maximum_capital_usd, capital_needed)
        if chosen_capital + 1e-9 < minimum_capital_usd:
            diagnostics.append(
                f"x{leverage}: required capital {chosen_capital:.2f} USD is below "
                f"autonomous minimum {minimum_capital_usd:.2f} USD"
            )
            continue
        if broker_minimum is not None and broker_minimum > chosen_capital + 1e-9:
            diagnostics.append(
                f"x{leverage}: broker minimum {broker_minimum:.2f} USD exceeds "
                f"risk-safe capital {chosen_capital:.2f} USD"
            )
            continue

        exposure = min(gate_maximum_notional_usd, chosen_capital * leverage)
        candidates.append(
            (
                exposure,
                leverage,
                settlement_type,
                broker_minimum,
                chosen_capital,
                capital_needed,
            )
        )

    if candidates:
        best_exposure = max(item[0] for item in candidates)
        # Among configurations reaching the best risk-approved exposure, use the
        # least leverage. If no leverage can reach the full desired exposure, this
        # selects the one that gets closest without exceeding it.
        viable = [item for item in candidates if abs(item[0] - best_exposure) <= 1e-9]
        exposure, leverage, settlement_type, broker_minimum, capital, capital_needed = min(
            viable, key=lambda item: item[1]
        )
        return (
            leverage,
            settlement_type,
            broker_minimum,
            capital,
            capital_needed,
        )

    detail = "; ".join(diagnostics) or "no viable broker configuration"
    raise RuntimeError(
        "No leverage satisfies autonomous real position bounds and risk limits: "
        + detail
    )
