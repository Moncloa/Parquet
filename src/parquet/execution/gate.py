from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from parquet.config import EtoroConfig, RiskConfig
from parquet.models import MarketObservation, RiskSnapshot, Side, TradeProposal
from parquet.risk import RiskEngine


@dataclass(frozen=True)
class ExecutionDecision:
    approved: bool
    reasons: tuple[str, ...]
    execution_price: float | None = None
    spread_bps: float | None = None
    adverse_slippage_bps: float | None = None
    stop_distance_bps: float | None = None
    minimum_stop_distance_bps: float | None = None
    stop_floor_components: dict[str, float] | None = None
    risk_budget_usd: float | None = None
    amount_usd: float | None = None

    def as_dict(self) -> dict[str, object]:
        return {
            "approved": self.approved,
            "reasons": list(self.reasons),
            "execution_price": self.execution_price,
            "spread_bps": self.spread_bps,
            "adverse_slippage_bps": self.adverse_slippage_bps,
            "stop_distance_bps": self.stop_distance_bps,
            "minimum_stop_distance_bps": self.minimum_stop_distance_bps,
            "stop_floor_components": self.stop_floor_components or {},
            "risk_budget_usd": self.risk_budget_usd,
            "amount_usd": self.amount_usd,
        }


class ExecutionGate:
    """Deterministic pre-trade gate. It never places a broker order."""

    def __init__(self, risk: RiskConfig, etoro: EtoroConfig) -> None:
        self.risk = risk
        self.etoro = etoro
        self.risk_engine = RiskEngine(risk)

    def evaluate(
        self,
        proposal: TradeProposal,
        snapshot: RiskSnapshot,
        observation: MarketObservation,
        *,
        now: datetime | None = None,
        history_context: dict[str, Any] | None = None,
    ) -> ExecutionDecision:
        current = (now or datetime.now(UTC)).astimezone(UTC)
        reasons = list(self.risk_engine.evaluate(proposal, snapshot, now=current).reasons)

        if proposal.symbol.upper() != observation.symbol.upper():
            reasons.append("symbol_mismatch")

        quote_age = max(
            0.0,
            (current - observation.observed_at.astimezone(UTC)).total_seconds(),
        )
        if quote_age > self.etoro.max_quote_age_seconds:
            reasons.append("quote_stale")

        if snapshot.as_of is None:
            reasons.append("risk_snapshot_timestamp_missing")
        else:
            snapshot_age = max(
                0.0,
                (current - snapshot.as_of.astimezone(UTC)).total_seconds(),
            )
            if snapshot_age > self.risk.max_risk_snapshot_age_seconds:
                reasons.append("risk_snapshot_stale")

        if snapshot.equity_usd is None:
            reasons.append("equity_unavailable")

        if not self.risk.allow_duplicate_symbol_positions:
            duplicate_symbol = proposal.symbol.upper() in {
                symbol.upper() for symbol in snapshot.open_symbols
            }
            duplicate_instrument = (
                observation.instrument_id is not None
                and observation.instrument_id in snapshot.open_instrument_ids
            )
            if duplicate_symbol or duplicate_instrument:
                reasons.append("duplicate_symbol_position")

        execution_price: float | None = None
        spread_bps: float | None = None
        adverse_slippage_bps: float | None = None
        if observation.bid is None or observation.ask is None:
            reasons.append("spread_unavailable")
        elif observation.ask < observation.bid:
            reasons.append("invalid_quote")
        else:
            mid = (observation.bid + observation.ask) / 2
            spread_bps = (observation.ask - observation.bid) / mid * 10_000
            if spread_bps > self.risk.max_spread_bps:
                reasons.append("spread_too_wide")

            execution_price = (
                observation.ask if proposal.side == Side.BUY else observation.bid
            )
            if proposal.side == Side.BUY:
                adverse_slippage_bps = max(
                    0.0,
                    (execution_price - proposal.entry) / proposal.entry * 10_000,
                )
            else:
                adverse_slippage_bps = max(
                    0.0,
                    (proposal.entry - execution_price) / proposal.entry * 10_000,
                )
            if adverse_slippage_bps > self.risk.max_entry_slippage_bps:
                reasons.append("entry_slippage_too_high")

        stop_distance_bps: float | None = None
        minimum_stop_distance_bps: float | None = None
        stop_floor_components: dict[str, float] = {}
        if proposal.stop_loss is not None:
            stop_reference = execution_price or proposal.entry
            if proposal.side == Side.BUY and proposal.stop_loss >= stop_reference:
                reasons.append("stop_not_below_execution_price")
            elif proposal.side == Side.SELL and proposal.stop_loss <= stop_reference:
                reasons.append("stop_not_above_execution_price")
            else:
                stop_distance_bps = (
                    abs(stop_reference - proposal.stop_loss) / stop_reference * 10_000
                )
                stop_floor_components = _stop_floor_components(
                    self.risk,
                    spread_bps=spread_bps,
                    history_context=history_context,
                )
                minimum_stop_distance_bps = max(stop_floor_components.values())
                if stop_distance_bps + 1e-9 < minimum_stop_distance_bps:
                    reasons.append("stop_too_tight_for_market")

        risk_budget_usd: float | None = None
        amount_usd: float | None = None
        if snapshot.equity_usd is not None and proposal.stop_loss is not None:
            risk_reference = execution_price or proposal.entry
            stop_distance_pct = abs(risk_reference - proposal.stop_loss) / risk_reference
            if stop_distance_pct <= 0:
                reasons.append("invalid_stop_distance")
            else:
                risk_budget_usd = (
                    snapshot.equity_usd * self.risk.max_risk_per_trade_pct / 100
                )
                amount_from_risk = risk_budget_usd / stop_distance_pct
                notional_cap = (
                    snapshot.equity_usd
                    * self.risk.max_position_notional_pct
                    / 100
                )
                amount_usd = min(amount_from_risk, notional_cap)

        return ExecutionDecision(
            approved=not reasons,
            reasons=tuple(dict.fromkeys(reasons)),
            execution_price=execution_price,
            spread_bps=spread_bps,
            adverse_slippage_bps=adverse_slippage_bps,
            stop_distance_bps=stop_distance_bps,
            minimum_stop_distance_bps=minimum_stop_distance_bps,
            stop_floor_components=stop_floor_components,
            risk_budget_usd=risk_budget_usd,
            amount_usd=amount_usd,
        )


def _stop_floor_components(
    risk: RiskConfig,
    *,
    spread_bps: float | None,
    history_context: dict[str, Any] | None,
) -> dict[str, float]:
    components = {"absolute_floor": float(risk.min_stop_distance_bps)}
    if spread_bps is not None:
        components["spread_floor"] = max(
            0.0,
            spread_bps * risk.min_stop_spread_multiple,
        )

    metrics = _history_metrics(history_context)
    volatility_bps = _optional_number(metrics.get("step_volatility_bps_60m"))
    if volatility_bps is not None:
        components["volatility_floor"] = max(
            0.0,
            volatility_bps * risk.min_stop_volatility_multiple,
        )

    range_pct = _optional_number(metrics.get("range_pct_60m"))
    if range_pct is not None:
        components["range_60m_floor"] = (
            abs(range_pct) * 100 * risk.min_stop_range_60m_fraction
        )

    recent_moves = [
        abs(value)
        for value in (
            _optional_number(metrics.get("change_pct_5m")),
            _optional_number(metrics.get("change_pct_15m")),
        )
        if value is not None
    ]
    if recent_moves:
        components["recent_move_floor"] = (
            max(recent_moves) * 100 * risk.min_stop_recent_move_fraction
        )
    return components


def _history_metrics(history_context: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(history_context, dict):
        return {}
    value = history_context.get("metrics")
    return value if isinstance(value, dict) else {}


def _optional_number(value: object) -> float | None:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    return None
