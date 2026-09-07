from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

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
    risk_budget_usd: float | None = None
    amount_usd: float | None = None

    def as_dict(self) -> dict[str, object]:
        return {
            "approved": self.approved,
            "reasons": list(self.reasons),
            "execution_price": self.execution_price,
            "spread_bps": self.spread_bps,
            "adverse_slippage_bps": self.adverse_slippage_bps,
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
    ) -> ExecutionDecision:
        current = (now or datetime.now(UTC)).astimezone(UTC)
        reasons = list(self.risk_engine.evaluate(proposal, snapshot, now=current).reasons)

        if proposal.symbol.upper() != observation.symbol.upper():
            reasons.append("symbol_mismatch")

        quote_age = max(0.0, (current - observation.observed_at.astimezone(UTC)).total_seconds())
        if quote_age > self.etoro.max_quote_age_seconds:
            reasons.append("quote_stale")

        if snapshot.as_of is None:
            reasons.append("risk_snapshot_timestamp_missing")
        else:
            snapshot_age = max(0.0, (current - snapshot.as_of.astimezone(UTC)).total_seconds())
            if snapshot_age > self.risk.max_risk_snapshot_age_seconds:
                reasons.append("risk_snapshot_stale")

        if snapshot.equity_usd is None:
            reasons.append("equity_unavailable")

        if (
            not self.risk.allow_duplicate_symbol_positions
            and proposal.symbol.upper() in {symbol.upper() for symbol in snapshot.open_symbols}
        ):
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

            execution_price = observation.ask if proposal.side == Side.BUY else observation.bid
            if proposal.side == Side.BUY:
                adverse_slippage_bps = max(
                    0.0, (execution_price - proposal.entry) / proposal.entry * 10_000
                )
            else:
                adverse_slippage_bps = max(
                    0.0, (proposal.entry - execution_price) / proposal.entry * 10_000
                )
            if adverse_slippage_bps > self.risk.max_entry_slippage_bps:
                reasons.append("entry_slippage_too_high")

        risk_budget_usd: float | None = None
        amount_usd: float | None = None
        if snapshot.equity_usd is not None and proposal.stop_loss is not None:
            stop_distance_pct = abs(proposal.entry - proposal.stop_loss) / proposal.entry
            if stop_distance_pct <= 0:
                reasons.append("invalid_stop_distance")
            else:
                risk_budget_usd = snapshot.equity_usd * self.risk.max_risk_per_trade_pct / 100
                amount_from_risk = risk_budget_usd / stop_distance_pct
                notional_cap = (
                    snapshot.equity_usd * self.risk.max_position_notional_pct / 100
                )
                amount_usd = min(amount_from_risk, notional_cap)

        return ExecutionDecision(
            approved=not reasons,
            reasons=tuple(dict.fromkeys(reasons)),
            execution_price=execution_price,
            spread_bps=spread_bps,
            adverse_slippage_bps=adverse_slippage_bps,
            risk_budget_usd=risk_budget_usd,
            amount_usd=amount_usd,
        )
