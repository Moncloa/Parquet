from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from parquet.config import RiskConfig
from parquet.models import RiskSnapshot, TradeProposal


@dataclass(frozen=True)
class RiskDecision:
    accepted: bool
    reasons: tuple[str, ...]


class RiskEngine:
    def __init__(self, config: RiskConfig) -> None:
        self.config = config

    def evaluate(
        self,
        proposal: TradeProposal,
        snapshot: RiskSnapshot,
        *,
        now: datetime | None = None,
    ) -> RiskDecision:
        current = now or datetime.now(UTC)
        reasons: list[str] = []

        if self.config.stop_loss_required and proposal.stop_loss is None:
            reasons.append("stop_loss_required")
        if proposal.expires_at <= current:
            reasons.append("signal_expired")
        if proposal.generated_at is not None:
            age_minutes = (current - proposal.generated_at).total_seconds() / 60
            if age_minutes > self.config.max_signal_age_minutes:
                reasons.append("signal_too_old")
        if snapshot.open_positions >= self.config.max_open_positions:
            reasons.append("max_open_positions")
        if snapshot.trades_today >= self.config.max_trades_per_day:
            reasons.append("max_trades_per_day")
        if snapshot.daily_pnl_pct <= -self.config.max_daily_loss_pct:
            reasons.append("max_daily_loss")
        if snapshot.weekly_pnl_pct <= -self.config.max_weekly_loss_pct:
            reasons.append("max_weekly_loss")

        return RiskDecision(not reasons, tuple(reasons))
