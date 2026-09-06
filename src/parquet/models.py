from __future__ import annotations

from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, Field, model_validator


class Bias(StrEnum):
    LONG = "LONG"
    SHORT = "SHORT"
    NEUTRAL = "NEUTRAL"


class Side(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


class TriggerAction(StrEnum):
    EXECUTE = "EXECUTE"
    REASSESS = "REASSESS"


class Trigger(BaseModel):
    type: str
    price: float | None = None
    timeframe: str | None = None
    confirmation: str | None = None


class WatchItem(BaseModel):
    symbol: str = Field(min_length=1)
    bias: Bias
    trigger: Trigger
    invalidation: float | None = None
    expires_at: datetime
    on_trigger: TriggerAction = TriggerAction.REASSESS
    rationale: str | None = None


class TradeProposal(BaseModel):
    symbol: str = Field(min_length=1)
    side: Side
    entry: float = Field(gt=0)
    stop_loss: float | None = Field(default=None, gt=0)
    take_profit: float | None = Field(default=None, gt=0)
    confidence: float = Field(ge=0, le=1)
    expires_at: datetime
    thesis: list[str] = Field(default_factory=list)
    risks: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_stop_direction(self) -> "TradeProposal":
        if self.stop_loss is None:
            return self
        if self.side == Side.BUY and self.stop_loss >= self.entry:
            raise ValueError("BUY stop_loss must be below entry")
        if self.side == Side.SELL and self.stop_loss <= self.entry:
            raise ValueError("SELL stop_loss must be above entry")
        return self


class NextReview(BaseModel):
    at: datetime
    reason: str = Field(min_length=1)


class MarketAnalysis(BaseModel):
    schema_version: int = 1
    analysis_id: str = Field(min_length=1)
    generated_at: datetime
    market_regime: str = "unknown"
    summary: str = ""
    watch: list[WatchItem] = Field(default_factory=list)
    trade_proposals: list[TradeProposal] = Field(default_factory=list)
    next_review: NextReview | None = None


class ReviewRequest(BaseModel):
    schema_version: int = 1
    request_id: str
    requested_at: datetime
    reason: str
    symbols: list[str] = Field(default_factory=list)
    context: dict[str, object] = Field(default_factory=dict)


class RiskSnapshot(BaseModel):
    open_positions: int = 0
    trades_today: int = 0
    daily_pnl_pct: float = 0.0
    weekly_pnl_pct: float = 0.0
