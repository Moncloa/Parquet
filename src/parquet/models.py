from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator


def _strict_output_schema_extra(schema: dict[str, Any]) -> None:
    """Make one Pydantic model object compatible with OpenAI strict outputs."""
    properties = schema.get("properties")
    if not isinstance(properties, dict):
        return
    schema["additionalProperties"] = False
    schema["required"] = list(properties)
    for property_schema in properties.values():
        if isinstance(property_schema, dict) and property_schema.get("default", ...) is None:
            property_schema.pop("default", None)


class StrictOutputModel(BaseModel):
    """Base class for models emitted through Codex structured output."""

    model_config = ConfigDict(json_schema_extra=_strict_output_schema_extra)


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


class TriggerType(StrEnum):
    PRICE_ABOVE = "price_above"
    PRICE_BELOW = "price_below"
    CLOSE_ABOVE = "close_above"
    CLOSE_BELOW = "close_below"


class Trigger(StrictOutputModel):
    type: TriggerType
    price: float = Field(gt=0)
    timeframe: str | None = None


class WatchItem(StrictOutputModel):
    watch_id: str = Field(min_length=1)
    symbol: str = Field(min_length=1)
    bias: Bias
    trigger: Trigger
    invalidation: float | None = Field(default=None, gt=0)
    expires_at: datetime
    on_trigger: TriggerAction = TriggerAction.REASSESS
    proposal_id: str | None = Field(default=None, min_length=1)
    rationale: str | None = None


class TradeProposal(StrictOutputModel):
    proposal_id: str = Field(min_length=1)
    symbol: str = Field(min_length=1)
    side: Side
    entry: float = Field(gt=0)
    stop_loss: float | None = Field(default=None, gt=0)
    take_profit: float | None = Field(default=None, gt=0)
    confidence: float = Field(ge=0, le=1)
    generated_at: datetime | None = None
    expires_at: datetime
    thesis: list[str] = Field(default_factory=list)
    risks: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_stop_direction(self) -> TradeProposal:
        if self.stop_loss is None:
            return self
        if self.side == Side.BUY and self.stop_loss >= self.entry:
            raise ValueError("BUY stop_loss must be below entry")
        if self.side == Side.SELL and self.stop_loss <= self.entry:
            raise ValueError("SELL stop_loss must be above entry")
        return self


class NextReview(StrictOutputModel):
    at: datetime
    reason: str = Field(min_length=1)


class MarketAnalysis(StrictOutputModel):
    schema_version: int = 1
    analysis_id: str = Field(min_length=1)
    review_request_id: str | None = Field(default=None, min_length=1)
    generated_at: datetime
    market_regime: str = "unknown"
    summary: str = ""
    sources: list[str] = Field(default_factory=list)
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
    as_of: datetime | None = None
    equity_usd: float | None = Field(default=None, gt=0)
    open_positions: int = 0
    trades_today: int = 0
    daily_pnl_pct: float = 0.0
    weekly_pnl_pct: float = 0.0
    open_symbols: list[str] = Field(default_factory=list)
    open_instrument_ids: list[int] = Field(default_factory=list)


class MarketObservation(BaseModel):
    symbol: str = Field(min_length=1)
    price: float = Field(gt=0)
    observed_at: datetime
    instrument_id: int | None = Field(default=None, gt=0)
    bid: float | None = Field(default=None, gt=0)
    ask: float | None = Field(default=None, gt=0)
    timeframe: str | None = None
    candle_closed: bool = False


class WatchEventType(StrEnum):
    TRIGGERED = "TRIGGERED"
    INVALIDATED = "INVALIDATED"
    EXPIRED = "EXPIRED"


class WatchEvent(BaseModel):
    watch_id: str
    symbol: str
    event: WatchEventType
    observed_at: datetime
    observed_price: float
    action: TriggerAction | None = None
    reason: str
