from datetime import UTC, datetime, timedelta

from parquet.models import (
    Bias,
    MarketObservation,
    Trigger,
    TriggerAction,
    TriggerType,
    WatchEventType,
    WatchItem,
)
from parquet.watch import WatchEngine


def watch(trigger_type: TriggerType = TriggerType.PRICE_ABOVE) -> WatchItem:
    now = datetime.now(UTC)
    return WatchItem(
        watch_id="w-1",
        symbol="NSDQ100",
        bias=Bias.LONG,
        trigger=Trigger(type=trigger_type, price=100, timeframe="5m"),
        invalidation=95,
        expires_at=now + timedelta(hours=1),
        on_trigger=TriggerAction.REASSESS,
    )


def test_price_trigger_fires() -> None:
    item = watch()
    observation = MarketObservation(
        symbol="NSDQ100",
        price=101,
        observed_at=datetime.now(UTC),
    )
    event = WatchEngine().evaluate(item, observation)
    assert event is not None
    assert event.event == WatchEventType.TRIGGERED
    assert event.action == TriggerAction.REASSESS


def test_close_trigger_requires_closed_matching_candle() -> None:
    item = watch(TriggerType.CLOSE_ABOVE)
    now = datetime.now(UTC)
    not_closed = MarketObservation(
        symbol="NSDQ100",
        price=101,
        observed_at=now,
        timeframe="5m",
        candle_closed=False,
    )
    wrong_timeframe = MarketObservation(
        symbol="NSDQ100",
        price=101,
        observed_at=now,
        timeframe="1m",
        candle_closed=True,
    )
    closed = MarketObservation(
        symbol="NSDQ100",
        price=101,
        observed_at=now,
        timeframe="5m",
        candle_closed=True,
    )
    engine = WatchEngine()
    assert engine.evaluate(item, not_closed) is None
    assert engine.evaluate(item, wrong_timeframe) is None
    assert engine.evaluate(item, closed) is not None


def test_invalidation_precedes_trigger() -> None:
    item = watch(TriggerType.PRICE_BELOW)
    observation = MarketObservation(
        symbol="NSDQ100",
        price=94,
        observed_at=datetime.now(UTC),
    )
    event = WatchEngine().evaluate(item, observation)
    assert event is not None
    assert event.event == WatchEventType.INVALIDATED
