from __future__ import annotations

from parquet.models import (
    Bias,
    MarketObservation,
    TriggerType,
    WatchEvent,
    WatchEventType,
    WatchItem,
)


class WatchEngine:
    """Evaluate deterministic watch conditions against market observations."""

    def evaluate(self, watch: WatchItem, observation: MarketObservation) -> WatchEvent | None:
        if observation.symbol != watch.symbol:
            return None

        if observation.observed_at >= watch.expires_at:
            return WatchEvent(
                watch_id=watch.watch_id,
                symbol=watch.symbol,
                event=WatchEventType.EXPIRED,
                observed_at=observation.observed_at,
                observed_price=observation.price,
                reason="watch_expired",
            )

        invalidation = watch.invalidation
        if invalidation is not None:
            invalidated = (
                watch.bias == Bias.LONG and observation.price <= invalidation
            ) or (
                watch.bias == Bias.SHORT and observation.price >= invalidation
            )
            if invalidated:
                return WatchEvent(
                    watch_id=watch.watch_id,
                    symbol=watch.symbol,
                    event=WatchEventType.INVALIDATED,
                    observed_at=observation.observed_at,
                    observed_price=observation.price,
                    reason="invalidation_level_reached",
                )

        trigger = watch.trigger
        if trigger.type in {TriggerType.CLOSE_ABOVE, TriggerType.CLOSE_BELOW}:
            if not observation.candle_closed:
                return None
            if trigger.timeframe and observation.timeframe != trigger.timeframe:
                return None

        is_triggered = {
            TriggerType.PRICE_ABOVE: observation.price >= trigger.price,
            TriggerType.PRICE_BELOW: observation.price <= trigger.price,
            TriggerType.CLOSE_ABOVE: observation.price >= trigger.price,
            TriggerType.CLOSE_BELOW: observation.price <= trigger.price,
        }[trigger.type]
        if not is_triggered:
            return None

        return WatchEvent(
            watch_id=watch.watch_id,
            symbol=watch.symbol,
            event=WatchEventType.TRIGGERED,
            observed_at=observation.observed_at,
            observed_price=observation.price,
            action=watch.on_trigger,
            reason=f"trigger:{trigger.type.value}",
        )
