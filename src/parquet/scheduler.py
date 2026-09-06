from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, time, timedelta
from zoneinfo import ZoneInfo

from parquet.config import StructuralReview


@dataclass(frozen=True)
class ScheduledReview:
    at: datetime
    reason: str
    source: str = "dynamic"

    @property
    def key(self) -> str:
        return f"{self.source}|{self.at.astimezone(UTC).isoformat()}|{self.reason}"


class ReviewQueue:
    def __init__(self) -> None:
        self._items: dict[str, ScheduledReview] = {}

    def add(self, review: ScheduledReview) -> None:
        self._items[review.key] = review

    def remove(self, review: ScheduledReview) -> None:
        self._items.pop(review.key, None)

    def due(self, now: datetime | None = None) -> list[ScheduledReview]:
        current = (now or datetime.now(UTC)).astimezone(UTC)
        due = [item for item in self._items.values() if item.at.astimezone(UTC) <= current]
        for item in due:
            self.remove(item)
        return sorted(due, key=lambda item: item.at)

    def pending(self) -> list[ScheduledReview]:
        return sorted(self._items.values(), key=lambda item: item.at)


def next_structural_review(
    rule: StructuralReview,
    now: datetime | None = None,
) -> ScheduledReview:
    current = (now or datetime.now(UTC)).astimezone(UTC)
    market_tz = ZoneInfo(rule.timezone)
    local_now = current.astimezone(market_tz)

    for day_offset in range(8):
        candidate_date = local_now.date() + timedelta(days=day_offset)
        if candidate_date.weekday() not in rule.weekdays:
            continue
        local_open = datetime.combine(
            candidate_date,
            time(hour=rule.hour, minute=rule.minute),
            tzinfo=market_tz,
        )
        candidate = local_open + timedelta(minutes=rule.offset_minutes)
        if candidate > local_now:
            return ScheduledReview(
                at=candidate.astimezone(UTC),
                reason=f"market_open:{rule.name}",
                source="structural",
            )

    raise RuntimeError(f"Unable to schedule structural review: {rule.name}")


def ensure_structural_reviews(
    queue: ReviewQueue,
    rules: list[StructuralReview],
    now: datetime | None = None,
) -> None:
    existing_reasons = {
        item.reason for item in queue.pending() if item.source == "structural"
    }
    for rule in rules:
        reason = f"market_open:{rule.name}"
        if reason not in existing_reasons:
            queue.add(next_structural_review(rule, now))
