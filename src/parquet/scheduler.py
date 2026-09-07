from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from functools import lru_cache
from typing import Any
from zoneinfo import ZoneInfo

import exchange_calendars as xcals  # type: ignore[import-untyped]

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


@lru_cache(maxsize=16)
def _exchange_calendar(name: str) -> Any:
    try:
        return xcals.get_calendar(name)
    except Exception as exc:
        raise ValueError(f"Unknown exchange calendar: {name}") from exc


def _is_trading_date(rule: StructuralReview, candidate_date: date) -> bool:
    if candidate_date.weekday() not in rule.weekdays:
        return False
    if rule.calendar is None:
        return True
    return bool(_exchange_calendar(rule.calendar).is_session(candidate_date.isoformat()))


def structural_review_is_valid(review: ScheduledReview, rule: StructuralReview) -> bool:
    if review.source != "structural" or review.reason != f"market_open:{rule.name}":
        return False
    local_date = review.at.astimezone(ZoneInfo(rule.timezone)).date()
    return _is_trading_date(rule, local_date)


def next_structural_review(
    rule: StructuralReview,
    now: datetime | None = None,
) -> ScheduledReview:
    current = (now or datetime.now(UTC)).astimezone(UTC)
    market_tz = ZoneInfo(rule.timezone)
    local_now = current.astimezone(market_tz)

    for day_offset in range(14):
        candidate_date = local_now.date() + timedelta(days=day_offset)
        if not _is_trading_date(rule, candidate_date):
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
    rules_by_reason = {f"market_open:{rule.name}": rule for rule in rules}
    for item in queue.pending():
        if item.source != "structural":
            continue
        rule = rules_by_reason.get(item.reason)
        if rule is None or not structural_review_is_valid(item, rule):
            queue.remove(item)

    existing_reasons = {
        item.reason for item in queue.pending() if item.source == "structural"
    }
    for rule in rules:
        reason = f"market_open:{rule.name}"
        if reason not in existing_reasons:
            queue.add(next_structural_review(rule, now))
