from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime


@dataclass(frozen=True)
class ScheduledReview:
    at: datetime
    reason: str


class ReviewQueue:
    def __init__(self) -> None:
        self._items: dict[str, ScheduledReview] = {}

    def add(self, review: ScheduledReview) -> None:
        key = review.at.isoformat() + "|" + review.reason
        self._items[key] = review

    def due(self, now: datetime | None = None) -> list[ScheduledReview]:
        current = now or datetime.now(UTC)
        due = [item for item in self._items.values() if item.at <= current]
        for item in due:
            self._items.pop(item.at.isoformat() + "|" + item.reason, None)
        return sorted(due, key=lambda item: item.at)

    def pending(self) -> list[ScheduledReview]:
        return sorted(self._items.values(), key=lambda item: item.at)
