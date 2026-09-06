from datetime import UTC, datetime, timedelta

from parquet.config import StructuralReview
from parquet.scheduler import (
    ReviewQueue,
    ScheduledReview,
    ensure_structural_reviews,
    next_structural_review,
)


def test_due_reviews_are_removed() -> None:
    now = datetime.now(UTC)
    queue = ReviewQueue()
    queue.add(ScheduledReview(now - timedelta(seconds=1), "due"))
    queue.add(ScheduledReview(now + timedelta(hours=1), "later"))
    assert [item.reason for item in queue.due(now)] == ["due"]
    assert [item.reason for item in queue.pending()] == ["later"]


def test_wall_street_schedule_uses_new_york_timezone() -> None:
    rule = StructuralReview(
        name="wall_street_open",
        timezone="America/New_York",
        hour=9,
        minute=30,
        offset_minutes=1,
    )
    before_us_dst = datetime(2026, 3, 6, 12, 0, tzinfo=UTC)
    after_us_dst = datetime(2026, 3, 9, 12, 0, tzinfo=UTC)

    assert next_structural_review(rule, before_us_dst).at.hour == 14
    assert next_structural_review(rule, after_us_dst).at.hour == 13


def test_structural_review_is_not_duplicated() -> None:
    rule = StructuralReview(
        name="europe_open",
        timezone="Europe/Berlin",
        hour=9,
        minute=0,
    )
    now = datetime(2026, 9, 7, 6, 0, tzinfo=UTC)
    queue = ReviewQueue()
    ensure_structural_reviews(queue, [rule], now)
    ensure_structural_reviews(queue, [rule], now)
    assert len(queue.pending()) == 1
