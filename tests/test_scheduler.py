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


def test_wall_street_skips_labor_day() -> None:
    rule = StructuralReview(
        name="wall_street_open",
        timezone="America/New_York",
        calendar="XNYS",
        hour=9,
        minute=30,
        offset_minutes=1,
    )
    labor_day = datetime(2026, 9, 7, 11, 0, tzinfo=UTC)

    review = next_structural_review(rule, labor_day)

    assert review.at == datetime(2026, 9, 8, 13, 31, tzinfo=UTC)


def test_invalid_persisted_holiday_review_is_replaced() -> None:
    rule = StructuralReview(
        name="wall_street_open",
        timezone="America/New_York",
        calendar="XNYS",
        hour=9,
        minute=30,
        offset_minutes=1,
    )
    queue = ReviewQueue()
    queue.add(
        ScheduledReview(
            at=datetime(2026, 9, 7, 13, 31, tzinfo=UTC),
            reason="market_open:wall_street_open",
            source="structural",
        )
    )

    ensure_structural_reviews(queue, [rule], datetime(2026, 9, 7, 11, 0, tzinfo=UTC))

    assert [item.at for item in queue.pending()] == [
        datetime(2026, 9, 8, 13, 31, tzinfo=UTC)
    ]


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
