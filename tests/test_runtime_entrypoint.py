from datetime import UTC, datetime, timedelta

from parquet.runtime_entrypoint import (
    _actionable_next_review_at,
    _chatgpt_review_slot,
    _same_chatgpt_review_slot,
)
from parquet.scheduler import ScheduledReview


def test_future_next_review_is_preserved() -> None:
    now = datetime(2026, 9, 9, 12, 0, tzinfo=UTC)
    review_at = now + timedelta(minutes=20)

    assert _actionable_next_review_at(review_at, now) == review_at


def test_slightly_late_next_review_runs_immediately() -> None:
    now = datetime(2026, 9, 9, 12, 0, tzinfo=UTC)
    review_at = now - timedelta(minutes=2)

    assert _actionable_next_review_at(review_at, now) == now


def test_stale_next_review_is_skipped() -> None:
    now = datetime(2026, 9, 9, 12, 0, tzinfo=UTC)
    review_at = now - timedelta(minutes=6)

    assert _actionable_next_review_at(review_at, now) is None


def test_chatgpt_review_slot_is_minute_scoped() -> None:
    first = ScheduledReview(
        at=datetime(2026, 9, 9, 13, 35, 1, tzinfo=UTC),
        reason="first reassessment",
        source="chatgpt",
    )
    second = ScheduledReview(
        at=datetime(2026, 9, 9, 13, 35, 59, tzinfo=UTC),
        reason="second reassessment",
        source="chatgpt",
    )

    assert _chatgpt_review_slot(first) == "2026-09-09T13:35:00+00:00"
    assert _same_chatgpt_review_slot(first, second)


def test_non_chatgpt_reviews_are_not_deduplicated() -> None:
    at = datetime(2026, 9, 9, 13, 35, tzinfo=UTC)
    chatgpt = ScheduledReview(at=at, reason="analysis", source="chatgpt")
    structural = ScheduledReview(at=at, reason="market_open:test", source="structural")

    assert not _same_chatgpt_review_slot(chatgpt, structural)
