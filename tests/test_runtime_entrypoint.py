from datetime import UTC, datetime, timedelta

from parquet.runtime_entrypoint import _actionable_next_review_at


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
