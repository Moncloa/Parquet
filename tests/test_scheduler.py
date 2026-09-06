from datetime import UTC, datetime, timedelta

from parquet.scheduler import ReviewQueue, ScheduledReview


def test_due_reviews_are_removed() -> None:
    now = datetime.now(UTC)
    queue = ReviewQueue()
    queue.add(ScheduledReview(now - timedelta(seconds=1), "due"))
    queue.add(ScheduledReview(now + timedelta(hours=1), "later"))
    assert [item.reason for item in queue.due(now)] == ["due"]
    assert [item.reason for item in queue.pending()] == ["later"]
