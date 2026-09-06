from datetime import UTC, datetime, timedelta

from parquet.models import Bias, Trigger, TriggerType, WatchItem
from parquet.scheduler import ScheduledReview
from parquet.storage import Storage


def test_reviews_survive_storage_roundtrip(tmp_path) -> None:
    storage = Storage(tmp_path / "parquet.db")
    review = ScheduledReview(
        at=datetime.now(UTC) + timedelta(minutes=5),
        reason="test-review",
        source="chatgpt",
    )
    storage.schedule_review(review)
    restored = storage.pending_reviews()
    assert len(restored) == 1
    assert restored[0].key == review.key


def test_active_watch_roundtrip(tmp_path) -> None:
    storage = Storage(tmp_path / "parquet.db")
    watch = WatchItem(
        watch_id="w-1",
        symbol="GOLD",
        bias=Bias.LONG,
        trigger=Trigger(type=TriggerType.PRICE_ABOVE, price=4500),
        expires_at=datetime.now(UTC) + timedelta(hours=1),
    )
    storage.save_watch("analysis-1", watch)
    restored = storage.active_watches()
    assert [item.watch_id for item in restored] == ["w-1"]
