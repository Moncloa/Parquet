from datetime import UTC, datetime, timedelta

import pytest

from parquet.models import ReviewRequest
from parquet.resilient_strategy import (
    ResilientStrategyDispatcher,
    _is_usage_limit_error,
)
from parquet.scheduler import ReviewQueue, ScheduledReview
from parquet.storage import Storage
from parquet.strategy import StrategyQueue
from parquet.runtime_entrypoint import _sync_persisted_reviews


def _request() -> ReviewRequest:
    now = datetime.now(UTC)
    return ReviewRequest(
        request_id="quota-request",
        requested_at=now,
        reason="quota-test",
        symbols=[],
        context={},
    )


def test_usage_limit_classifier() -> None:
    assert _is_usage_limit_error("You've hit your usage limit. Try again later")
    assert _is_usage_limit_error("codex_error_info: usage_limit_exceeded")
    assert not _is_usage_limit_error("Codex process timed out")


@pytest.mark.asyncio
async def test_usage_limit_schedules_one_fresh_retry(tmp_path) -> None:
    storage = Storage(tmp_path / "parquet.db")
    queue = StrategyQueue(tmp_path / "exchange")
    request = _request()
    queue.enqueue(request)
    queue.write_error(
        request.request_id,
        "Codex exited 1: ERROR: You've hit your usage limit. Try again at 5:41 PM.",
    )

    class FakeBridge:
        async def post_analysis(self, analysis) -> None:  # type: ignore[no-untyped-def]
            raise AssertionError("not used")

    dispatcher = ResilientStrategyDispatcher(
        queue_dir=tmp_path / "exchange",
        state_db=tmp_path / "parquet.db",
        storage=storage,
        bridge=FakeBridge(),  # type: ignore[arg-type]
    )

    assert await dispatcher.poll_results_once() == 1
    retries = [r for r in storage.pending_reviews() if r.source == "strategy_retry"]
    assert len(retries) == 1
    assert retries[0].reason == "strategy_retry:codex_usage_limit"
    assert retries[0].at >= request.requested_at + timedelta(minutes=14)
    assert storage.get("strategy_usage_limited") == "1"
    assert storage.get("strategy_usage_retry_at") == retries[0].at.isoformat()
    assert queue.pending_count() == 0


def test_runtime_syncs_and_removes_persisted_strategy_retry(tmp_path) -> None:
    storage = Storage(tmp_path / "parquet.db")
    queue = ReviewQueue()
    retry = ScheduledReview(
        at=datetime(2026, 9, 9, 15, 5, tzinfo=UTC),
        reason="strategy_retry:codex_usage_limit",
        source="strategy_retry",
    )
    storage.schedule_review(retry)

    _sync_persisted_reviews(storage.pending_reviews(), queue)
    assert queue.pending() == [retry]

    storage.delete_review(retry)
    _sync_persisted_reviews(storage.pending_reviews(), queue)
    assert queue.pending() == []
