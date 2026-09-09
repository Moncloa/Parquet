from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

from parquet.scheduler import ScheduledReview
from parquet.strategy import StrategyDispatcher, _redact

_USAGE_LIMIT_RETRY_DELAY = timedelta(minutes=15)
_USAGE_LIMIT_RETRY_SOURCE = "strategy_retry"
_USAGE_LIMIT_RETRY_REASON = "strategy_retry:codex_usage_limit"


class ResilientStrategyDispatcher(StrategyDispatcher):
    """Dispatcher that converts Codex quota failures into fresh future reviews."""

    async def poll_results_once(self) -> int:
        consumed = 0
        for path in self.queue.error_paths():
            request_id = path.stem
            try:
                record = self.queue.read_error(path)
                request_id = record.request_id
                self.storage.set("strategy_last_error", record.error)
                self.storage.set(
                    "strategy_last_error_at",
                    record.failed_at.astimezone(UTC).isoformat(),
                )
                if _is_usage_limit_error(record.error):
                    retry = _schedule_usage_limit_retry(self.storage, record.failed_at)
                    self.storage.set("strategy_usage_limited", "1")
                    self.storage.set(
                        "strategy_usage_retry_at",
                        retry.at.astimezone(UTC).isoformat(),
                    )
                    self.storage.add_event(
                        "strategy_usage_limit",
                        json.dumps(
                            {
                                "request_id": request_id,
                                "failed_at": record.failed_at.astimezone(UTC).isoformat(),
                                "retry_at": retry.at.astimezone(UTC).isoformat(),
                                "retry_reason": retry.reason,
                            }
                        ),
                    )
                else:
                    self.storage.add_event("strategy_analysis_error", record.model_dump_json())
            except Exception as exc:
                self.storage.set("strategy_last_error", _redact(str(exc))[:1000])
                self.storage.set("strategy_last_error_at", datetime.now(UTC).isoformat())
            self.queue.acknowledge(request_id)
            consumed += 1

        previous_success = self.storage.get("strategy_last_success_at")
        consumed += await super().poll_results_once()
        current_success = self.storage.get("strategy_last_success_at")
        if current_success and current_success != previous_success:
            self.storage.set("strategy_usage_limited", "0")
            self.storage.set("strategy_usage_retry_at", "")
            for review in self.storage.pending_reviews():
                if review.source == _USAGE_LIMIT_RETRY_SOURCE:
                    self.storage.delete_review(review)
        return consumed


def _is_usage_limit_error(error: str) -> bool:
    lowered = error.lower()
    return "usage_limit_exceeded" in lowered or "hit your usage limit" in lowered


def _schedule_usage_limit_retry(storage, failed_at: datetime) -> ScheduledReview:  # type: ignore[no-untyped-def]
    proposed_at = failed_at.astimezone(UTC) + _USAGE_LIMIT_RETRY_DELAY
    existing = [
        review
        for review in storage.pending_reviews()
        if review.source == _USAGE_LIMIT_RETRY_SOURCE
    ]
    if existing:
        retry_at = min([proposed_at, *(review.at.astimezone(UTC) for review in existing)])
        for review in existing:
            storage.delete_review(review)
    else:
        retry_at = proposed_at

    retry = ScheduledReview(
        at=retry_at,
        reason=_USAGE_LIMIT_RETRY_REASON,
        source=_USAGE_LIMIT_RETRY_SOURCE,
    )
    storage.schedule_review(retry)
    return retry
