from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

from parquet import api as api_module
from parquet import main as cli_main
from parquet.enhanced_orchestrator import AutonomousOrchestrator as EnhancedAutonomousOrchestrator
from parquet.models import MarketAnalysis
from parquet.resilient_strategy import ResilientStrategyDispatcher
from parquet.scheduler import ReviewQueue, ScheduledReview

_NEXT_REVIEW_GRACE = timedelta(minutes=5)


class AutonomousOrchestrator(EnhancedAutonomousOrchestrator):
    """Runtime orchestrator with guarded dynamic reviews and cross-process retries."""

    def __init__(self, *args: object, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)
        self._restore_latest_chatgpt_review()

    def ensure_structural_reviews(self, now: datetime | None = None) -> None:
        _sync_persisted_reviews(self.storage.pending_reviews(), self.reviews)
        super().ensure_structural_reviews(now)

    def add_review(self, review: ScheduledReview) -> None:
        if review.source == "chatgpt":
            self._replace_chatgpt_review(review)
            return
        super().add_review(review)

    def _replace_chatgpt_review(
        self,
        review: ScheduledReview | None,
        *,
        analysis_id: str | None = None,
    ) -> None:
        """Keep at most one dynamic ChatGPT review; the newest analysis owns it."""
        kept_existing = False
        for existing in list(self.reviews.pending()):
            if existing.source != "chatgpt":
                continue
            if review is not None and not kept_existing and existing.key == review.key:
                kept_existing = True
                continue
            self.reviews.remove(existing)
            self.storage.delete_review(existing)
            self.storage.add_event(
                "superseded_next_review_removed",
                json.dumps(
                    {
                        "analysis_id": analysis_id,
                        "removed_at": existing.at.astimezone(UTC).isoformat(),
                        "removed_reason": existing.reason,
                        "replacement_at": (
                            None if review is None else review.at.astimezone(UTC).isoformat()
                        ),
                        "replacement_reason": None if review is None else review.reason,
                    }
                ),
            )

        if review is not None and not kept_existing:
            super().add_review(review)

    def _restore_latest_chatgpt_review(self) -> None:
        """Repair persisted dynamic schedules from the latest stored analysis on startup."""
        analysis_id = self.storage.get("latest_analysis_id")
        if not analysis_id:
            return
        analysis = _load_stored_analysis(self.storage.path, analysis_id)
        if analysis is None:
            self.storage.add_event(
                "latest_analysis_restore_error",
                json.dumps({"analysis_id": analysis_id, "error": "analysis payload not found"}),
            )
            return

        current = datetime.now(UTC)
        review = _scheduled_chatgpt_review(analysis, current)
        self._replace_chatgpt_review(review, analysis_id=analysis.analysis_id)
        if analysis.next_review is not None and review is None:
            requested_at = analysis.next_review.at.astimezone(UTC)
            self.storage.add_event(
                "stale_next_review_skipped",
                json.dumps(
                    {
                        "analysis_id": analysis.analysis_id,
                        "requested_at": requested_at.isoformat(),
                        "processed_at": current.isoformat(),
                        "age_seconds": round((current - requested_at).total_seconds(), 3),
                        "reason": analysis.next_review.reason,
                        "source": "startup_restore",
                    }
                ),
            )

    def process_analysis(self, analysis: MarketAnalysis) -> None:
        self.storage.save_analysis(
            analysis.analysis_id,
            analysis.generated_at.isoformat(),
            analysis.model_dump_json(),
        )
        for proposal in analysis.trade_proposals:
            self.storage.save_proposal(analysis.analysis_id, proposal)
        for watch in analysis.watch:
            self.storage.save_watch(analysis.analysis_id, watch)

        current = datetime.now(UTC)
        review = _scheduled_chatgpt_review(analysis, current)
        self._replace_chatgpt_review(review, analysis_id=analysis.analysis_id)
        if analysis.next_review is not None and review is None:
            requested_at = analysis.next_review.at.astimezone(UTC)
            self.storage.add_event(
                "stale_next_review_skipped",
                json.dumps(
                    {
                        "analysis_id": analysis.analysis_id,
                        "requested_at": requested_at.isoformat(),
                        "processed_at": current.isoformat(),
                        "age_seconds": round((current - requested_at).total_seconds(), 3),
                        "reason": analysis.next_review.reason,
                        "source": "analysis_ingest",
                    }
                ),
            )

        self.storage.set("latest_analysis_id", analysis.analysis_id)


def _sync_persisted_reviews(
    persisted: list[ScheduledReview],
    queue: ReviewQueue,
) -> None:
    persisted_by_key = {review.key: review for review in persisted}
    in_memory = {review.key: review for review in queue.pending()}

    # Strategy retry reviews can be cancelled by the dispatcher after a successful
    # analysis, so mirror those deletions into the long-running orchestrator queue.
    for key, review in in_memory.items():
        if review.source == "strategy_retry" and key not in persisted_by_key:
            queue.remove(review)

    in_memory_keys = {review.key for review in queue.pending()}
    for key, review in persisted_by_key.items():
        if key not in in_memory_keys:
            queue.add(review)


def _load_stored_analysis(path: Path, analysis_id: str) -> MarketAnalysis | None:
    try:
        with sqlite3.connect(path) as conn:
            row = conn.execute(
                "SELECT payload FROM analyses WHERE analysis_id = ?",
                (analysis_id,),
            ).fetchone()
    except sqlite3.Error:
        return None
    if row is None:
        return None
    try:
        return MarketAnalysis.model_validate_json(str(row[0]))
    except Exception:
        return None


def _scheduled_chatgpt_review(
    analysis: MarketAnalysis,
    now: datetime,
) -> ScheduledReview | None:
    if analysis.next_review is None:
        return None
    review_at = _actionable_next_review_at(analysis.next_review.at, now)
    if review_at is None:
        return None
    return ScheduledReview(
        at=review_at,
        reason=analysis.next_review.reason,
        source="chatgpt",
    )


def _actionable_next_review_at(value: datetime, now: datetime) -> datetime | None:
    review_at = value.astimezone(UTC)
    current = now.astimezone(UTC)
    if review_at < current - _NEXT_REVIEW_GRACE:
        return None
    return max(review_at, current)


def main() -> None:
    # Keep the mature CLI/API implementations while replacing only the runtime
    # orchestrator and the strategy dispatcher used by the API lifespan.
    cli_main.AutonomousOrchestrator = AutonomousOrchestrator  # type: ignore[attr-defined]
    api_module.StrategyDispatcher = ResilientStrategyDispatcher
    cli_main.main()


if __name__ == "__main__":
    main()
