from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

from parquet import main as cli_main
from parquet.enhanced_orchestrator import AutonomousOrchestrator as EnhancedAutonomousOrchestrator
from parquet.models import MarketAnalysis
from parquet.scheduler import ScheduledReview

_NEXT_REVIEW_GRACE = timedelta(minutes=5)


class AutonomousOrchestrator(EnhancedAutonomousOrchestrator):
    """Runtime orchestrator with stale-analysis catch-up protection."""

    def __init__(self, *args: object, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)
        self._coalesce_chatgpt_reviews()

    def add_review(self, review: ScheduledReview) -> None:
        if review.source == "chatgpt":
            duplicate = next(
                (
                    existing
                    for existing in self.reviews.pending()
                    if _same_chatgpt_review_slot(existing, review)
                ),
                None,
            )
            if duplicate is not None:
                self.storage.add_event(
                    "duplicate_next_review_suppressed",
                    json.dumps(
                        {
                            "kept_at": duplicate.at.astimezone(UTC).isoformat(),
                            "kept_reason": duplicate.reason,
                            "suppressed_at": review.at.astimezone(UTC).isoformat(),
                            "suppressed_reason": review.reason,
                        }
                    ),
                )
                return
        super().add_review(review)

    def _coalesce_chatgpt_reviews(self) -> None:
        kept: dict[str, ScheduledReview] = {}
        for review in list(self.reviews.pending()):
            if review.source != "chatgpt":
                continue
            slot = _chatgpt_review_slot(review)
            existing = kept.get(slot)
            if existing is None:
                kept[slot] = review
                continue
            self.reviews.remove(review)
            self.storage.delete_review(review)
            self.storage.add_event(
                "duplicate_next_review_removed",
                json.dumps(
                    {
                        "slot": slot,
                        "kept_reason": existing.reason,
                        "removed_reason": review.reason,
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

        if analysis.next_review is not None:
            current = datetime.now(UTC)
            review_at = _actionable_next_review_at(analysis.next_review.at, current)
            if review_at is None:
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
                        }
                    ),
                )
            else:
                self.add_review(
                    ScheduledReview(
                        at=review_at,
                        reason=analysis.next_review.reason,
                        source="chatgpt",
                    )
                )

        self.storage.set("latest_analysis_id", analysis.analysis_id)


def _chatgpt_review_slot(review: ScheduledReview) -> str:
    return review.at.astimezone(UTC).replace(second=0, microsecond=0).isoformat()


def _same_chatgpt_review_slot(left: ScheduledReview, right: ScheduledReview) -> bool:
    return (
        left.source == "chatgpt"
        and right.source == "chatgpt"
        and _chatgpt_review_slot(left) == _chatgpt_review_slot(right)
    )


def _actionable_next_review_at(value: datetime, now: datetime) -> datetime | None:
    review_at = value.astimezone(UTC)
    current = now.astimezone(UTC)
    if review_at < current - _NEXT_REVIEW_GRACE:
        return None
    return max(review_at, current)


def main() -> None:
    # Keep the mature CLI implementation in parquet.main while replacing only the
    # orchestrator class used by serve/once/manual-review commands.
    cli_main.AutonomousOrchestrator = AutonomousOrchestrator  # type: ignore[attr-defined]
    cli_main.main()


if __name__ == "__main__":
    main()
