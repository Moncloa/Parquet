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


def _actionable_next_review_at(value: datetime, now: datetime) -> datetime | None:
    review_at = value.astimezone(UTC)
    current = now.astimezone(UTC)
    if review_at < current - _NEXT_REVIEW_GRACE:
        return None
    return max(review_at, current)


def main() -> None:
    # Keep the mature CLI implementation in parquet.main while replacing only the
    # orchestrator class used by serve/once/manual-review commands.
    cli_main.AutonomousOrchestrator = AutonomousOrchestrator
    cli_main.main()


if __name__ == "__main__":
    main()
