from datetime import UTC, datetime, timedelta
from pathlib import Path

from parquet.models import MarketAnalysis, NextReview
from parquet.runtime_entrypoint import (
    _actionable_next_review_at,
    _load_stored_analysis,
    _scheduled_chatgpt_review,
)
from parquet.storage import Storage


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


def test_analysis_next_review_becomes_chatgpt_schedule() -> None:
    now = datetime(2026, 9, 9, 13, 16, tzinfo=UTC)
    analysis = MarketAnalysis(
        analysis_id="latest-analysis",
        generated_at=now,
        next_review=NextReview(
            at=datetime(2026, 9, 9, 13, 45, tzinfo=UTC),
            reason="Reassess after the open",
        ),
    )

    review = _scheduled_chatgpt_review(analysis, now)

    assert review is not None
    assert review.at == datetime(2026, 9, 9, 13, 45, tzinfo=UTC)
    assert review.reason == "Reassess after the open"
    assert review.source == "chatgpt"


def test_latest_analysis_can_be_restored_from_storage(tmp_path: Path) -> None:
    storage = Storage(tmp_path / "state.db")
    analysis = MarketAnalysis(
        analysis_id="latest-analysis",
        generated_at=datetime(2026, 9, 9, 13, 5, tzinfo=UTC),
        next_review=NextReview(
            at=datetime(2026, 9, 9, 13, 45, tzinfo=UTC),
            reason="Latest analysis owns this review",
        ),
    )
    storage.save_analysis(
        analysis.analysis_id,
        analysis.generated_at.isoformat(),
        analysis.model_dump_json(),
    )

    restored = _load_stored_analysis(storage.path, analysis.analysis_id)

    assert restored is not None
    assert restored.analysis_id == analysis.analysis_id
    assert restored.next_review is not None
    assert restored.next_review.at == analysis.next_review.at
