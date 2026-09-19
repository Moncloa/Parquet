from datetime import UTC, datetime
from pathlib import Path

from parquet.config import Settings, StrategyConfig
from parquet.controls import (
    begin_manual_review,
    is_analysis_only_request,
    manual_review_status,
)
from parquet.models import MarketAnalysis, ReviewRequest
from parquet.storage import Storage
from parquet.strategy_policy import RoutedStrategyWorker


class FakeOrchestrator:
    def __init__(self, storage: Storage) -> None:
        self.storage = storage
        self.bridge = object()


def test_begin_manual_review_is_analysis_only_and_trackable(tmp_path: Path) -> None:
    storage = Storage(tmp_path / "state.db")
    orchestrator = FakeOrchestrator(storage)
    settings = Settings(
        strategy=StrategyConfig(
            enabled=True,
            provider="local_ollama",
            queue_dir=tmp_path / "exchange",
        )
    )

    request_id = begin_manual_review(
        settings,
        orchestrator,
        provider="codex_cli",
    )
    status = manual_review_status(storage, settings.strategy.queue_dir, request_id)

    assert status["state"] == "collecting_context"
    assert status["provider"] == "codex_cli"
    assert status["progress_pct"] == 15


def test_manual_review_status_completes_from_observational_analysis(tmp_path: Path) -> None:
    storage = Storage(tmp_path / "state.db")
    request_id = "review-control-1"
    storage.add_event(
        "manual_review_requested",
        (
            '{"request_id":"review-control-1","provider":"local_ollama",'
            '"analysis_only":true}'
        ),
    )
    storage.add_event(
        "review_request",
        (
            '{"schema_version":1,"request_id":"review-control-1",'
            '"requested_at":"2026-09-19T12:00:00Z","reason":"manual_web:local_ollama",'
            '"symbols":[],"context":{"_parquet_control":{'
            '"strategy_provider":"local_ollama","analysis_only":true}}}'
        ),
    )
    analysis = MarketAnalysis(
        analysis_id="local-analysis-control-1",
        review_request_id=request_id,
        generated_at=datetime(2026, 9, 19, 12, 1, tzinfo=UTC),
        market_regime="mixed",
        summary="No trade; spread too wide.",
    )
    storage.save_analysis(
        analysis.analysis_id,
        analysis.generated_at.isoformat(),
        analysis.model_dump_json(),
    )

    status = manual_review_status(storage, tmp_path / "exchange", request_id)

    assert status["state"] == "completed"
    assert status["progress_pct"] == 100
    assert status["analysis"]["summary"] == "No trade; spread too wide."
    assert is_analysis_only_request(storage, request_id) is True


def test_router_uses_explicit_provider_per_request(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("PARQUET_STRATEGY_QUEUE", str(tmp_path / "exchange"))
    router = RoutedStrategyWorker("local_ollama")
    request = ReviewRequest(
        request_id="request-1",
        requested_at=datetime(2026, 9, 19, 12, 0, tzinfo=UTC),
        reason="manual_web:codex_cli",
        context={
            "_parquet_control": {
                "strategy_provider": "codex_cli",
                "analysis_only": True,
            }
        },
    )

    assert router.provider_for(request) == "codex_cli"
    assert router.is_explicit(request) is True
