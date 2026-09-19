from datetime import UTC, datetime
from pathlib import Path

from parquet.config import ExecutionConfig, Settings, StrategyConfig
from parquet.controls import (
    begin_manual_review,
    is_analysis_only_request,
    manual_review_status,
)
from parquet.models import MarketAnalysis, ReviewRequest, Side, TradeProposal
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



def test_operational_review_is_not_analysis_only(tmp_path: Path) -> None:
    storage = Storage(tmp_path / "state.db")
    orchestrator = FakeOrchestrator(storage)
    settings = Settings(
        strategy=StrategyConfig(
            enabled=True,
            provider="local_ollama",
            queue_dir=tmp_path / "exchange",
        ),
        execution=ExecutionConfig(
            autonomous_enabled=True,
            autonomous_mode="shadow",
        ),
    )

    request_id = begin_manual_review(
        settings,
        orchestrator,
        provider="codex_cli",
        allow_execution=True,
    )
    status = manual_review_status(storage, settings.strategy.queue_dir, request_id)

    assert status["analysis_only"] is False
    assert status["allow_execution"] is True
    assert status["execution_mode"] == "shadow"


def test_operational_review_tracks_execution_rejection(tmp_path: Path) -> None:
    storage = Storage(tmp_path / "state.db")
    request_id = "operational-review-1"
    generated_at = datetime(2026, 9, 19, 12, 1, tzinfo=UTC)
    proposal = TradeProposal(
        proposal_id="proposal-1",
        symbol="STRK",
        side=Side.SELL,
        entry=50.0,
        stop_loss=51.0,
        take_profit=47.0,
        confidence=0.8,
        generated_at=generated_at,
        expires_at=datetime(2026, 9, 19, 12, 15, tzinfo=UTC),
    )
    analysis = MarketAnalysis(
        analysis_id="analysis-operational-1",
        review_request_id=request_id,
        generated_at=generated_at,
        market_regime="momentum",
        summary="STRK downside momentum",
        trade_proposals=[proposal],
    )
    storage.add_event(
        "manual_review_requested",
        (
            '{"request_id":"operational-review-1","provider":"codex_cli",'
            '"analysis_only":false,"allow_execution":true,"execution_mode":"real"}'
        ),
    )
    storage.save_analysis(
        analysis.analysis_id,
        analysis.generated_at.isoformat(),
        analysis.model_dump_json(),
    )
    storage.add_event(
        "direct_proposal_execution_rejected",
        (
            '{"proposal_id":"proposal-1","symbol":"STRK",'
            '"reasons":["spread_too_wide"]}'
        ),
    )

    status = manual_review_status(storage, tmp_path / "exchange", request_id)

    assert status["state"] == "completed"
    assert status["progress_pct"] == 100
    assert status["execution"][0]["state"] == "REJECTED"
    assert status["execution"][0]["reason"] == "spread_too_wide"


def test_operational_review_without_proposals_completes(tmp_path: Path) -> None:
    storage = Storage(tmp_path / "state.db")
    request_id = "operational-review-no-trade"
    storage.add_event(
        "manual_review_requested",
        (
            '{"request_id":"operational-review-no-trade","provider":"local_ollama",'
            '"analysis_only":false,"allow_execution":true,"execution_mode":"real"}'
        ),
    )
    analysis = MarketAnalysis(
        analysis_id="analysis-no-trade",
        review_request_id=request_id,
        generated_at=datetime(2026, 9, 19, 12, 1, tzinfo=UTC),
        summary="No trade",
    )
    storage.save_analysis(
        analysis.analysis_id,
        analysis.generated_at.isoformat(),
        analysis.model_dump_json(),
    )

    status = manual_review_status(storage, tmp_path / "exchange", request_id)

    assert status["state"] == "completed"
    assert status["execution"] == []
    assert "no trade proposal" in str(status["message"]).lower()
