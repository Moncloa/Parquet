from datetime import UTC, datetime, timedelta

import httpx
import pytest

from parquet import strategy_worker
from parquet.config import StrategyConfig
from parquet.models import MarketAnalysis, ReviewRequest
from parquet.strategy_local import LocalStrategySettings, LocalStrategyWorker


def test_strategy_config_accepts_local_ollama() -> None:
    config = StrategyConfig(enabled=True, provider="local_ollama")
    assert config.provider == "local_ollama"


def test_local_strategy_settings_reject_non_loopback(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PARQUET_LOCAL_LLM_URL", "https://example.com")
    with pytest.raises(ValueError, match="loopback"):
        LocalStrategySettings.from_env()


@pytest.mark.asyncio
async def test_local_strategy_worker_validates_structured_analysis(tmp_path) -> None:
    now = datetime.now(UTC)
    request = ReviewRequest(
        request_id="local-request-1",
        requested_at=now,
        reason="test",
        symbols=[],
        context={},
    )
    expected = MarketAnalysis(
        schema_version=1,
        analysis_id="local-analysis-1",
        review_request_id=request.request_id,
        generated_at=now + timedelta(seconds=1),
        market_regime="test",
        summary="No supplied candidate is tradable.",
        sources=[],
        watch=[],
        trade_proposals=[],
        next_review=None,
    )

    async def handler(http_request: httpx.Request) -> httpx.Response:
        assert http_request.url.path == "/api/chat"
        return httpx.Response(
            200,
            json={"message": {"role": "assistant", "content": expected.model_dump_json()}},
        )

    previous_prompt = strategy_worker.__dict__.get("_strategy_prompt")
    strategy_worker.__dict__["_strategy_prompt"] = lambda _: "Return the supplied valid test result."
    try:
        worker = LocalStrategyWorker(
            LocalStrategySettings(queue_dir=tmp_path),
            transport=httpx.MockTransport(handler),
        )
        analysis = await worker.analyze(request)
    finally:
        if previous_prompt is None:
            strategy_worker.__dict__.pop("_strategy_prompt", None)
        else:
            strategy_worker.__dict__["_strategy_prompt"] = previous_prompt

    assert analysis.analysis_id == expected.analysis_id
    assert analysis.review_request_id == request.request_id
