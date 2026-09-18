from datetime import UTC, datetime, timedelta

import httpx
import pytest

from parquet import strategy_worker
from parquet.config import StrategyConfig
from parquet.models import MarketAnalysis, ReviewRequest
from parquet.strategy_local import (
    LocalStrategySettings,
    LocalStrategyWorker,
    _compact_local_strategy_request,
)


def test_strategy_config_accepts_local_ollama() -> None:
    config = StrategyConfig(enabled=True, provider="local_ollama")
    assert config.provider == "local_ollama"


def test_local_strategy_settings_reject_non_loopback(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PARQUET_LOCAL_LLM_URL", "https://example.com")
    with pytest.raises(ValueError, match="loopback"):
        LocalStrategySettings.from_env()


def test_local_strategy_request_uses_screener_shortlist_and_bounds_history() -> None:
    now = datetime.now(UTC)
    request = ReviewRequest(
        request_id="compact-local-1",
        requested_at=now,
        reason="test",
        symbols=["AAA", "BBB", "CCC"],
        context={
            "market_data": {
                "quotes": {
                    "AAA": {"bid": 10.0, "ask": 10.1},
                    "BBB": {"bid": 20.0, "ask": 20.1},
                    "CCC": {"bid": 30.0, "ask": 30.1},
                },
                "history": {
                    symbol: {
                        "sample_count": 20,
                        "span_minutes": 20.0,
                        "points": [{"t": str(i), "p": float(i + 1)} for i in range(10)],
                        "metrics": {"change_pct_5m": 1.0},
                    }
                    for symbol in ("AAA", "BBB", "CCC")
                },
                "wide_scanner": {
                    "enabled": True,
                    "source": "live_service_scanner",
                    "ranking": "test",
                    "filters": {},
                    "subscribed_instruments": 500,
                    "candidates": [
                        {"symbol": "AAA", "score": 80.0, "points": [1, 2, 3]},
                        {"symbol": "BBB", "score": 90.0, "points": [4, 5, 6]},
                        {"symbol": "CCC", "score": 70.0, "points": [7, 8, 9]},
                    ],
                    "local_screener": {
                        "enabled": True,
                        "status": "ok",
                        "model": "qwen3.5:4b",
                        "telemetry": {"prompt_tokens": 1234},
                        "shortlist": [
                            {
                                "symbol": "BBB",
                                "classification": "MOMENTUM",
                                "score": 88,
                                "reason": "clean continuation",
                            },
                            {
                                "symbol": "AAA",
                                "classification": "WATCH",
                                "score": 65,
                                "reason": "interesting",
                            },
                        ],
                        "advisory_only": True,
                    },
                },
            },
            "risk_snapshot": {"equity_usd": 10000.0, "open_positions": 1},
        },
    )

    settings = LocalStrategySettings(
        max_candidates=2,
        history_points=3,
        max_output_tokens=768,
    )
    compact = _compact_local_strategy_request(request, settings)

    assert compact.symbols == ["BBB", "AAA"]
    market_data = compact.context["market_data"]
    assert isinstance(market_data, dict)
    assert list(market_data["quotes"]) == ["AAA", "BBB"]
    assert list(market_data["history"]) == ["AAA", "BBB"]
    assert all(
        len(value["points"]) <= 3
        for value in market_data["history"].values()
        if isinstance(value, dict)
    )

    scanner = market_data["wide_scanner"]
    assert isinstance(scanner, dict)
    assert scanner["selected_for_local_strategy"] == ["BBB", "AAA"]
    assert "subscribed_instruments" not in scanner
    assert [item["symbol"] for item in scanner["candidates"]] == ["AAA", "BBB"]
    assert all("points" not in item for item in scanner["candidates"])

    local_screener = scanner["local_screener"]
    assert isinstance(local_screener, dict)
    assert "telemetry" not in local_screener
    assert [item["symbol"] for item in local_screener["shortlist"]] == ["BBB", "AAA"]


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
        payload = __import__("json").loads(http_request.content)
        assert payload["options"]["num_predict"] == 768
        return httpx.Response(
            200,
            json={
                "message": {"role": "assistant", "content": expected.model_dump_json()},
                "prompt_eval_count": 250,
                "eval_count": 80,
                "eval_duration": 40_000_000_000,
            },
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
    assert worker._last_inference["prompt_tokens"] == 250
    assert worker._last_inference["eval_tokens"] == 80
    assert worker._last_inference["eval_tokens_per_second"] == 2.0
