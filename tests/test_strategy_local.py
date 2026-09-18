from datetime import UTC, datetime

import httpx
import pytest

from parquet.config import StrategyConfig
from parquet.models import ReviewRequest, Side
from parquet.strategy_local import (
    LocalStrategyDecision,
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


def test_local_strategy_request_uses_screener_shortlist_and_metrics_only() -> None:
    now = datetime.now(UTC)
    request = ReviewRequest(
        request_id="compact-local-1",
        requested_at=now,
        reason="test",
        symbols=["AAA", "BBB", "CCC"],
        context={
            "market_data": {
                "provider": "etoro",
                "captured_at": now.isoformat(),
                "quotes": {
                    "AAA": {"bid": 10.0, "ask": 10.1, "stale": False, "extra": "drop"},
                    "BBB": {"bid": 20.0, "ask": 20.1, "stale": False},
                    "CCC": {"bid": 30.0, "ask": 30.1, "stale": False},
                },
                "history": {
                    symbol: {
                        "sample_count": 20,
                        "span_minutes": 20.0,
                        "points": [{"t": str(i), "p": float(i + 1)} for i in range(10)],
                        "metrics": {"change_pct_5m": 1.0, "high_60m": 21.0},
                    }
                    for symbol in ("AAA", "BBB", "CCC")
                },
                "wide_scanner": {
                    "source": "live_service_scanner",
                    "ranking": "test",
                    "subscribed_instruments": 500,
                    "candidates": [
                        {
                            "symbol": "AAA",
                            "score": 80.0,
                            "change_pct_5m": 1.1,
                            "points": [1, 2, 3],
                            "name": "drop",
                        },
                        {
                            "symbol": "BBB",
                            "score": 90.0,
                            "change_pct_5m": 1.5,
                            "points": [4, 5, 6],
                        },
                        {
                            "symbol": "CCC",
                            "score": 70.0,
                            "change_pct_5m": 0.5,
                            "points": [7, 8, 9],
                        },
                    ],
                    "local_screener": {
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
                    },
                },
            },
            "risk_snapshot": {"equity_usd": 10000.0, "open_positions": 1},
            "unrelated": {"large": "drop me"},
        },
    )

    compact = _compact_local_strategy_request(
        request,
        LocalStrategySettings(max_candidates=2),
    )

    assert compact.symbols == ["BBB", "AAA"]
    assert set(compact.context) == {"market_data", "risk_snapshot"}
    market_data = compact.context["market_data"]
    assert isinstance(market_data, dict)
    assert list(market_data["quotes"]) == ["AAA", "BBB"]
    assert "extra" not in market_data["quotes"]["AAA"]
    assert list(market_data["history"]) == ["AAA", "BBB"]
    assert all(
        "points" not in value
        for value in market_data["history"].values()
        if isinstance(value, dict)
    )

    scanner = market_data["wide_scanner"]
    assert isinstance(scanner, dict)
    assert scanner["selected_for_local_strategy"] == ["BBB", "AAA"]
    assert "subscribed_instruments" not in scanner
    assert [item["symbol"] for item in scanner["candidates"]] == ["AAA", "BBB"]
    assert all("points" not in item and "name" not in item for item in scanner["candidates"])

    local_screener = scanner["local_screener"]
    assert isinstance(local_screener, dict)
    assert "telemetry" not in local_screener
    assert [item["symbol"] for item in local_screener["shortlist"]] == ["BBB", "AAA"]


@pytest.mark.asyncio
async def test_local_strategy_worker_maps_compact_decision_to_market_analysis(tmp_path) -> None:
    now = datetime.now(UTC)
    request = ReviewRequest(
        request_id="local-request-1",
        requested_at=now,
        reason="test",
        symbols=["AAA"],
        context={
            "market_data": {
                "provider": "etoro",
                "captured_at": now.isoformat(),
                "quotes": {
                    "AAA": {
                        "bid": 10.0,
                        "ask": 10.1,
                        "timestamp": now.isoformat(),
                        "age_seconds": 0.0,
                        "stale": False,
                    }
                },
                "history": {
                    "AAA": {
                        "sample_count": 20,
                        "span_minutes": 20.0,
                        "metrics": {"change_pct_5m": 1.0},
                    }
                },
                "wide_scanner": {
                    "source": "live_service_scanner",
                    "ranking": "test",
                    "candidates": [{"symbol": "AAA", "score": 90.0}],
                    "local_screener": {
                        "status": "ok",
                        "model": "qwen3.5:4b",
                        "shortlist": [
                            {
                                "symbol": "AAA",
                                "classification": "MOMENTUM",
                                "score": 90,
                                "reason": "clean continuation",
                            }
                        ],
                    },
                },
            }
        },
    )
    decision = LocalStrategyDecision.model_validate(
        {
            "market_regime": "momentum",
            "summary": "AAA has the cleanest supplied setup.",
            "proposal": {
                "symbol": "AAA",
                "side": "BUY",
                "stop_loss": 9.9,
                "take_profit": 10.4,
                "confidence": 0.72,
                "ttl_minutes": 10,
                "rationale": "positive short-term momentum",
                "risk": "momentum can fade",
            },
            "watch": None,
            "next_review_minutes": 5,
        }
    )

    async def handler(http_request: httpx.Request) -> httpx.Response:
        assert http_request.url.path == "/api/chat"
        payload = __import__("json").loads(http_request.content)
        assert payload["options"]["num_ctx"] == 4096
        assert payload["options"]["num_predict"] == 384
        return httpx.Response(
            200,
            json={
                "message": {"role": "assistant", "content": decision.model_dump_json()},
                "prompt_eval_count": 180,
                "eval_count": 95,
                "eval_duration": 47_500_000_000,
            },
        )

    worker = LocalStrategyWorker(
        LocalStrategySettings(queue_dir=tmp_path),
        transport=httpx.MockTransport(handler),
    )
    analysis = await worker.analyze(request)

    assert analysis.review_request_id == request.request_id
    assert analysis.sources == []
    assert len(analysis.trade_proposals) == 1
    proposal = analysis.trade_proposals[0]
    assert proposal.symbol == "AAA"
    assert proposal.side == Side.BUY
    assert proposal.entry == 10.1
    assert proposal.stop_loss == 9.9
    assert proposal.take_profit == 10.4
    assert worker._last_inference["prompt_tokens"] == 180
    assert worker._last_inference["eval_tokens"] == 95
    assert worker._last_inference["eval_tokens_per_second"] == 2.0
