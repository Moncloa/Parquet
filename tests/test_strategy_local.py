from datetime import UTC, datetime

import httpx
import pytest

from parquet.config import StrategyConfig
from parquet.models import ReviewRequest, Side
from parquet.strategy_local import (
    LocalStrategyDecision,
    LocalStrategySettings,
    LocalStrategyWorker,
    _local_prompt_data,
)


def test_strategy_config_accepts_local_ollama() -> None:
    config = StrategyConfig(enabled=True, provider="local_ollama")
    assert config.provider == "local_ollama"


def test_local_strategy_settings_reject_non_loopback(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PARQUET_LOCAL_LLM_URL", "https://example.com")
    with pytest.raises(ValueError, match="loopback"):
        LocalStrategySettings.from_env()


def test_local_prompt_data_uses_two_fresh_qwen_candidates_and_aliases() -> None:
    now = datetime.now(UTC)
    request = ReviewRequest(
        request_id="flat-local-1",
        requested_at=now,
        reason="test",
        symbols=["AAA", "BBB", "CCC", "AIR.PA"],
        context={
            "market_data": {
                "quotes": {
                    "AAA": {
                        "bid": 10.0,
                        "ask": 10.1,
                        "age_seconds": 1.2,
                        "stale": False,
                    },
                    "BBB": {
                        "bid": 20.0,
                        "ask": 20.1,
                        "age_seconds": 1.0,
                        "stale": False,
                    },
                    "CCC": {
                        "bid": 30.0,
                        "ask": 30.1,
                        "age_seconds": 80.0,
                        "stale": True,
                    },
                    "AIR.PA": {
                        "bid": 200.0,
                        "ask": 200.2,
                        "stale": False,
                    },
                },
                "history": {
                    "AAA": {
                        "metrics": {
                            "change_pct_5m": 1.0,
                            "change_pct_15m": 1.5,
                            "high_60m": 10.2,
                            "low_60m": 9.8,
                            "ignored": 999,
                        }
                    },
                    "BBB": {
                        "metrics": {
                            "change_pct_5m": 2.0,
                            "range_pct_60m": 3.0,
                        }
                    },
                },
                "wide_scanner": {
                    "candidates": [
                        {
                            "symbol": "AAA",
                            "score": 80.0,
                            "directional_efficiency": 0.7,
                            "persistence": 0.8,
                            "points": [1, 2, 3],
                        },
                        {
                            "symbol": "BBB",
                            "score": 90.0,
                            "change_pct_5m": 2.0,
                            "spread_bps": 4.0,
                        },
                        {"symbol": "CCC", "score": 95.0},
                        {"symbol": "AIR.PA", "score": 99.0},
                    ],
                    "local_screener": {
                        "shortlist": [
                            {
                                "symbol": "CCC",
                                "classification": "MOMENTUM",
                                "score": 95,
                                "reason": "stale candidate",
                            },
                            {
                                "symbol": "AIR.PA",
                                "classification": "MOMENTUM",
                                "score": 94,
                                "reason": "excluded",
                            },
                            {
                                "symbol": "BBB",
                                "classification": "MOMENTUM",
                                "score": 90,
                                "reason": "clean continuation",
                            },
                            {
                                "symbol": "AAA",
                                "classification": "WATCH",
                                "score": 70,
                                "reason": "secondary setup",
                            },
                        ]
                    },
                },
            },
            "risk_snapshot": {"equity_usd": 10000.0},
        },
    )

    data, selected = _local_prompt_data(request, 2)

    assert selected == ["BBB", "AAA"]
    candidates = data["c"]
    assert isinstance(candidates, list)
    assert [item["s"] for item in candidates] == ["BBB", "AAA"]
    assert candidates[0]["b"] == 20.0
    assert candidates[0]["a"] == 20.1
    assert candidates[0]["h"] == {"c5": 2.0, "range60": 3.0}
    assert candidates[0]["r"] == {"score": 90.0, "c5": 2.0, "spread": 4.0}
    assert candidates[0]["q"]["class"] == "MOMENTUM"
    assert candidates[1]["h"]["c15"] == 1.5
    assert "ignored" not in candidates[1]["h"]


@pytest.mark.asyncio
async def test_local_strategy_worker_maps_flat_buy_to_market_analysis(tmp_path) -> None:
    now = datetime.now(UTC)
    request = ReviewRequest(
        request_id="local-request-1",
        requested_at=now,
        reason="test",
        symbols=["AAA"],
        context={
            "market_data": {
                "quotes": {
                    "AAA": {
                        "bid": 10.0,
                        "ask": 10.1,
                        "age_seconds": 0.0,
                        "stale": False,
                    }
                },
                "history": {
                    "AAA": {
                        "metrics": {
                            "change_pct_5m": 1.0,
                            "high_60m": 10.2,
                            "low_60m": 9.8,
                        }
                    }
                },
                "wide_scanner": {
                    "candidates": [
                        {
                            "symbol": "AAA",
                            "score": 90.0,
                            "directional_efficiency": 0.8,
                        }
                    ],
                    "local_screener": {
                        "shortlist": [
                            {
                                "symbol": "AAA",
                                "classification": "MOMENTUM",
                                "score": 90,
                                "reason": "clean continuation",
                            }
                        ]
                    },
                },
            }
        },
    )
    decision = LocalStrategyDecision.model_validate(
        {
            "action": "BUY",
            "symbol": "AAA",
            "stop": 9.9,
            "target": 10.4,
            "trigger": None,
            "confidence": 0.72,
            "reason": "positive short-term momentum",
        }
    )

    async def handler(http_request: httpx.Request) -> httpx.Response:
        assert http_request.url.path == "/api/chat"
        payload = __import__("json").loads(http_request.content)
        assert payload["options"]["num_ctx"] == 3072
        assert payload["options"]["num_predict"] == 224
        assert len(payload["messages"][1]["content"]) < 2000
        return httpx.Response(
            200,
            json={
                "message": {"role": "assistant", "content": decision.model_dump_json()},
                "prompt_eval_count": 700,
                "eval_count": 110,
                "prompt_eval_duration": 35_000_000_000,
                "eval_duration": 30_000_000_000,
            },
        )

    worker = LocalStrategyWorker(
        LocalStrategySettings(queue_dir=tmp_path),
        transport=httpx.MockTransport(handler),
    )
    analysis = await worker.analyze(request)

    assert analysis.review_request_id == request.request_id
    assert analysis.sources == []
    assert analysis.market_regime == "local_compact"
    assert len(analysis.trade_proposals) == 1
    proposal = analysis.trade_proposals[0]
    assert proposal.symbol == "AAA"
    assert proposal.side == Side.BUY
    assert proposal.entry == 10.1
    assert proposal.stop_loss == 9.9
    assert proposal.take_profit == 10.4
    assert worker._last_inference["prompt_tokens"] == 700
    assert worker._last_inference["eval_tokens"] == 110


@pytest.mark.asyncio
async def test_local_strategy_worker_skips_ollama_without_fresh_candidates(tmp_path) -> None:
    now = datetime.now(UTC)
    request = ReviewRequest(
        request_id="local-empty-1",
        requested_at=now,
        reason="test",
        symbols=["AAA"],
        context={
            "market_data": {
                "quotes": {
                    "AAA": {
                        "bid": 10.0,
                        "ask": 10.1,
                        "stale": True,
                    }
                }
            }
        },
    )

    worker = LocalStrategyWorker(
        LocalStrategySettings(queue_dir=tmp_path),
        transport=httpx.MockTransport(
            lambda _: pytest.fail("Ollama should not be called")
        ),
    )
    analysis = await worker.analyze(request)

    assert analysis.trade_proposals == []
    assert analysis.watch == []
    assert worker._last_inference["status"] == "skipped_no_fresh_candidates"
