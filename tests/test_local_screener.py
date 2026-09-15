import json

import httpx
import pytest

from parquet.config import LocalScreenerConfig
from parquet.local_screener import LocalScreenerClient


def _candidates(count: int = 12) -> list[dict[str, object]]:
    return [
        {
            "symbol": f"SYM{index}",
            "score": 1.0 - index / 100.0,
            "change_pct_2m": 0.2 + index / 100.0,
            "change_pct_5m": 0.4 + index / 100.0,
            "change_pct_15m": 0.8 + index / 100.0,
            "directional_efficiency": 0.8,
            "persistence": 0.75,
            "spread_bps": 4.0,
            "points": [{"t": "ignored", "p": 1.0}],
        }
        for index in range(count)
    ]


@pytest.mark.asyncio
async def test_local_screener_is_batched_structured_and_non_thinking() -> None:
    captured: dict[str, object] = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        payload = json.loads(request.content)
        captured.update(payload)
        result = {
            "shortlist": [
                {
                    "symbol": "SYM0",
                    "classification": "MOMENTUM",
                    "score": 91,
                    "reason": "Strong multi-window move with high directional efficiency.",
                },
                {
                    "symbol": "SYM1",
                    "classification": "WATCH",
                    "score": 72,
                    "reason": "Positive momentum but weaker relative ranking.",
                },
            ]
        }
        return httpx.Response(
            200,
            json={
                "message": {"content": json.dumps(result)},
                "load_duration": 5_000_000,
                "prompt_eval_count": 200,
                "prompt_eval_duration": 2_000_000_000,
                "eval_count": 60,
                "eval_duration": 10_000_000_000,
            },
        )

    config = LocalScreenerConfig(input_candidates=10, output_candidates=3)
    client = LocalScreenerClient(config, transport=httpx.MockTransport(handler))
    result, elapsed = await client.screen(_candidates())

    assert [item.symbol for item in result.shortlist] == ["SYM0", "SYM1"]
    assert elapsed >= 0
    assert captured["model"] == "qwen3.5:4b"
    assert captured["think"] is False
    assert captured["stream"] is False
    assert isinstance(captured["format"], dict)
    messages = captured["messages"]
    assert isinstance(messages, list)
    prompt = messages[1]["content"]
    assert "SYM9" in prompt
    assert "SYM10" not in prompt
    assert '"points"' not in prompt
    assert '"deterministic_rank_score"' in prompt
    assert "independent advisory confidence" in prompt
    assert captured["options"]["num_predict"] == 128
    assert client.last_telemetry == {
        "load_ms": 5.0,
        "prompt_tokens": 200,
        "prompt_ms": 2000.0,
        "eval_tokens": 60,
        "eval_ms": 10000.0,
        "eval_tokens_per_second": 6.0,
    }


@pytest.mark.asyncio
async def test_local_screener_rejects_invented_symbol() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        result = {
            "shortlist": [
                {
                    "symbol": "AIR.PA",
                    "classification": "MOMENTUM",
                    "score": 99,
                    "reason": "Invented candidate.",
                }
            ]
        }
        return httpx.Response(200, json={"message": {"content": json.dumps(result)}})

    client = LocalScreenerClient(
        LocalScreenerConfig(),
        transport=httpx.MockTransport(handler),
    )
    with pytest.raises(RuntimeError, match="unknown symbols"):
        await client.screen(_candidates())


@pytest.mark.asyncio
async def test_local_screener_rejects_zero_advisory_score() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        result = {
            "shortlist": [
                {
                    "symbol": "SYM0",
                    "classification": "WATCH",
                    "score": 0,
                    "reason": "Copied deterministic score.",
                }
            ]
        }
        return httpx.Response(200, json={"message": {"content": json.dumps(result)}})

    client = LocalScreenerClient(
        LocalScreenerConfig(),
        transport=httpx.MockTransport(handler),
    )
    with pytest.raises(RuntimeError, match="invalid structured output"):
        await client.screen(_candidates())


def test_local_screener_endpoint_is_loopback_only() -> None:
    with pytest.raises(ValueError, match="loopback"):
        LocalScreenerConfig(base_url="http://192.168.50.22:11434")
