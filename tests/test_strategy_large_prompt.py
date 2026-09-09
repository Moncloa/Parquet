import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from parquet.models import ReviewRequest
from parquet.strategy import CodexWorkerSettings
from parquet.strategy_worker import StdinCodexStrategyWorker, _compact_strategy_request


def _wide_request(*, point_count: int = 2000) -> ReviewRequest:
    now = datetime.now(UTC)
    points = [
        {
            "t": now.isoformat(),
            "p": 100.0 + index / 1000.0,
            "padding": "x" * 100,
        }
        for index in range(point_count)
    ]
    return ReviewRequest(
        request_id="wide-request-001",
        requested_at=now,
        reason="wide_market_test",
        symbols=["TEST"],
        context={
            "market_data": {
                "quotes": {
                    "TEST": {
                        "instrument_id": 1,
                        "bid": 100.0,
                        "ask": 100.1,
                        "timestamp": now.isoformat(),
                        "stale": False,
                    }
                },
                "wide_scanner": {
                    "ranking": "time_window_momentum_quality_v2",
                    "candidates": [
                        {
                            "instrument_id": 1,
                            "symbol": "TEST",
                            "score": 1.25,
                            "change_pct_2m": 0.2,
                            "change_pct_5m": 0.4,
                            "change_pct_15m": 0.7,
                            "persistence": 0.9,
                            "directional_efficiency": 0.8,
                            "points": points,
                        }
                    ],
                },
            }
        },
    )


def _analysis_json(request: ReviewRequest) -> str:
    return json.dumps(
        {
            "schema_version": 1,
            "analysis_id": "analysis-large-prompt",
            "review_request_id": request.request_id,
            "generated_at": datetime.now(UTC).isoformat(),
            "market_regime": "test",
            "summary": "NO TRADE",
            "sources": ["https://example.com/market"],
            "watch": [],
            "trade_proposals": [],
            "next_review": None,
        }
    )


def test_compact_strategy_request_drops_only_raw_points() -> None:
    request = _wide_request(point_count=3)
    compact = _compact_strategy_request(request)

    original_wide = request.context["market_data"]["wide_scanner"]
    compact_wide = compact.context["market_data"]["wide_scanner"]
    original_candidate = original_wide["candidates"][0]
    compact_candidate = compact_wide["candidates"][0]

    assert "points" in original_candidate
    assert "points" not in compact_candidate
    assert compact_candidate["score"] == 1.25
    assert compact_candidate["change_pct_15m"] == 0.7
    assert compact_wide["raw_points_omitted_from_strategy_prompt"] is True


@pytest.mark.asyncio
async def test_large_review_prompt_uses_stdin_sentinel(tmp_path, monkeypatch) -> None:
    request = _wide_request()
    captured: dict[str, object] = {}

    async def fake_run_codex_stdin(
        args: list[str],
        *,
        prompt: str,
        cwd: Path,
        env: dict[str, str],
        timeout_seconds: int,
    ) -> None:
        captured["args"] = list(args)
        captured["prompt"] = prompt
        captured["cwd"] = cwd
        captured["env"] = dict(env)
        output_index = args.index("--output-last-message") + 1
        Path(args[output_index]).write_text(_analysis_json(request), encoding="utf-8")

    monkeypatch.setattr("parquet.strategy_worker._run_codex_stdin", fake_run_codex_stdin)
    worker = StdinCodexStrategyWorker(
        CodexWorkerSettings(
            queue_dir=tmp_path / "exchange",
            codex_home=tmp_path / "codex",
            work_dir=tmp_path / "work",
        )
    )

    analysis = await worker.analyze(request)

    args = captured["args"]
    assert isinstance(args, list)
    assert args[-1] == "-"
    assert all(len(item) < 10_000 for item in args)

    prompt = captured["prompt"]
    assert isinstance(prompt, str)
    assert '"points"' not in prompt
    assert '"score": 1.25' in prompt
    assert len(request.model_dump_json()) > 200_000
    assert analysis.review_request_id == request.request_id
