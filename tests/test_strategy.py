import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from parquet.models import MarketAnalysis, ReviewRequest
from parquet.storage import Storage
from parquet.strategy import (
    CodexStrategyWorker,
    CodexWorkerSettings,
    StrategyDispatcher,
    StrategyQueue,
)


def _request(*, stale: bool = False, symbol: str = "EURUSD") -> ReviewRequest:
    now = datetime.now(UTC)
    return ReviewRequest(
        request_id="request-001",
        requested_at=now,
        reason="test",
        symbols=[symbol],
        context={
            "market_data": {
                "quotes": {
                    symbol: {
                        "instrument_id": 1,
                        "bid": 1.1000,
                        "ask": 1.1001,
                        "timestamp": now.isoformat(),
                        "stale": stale,
                    }
                }
            },
            "risk_snapshot": {
                "as_of": now.isoformat(),
                "equity_usd": 10_000.0,
                "open_positions": 0,
            },
        },
    )


def _analysis_json(request: ReviewRequest, *, proposal: bool = False) -> str:
    now = datetime.now(UTC)
    proposals = []
    if proposal:
        proposals.append(
            {
                "proposal_id": "proposal-001",
                "symbol": request.symbols[0],
                "side": "BUY",
                "entry": 1.1001,
                "stop_loss": 1.0990,
                "take_profit": 1.1020,
                "confidence": 0.7,
                "generated_at": now.isoformat(),
                "expires_at": (now + timedelta(minutes=10)).isoformat(),
                "thesis": ["test"],
                "risks": ["test"],
            }
        )
    return json.dumps(
        {
            "schema_version": 1,
            "analysis_id": "analysis-001",
            "review_request_id": request.request_id,
            "generated_at": now.isoformat(),
            "market_regime": "test",
            "summary": "NO TRADE" if not proposal else "Test proposal",
            "sources": ["https://example.com/market"],
            "watch": [],
            "trade_proposals": proposals,
            "next_review": None,
        }
    )


@pytest.mark.asyncio
async def test_codex_worker_uses_restricted_structured_exec(tmp_path, monkeypatch) -> None:
    request = _request()
    captured: dict[str, object] = {}

    async def fake_run_codex(args, *, cwd, env, timeout_seconds):
        captured["args"] = list(args)
        captured["cwd"] = cwd
        captured["env"] = dict(env)
        output_index = args.index("--output-last-message") + 1
        Path(args[output_index]).write_text(_analysis_json(request), encoding="utf-8")

    monkeypatch.setattr("parquet.strategy._run_codex", fake_run_codex)
    worker = CodexStrategyWorker(
        CodexWorkerSettings(
            queue_dir=tmp_path / "exchange",
            codex_home=tmp_path / "codex",
            work_dir=tmp_path / "work",
            web_search=True,
        )
    )
    analysis = await worker.analyze(request)

    args = captured["args"]
    assert isinstance(args, list)
    assert "--search" in args
    assert args[args.index("--ask-for-approval") + 1] == "never"
    assert args[args.index("--sandbox") + 1] == "read-only"
    assert "--output-schema" in args
    assert "--ephemeral" in args
    env = captured["env"]
    assert isinstance(env, dict)
    assert "OPENAI_API_KEY" not in env
    assert analysis.review_request_id == request.request_id


@pytest.mark.asyncio
async def test_codex_worker_rejects_stale_quote_proposal(tmp_path, monkeypatch) -> None:
    request = _request(stale=True, symbol="GOLD")

    async def fake_run_codex(args, *, cwd, env, timeout_seconds):
        output_index = args.index("--output-last-message") + 1
        Path(args[output_index]).write_text(
            _analysis_json(request, proposal=True), encoding="utf-8"
        )

    monkeypatch.setattr("parquet.strategy._run_codex", fake_run_codex)
    worker = CodexStrategyWorker(
        CodexWorkerSettings(
            queue_dir=tmp_path / "exchange",
            codex_home=tmp_path / "codex",
            work_dir=tmp_path / "work",
        )
    )

    with pytest.raises(RuntimeError, match="stale"):
        await worker.analyze(request)


@pytest.mark.asyncio
async def test_codex_worker_rejects_wrong_request_id(tmp_path, monkeypatch) -> None:
    request = _request()
    wrong = json.loads(_analysis_json(request))
    wrong["review_request_id"] = "different-request"

    async def fake_run_codex(args, *, cwd, env, timeout_seconds):
        output_index = args.index("--output-last-message") + 1
        Path(args[output_index]).write_text(json.dumps(wrong), encoding="utf-8")

    monkeypatch.setattr("parquet.strategy._run_codex", fake_run_codex)
    worker = CodexStrategyWorker(
        CodexWorkerSettings(
            queue_dir=tmp_path / "exchange",
            codex_home=tmp_path / "codex",
            work_dir=tmp_path / "work",
        )
    )

    with pytest.raises(RuntimeError, match="review_request_id"):
        await worker.analyze(request)


def test_dispatcher_does_not_replay_historical_reviews(tmp_path) -> None:
    storage = Storage(tmp_path / "parquet.db")
    historical = _request()
    storage.add_event("review_request", historical.model_dump_json())

    class FakeBridge:
        async def post_analysis(self, analysis: MarketAnalysis) -> None:
            raise AssertionError("not used")

    dispatcher = StrategyDispatcher(
        queue_dir=tmp_path / "exchange",
        state_db=tmp_path / "parquet.db",
        storage=storage,
        bridge=FakeBridge(),  # type: ignore[arg-type]
    )

    assert dispatcher.poll_requests_once() == 0
    assert StrategyQueue(tmp_path / "exchange").pending_count() == 0

    new_request = historical.model_copy(update={"request_id": "request-002"})
    storage.add_event("review_request", new_request.model_dump_json())
    assert dispatcher.poll_requests_once() == 1
    assert StrategyQueue(tmp_path / "exchange").pending_count() == 1


@pytest.mark.asyncio
async def test_dispatcher_publishes_valid_result_and_acks_queue(tmp_path) -> None:
    storage = Storage(tmp_path / "parquet.db")
    queue = StrategyQueue(tmp_path / "exchange")
    request = _request()
    queue.enqueue(request)
    analysis = MarketAnalysis.model_validate_json(_analysis_json(request))
    queue.write_result(request.request_id, analysis)
    published: list[MarketAnalysis] = []

    class FakeBridge:
        async def post_analysis(self, item: MarketAnalysis) -> None:
            published.append(item)

    dispatcher = StrategyDispatcher(
        queue_dir=tmp_path / "exchange",
        state_db=tmp_path / "parquet.db",
        storage=storage,
        bridge=FakeBridge(),  # type: ignore[arg-type]
    )
    assert await dispatcher.poll_results_once() == 1
    assert published == [analysis]
    assert queue.pending_count() == 0
    assert storage.get("strategy_last_analysis_id") == analysis.analysis_id
