from __future__ import annotations

import asyncio
import copy
import json
import os
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from uuid import uuid4

import httpx
from pydantic import BaseModel, Field

from parquet.models import (
    Bias,
    MarketAnalysis,
    NextReview,
    ReviewRequest,
    Side,
    TradeProposal,
    Trigger,
    TriggerAction,
    TriggerType,
    WatchItem,
)
from parquet.strategy import StrategyQueue, _redact, _validate_analysis_for_request


@dataclass(frozen=True)
class LocalStrategySettings:
    queue_dir: Path = Path("/var/lib/parquet-exchange")
    base_url: str = "http://127.0.0.1:11434"
    model: str = "hf.co/mradermacher/ODA-Fin-SFT-8B-GGUF:Q5_K_M"
    timeout_seconds: int = 300
    poll_seconds: float = 5.0
    keep_alive: str = "15m"
    context_length: int = 4096
    max_output_tokens: int = 384
    max_candidates: int = 3

    @classmethod
    def from_env(cls) -> LocalStrategySettings:
        base_url = os.getenv("PARQUET_LOCAL_LLM_URL", "http://127.0.0.1:11434").rstrip("/")
        parsed = urlparse(base_url)
        if parsed.scheme != "http" or parsed.hostname not in {"127.0.0.1", "localhost", "::1"}:
            raise ValueError("PARQUET_LOCAL_LLM_URL must use HTTP on loopback")
        model = os.getenv(
            "PARQUET_LOCAL_LLM_MODEL",
            "hf.co/mradermacher/ODA-Fin-SFT-8B-GGUF:Q5_K_M",
        ).strip()
        if not model:
            raise ValueError("PARQUET_LOCAL_LLM_MODEL cannot be empty")
        max_candidates = int(os.getenv("PARQUET_LOCAL_LLM_MAX_CANDIDATES", "3"))
        max_output_tokens = int(os.getenv("PARQUET_LOCAL_LLM_MAX_OUTPUT_TOKENS", "384"))
        if max_candidates < 1:
            raise ValueError("PARQUET_LOCAL_LLM_MAX_CANDIDATES must be at least 1")
        if max_output_tokens < 192:
            raise ValueError("PARQUET_LOCAL_LLM_MAX_OUTPUT_TOKENS must be at least 192")
        return cls(
            queue_dir=Path(os.getenv("PARQUET_STRATEGY_QUEUE", "/var/lib/parquet-exchange")),
            base_url=base_url,
            model=model,
            timeout_seconds=int(os.getenv("PARQUET_LOCAL_LLM_TIMEOUT_SECONDS", "300")),
            poll_seconds=float(os.getenv("PARQUET_STRATEGY_POLL_SECONDS", "5")),
            keep_alive=os.getenv("PARQUET_LOCAL_LLM_KEEP_ALIVE", "15m"),
            context_length=int(os.getenv("PARQUET_LOCAL_LLM_CONTEXT_LENGTH", "4096")),
            max_output_tokens=max_output_tokens,
            max_candidates=max_candidates,
        )


class LocalProposalDecision(BaseModel):
    symbol: str = Field(min_length=1)
    side: Side
    stop_loss: float = Field(gt=0)
    take_profit: float | None
    confidence: float = Field(ge=0, le=1)
    ttl_minutes: int = Field(ge=2, le=30)
    rationale: str = Field(min_length=1, max_length=180)
    risk: str | None


class LocalWatchDecision(BaseModel):
    symbol: str = Field(min_length=1)
    bias: Bias
    trigger_type: TriggerType
    trigger_price: float = Field(gt=0)
    invalidation: float | None = Field(gt=0)
    ttl_minutes: int = Field(ge=2, le=30)
    rationale: str = Field(min_length=1, max_length=180)


class LocalStrategyDecision(BaseModel):
    market_regime: str = Field(min_length=1, max_length=80)
    summary: str = Field(min_length=1, max_length=240)
    proposal: LocalProposalDecision | None
    watch: LocalWatchDecision | None
    next_review_minutes: int = Field(ge=1, le=60)


class LocalStrategyWorker:
    """Isolated recurring strategy analyst backed by loopback-only Ollama.

    Qwen's advisory shortlist constrains the expensive ODA-Fin review. ODA-Fin
    returns a compact decision, while Parquet deterministically constructs and
    validates the full MarketAnalysis object.
    """

    def __init__(
        self,
        settings: LocalStrategySettings,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.settings = settings
        self.queue = StrategyQueue(settings.queue_dir)
        self.transport = transport
        self._ready = False
        self._status_message = "not checked"
        self._last_runtime_check = 0.0
        self._last_inference: dict[str, object] = {}

    async def check_runtime(self, *, force: bool = False) -> tuple[bool, str]:
        now = time.monotonic()
        if not force and now - self._last_runtime_check < 60:
            return self._ready, self._status_message
        self._last_runtime_check = now
        try:
            async with httpx.AsyncClient(
                timeout=min(float(self.settings.timeout_seconds), 20.0),
                transport=self.transport,
            ) as client:
                version = await client.get(f"{self.settings.base_url}/api/version")
                version.raise_for_status()
                shown = await client.post(
                    f"{self.settings.base_url}/api/show",
                    json={"model": self.settings.model},
                )
                shown.raise_for_status()
        except Exception as exc:
            self._ready = False
            self._status_message = _redact(
                f"local Ollama/model unavailable: {type(exc).__name__}: {exc}"
            )[:500]
        else:
            self._ready = True
            self._status_message = f"local Ollama ready: {self.settings.model}"
        return self._ready, self._status_message

    async def analyze(self, request: ReviewRequest) -> MarketAnalysis:
        prompt_request = _compact_local_strategy_request(request, self.settings)
        prompt = _local_strategy_prompt(prompt_request)

        payload: dict[str, Any] = {
            "model": self.settings.model,
            "stream": False,
            "think": False,
            "keep_alive": self.settings.keep_alive,
            "format": LocalStrategyDecision.model_json_schema(),
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "You are Parquet's local intraday strategy analyst. "
                        "Use only supplied data. No web, no invented news, no broker authority. "
                        "Return schema-valid JSON only and keep text fields terse."
                    ),
                },
                {"role": "user", "content": prompt},
            ],
            "options": {
                "temperature": 0,
                "num_ctx": self.settings.context_length,
                "num_predict": self.settings.max_output_tokens,
            },
        }
        started = time.monotonic()
        try:
            async with httpx.AsyncClient(
                timeout=float(self.settings.timeout_seconds),
                transport=self.transport,
            ) as client:
                response = await client.post(f"{self.settings.base_url}/api/chat", json=payload)
        except httpx.RequestError as exc:
            self._last_inference = {
                "request_id": request.request_id,
                "prompt_chars": len(prompt),
                "selected_symbols": prompt_request.symbols,
                "wall_ms": round((time.monotonic() - started) * 1000.0, 1),
                "status": "request_error",
                "error": type(exc).__name__,
            }
            raise RuntimeError(
                f"local strategy request failed: {type(exc).__name__}: {exc}"
            ) from exc

        elapsed = time.monotonic() - started
        if response.is_error:
            self._last_inference = {
                "request_id": request.request_id,
                "prompt_chars": len(prompt),
                "selected_symbols": prompt_request.symbols,
                "wall_ms": round(elapsed * 1000.0, 1),
                "status": f"http_{response.status_code}",
            }
            raise RuntimeError(
                f"local strategy HTTP {response.status_code}: {_redact(response.text)[:1000]}"
            )

        try:
            body = response.json()
        except ValueError as exc:
            raise RuntimeError("local strategy returned invalid Ollama JSON") from exc

        self._last_inference = {
            "request_id": request.request_id,
            "prompt_chars": len(prompt),
            "selected_symbols": prompt_request.symbols,
            "wall_ms": round(elapsed * 1000.0, 1),
            "status": "ok",
            **_ollama_telemetry(body),
        }

        message = body.get("message") if isinstance(body, dict) else None
        raw = message.get("content") if isinstance(message, dict) else None
        if not isinstance(raw, str) or not raw.strip():
            raise RuntimeError("local strategy response is missing message.content")
        try:
            decision = LocalStrategyDecision.model_validate_json(raw)
        except Exception as exc:
            raise RuntimeError(
                f"local strategy returned invalid compact decision JSON: {exc}"
            ) from exc

        analysis = _decision_to_analysis(
            decision,
            original_request=request,
            prompt_request=prompt_request,
            generated_at=datetime.now(UTC),
        )
        _validate_analysis_for_request(analysis, request)
        return analysis

    async def run_once(self) -> int:
        ready, _ = await self.check_runtime()
        self._write_status()
        if not ready:
            return 0

        pending: list[tuple[Path, ReviewRequest]] = []
        processed = 0
        for path in self.queue.request_paths():
            try:
                request = self.queue.read_request(path)
            except Exception as exc:
                self.queue.write_error(path.stem, f"Invalid strategy request JSON: {exc}")
                processed += 1
                continue
            if self.queue.result_exists(request.request_id) or self.queue.error_exists(
                request.request_id
            ):
                continue
            pending.append((path, request))

        if not pending:
            return processed

        pending.sort(key=lambda item: item[1].requested_at)
        _, latest_request = pending[-1]
        for _, stale_request in pending[:-1]:
            self.queue.write_error(
                stale_request.request_id,
                f"superseded by newer local strategy request {latest_request.request_id}",
            )
            processed += 1

        try:
            analysis = await self.analyze(latest_request)
        except Exception as exc:
            self.queue.write_error(latest_request.request_id, str(exc))
        else:
            self.queue.write_result(latest_request.request_id, analysis)
        processed += 1
        self._write_status()
        return processed

    async def run_forever(self) -> None:
        self.queue.ensure_dirs()
        await self.check_runtime(force=True)
        self._write_status()
        while True:
            try:
                await self.run_once()
            except Exception as exc:
                self._status_message = _redact(f"worker loop error: {exc}")[:500]
                self._write_status()
            await asyncio.sleep(self.settings.poll_seconds)

    def _write_status(self) -> None:
        self.queue.write_worker_status(
            {
                "heartbeat_at": datetime.now(UTC).isoformat(),
                "provider": "local_ollama",
                "provider_ready": self._ready,
                "provider_status": self._status_message,
                "local_model": self.settings.model,
                "pending_requests": self.queue.pending_count(),
                "last_inference": self._last_inference,
                "codex_authenticated": self._ready,
                "codex_status": "not used by local_ollama provider",
            }
        )


def _local_strategy_prompt(request: ReviewRequest) -> str:
    return (
        "Evaluate the supplied intraday candidates and return one compact decision. "
        "Compare all supplied symbols. AIR/AIR.PA is forbidden. "
        "Current executable prices come only from quotes. "
        "For a proposal, choose BUY or SELL plus stop_loss, optional take_profit, "
        "confidence, ttl_minutes and a terse rationale; Parquet derives entry from "
        "current ask for BUY or bid for SELL. Prefer reward/risk around 1.5+ when justified. "
        "Use proposal=null if no immediate trade is justified. "
        "Use watch only for a near-threshold REASSESS setup; otherwise watch=null. "
        "Never invent news or unavailable data. Keep summary under two short sentences. "
        "next_review_minutes must be 1-60.\nDATA="
        + json.dumps(request.model_dump(mode="json"), separators=(",", ":"), default=str)
    )


def _compact_local_strategy_request(
    request: ReviewRequest,
    settings: LocalStrategySettings,
) -> ReviewRequest:
    context = copy.deepcopy(request.context)
    market_data = context.get("market_data")
    if not isinstance(market_data, dict):
        return request.model_copy(update={"context": context})

    wide_scanner = market_data.get("wide_scanner")
    selected = _selected_symbols(wide_scanner, settings.max_candidates)

    quotes = market_data.get("quotes")
    if not selected and isinstance(quotes, dict):
        allowed = {symbol.upper() for symbol in request.symbols}
        selected = [
            str(symbol)
            for symbol in quotes
            if str(symbol).upper() in allowed
        ][: settings.max_candidates]
    selected_upper = {symbol.upper() for symbol in selected}

    compact_quotes: dict[str, object] = {}
    if isinstance(quotes, dict):
        for symbol, value in quotes.items():
            if str(symbol).upper() not in selected_upper or not isinstance(value, dict):
                continue
            compact_quotes[str(symbol)] = {
                key: value[key]
                for key in (
                    "instrument_id",
                    "bid",
                    "ask",
                    "last_price",
                    "timestamp",
                    "age_seconds",
                    "stale",
                )
                if key in value
            }

    compact_history: dict[str, object] = {}
    history = market_data.get("history")
    if isinstance(history, dict):
        for symbol, value in history.items():
            if str(symbol).upper() not in selected_upper or not isinstance(value, dict):
                continue
            compact_history[str(symbol)] = {
                key: value[key]
                for key in (
                    "sample_count",
                    "span_minutes",
                    "first_at",
                    "last_at",
                    "metrics",
                )
                if key in value
            }

    compact_market: dict[str, object] = {
        "provider": market_data.get("provider", "etoro"),
        "captured_at": market_data.get("captured_at"),
        "quotes": compact_quotes,
        "history": compact_history,
    }

    if isinstance(wide_scanner, dict):
        candidates = wide_scanner.get("candidates")
        compact_candidates = []
        if isinstance(candidates, list):
            compact_candidates = [
                _compact_scanner_candidate(candidate)
                for candidate in candidates
                if isinstance(candidate, dict)
                and isinstance(candidate.get("symbol"), str)
                and str(candidate["symbol"]).upper() in selected_upper
            ]

        local_screener = wide_scanner.get("local_screener")
        compact_screener: dict[str, object] | None = None
        if isinstance(local_screener, dict):
            shortlist = local_screener.get("shortlist")
            compact_screener = {
                "status": local_screener.get("status"),
                "model": local_screener.get("model"),
                "shortlist": [
                    item
                    for item in shortlist
                    if isinstance(item, dict)
                    and isinstance(item.get("symbol"), str)
                    and str(item["symbol"]).upper() in selected_upper
                ]
                if isinstance(shortlist, list)
                else [],
            }

        compact_scanner: dict[str, object] = {
            "source": wide_scanner.get("source"),
            "ranking": wide_scanner.get("ranking"),
            "candidates": compact_candidates,
            "selected_for_local_strategy": selected,
        }
        if compact_screener is not None:
            compact_scanner["local_screener"] = compact_screener
        compact_market["wide_scanner"] = compact_scanner

    compact_context: dict[str, object] = {"market_data": compact_market}
    risk_snapshot = context.get("risk_snapshot")
    if isinstance(risk_snapshot, dict):
        compact_context["risk_snapshot"] = risk_snapshot

    return request.model_copy(
        update={
            "symbols": selected,
            "context": compact_context,
        }
    )


def _compact_scanner_candidate(candidate: dict[str, object]) -> dict[str, object]:
    keys = (
        "symbol",
        "score",
        "change_pct_stream",
        "change_pct_2m",
        "change_pct_5m",
        "change_pct_15m",
        "directional_efficiency",
        "persistence",
        "spike_ratio",
        "tick_rate_per_min",
        "acceleration_pct_per_min",
        "step_volatility_bps",
        "span_minutes",
        "spread_bps",
    )
    return {key: candidate[key] for key in keys if key in candidate}


def _selected_symbols(wide_scanner: object, limit: int) -> list[str]:
    if not isinstance(wide_scanner, dict):
        return []

    candidates = wide_scanner.get("candidates")
    candidate_symbols = (
        {
            str(item["symbol"]).upper(): str(item["symbol"])
            for item in candidates
            if isinstance(item, dict)
            and isinstance(item.get("symbol"), str)
            and str(item["symbol"]).strip()
        }
        if isinstance(candidates, list)
        else {}
    )

    local_screener = wide_scanner.get("local_screener")
    shortlist = local_screener.get("shortlist") if isinstance(local_screener, dict) else None
    selected: list[str] = []
    seen: set[str] = set()

    if isinstance(shortlist, list):
        for item in shortlist:
            if not isinstance(item, dict):
                continue
            raw_symbol = item.get("symbol")
            if not isinstance(raw_symbol, str):
                continue
            key = raw_symbol.upper()
            if key not in candidate_symbols or key in seen:
                continue
            selected.append(candidate_symbols[key])
            seen.add(key)
            if len(selected) >= limit:
                return selected

    if isinstance(candidates, list):
        for item in candidates:
            if not isinstance(item, dict):
                continue
            raw_symbol = item.get("symbol")
            if not isinstance(raw_symbol, str) or not raw_symbol.strip():
                continue
            key = raw_symbol.upper()
            if key in seen:
                continue
            selected.append(raw_symbol)
            seen.add(key)
            if len(selected) >= limit:
                break
    return selected


def _decision_to_analysis(
    decision: LocalStrategyDecision,
    *,
    original_request: ReviewRequest,
    prompt_request: ReviewRequest,
    generated_at: datetime,
) -> MarketAnalysis:
    allowed = {symbol.upper() for symbol in prompt_request.symbols}

    proposal: TradeProposal | None = None
    if decision.proposal is not None:
        proposal_decision = decision.proposal
        if proposal_decision.symbol.upper() not in allowed:
            raise RuntimeError(
                f"local strategy proposed non-shortlisted symbol: {proposal_decision.symbol}"
            )
        quote = _quote_for_symbol(original_request, proposal_decision.symbol)
        entry_raw = (
            quote.get("ask")
            if proposal_decision.side == Side.BUY
            else quote.get("bid")
        )
        if not isinstance(entry_raw, (int, float)) or isinstance(entry_raw, bool):
            raise RuntimeError(
                f"missing executable quote for {proposal_decision.symbol}"
            )
        proposal = TradeProposal(
            proposal_id=f"local-{uuid4().hex[:12]}",
            symbol=proposal_decision.symbol,
            side=proposal_decision.side,
            entry=float(entry_raw),
            stop_loss=proposal_decision.stop_loss,
            take_profit=proposal_decision.take_profit,
            confidence=proposal_decision.confidence,
            generated_at=generated_at,
            expires_at=generated_at
            + timedelta(minutes=proposal_decision.ttl_minutes),
            thesis=[proposal_decision.rationale],
            risks=(
                []
                if proposal_decision.risk is None
                else [proposal_decision.risk]
            ),
        )

    watch: WatchItem | None = None
    if decision.watch is not None:
        watch_decision = decision.watch
        if watch_decision.symbol.upper() not in allowed:
            raise RuntimeError(
                f"local strategy watched non-shortlisted symbol: {watch_decision.symbol}"
            )
        watch = WatchItem(
            watch_id=f"local-watch-{uuid4().hex[:12]}",
            symbol=watch_decision.symbol,
            bias=watch_decision.bias,
            trigger=Trigger(
                type=watch_decision.trigger_type,
                price=watch_decision.trigger_price,
                timeframe=None,
            ),
            invalidation=watch_decision.invalidation,
            expires_at=generated_at
            + timedelta(minutes=watch_decision.ttl_minutes),
            on_trigger=TriggerAction.REASSESS,
            proposal_id=None,
            rationale=watch_decision.rationale,
        )

    return MarketAnalysis(
        schema_version=1,
        analysis_id=f"local-analysis-{uuid4().hex[:12]}",
        review_request_id=original_request.request_id,
        generated_at=generated_at,
        market_regime=decision.market_regime,
        summary=decision.summary,
        sources=[],
        watch=[] if watch is None else [watch],
        trade_proposals=[] if proposal is None else [proposal],
        next_review=NextReview(
            at=generated_at + timedelta(minutes=decision.next_review_minutes),
            reason="local_strategy_followup",
        ),
    )


def _quote_for_symbol(request: ReviewRequest, symbol: str) -> dict[str, object]:
    market_data = request.context.get("market_data")
    if not isinstance(market_data, dict):
        raise RuntimeError("review request has no market_data")
    quotes = market_data.get("quotes")
    if not isinstance(quotes, dict):
        raise RuntimeError("review request has no quotes")
    direct = quotes.get(symbol)
    if isinstance(direct, dict):
        return direct
    for key, value in quotes.items():
        if str(key).upper() == symbol.upper() and isinstance(value, dict):
            return value
    raise RuntimeError(f"review request has no quote for {symbol}")


def _ollama_telemetry(body: object) -> dict[str, object]:
    if not isinstance(body, dict):
        return {}
    result: dict[str, object] = {}
    for input_key, output_key in (
        ("prompt_eval_count", "prompt_tokens"),
        ("eval_count", "eval_tokens"),
    ):
        value = body.get(input_key)
        if isinstance(value, int) and not isinstance(value, bool):
            result[output_key] = value

    for input_key, output_key in (
        ("total_duration", "ollama_total_ms"),
        ("load_duration", "load_ms"),
        ("prompt_eval_duration", "prompt_ms"),
        ("eval_duration", "eval_ms"),
    ):
        value = body.get(input_key)
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            result[output_key] = round(float(value) / 1_000_000.0, 1)

    eval_count = body.get("eval_count")
    eval_duration = body.get("eval_duration")
    if (
        isinstance(eval_count, int)
        and not isinstance(eval_count, bool)
        and isinstance(eval_duration, (int, float))
        and not isinstance(eval_duration, bool)
        and eval_duration > 0
    ):
        result["eval_tokens_per_second"] = round(
            eval_count / (float(eval_duration) / 1_000_000_000.0),
            3,
        )
    return result


def main() -> None:
    settings = LocalStrategySettings.from_env()
    asyncio.run(LocalStrategyWorker(settings).run_forever())


if __name__ == "__main__":
    main()
