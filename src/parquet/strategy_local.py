from __future__ import annotations

import asyncio
import json
import os
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
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
    timeout_seconds: int = 180
    poll_seconds: float = 5.0
    keep_alive: str = "15m"
    context_length: int = 3072
    max_output_tokens: int = 224
    max_candidates: int = 2

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
        max_candidates = int(os.getenv("PARQUET_LOCAL_LLM_MAX_CANDIDATES", "2"))
        max_output_tokens = int(os.getenv("PARQUET_LOCAL_LLM_MAX_OUTPUT_TOKENS", "224"))
        if max_candidates < 1:
            raise ValueError("PARQUET_LOCAL_LLM_MAX_CANDIDATES must be at least 1")
        if max_output_tokens < 128:
            raise ValueError("PARQUET_LOCAL_LLM_MAX_OUTPUT_TOKENS must be at least 128")
        return cls(
            queue_dir=Path(os.getenv("PARQUET_STRATEGY_QUEUE", "/var/lib/parquet-exchange")),
            base_url=base_url,
            model=model,
            timeout_seconds=int(os.getenv("PARQUET_LOCAL_LLM_TIMEOUT_SECONDS", "180")),
            poll_seconds=float(os.getenv("PARQUET_STRATEGY_POLL_SECONDS", "5")),
            keep_alive=os.getenv("PARQUET_LOCAL_LLM_KEEP_ALIVE", "15m"),
            context_length=int(os.getenv("PARQUET_LOCAL_LLM_CONTEXT_LENGTH", "3072")),
            max_output_tokens=max_output_tokens,
            max_candidates=max_candidates,
        )


class LocalAction(StrEnum):
    NONE = "NONE"
    BUY = "BUY"
    SELL = "SELL"
    WATCH_BUY = "WATCH_BUY"
    WATCH_SELL = "WATCH_SELL"


class LocalStrategyDecision(BaseModel):
    action: LocalAction
    symbol: str | None
    stop: float | None = Field(gt=0)
    target: float | None = Field(gt=0)
    trigger: float | None = Field(gt=0)
    confidence: float = Field(ge=0, le=1)
    reason: str = Field(min_length=1, max_length=120)


class LocalStrategyWorker:
    """CPU-bounded recurring local strategy analyst."""

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
        prompt_data, selected = _local_prompt_data(request, self.settings.max_candidates)
        if not selected:
            generated_at = datetime.now(UTC)
            self._last_inference = {
                "request_id": request.request_id,
                "prompt_chars": 0,
                "selected_symbols": [],
                "wall_ms": 0.0,
                "status": "skipped_no_fresh_candidates",
            }
            analysis = _decision_to_analysis(
                LocalStrategyDecision(
                    action=LocalAction.NONE,
                    symbol=None,
                    stop=None,
                    target=None,
                    trigger=None,
                    confidence=0.0,
                    reason="No fresh shortlisted candidate with executable bid/ask.",
                ),
                original_request=request,
                selected_symbols=[],
                generated_at=generated_at,
            )
            _validate_analysis_for_request(analysis, request)
            return analysis

        prompt = _local_strategy_prompt(prompt_data)
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
                        "You are an intraday financial setup validator. "
                        "Use only supplied data. No news invention. No broker authority. "
                        "Return schema-valid JSON only."
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
                "selected_symbols": selected,
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
                "selected_symbols": selected,
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
            "selected_symbols": selected,
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
            raise RuntimeError(f"local strategy returned invalid flat decision JSON: {exc}") from exc

        analysis = _decision_to_analysis(
            decision,
            original_request=request,
            selected_symbols=selected,
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


def _local_strategy_prompt(data: dict[str, object]) -> str:
    return (
        "Choose best intraday setup. action=NONE|BUY|SELL|WATCH_BUY|WATCH_SELL. "
        "BUY/SELL: symbol+stop, target optional, trigger=null. "
        "WATCH_*: symbol+trigger, stop optional invalidation, target=null. "
        "NONE: symbol/stop/target/trigger=null. Entry later uses ask(BUY)/bid(SELL). "
        "Prefer RR>=1.5. confidence 0..1; reason<=120 chars. AIR/AIR.PA forbidden. "
        "Keys s=symbol,b=bid,a=ask,age=quote age,q=Qwen,h=history,r=scanner. DATA="
        + json.dumps(data, separators=(",", ":"), default=str)
    )


def _local_prompt_data(
    request: ReviewRequest,
    limit: int,
) -> tuple[dict[str, object], list[str]]:
    market_data = request.context.get("market_data")
    if not isinstance(market_data, dict):
        return {"c": []}, []
    quotes = market_data.get("quotes")
    if not isinstance(quotes, dict):
        return {"c": []}, []

    wide_scanner = market_data.get("wide_scanner")
    ranked = _ranked_symbols(wide_scanner) or [str(symbol) for symbol in request.symbols]

    selected: list[str] = []
    candidates: list[dict[str, object]] = []
    for symbol in ranked:
        if len(selected) >= limit:
            break
        if symbol.upper() in {"AIR", "AIR.PA"}:
            continue
        quote = _quote_from_mapping(quotes, symbol)
        if quote is None or quote.get("stale") is True:
            continue
        bid = quote.get("bid")
        ask = quote.get("ask")
        if not _positive_number(bid) or not _positive_number(ask):
            continue

        candidate: dict[str, object] = {"s": symbol, "b": bid, "a": ask}
        age = quote.get("age_seconds")
        if isinstance(age, (int, float)) and not isinstance(age, bool):
            candidate["age"] = round(float(age), 2)

        history = market_data.get("history")
        if isinstance(history, dict):
            history_value = _value_for_symbol(history, symbol)
            if isinstance(history_value, dict):
                compact_history = _compact_history_metrics(history_value.get("metrics"))
                if compact_history:
                    candidate["h"] = compact_history

        scanner_candidate = _scanner_candidate(wide_scanner, symbol)
        if scanner_candidate is not None:
            compact_scanner = _compact_scanner_metrics(scanner_candidate)
            if compact_scanner:
                candidate["r"] = compact_scanner

        qwen = _qwen_candidate(wide_scanner, symbol)
        if qwen is not None:
            compact_qwen: dict[str, object] = {}
            classification = qwen.get("classification")
            score = qwen.get("score")
            reason = qwen.get("reason")
            if isinstance(classification, str):
                compact_qwen["class"] = classification
            if _finite_number(score):
                compact_qwen["score"] = score
            if isinstance(reason, str) and reason:
                compact_qwen["why"] = reason[:80]
            if compact_qwen:
                candidate["q"] = compact_qwen

        selected.append(symbol)
        candidates.append(candidate)

    return {"c": candidates}, selected


def _ranked_symbols(wide_scanner: object) -> list[str]:
    if not isinstance(wide_scanner, dict):
        return []
    candidates = wide_scanner.get("candidates")
    known = (
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

    result: list[str] = []
    seen: set[str] = set()
    local_screener = wide_scanner.get("local_screener")
    shortlist = local_screener.get("shortlist") if isinstance(local_screener, dict) else None
    if isinstance(shortlist, list):
        for item in shortlist:
            if not isinstance(item, dict):
                continue
            symbol = item.get("symbol")
            if not isinstance(symbol, str):
                continue
            key = symbol.upper()
            if key in seen:
                continue
            result.append(known.get(key, symbol))
            seen.add(key)

    if isinstance(candidates, list):
        for item in candidates:
            if not isinstance(item, dict):
                continue
            symbol = item.get("symbol")
            if not isinstance(symbol, str) or not symbol.strip():
                continue
            key = symbol.upper()
            if key in seen:
                continue
            result.append(symbol)
            seen.add(key)
    return result


def _compact_history_metrics(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        return {}
    aliases = {
        "change_pct_5m": "c5",
        "change_pct_15m": "c15",
        "change_pct_60m": "c60",
        "high_60m": "hi60",
        "low_60m": "lo60",
        "range_pct_60m": "range60",
        "step_volatility_bps_60m": "vol60",
    }
    return {
        alias: value[key]
        for key, alias in aliases.items()
        if _finite_number(value.get(key))
    }


def _compact_scanner_metrics(value: dict[str, object]) -> dict[str, object]:
    aliases = {
        "score": "score",
        "change_pct_stream": "stream",
        "change_pct_2m": "c2",
        "change_pct_5m": "c5",
        "change_pct_15m": "c15",
        "directional_efficiency": "eff",
        "persistence": "pers",
        "spike_ratio": "spike",
        "tick_rate_per_min": "tick",
        "acceleration_pct_per_min": "accel",
        "step_volatility_bps": "vol",
        "spread_bps": "spread",
    }
    return {
        alias: value[key]
        for key, alias in aliases.items()
        if _finite_number(value.get(key))
    }


def _scanner_candidate(wide_scanner: object, symbol: str) -> dict[str, object] | None:
    if not isinstance(wide_scanner, dict):
        return None
    candidates = wide_scanner.get("candidates")
    if not isinstance(candidates, list):
        return None
    for item in candidates:
        if (
            isinstance(item, dict)
            and isinstance(item.get("symbol"), str)
            and str(item["symbol"]).upper() == symbol.upper()
        ):
            return item
    return None


def _qwen_candidate(wide_scanner: object, symbol: str) -> dict[str, object] | None:
    if not isinstance(wide_scanner, dict):
        return None
    local_screener = wide_scanner.get("local_screener")
    shortlist = local_screener.get("shortlist") if isinstance(local_screener, dict) else None
    if not isinstance(shortlist, list):
        return None
    for item in shortlist:
        if (
            isinstance(item, dict)
            and isinstance(item.get("symbol"), str)
            and str(item["symbol"]).upper() == symbol.upper()
        ):
            return item
    return None


def _decision_to_analysis(
    decision: LocalStrategyDecision,
    *,
    original_request: ReviewRequest,
    selected_symbols: list[str],
    generated_at: datetime,
) -> MarketAnalysis:
    allowed = {symbol.upper() for symbol in selected_symbols}
    proposal: TradeProposal | None = None
    watch: WatchItem | None = None

    if decision.action != LocalAction.NONE:
        if decision.symbol is None or decision.symbol.upper() not in allowed:
            raise RuntimeError("local strategy chose a non-shortlisted symbol")

    if decision.action in {LocalAction.BUY, LocalAction.SELL}:
        if decision.symbol is None or decision.stop is None:
            raise RuntimeError("BUY/SELL decision requires symbol and stop")
        if decision.trigger is not None:
            raise RuntimeError("BUY/SELL decision must not set trigger")
        side = Side.BUY if decision.action == LocalAction.BUY else Side.SELL
        quote = _quote_for_symbol(original_request, decision.symbol)
        entry_raw = quote.get("ask") if side == Side.BUY else quote.get("bid")
        if (
            not isinstance(entry_raw, (int, float))
            or isinstance(entry_raw, bool)
            or entry_raw <= 0
        ):
            raise RuntimeError(f"missing executable quote for {decision.symbol}")
        proposal = TradeProposal(
            proposal_id=f"local-{uuid4().hex[:12]}",
            symbol=decision.symbol,
            side=side,
            entry=float(entry_raw),
            stop_loss=decision.stop,
            take_profit=decision.target,
            confidence=decision.confidence,
            generated_at=generated_at,
            expires_at=generated_at + timedelta(minutes=10),
            thesis=[decision.reason],
            risks=[],
        )
    elif decision.action in {LocalAction.WATCH_BUY, LocalAction.WATCH_SELL}:
        if decision.symbol is None or decision.trigger is None:
            raise RuntimeError("WATCH decision requires symbol and trigger")
        if decision.target is not None:
            raise RuntimeError("WATCH decision must not set target")
        is_buy = decision.action == LocalAction.WATCH_BUY
        watch = WatchItem(
            watch_id=f"local-watch-{uuid4().hex[:12]}",
            symbol=decision.symbol,
            bias=Bias.LONG if is_buy else Bias.SHORT,
            trigger=Trigger(
                type=TriggerType.PRICE_ABOVE if is_buy else TriggerType.PRICE_BELOW,
                price=decision.trigger,
                timeframe=None,
            ),
            invalidation=decision.stop,
            expires_at=generated_at + timedelta(minutes=10),
            on_trigger=TriggerAction.REASSESS,
            proposal_id=None,
            rationale=decision.reason,
        )

    return MarketAnalysis(
        schema_version=1,
        analysis_id=f"local-analysis-{uuid4().hex[:12]}",
        review_request_id=original_request.request_id,
        generated_at=generated_at,
        market_regime="local_compact",
        summary=decision.reason,
        sources=[],
        watch=[] if watch is None else [watch],
        trade_proposals=[] if proposal is None else [proposal],
        next_review=NextReview(
            at=generated_at + timedelta(minutes=5),
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
    quote = _quote_from_mapping(quotes, symbol)
    if quote is None:
        raise RuntimeError(f"review request has no quote for {symbol}")
    return quote


def _quote_from_mapping(
    quotes: dict[object, object],
    symbol: str,
) -> dict[str, object] | None:
    direct = quotes.get(symbol)
    if isinstance(direct, dict):
        return direct
    for key, value in quotes.items():
        if str(key).upper() == symbol.upper() and isinstance(value, dict):
            return value
    return None


def _value_for_symbol(mapping: dict[object, object], symbol: str) -> object:
    direct = mapping.get(symbol)
    if direct is not None:
        return direct
    for key, value in mapping.items():
        if str(key).upper() == symbol.upper():
            return value
    return None


def _positive_number(value: object) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and float(value) > 0
    )


def _finite_number(value: object) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


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
