from __future__ import annotations

import asyncio
import copy
import os
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import httpx

from parquet.models import MarketAnalysis, ReviewRequest
from parquet.strategy import StrategyQueue, _redact, _validate_analysis_for_request
from parquet.strategy_worker import _compact_strategy_request


@dataclass(frozen=True)
class LocalStrategySettings:
    queue_dir: Path = Path("/var/lib/parquet-exchange")
    base_url: str = "http://127.0.0.1:11434"
    model: str = "hf.co/mradermacher/ODA-Fin-SFT-8B-GGUF:Q5_K_M"
    timeout_seconds: int = 180
    poll_seconds: float = 5.0
    keep_alive: str = "15m"
    context_length: int = 8192
    max_output_tokens: int = 768
    max_candidates: int = 5
    history_points: int = 12

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
        max_candidates = int(os.getenv("PARQUET_LOCAL_LLM_MAX_CANDIDATES", "5"))
        history_points = int(os.getenv("PARQUET_LOCAL_LLM_HISTORY_POINTS", "12"))
        max_output_tokens = int(os.getenv("PARQUET_LOCAL_LLM_MAX_OUTPUT_TOKENS", "768"))
        if max_candidates < 1:
            raise ValueError("PARQUET_LOCAL_LLM_MAX_CANDIDATES must be at least 1")
        if history_points < 2:
            raise ValueError("PARQUET_LOCAL_LLM_HISTORY_POINTS must be at least 2")
        if max_output_tokens < 256:
            raise ValueError("PARQUET_LOCAL_LLM_MAX_OUTPUT_TOKENS must be at least 256")
        return cls(
            queue_dir=Path(os.getenv("PARQUET_STRATEGY_QUEUE", "/var/lib/parquet-exchange")),
            base_url=base_url,
            model=model,
            timeout_seconds=int(os.getenv("PARQUET_LOCAL_LLM_TIMEOUT_SECONDS", "180")),
            poll_seconds=float(os.getenv("PARQUET_STRATEGY_POLL_SECONDS", "5")),
            keep_alive=os.getenv("PARQUET_LOCAL_LLM_KEEP_ALIVE", "15m"),
            context_length=int(os.getenv("PARQUET_LOCAL_LLM_CONTEXT_LENGTH", "8192")),
            max_output_tokens=max_output_tokens,
            max_candidates=max_candidates,
            history_points=history_points,
        )


class LocalStrategyWorker:
    """Isolated recurring strategy analyst backed by loopback-only Ollama.

    Qwen's advisory shortlist is used to constrain the expensive ODA-Fin review to
    the most relevant candidates. The worker has no broker credentials and no
    direct execution authority.
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
        from parquet import strategy_worker

        prompt_request = _compact_local_strategy_request(request, self.settings)
        prompt_fn = strategy_worker.__dict__.get("_strategy_prompt")
        if not callable(prompt_fn):
            raise RuntimeError("strategy policy prompt is not installed")
        prompt = prompt_fn(prompt_request)
        if not isinstance(prompt, str):
            raise RuntimeError("strategy policy prompt did not return text")

        payload: dict[str, Any] = {
            "model": self.settings.model,
            "stream": False,
            "think": False,
            "keep_alive": self.settings.keep_alive,
            "format": MarketAnalysis.model_json_schema(),
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "You are Parquet's local intraday strategy analyst. "
                        "Use only the supplied market data and policy. You cannot browse the web, "
                        "you have no broker authority, and you must return schema-valid JSON only. "
                        "Be concise: prefer one strong proposal or a short no-trade explanation."
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
        content = message.get("content") if isinstance(message, dict) else None
        if not isinstance(content, str) or not content.strip():
            raise RuntimeError("local strategy response is missing message.content")
        try:
            analysis = MarketAnalysis.model_validate_json(content)
        except Exception as exc:
            raise RuntimeError(f"local strategy returned invalid MarketAnalysis JSON: {exc}") from exc
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
                # Temporary compatibility with the current health endpoint. This field
                # means worker-ready here; Codex is not invoked by this provider.
                "codex_authenticated": self._ready,
                "codex_status": "not used by local_ollama provider",
            }
        )


def _compact_local_strategy_request(
    request: ReviewRequest,
    settings: LocalStrategySettings,
) -> ReviewRequest:
    """Build a bounded ODA-Fin prompt from Qwen's shortlist and eToro context."""

    compacted = _compact_strategy_request(request)
    context = copy.deepcopy(compacted.context)
    market_data = context.get("market_data")
    if not isinstance(market_data, dict):
        return compacted

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

    if isinstance(quotes, dict):
        market_data["quotes"] = {
            str(symbol): value
            for symbol, value in quotes.items()
            if str(symbol).upper() in selected_upper
        }

    history = market_data.get("history")
    if isinstance(history, dict):
        market_data["history"] = {
            str(symbol): _compact_history(value, settings.history_points)
            for symbol, value in history.items()
            if str(symbol).upper() in selected_upper
        }

    unresolved = market_data.get("unresolved_symbols")
    if isinstance(unresolved, list):
        market_data["unresolved_symbols"] = [
            symbol
            for symbol in unresolved
            if isinstance(symbol, str) and symbol.upper() in selected_upper
        ]

    if isinstance(wide_scanner, dict):
        scanner_candidates = wide_scanner.get("candidates")
        filtered_candidates = []
        if isinstance(scanner_candidates, list):
            filtered_candidates = [
                candidate
                for candidate in scanner_candidates
                if isinstance(candidate, dict)
                and isinstance(candidate.get("symbol"), str)
                and str(candidate["symbol"]).upper() in selected_upper
            ]

        local_screener = wide_scanner.get("local_screener")
        compact_screener: dict[str, object] | None = None
        if isinstance(local_screener, dict):
            shortlist = local_screener.get("shortlist")
            compact_screener = {
                key: local_screener[key]
                for key in ("enabled", "status", "model", "advisory_only")
                if key in local_screener
            }
            compact_screener["shortlist"] = (
                [
                    item
                    for item in shortlist
                    if isinstance(item, dict)
                    and isinstance(item.get("symbol"), str)
                    and str(item["symbol"]).upper() in selected_upper
                ]
                if isinstance(shortlist, list)
                else []
            )

        compact_scanner: dict[str, object] = {
            key: wide_scanner[key]
            for key in ("enabled", "source", "ranking", "filters")
            if key in wide_scanner
        }
        compact_scanner["candidates"] = filtered_candidates
        compact_scanner["raw_points_omitted_from_strategy_prompt"] = True
        compact_scanner["selected_for_local_strategy"] = selected
        if compact_screener is not None:
            compact_scanner["local_screener"] = compact_screener
        market_data["wide_scanner"] = compact_scanner

    return compacted.model_copy(
        update={
            "symbols": selected,
            "context": context,
        }
    )


def _selected_symbols(wide_scanner: object, limit: int) -> list[str]:
    if not isinstance(wide_scanner, dict):
        return []

    candidates = wide_scanner.get("candidates")
    candidate_symbols = {
        str(item["symbol"]).upper(): str(item["symbol"])
        for item in candidates
        if isinstance(item, dict)
        and isinstance(item.get("symbol"), str)
        and str(item["symbol"]).strip()
    } if isinstance(candidates, list) else {}

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


def _compact_history(value: object, point_limit: int) -> object:
    if not isinstance(value, dict):
        return value
    compact = {key: item for key, item in value.items() if key != "points"}
    points = value.get("points")
    if isinstance(points, list):
        compact["points"] = _downsample_items(points, point_limit)
    return compact


def _downsample_items(items: list[object], limit: int) -> list[object]:
    if len(items) <= limit:
        return items
    if limit <= 1:
        return items[-1:]
    indexes = {
        round(index * (len(items) - 1) / (limit - 1))
        for index in range(limit)
    }
    return [items[index] for index in sorted(indexes)]


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
