from __future__ import annotations

import asyncio
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
    max_output_tokens: int = 2048

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
        return cls(
            queue_dir=Path(os.getenv("PARQUET_STRATEGY_QUEUE", "/var/lib/parquet-exchange")),
            base_url=base_url,
            model=model,
            timeout_seconds=int(os.getenv("PARQUET_LOCAL_LLM_TIMEOUT_SECONDS", "180")),
            poll_seconds=float(os.getenv("PARQUET_STRATEGY_POLL_SECONDS", "5")),
            keep_alive=os.getenv("PARQUET_LOCAL_LLM_KEEP_ALIVE", "15m"),
            context_length=int(os.getenv("PARQUET_LOCAL_LLM_CONTEXT_LENGTH", "8192")),
            max_output_tokens=int(os.getenv("PARQUET_LOCAL_LLM_MAX_OUTPUT_TOKENS", "2048")),
        )


class LocalStrategyWorker:
    """Isolated recurring strategy analyst backed by loopback-only Ollama.

    The worker reads sanitized review requests and emits validated MarketAnalysis
    objects. It has no broker credentials and no direct execution authority.
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

        prompt_request = _compact_strategy_request(request)
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
                        "you have no broker authority, and you must return schema-valid JSON only."
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
        try:
            async with httpx.AsyncClient(
                timeout=float(self.settings.timeout_seconds),
                transport=self.transport,
            ) as client:
                response = await client.post(f"{self.settings.base_url}/api/chat", json=payload)
        except httpx.RequestError as exc:
            raise RuntimeError(
                f"local strategy request failed: {type(exc).__name__}: {exc}"
            ) from exc
        if response.is_error:
            raise RuntimeError(
                f"local strategy HTTP {response.status_code}: {_redact(response.text)[:1000]}"
            )
        try:
            body = response.json()
        except ValueError as exc:
            raise RuntimeError("local strategy returned invalid Ollama JSON") from exc
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
            try:
                analysis = await self.analyze(request)
            except Exception as exc:
                self.queue.write_error(request.request_id, str(exc))
            else:
                self.queue.write_result(request.request_id, analysis)
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
                # Temporary compatibility with the current health endpoint. This field
                # means worker-ready here; Codex is not invoked by this provider.
                "codex_authenticated": self._ready,
                "codex_status": "not used by local_ollama provider",
            }
        )


def main() -> None:
    settings = LocalStrategySettings.from_env()
    asyncio.run(LocalStrategyWorker(settings).run_forever())


if __name__ == "__main__":
    main()
