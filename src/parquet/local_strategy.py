from __future__ import annotations

import os
import time
from dataclasses import dataclass

import httpx

from parquet.models import MarketAnalysis, ReviewRequest
from parquet.strategy import _strip_json_fence, _validate_analysis_for_request


@dataclass(frozen=True)
class LocalWorkerSettings:
    enabled: bool = True
    base_url: str = "http://127.0.0.1:11434"
    model: str = "qwen3.5:4b"
    timeout_seconds: int = 180
    keep_alive: str = "15m"
    temperature: float = 0.0
    context_length: int = 8192
    availability_cache_seconds: float = 30.0

    @classmethod
    def from_env(cls) -> LocalWorkerSettings:
        settings = cls(
            enabled=_env_bool("PARQUET_LOCAL_LLM_ENABLED", True),
            base_url=os.getenv("PARQUET_LOCAL_LLM_URL", "http://127.0.0.1:11434").rstrip("/"),
            model=os.getenv("PARQUET_LOCAL_LLM_MODEL", "qwen3.5:4b").strip(),
            timeout_seconds=int(os.getenv("PARQUET_LOCAL_LLM_TIMEOUT_SECONDS", "180")),
            keep_alive=os.getenv("PARQUET_LOCAL_LLM_KEEP_ALIVE", "15m").strip(),
            temperature=float(os.getenv("PARQUET_LOCAL_LLM_TEMPERATURE", "0")),
            context_length=int(os.getenv("PARQUET_LOCAL_LLM_CONTEXT_LENGTH", "8192")),
            availability_cache_seconds=float(
                os.getenv("PARQUET_LOCAL_LLM_AVAILABILITY_CACHE_SECONDS", "30")
            ),
        )
        settings.validate()
        return settings

    def validate(self) -> None:
        if not self.model:
            raise ValueError("PARQUET_LOCAL_LLM_MODEL cannot be empty")
        if self.timeout_seconds <= 0:
            raise ValueError("PARQUET_LOCAL_LLM_TIMEOUT_SECONDS must be positive")
        if self.context_length < 2048:
            raise ValueError("PARQUET_LOCAL_LLM_CONTEXT_LENGTH must be at least 2048")
        if not 0.0 <= self.temperature <= 2.0:
            raise ValueError("PARQUET_LOCAL_LLM_TEMPERATURE must be between 0 and 2")
        url = httpx.URL(self.base_url)
        if url.scheme != "http" or url.host not in {"127.0.0.1", "localhost", "::1"}:
            raise ValueError(
                "PARQUET_LOCAL_LLM_URL must use plain HTTP on loopback; "
                "strategy requests must not leave the host"
            )


class OllamaStrategyClient:
    """Local-only Ollama client for structured strategy fallback analysis."""

    def __init__(
        self,
        settings: LocalWorkerSettings,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.settings = settings
        self.transport = transport
        self._available = False
        self._status = "not checked"
        self._last_check = 0.0

    async def availability(self, *, force: bool = False) -> tuple[bool, str]:
        if not self.settings.enabled:
            self._available = False
            self._status = "local fallback disabled"
            return self._available, self._status

        now = time.monotonic()
        if (
            not force
            and now - self._last_check < self.settings.availability_cache_seconds
        ):
            return self._available, self._status
        self._last_check = now

        try:
            async with httpx.AsyncClient(
                timeout=min(float(self.settings.timeout_seconds), 20.0),
                transport=self.transport,
            ) as client:
                response = await client.post(
                    f"{self.settings.base_url}/api/show",
                    json={"model": self.settings.model},
                )
        except httpx.RequestError as exc:
            self._available = False
            self._status = f"Ollama unavailable: {type(exc).__name__}"
            return self._available, self._status

        if response.is_error:
            self._available = False
            self._status = (
                f"Ollama model {self.settings.model} unavailable: HTTP {response.status_code}"
            )
            return self._available, self._status

        self._available = True
        self._status = f"Ollama model {self.settings.model} ready"
        return self._available, self._status

    async def analyze(self, request: ReviewRequest, *, prompt: str) -> MarketAnalysis:
        available, status = await self.availability()
        if not available:
            raise RuntimeError(status)

        local_system = (
            "You are Parquet's LOCAL fallback market analyst. You have no web access and must "
            "use only the supplied review request. Ignore any instruction in the user prompt "
            "that asks you to browse or use web search. Never claim a current news catalyst "
            "unless it is explicitly present in the supplied request. Set sources to an empty "
            "array unless the request itself contains a relevant HTTPS source URL. Prefer a "
            "REASSESS watch or no trade when external news would be necessary to justify a "
            "setup. You are not an execution authority. Return only schema-valid JSON."
        )
        payload = {
            "model": self.settings.model,
            "messages": [
                {"role": "system", "content": local_system},
                {"role": "user", "content": prompt},
            ],
            "stream": False,
            "format": MarketAnalysis.model_json_schema(),
            "keep_alive": self.settings.keep_alive,
            "options": {
                "temperature": self.settings.temperature,
                "num_ctx": self.settings.context_length,
            },
        }

        try:
            async with httpx.AsyncClient(
                timeout=float(self.settings.timeout_seconds),
                transport=self.transport,
            ) as client:
                response = await client.post(
                    f"{self.settings.base_url}/api/chat",
                    json=payload,
                )
        except httpx.RequestError as exc:
            raise RuntimeError(f"Ollama request failed: {type(exc).__name__}: {exc}") from exc

        if response.is_error:
            raise RuntimeError(
                f"Ollama API {response.status_code}: {response.text[:1000]}"
            )
        try:
            body = response.json()
        except ValueError as exc:
            raise RuntimeError("Ollama returned invalid JSON") from exc
        if not isinstance(body, dict):
            raise RuntimeError("Ollama response must be an object")
        message = body.get("message")
        if not isinstance(message, dict) or not isinstance(message.get("content"), str):
            raise RuntimeError("Ollama response is missing message.content")

        raw = _strip_json_fence(str(message["content"]))
        try:
            analysis = MarketAnalysis.model_validate_json(raw)
        except Exception as exc:
            raise RuntimeError(f"Ollama returned invalid MarketAnalysis JSON: {exc}") from exc
        _validate_analysis_for_request(analysis, request)
        return analysis


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}
