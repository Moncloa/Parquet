from __future__ import annotations

import asyncio
import json
import os
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import httpx

from parquet.models import MarketAnalysis, ReviewRequest
from parquet.strategy import StrategyQueue, _redact, _validate_analysis_for_request
from parquet.strategy_worker import _compact_strategy_request


@dataclass(frozen=True)
class OpenAIStrategySettings:
    queue_dir: Path = Path("/var/lib/parquet-exchange")
    api_key_file: Path = Path("/etc/parquet-strategy/openai_api_key")
    base_url: str = "https://api.openai.com/v1"
    model: str = "gpt-5.6-terra"
    reasoning_effort: str = "medium"
    timeout_seconds: int = 180
    poll_seconds: float = 5.0
    web_search: bool = True

    @classmethod
    def from_env(cls) -> OpenAIStrategySettings:
        effort = os.getenv("PARQUET_OPENAI_REASONING_EFFORT", "medium").strip().lower()
        if effort not in {"none", "low", "medium", "high", "xhigh", "max"}:
            raise ValueError(f"Invalid PARQUET_OPENAI_REASONING_EFFORT: {effort}")
        return cls(
            queue_dir=Path(os.getenv("PARQUET_STRATEGY_QUEUE", "/var/lib/parquet-exchange")),
            api_key_file=Path(
                os.getenv(
                    "PARQUET_OPENAI_API_KEY_FILE",
                    "/etc/parquet-strategy/openai_api_key",
                )
            ),
            base_url=os.getenv("PARQUET_OPENAI_BASE_URL", "https://api.openai.com/v1").rstrip("/"),
            model=os.getenv("PARQUET_OPENAI_MODEL", "gpt-5.6-terra").strip(),
            reasoning_effort=effort,
            timeout_seconds=int(os.getenv("PARQUET_OPENAI_TIMEOUT_SECONDS", "180")),
            poll_seconds=float(os.getenv("PARQUET_STRATEGY_POLL_SECONDS", "5")),
            web_search=_env_bool("PARQUET_OPENAI_WEB_SEARCH", True),
        )


class OpenAIStrategyWorker:
    """Isolated strategy worker using the OpenAI Responses API.

    This process only reads sanitized ReviewRequest objects from the strategy queue
    and writes validated MarketAnalysis results back to that queue. It has no broker
    credentials and no authority to size or submit orders.
    """

    def __init__(self, settings: OpenAIStrategySettings) -> None:
        self.settings = settings
        self.queue = StrategyQueue(settings.queue_dir)
        self._authenticated = False
        self._auth_message = "not checked"

    def _api_key(self) -> str:
        try:
            value = self.settings.api_key_file.read_text(encoding="utf-8").strip()
        except FileNotFoundError as exc:
            raise RuntimeError(
                f"OpenAI API key file not found: {self.settings.api_key_file}"
            ) from exc
        if not value:
            raise RuntimeError(f"OpenAI API key file is empty: {self.settings.api_key_file}")
        return value

    def check_credentials(self) -> tuple[bool, str]:
        try:
            self._api_key()
        except Exception as exc:
            self._authenticated = False
            self._auth_message = _redact(str(exc))[:500]
        else:
            self._authenticated = True
            self._auth_message = "OpenAI API key file loaded"
        return self._authenticated, self._auth_message

    async def analyze(self, request: ReviewRequest) -> MarketAnalysis:
        from parquet import strategy_worker

        prompt_request = _compact_strategy_request(request)
        prompt_builder = cast(
            Callable[[ReviewRequest], str],
            strategy_worker.__dict__["_strategy_prompt"],
        )
        prompt = prompt_builder(prompt_request)
        schema = MarketAnalysis.model_json_schema()
        payload: dict[str, Any] = {
            "model": self.settings.model,
            "input": prompt,
            "reasoning": {"effort": self.settings.reasoning_effort},
            "text": {
                "format": {
                    "type": "json_schema",
                    "name": "market_analysis",
                    "strict": True,
                    "schema": schema,
                }
            },
            "store": False,
        }
        if self.settings.web_search:
            payload["tools"] = [{"type": "web_search_preview"}]

        headers = {
            "Authorization": f"Bearer {self._api_key()}",
            "Content-Type": "application/json",
        }
        timeout = httpx.Timeout(float(self.settings.timeout_seconds))
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.post(
                f"{self.settings.base_url}/responses",
                headers=headers,
                json=payload,
            )
        if response.status_code >= 400:
            detail = _safe_error_detail(response)
            raise RuntimeError(
                f"OpenAI Responses API returned HTTP {response.status_code}: {detail}"
            )

        try:
            body = response.json()
        except ValueError as exc:
            raise RuntimeError("OpenAI Responses API returned invalid JSON") from exc
        if not isinstance(body, dict):
            raise RuntimeError("OpenAI Responses API returned a non-object response")
        if body.get("status") not in {None, "completed"}:
            raise RuntimeError(
                f"OpenAI response did not complete: status={body.get('status')!r} "
                f"error={_redact(str(body.get('error')))[:500]}"
            )

        raw = _response_output_text(body)
        try:
            analysis = MarketAnalysis.model_validate_json(raw)
        except Exception as exc:
            raise RuntimeError(f"OpenAI returned invalid MarketAnalysis JSON: {exc}") from exc
        _validate_analysis_for_request(analysis, request)
        return analysis

    async def run_once(self) -> int:
        authenticated, _ = self.check_credentials()
        self._write_status()
        if not authenticated:
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
        self.check_credentials()
        self._write_status()
        while True:
            try:
                await self.run_once()
            except Exception as exc:
                self._auth_message = _redact(f"worker loop error: {exc}")[:500]
                self._write_status()
            await asyncio.sleep(self.settings.poll_seconds)

    def _write_status(self) -> None:
        self.queue.write_worker_status(
            {
                "heartbeat_at": datetime.now(UTC).isoformat(),
                "provider": "openai_api",
                "provider_authenticated": self._authenticated,
                # Compatibility with the current health endpoint, which historically
                # used this field as the generic worker-authenticated gate.
                "codex_authenticated": self._authenticated,
                "codex_status": self._auth_message,
                "openai_model": self.settings.model,
                "pending_requests": self.queue.pending_count(),
            }
        )


def _response_output_text(body: dict[str, Any]) -> str:
    direct = body.get("output_text")
    if isinstance(direct, str) and direct.strip():
        return direct.strip()

    chunks: list[str] = []
    output = body.get("output")
    if isinstance(output, list):
        for item in output:
            if not isinstance(item, dict) or item.get("type") != "message":
                continue
            content = item.get("content")
            if not isinstance(content, list):
                continue
            for part in content:
                if (
                    isinstance(part, dict)
                    and part.get("type") == "output_text"
                    and isinstance(part.get("text"), str)
                ):
                    chunks.append(str(part["text"]))
    joined = "".join(chunks).strip()
    if not joined:
        raise RuntimeError("OpenAI response contained no output_text")
    return joined


def _safe_error_detail(response: httpx.Response) -> str:
    try:
        body = response.json()
    except ValueError:
        return _redact(response.text)[-1000:]
    if isinstance(body, dict):
        error = body.get("error")
        if isinstance(error, dict):
            message = error.get("message")
            if isinstance(message, str):
                return _redact(message)[:1000]
    return _redact(json.dumps(body, default=str))[:1000]


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def main() -> None:
    settings = OpenAIStrategySettings.from_env()
    asyncio.run(OpenAIStrategyWorker(settings).run_forever())


if __name__ == "__main__":
    main()
