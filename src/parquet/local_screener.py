from __future__ import annotations

import json
import time
from enum import StrEnum
from typing import Any

import httpx
from pydantic import BaseModel, Field, model_validator

from parquet.config import LocalScreenerConfig


class LocalScreenLabel(StrEnum):
    MOMENTUM = "MOMENTUM"
    MEAN_REVERSION = "MEAN_REVERSION"
    WATCH = "WATCH"


class LocalScreenDecision(BaseModel):
    symbol: str = Field(min_length=1)
    classification: LocalScreenLabel
    score: int = Field(ge=1, le=100)
    reason: str = Field(min_length=1, max_length=140)


class LocalScreenResult(BaseModel):
    shortlist: list[LocalScreenDecision] = Field(default_factory=list, max_length=5)

    @model_validator(mode="after")
    def unique_symbols(self) -> LocalScreenResult:
        symbols = [item.symbol.upper() for item in self.shortlist]
        if len(symbols) != len(set(symbols)):
            raise ValueError("local screener returned duplicate symbols")
        return self


class LocalScreenerClient:
    """Advisory-only local Ollama screener.

    The output schema contains no prices, orders, watches or execution actions. It
    can only rank already-approved deterministic scanner candidates.
    """

    def __init__(
        self,
        config: LocalScreenerConfig,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.config = config
        self.transport = transport

    async def screen(self, candidates: list[dict[str, Any]]) -> tuple[LocalScreenResult, float]:
        compact = _compact_candidates(candidates[: self.config.input_candidates])
        if not compact:
            return LocalScreenResult(), 0.0

        allowed = {str(item["symbol"]).upper() for item in compact}
        schema = LocalScreenResult.model_json_schema()
        prompt = (
            "Rank the supplied intraday candidates using only the supplied metrics. "
            "Do not infer news, catalysts, institutional activity, technical indicators, "
            "spread trends, volume trends or any other fact unless that trend/value is "
            "explicitly present. This is advisory screening only, not a trade instruction. "
            f"Return at most {self.config.output_candidates} candidates. Choose only symbols "
            "from the input. Use MOMENTUM for clean directional continuation, MEAN_REVERSION "
            "for an overextended/noisy reversal candidate, WATCH when interesting but not "
            "clear enough. Return an empty shortlist if none deserves escalation. "
            "The output score is an independent advisory confidence from 1 to 100; it is NOT "
            "the deterministic_rank_score from the input. Use 50 for borderline evidence, "
            "70 for clear evidence and 90+ only for exceptional evidence. Omit candidates "
            "below 50 rather than returning a low score. Keep each reason factual and under "
            "12 words.\n\nCandidates:\n"
            + json.dumps(compact, separators=(",", ":"), sort_keys=True)
        )
        payload = {
            "model": self.config.model,
            "stream": False,
            "think": False,
            "keep_alive": self.config.keep_alive,
            "format": schema,
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "You are Parquet's local quantitative screener. Use only supplied "
                        "data and return schema-valid JSON. You are not an execution authority."
                    ),
                },
                {"role": "user", "content": prompt},
            ],
            "options": {
                "temperature": 0,
                "num_ctx": self.config.context_length,
                "num_predict": self.config.max_output_tokens,
            },
        }

        started = time.monotonic()
        try:
            async with httpx.AsyncClient(
                timeout=float(self.config.timeout_seconds),
                transport=self.transport,
            ) as client:
                response = await client.post(f"{self.config.base_url}/api/chat", json=payload)
        except httpx.RequestError as exc:
            raise RuntimeError(f"local screener request failed: {type(exc).__name__}: {exc}") from exc
        elapsed = time.monotonic() - started

        if response.is_error:
            raise RuntimeError(f"local screener HTTP {response.status_code}: {response.text[:500]}")
        try:
            body = response.json()
        except ValueError as exc:
            raise RuntimeError("local screener returned invalid Ollama JSON") from exc
        message = body.get("message") if isinstance(body, dict) else None
        content = message.get("content") if isinstance(message, dict) else None
        if not isinstance(content, str) or not content.strip():
            raise RuntimeError("local screener response is missing message.content")
        try:
            result = LocalScreenResult.model_validate_json(content)
        except Exception as exc:
            raise RuntimeError(f"local screener returned invalid structured output: {exc}") from exc

        if len(result.shortlist) > self.config.output_candidates:
            raise RuntimeError("local screener returned too many candidates")
        unknown = [item.symbol for item in result.shortlist if item.symbol.upper() not in allowed]
        if unknown:
            raise RuntimeError(f"local screener returned unknown symbols: {unknown}")
        return result, elapsed


def _compact_candidates(candidates: list[dict[str, Any]]) -> list[dict[str, object]]:
    fields = (
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
    compact: list[dict[str, object]] = []
    for candidate in candidates:
        symbol = candidate.get("symbol")
        if not isinstance(symbol, str) or not symbol.strip():
            continue
        item: dict[str, object] = {"symbol": symbol}
        for field in fields:
            value = candidate.get(field)
            if not isinstance(value, (int, float)) or isinstance(value, bool):
                continue
            output_field = "deterministic_rank_score" if field == "score" else field
            item[output_field] = value
        compact.append(item)
    return compact
