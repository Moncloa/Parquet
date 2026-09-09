from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import httpx


@dataclass(frozen=True)
class CandlePoint:
    instrument_id: int
    from_date: datetime
    open: float | None
    high: float | None
    low: float | None
    close: float
    volume: float | None


class EtoroCandleClient:
    """Read-only eToro candle-history client used to bootstrap new candidates."""

    def __init__(
        self,
        *,
        api_key: str,
        user_key: str | None,
        base_url: str = "https://public-api.etoro.com/api/v1",
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.api_key = api_key
        self.user_key = user_key
        self.base_url = base_url.rstrip("/")
        self.transport = transport

    def _headers(self) -> dict[str, str]:
        headers = {
            "x-api-key": self.api_key,
            "x-request-id": str(uuid4()),
            "accept": "application/json",
        }
        if self.user_key:
            headers["x-user-key"] = self.user_key
        return headers

    async def candles(
        self,
        instrument_id: int,
        *,
        interval: str = "OneMinute",
        count: int = 120,
        direction: str = "asc",
    ) -> list[CandlePoint]:
        if direction not in {"asc", "desc"}:
            raise ValueError("direction must be asc or desc")
        if count < 1 or count > 1000:
            raise ValueError("count must be between 1 and 1000")
        path = (
            f"/market-data/instruments/{instrument_id}/history/candles/"
            f"{direction}/{interval}/{count}"
        )
        async with httpx.AsyncClient(timeout=20, transport=self.transport) as client:
            response = await client.get(
                f"{self.base_url}{path}",
                headers=self._headers(),
            )
        response.raise_for_status()
        return _parse_candles(response.json(), expected_instrument_id=instrument_id)


def _parse_candles(body: Any, *, expected_instrument_id: int) -> list[CandlePoint]:
    raw_items: list[dict[str, Any]] = []
    if isinstance(body, dict):
        candidate = body.get("candles")
        if isinstance(candidate, list):
            for group in candidate:
                if not isinstance(group, dict):
                    continue
                nested = group.get("candles")
                if isinstance(nested, list):
                    raw_items.extend(item for item in nested if isinstance(item, dict))
                elif _has_price_fields(group):
                    raw_items.append(group)
        data = body.get("data")
        if not raw_items and isinstance(data, dict):
            return _parse_candles(data, expected_instrument_id=expected_instrument_id)
    elif isinstance(body, list):
        raw_items = [item for item in body if isinstance(item, dict)]

    result: list[CandlePoint] = []
    for item in raw_items:
        close = _optional_float(_first(item, "close", "Close"))
        raw_time = _first(item, "fromDate", "timestamp", "date", "time")
        if close is None or raw_time is None:
            continue
        raw_id = _first(item, "instrumentID", "instrumentId", "InstrumentID")
        instrument_id = expected_instrument_id if raw_id is None else int(raw_id)
        if instrument_id != expected_instrument_id:
            continue
        result.append(
            CandlePoint(
                instrument_id=instrument_id,
                from_date=_parse_timestamp(raw_time),
                open=_optional_float(_first(item, "open", "Open")),
                high=_optional_float(_first(item, "high", "High")),
                low=_optional_float(_first(item, "low", "Low")),
                close=close,
                volume=_optional_float(_first(item, "volume", "Volume")),
            )
        )
    result.sort(key=lambda candle: candle.from_date)
    return result


def _has_price_fields(item: dict[str, Any]) -> bool:
    return _first(item, "close", "Close") is not None


def _parse_timestamp(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    else:
        raise ValueError(f"Unsupported candle timestamp: {value!r}")
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _first(item: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = item.get(key)
        if value is not None:
            return value
    return None


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)
