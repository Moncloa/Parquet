from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import httpx
from pydantic import BaseModel

from parquet.models import MarketObservation


class EtoroApiError(RuntimeError):
    def __init__(self, status_code: int, message: str) -> None:
        super().__init__(f"eToro API {status_code}: {message}")
        self.status_code = status_code


class EtoroRateLimitError(EtoroApiError):
    def __init__(self, retry_after_seconds: float | None) -> None:
        super().__init__(429, "rate limit exceeded")
        self.retry_after_seconds = retry_after_seconds


class InstrumentSearchHit(BaseModel):
    instrument_id: int
    symbol: str | None = None
    name: str | None = None


class InstrumentRate(BaseModel):
    instrument_id: int
    symbol: str | None = None
    bid: float | None = None
    ask: float | None = None
    last_price: float | None = None
    change: float | None = None
    timestamp: datetime

    def observation(self) -> MarketObservation:
        price = self.last_price
        if price is None and self.bid is not None and self.ask is not None:
            price = (self.bid + self.ask) / 2
        if price is None:
            raise ValueError(f"No usable price for instrument {self.instrument_id}")
        if self.symbol is None:
            raise ValueError(f"No symbol for instrument {self.instrument_id}")
        return MarketObservation(
            symbol=self.symbol,
            price=price,
            observed_at=self.timestamp,
        )


class EtoroMarketDataClient:
    """Read-only eToro Public API client."""

    def __init__(
        self,
        *,
        api_key: str,
        user_key: str | None = None,
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

    async def _get(self, path: str, *, params: dict[str, str]) -> Any:
        async with httpx.AsyncClient(timeout=20, transport=self.transport) as client:
            response = await client.get(
                f"{self.base_url}{path}",
                headers=self._headers(),
                params=params,
            )
        if response.status_code == 429:
            raw_retry = response.headers.get("Retry-After")
            retry_after = None
            if raw_retry is not None:
                try:
                    retry_after = float(raw_retry)
                except ValueError:
                    retry_after = None
            raise EtoroRateLimitError(retry_after)
        if response.is_error:
            raise EtoroApiError(response.status_code, response.text[:500])
        return response.json()

    async def search(self, query: str) -> list[InstrumentSearchHit]:
        body = await self._get(
            "/market-data/search",
            params={"internalSymbolFull": query},
        )
        items = _search_items(body)
        results: list[InstrumentSearchHit] = []
        for item in items:
            raw_id = _first(item, "instrumentId", "instrumentID", "InstrumentID")
            if raw_id is None:
                continue
            results.append(
                InstrumentSearchHit(
                    instrument_id=int(raw_id),
                    symbol=_optional_str(
                        _first(item, "internalSymbolFull", "symbol", "Symbol")
                    ),
                    name=_optional_str(
                        _first(
                            item,
                            "displayname",
                            "displayName",
                            "name",
                            "instrumentName",
                        )
                    ),
                )
            )
        return results

    async def rates(self, instrument_ids: list[int]) -> list[InstrumentRate]:
        if not instrument_ids:
            return []
        body = await self._get(
            "/market-data/instruments/rates",
            params={"instrumentIds": ",".join(str(value) for value in instrument_ids)},
        )
        items = _rate_items(body)
        result: list[InstrumentRate] = []
        for item in items:
            raw_id = _first(item, "instrumentId", "instrumentID", "InstrumentID")
            if raw_id is None:
                continue
            timestamp = _parse_timestamp(_first(item, "timestamp", "date"))
            result.append(
                InstrumentRate(
                    instrument_id=int(raw_id),
                    symbol=_optional_str(
                        _first(item, "symbol", "internalSymbolFull", "Symbol")
                    ),
                    bid=_optional_float(item.get("bid")),
                    ask=_optional_float(item.get("ask")),
                    last_price=_optional_float(
                        _first(item, "lastPrice", "lastExecution")
                    ),
                    change=_optional_float(item.get("change")),
                    timestamp=timestamp,
                )
            )
        return result


def _search_items(body: Any) -> list[dict[str, Any]]:
    if not isinstance(body, dict):
        return []
    candidate: Any = body.get("items")
    if candidate is None:
        candidate = body.get("data")
        if isinstance(candidate, dict):
            candidate = candidate.get("items") or candidate.get("data")
    if not isinstance(candidate, list):
        return []
    return [item for item in candidate if isinstance(item, dict)]


def _rate_items(body: Any) -> list[dict[str, Any]]:
    if not isinstance(body, dict):
        return []
    candidate: Any = body.get("rates")
    if candidate is None:
        data = body.get("data")
        if isinstance(data, dict):
            candidate = data.get("rates")
        elif isinstance(data, list):
            candidate = data
    if not isinstance(candidate, list):
        return []
    return [item for item in candidate if isinstance(item, dict)]


def _parse_timestamp(value: Any) -> datetime:
    if isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.astimezone(UTC)
    return datetime.now(UTC)


def _first(item: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = item.get(key)
        if value is not None:
            return value
    return None


def _optional_str(value: Any) -> str | None:
    return None if value is None else str(value)


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)
