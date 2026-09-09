from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import uuid4

import httpx


@dataclass(frozen=True)
class UniverseInstrument:
    instrument_id: int
    symbol: str | None = None
    name: str | None = None
    instrument_type_id: int | None = None
    exchange_id: int | None = None
    is_market_open: bool | None = None
    official_closing_price: float | None = None


class EtoroUniverseClient:
    """Read-only helpers for broad instrument discovery and metadata enrichment."""

    def __init__(
        self,
        *,
        api_key: str,
        user_key: str,
        base_url: str = "https://public-api.etoro.com/api/v1",
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.api_key = api_key
        self.user_key = user_key
        self.base_url = base_url.rstrip("/")
        self.transport = transport

    def _headers(self) -> dict[str, str]:
        return {
            "x-api-key": self.api_key,
            "x-user-key": self.user_key,
            "x-request-id": str(uuid4()),
            "accept": "application/json",
        }

    async def _get(self, path: str, params: dict[str, str] | None = None) -> Any:
        async with httpx.AsyncClient(timeout=30, transport=self.transport) as client:
            response = await client.get(
                f"{self.base_url}{path}",
                headers=self._headers(),
                params=params or {},
            )
        response.raise_for_status()
        return response.json()

    async def open_instrument_ids(self) -> list[int]:
        body = await self._get("/market-data/instruments/history/closing-price")
        items = _items(body, "closingPrices")
        ids: list[int] = []
        for item in items:
            raw_id = _first(item, "instrumentId", "instrumentID", "InstrumentID")
            if raw_id is None:
                continue
            is_open = _first(item, "isMarketOpen", "marketOpen", "IsMarketOpen")
            if is_open is False:
                continue
            ids.append(int(raw_id))
        return sorted(set(ids))

    async def metadata(self, instrument_ids: list[int]) -> dict[int, UniverseInstrument]:
        if not instrument_ids:
            return {}
        result: dict[int, UniverseInstrument] = {}
        for start in range(0, len(instrument_ids), 100):
            batch = instrument_ids[start : start + 100]
            body = await self._get(
                "/market-data/instruments",
                params={"instrumentIds": ",".join(str(value) for value in batch)},
            )
            # Official eToro response key is `instrumentDisplayDatas` and its
            # field names differ from the search/rates endpoints.
            for item in _items(body, "instrumentDisplayDatas"):
                raw_id = _first(item, "instrumentID", "instrumentId", "InstrumentID")
                if raw_id is None:
                    continue
                instrument_id = int(raw_id)
                result[instrument_id] = UniverseInstrument(
                    instrument_id=instrument_id,
                    symbol=_optional_str(
                        _first(
                            item,
                            "symbolFull",
                            "internalSymbolFull",
                            "symbol",
                            "Symbol",
                        )
                    ),
                    name=_optional_str(
                        _first(
                            item,
                            "instrumentDisplayName",
                            "displayname",
                            "displayName",
                            "name",
                            "instrumentName",
                        )
                    ),
                    instrument_type_id=_optional_int(
                        _first(
                            item,
                            "instrumentTypeID",
                            "instrumentTypeId",
                        )
                    ),
                    exchange_id=_optional_int(
                        _first(item, "exchangeID", "exchangeId")
                    ),
                )
        return result


def rotate_universe(
    instrument_ids: list[int],
    *,
    offset: int,
    limit: int,
    pinned: list[int] | None = None,
) -> tuple[list[int], int]:
    unique = sorted(set(instrument_ids))
    pinned_ids = [value for value in sorted(set(pinned or [])) if value in unique]
    remaining = [value for value in unique if value not in set(pinned_ids)]
    room = max(0, limit - len(pinned_ids))
    if room == 0 or not remaining:
        return pinned_ids[:limit], offset
    start = offset % len(remaining)
    rotated = remaining[start:] + remaining[:start]
    selected = pinned_ids + rotated[:room]
    next_offset = (start + room) % len(remaining)
    return selected, next_offset


def _items(body: Any, preferred_key: str) -> list[dict[str, Any]]:
    if isinstance(body, list):
        return [item for item in body if isinstance(item, dict)]
    if not isinstance(body, dict):
        return []
    candidate: Any = body.get(preferred_key)
    if candidate is None:
        candidate = body.get("items")
    if candidate is None:
        candidate = body.get("data")
        if isinstance(candidate, dict):
            candidate = candidate.get(preferred_key) or candidate.get("items") or candidate.get("data")
    if not isinstance(candidate, list):
        return []
    return [item for item in candidate if isinstance(item, dict)]


def _first(item: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if item.get(key) is not None:
            return item[key]
    return None


def _optional_str(value: Any) -> str | None:
    return None if value is None else str(value)


def _optional_int(value: Any) -> int | None:
    return None if value is None else int(value)
