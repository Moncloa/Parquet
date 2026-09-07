from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import httpx
from pydantic import BaseModel, Field

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
            instrument_id=self.instrument_id,
            bid=self.bid,
            ask=self.ask,
        )


class EtoroAccountSnapshot(BaseModel):
    captured_at: datetime
    equity_usd: float = Field(ge=0)
    available_cash_usd: float
    invested_usd: float
    unrealized_pnl_usd: float
    credit_usd: float
    open_positions: int = Field(ge=0)
    open_instrument_ids: list[int] = Field(default_factory=list)
    open_symbols: list[str] = Field(default_factory=list)


class EtoroMarketDataClient:
    """Read-only eToro Public API client for market and account state."""

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

    async def account_snapshot(self, *, now: datetime | None = None) -> EtoroAccountSnapshot:
        body = await self._get("/trading/info/real/pnl", params={})
        portfolio = _client_portfolio(body)
        account_currency_id = _optional_int(portfolio.get("accountCurrencyId"))
        if account_currency_id not in (None, 1):
            raise ValueError(
                f"Only USD equity is supported; accountCurrencyId={account_currency_id}"
            )

        positions = _dict_list(portfolio.get("positions"))
        mirrors = _dict_list(portfolio.get("mirrors"))
        orders = _dict_list(portfolio.get("orders"))
        orders_for_open = [
            item
            for item in _dict_list(portfolio.get("ordersForOpen"))
            if _mirror_id(item) == 0
        ]

        credit = _required_float(portfolio.get("credit"), "clientPortfolio.credit")
        open_order_amount = sum(_amount(item) for item in orders_for_open)
        order_amount = sum(_amount(item) for item in orders)
        available_cash = credit - open_order_amount - order_amount

        mirror_positions = [
            position
            for mirror in mirrors
            for position in _dict_list(mirror.get("positions"))
        ]
        direct_invested = sum(_amount(position) for position in positions)
        mirror_position_invested = sum(_amount(position) for position in mirror_positions)
        mirror_available_net = sum(
            _float_or_zero(mirror.get("availableAmount"))
            - _float_or_zero(mirror.get("closedPositionsNetProfit"))
            for mirror in mirrors
        )
        external_costs = sum(
            _float_or_zero(item.get("totalExternalCosts")) for item in orders_for_open
        )
        invested = (
            direct_invested
            + mirror_position_invested
            + mirror_available_net
            + open_order_amount
            + order_amount
            + external_costs
        )

        nested_unrealized = sum(_position_pnl(position) for position in positions)
        nested_unrealized += sum(_position_pnl(position) for position in mirror_positions)
        nested_unrealized += sum(
            _float_or_zero(mirror.get("closedPositionsNetProfit")) for mirror in mirrors
        )
        aggregate_unrealized = _optional_float(portfolio.get("unrealizedPnL"))
        unrealized = nested_unrealized if aggregate_unrealized is None else aggregate_unrealized

        all_positions = positions + mirror_positions
        instrument_ids = sorted(
            {
                instrument_id
                for position in all_positions
                if (instrument_id := _position_instrument_id(position)) is not None
            }
        )
        symbols = sorted(
            {
                symbol
                for position in all_positions
                if (symbol := _position_symbol(position)) is not None
            }
        )

        captured_at = (now or datetime.now(UTC)).astimezone(UTC)
        return EtoroAccountSnapshot(
            captured_at=captured_at,
            equity_usd=available_cash + invested + unrealized,
            available_cash_usd=available_cash,
            invested_usd=invested,
            unrealized_pnl_usd=unrealized,
            credit_usd=credit,
            open_positions=len(all_positions),
            open_instrument_ids=instrument_ids,
            open_symbols=symbols,
        )


def _client_portfolio(body: Any) -> dict[str, Any]:
    if not isinstance(body, dict):
        raise ValueError("eToro PnL response must be an object")
    candidate: Any = body.get("clientPortfolio")
    if candidate is None:
        data = body.get("data")
        if isinstance(data, dict):
            candidate = data.get("clientPortfolio") or data
    if not isinstance(candidate, dict):
        raise ValueError("eToro PnL response is missing clientPortfolio")
    return candidate


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


def _dict_list(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


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


def _position_instrument_id(position: dict[str, Any]) -> int | None:
    return _optional_int(_first(position, "instrumentId", "instrumentID", "InstrumentID"))


def _position_symbol(position: dict[str, Any]) -> str | None:
    return _optional_str(
        _first(position, "symbol", "internalSymbolFull", "instrumentSymbol", "Symbol")
    )


def _position_pnl(position: dict[str, Any]) -> float:
    value = position.get("unrealizedPnL")
    if isinstance(value, dict):
        return _float_or_zero(_first(value, "pnL", "pnl", "PnL"))
    return _float_or_zero(value)


def _amount(item: dict[str, Any]) -> float:
    return _float_or_zero(item.get("amount"))


def _mirror_id(item: dict[str, Any]) -> int:
    value = _optional_int(_first(item, "mirrorID", "mirrorId", "mirrorid"))
    return 0 if value is None else value


def _required_float(value: Any, label: str) -> float:
    parsed = _optional_float(value)
    if parsed is None:
        raise ValueError(f"Missing numeric {label}")
    return parsed


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)


def _optional_str(value: Any) -> str | None:
    return None if value is None else str(value)


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def _float_or_zero(value: Any) -> float:
    parsed = _optional_float(value)
    return 0.0 if parsed is None else parsed
