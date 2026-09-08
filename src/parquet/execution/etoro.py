from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import uuid4

import httpx


class EtoroExecutionError(RuntimeError):
    def __init__(self, status_code: int, message: str, request_id: str) -> None:
        super().__init__(f"eToro execution API {status_code}: {message}")
        self.status_code = status_code
        self.request_id = request_id


class EtoroExecutionTransportError(RuntimeError):
    """Transport failure where the broker outcome may be unknown."""

    def __init__(self, message: str, request_id: str) -> None:
        super().__init__(f"eToro execution transport error: {message}")
        self.request_id = request_id


@dataclass(frozen=True)
class EtoroIdentity:
    gcid: int
    real_cid: int | None
    demo_cid: int | None
    scopes: frozenset[str]


@dataclass(frozen=True)
class EtoroOrderResult:
    request_id: str
    payload: dict[str, Any]
    response: dict[str, Any]


@dataclass(frozen=True)
class EtoroEligibilityResult:
    request_id: str
    instrument_id: int
    symbol: str | None
    min_position_exposure: float | None
    allow_open_position: bool
    leverage_configs: tuple[dict[str, Any], ...]
    response: dict[str, Any]

    def matching_config(self, *, direction: str, leverage: int = 1) -> dict[str, Any]:
        normalized_direction = direction.upper()
        for config in self.leverage_configs:
            if str(config.get("direction", "")).upper() != normalized_direction:
                continue
            values = config.get("leverageValues")
            if isinstance(values, list) and leverage in {int(value) for value in values}:
                return config
        raise RuntimeError(
            f"eToro eligibility does not allow leverage x{leverage} for "
            f"{normalized_direction} instrument {self.instrument_id}"
        )

    def allowed_leverages(self, *, direction: str) -> tuple[int, ...]:
        normalized_direction = direction.upper()
        values: set[int] = set()
        for config in self.leverage_configs:
            if str(config.get("direction", "")).upper() != normalized_direction:
                continue
            raw_values = config.get("leverageValues")
            if not isinstance(raw_values, list):
                continue
            for value in raw_values:
                parsed = int(value)
                if parsed > 0:
                    values.add(parsed)
        return tuple(sorted(values))

    def minimum_amount(self, *, direction: str, leverage: int = 1) -> float | None:
        if leverage <= 0:
            raise ValueError("leverage must be positive")
        config = self.matching_config(direction=direction, leverage=leverage)
        candidates: list[float] = []
        if self.min_position_exposure is not None:
            candidates.append(self.min_position_exposure / leverage)
        raw_minimum = config.get("minPositionAmount")
        if raw_minimum is not None:
            candidates.append(float(raw_minimum))
        return max(candidates) if candidates else None

    def settlement_type(self, *, direction: str, leverage: int = 1) -> str:
        config = self.matching_config(direction=direction, leverage=leverage)
        raw = config.get("settlementType")
        if raw is None or not str(raw).strip():
            raise RuntimeError(
                f"eToro eligibility omitted settlementType for {direction.upper()} "
                f"instrument {self.instrument_id} leverage x{leverage}"
            )
        return str(raw)


@dataclass(frozen=True)
class EtoroCostComponent:
    cost_type: str
    amount: float
    currency: str


@dataclass(frozen=True)
class EtoroCostResult:
    request_id: str
    instrument_id: int
    symbol: str | None
    costs: tuple[EtoroCostComponent, ...]
    last_updated: datetime
    response: dict[str, Any]

    @property
    def totals_by_currency(self) -> dict[str, float]:
        totals: dict[str, float] = {}
        for cost in self.costs:
            currency = cost.currency.upper()
            totals[currency] = totals.get(currency, 0.0) + cost.amount
        return totals

    @property
    def total_usd(self) -> float:
        """Return only cost components already denominated in USD.

        eToro may return financing components in an instrument-specific currency,
        so non-USD values must remain separate until an explicit FX conversion is
        performed by a higher layer.
        """
        return self.totals_by_currency.get("USD", 0.0)


@dataclass(frozen=True)
class EtoroOrderLookupResult:
    request_id: str
    response: dict[str, Any]

    @property
    def order_id(self) -> str | None:
        return _extract_id(self.response, "orderId", "orderID", "order_id")

    @property
    def status_id(self) -> int | None:
        status = self.response.get("status")
        if not isinstance(status, dict):
            return None
        raw = status.get("id")
        return None if raw is None else int(raw)

    @property
    def status_name(self) -> str | None:
        status = self.response.get("status")
        if not isinstance(status, dict):
            return None
        raw = status.get("name")
        return None if raw is None else str(raw)

    @property
    def error_code(self) -> str | None:
        status = self.response.get("status")
        if not isinstance(status, dict):
            return None
        raw = status.get("errorCode")
        return None if raw is None else str(raw)

    @property
    def error_message(self) -> str | None:
        status = self.response.get("status")
        if not isinstance(status, dict):
            return None
        raw = status.get("errorMessage")
        return None if raw is None else str(raw)

    @property
    def position_ids(self) -> tuple[str, ...]:
        executions = self.response.get("positionExecutions")
        if not isinstance(executions, list):
            return ()
        ids: list[str] = []
        for item in executions:
            if not isinstance(item, dict):
                continue
            value = item.get("positionId")
            if value is not None and str(value).strip():
                ids.append(str(value))
        return tuple(ids)


def market_order_payload(
    *,
    transaction: str,
    instrument_id: int,
    amount_usd: float,
    stop_loss_rate: float,
    take_profit_rate: float | None = None,
    settlement_type: str | None = None,
    leverage: int = 1,
) -> dict[str, Any]:
    if leverage <= 0:
        raise ValueError("leverage must be positive")
    normalized = _normalize_open_transaction(transaction)
    payload: dict[str, Any] = {
        "action": "open",
        "transaction": normalized,
        "instrumentId": instrument_id,
        "orderType": "mkt",
        "amount": amount_usd,
        "orderCurrency": "usd",
        "leverage": leverage,
        "stopLossRate": stop_loss_rate,
        "stopLossType": "fixed",
    }
    if settlement_type is not None:
        payload["settlementType"] = settlement_type
    if take_profit_rate is not None:
        payload["takeProfitRate"] = take_profit_rate
    return payload


def market_buy_payload(
    *,
    instrument_id: int,
    amount_usd: float,
    stop_loss_rate: float,
    take_profit_rate: float | None = None,
    settlement_type: str | None = None,
    leverage: int = 1,
) -> dict[str, Any]:
    return market_order_payload(
        transaction="buy",
        instrument_id=instrument_id,
        amount_usd=amount_usd,
        stop_loss_rate=stop_loss_rate,
        take_profit_rate=take_profit_rate,
        settlement_type=settlement_type,
        leverage=leverage,
    )


class EtoroExecutionClient:
    """eToro real execution client with no automatic write retries."""

    def __init__(
        self,
        *,
        api_key: str,
        user_key: str,
        base_url: str = "https://public-api.etoro.com/api/v2",
        identity_base_url: str = "https://public-api.etoro.com/api/v1",
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.api_key = api_key
        self.user_key = user_key
        self.base_url = base_url.rstrip("/")
        self.identity_base_url = identity_base_url.rstrip("/")
        self.transport = transport

    def _headers(self, request_id: str, *, json_body: bool = False) -> dict[str, str]:
        headers = {
            "x-api-key": self.api_key,
            "x-user-key": self.user_key,
            "x-request-id": request_id,
            "accept": "application/json",
        }
        if json_body:
            headers["content-type"] = "application/json"
        return headers

    async def identity(self) -> EtoroIdentity:
        request_id = str(uuid4())
        try:
            async with httpx.AsyncClient(timeout=20, transport=self.transport) as client:
                response = await client.get(
                    f"{self.identity_base_url}/me",
                    headers=self._headers(request_id),
                )
        except httpx.RequestError as exc:
            raise EtoroExecutionTransportError(repr(exc), request_id) from exc

        if response.is_error:
            raise EtoroExecutionError(response.status_code, response.text[:1000], request_id)
        body = _json_object(response, request_id, "identity")

        raw_gcid = body.get("gcid")
        if raw_gcid is None:
            raise EtoroExecutionError(response.status_code, "identity response missing gcid", request_id)
        scopes = body.get("scopes")
        return EtoroIdentity(
            gcid=int(raw_gcid),
            real_cid=_optional_int(body.get("realCid")),
            demo_cid=_optional_int(body.get("demoCid")),
            scopes=(
                frozenset(str(item) for item in scopes)
                if isinstance(scopes, list)
                else frozenset()
            ),
        )

    async def instrument_eligibility(self, *, instrument_id: int) -> EtoroEligibilityResult:
        request_id = str(uuid4())
        payload = {
            "instrumentIds": [instrument_id],
            "currency": "USD",
        }
        try:
            async with httpx.AsyncClient(timeout=20, transport=self.transport) as client:
                response = await client.post(
                    f"{self.base_url}/trading/info/eligibility",
                    headers=self._headers(request_id, json_body=True),
                    json=payload,
                )
        except httpx.RequestError as exc:
            raise EtoroExecutionTransportError(repr(exc), request_id) from exc

        if response.is_error:
            raise EtoroExecutionError(response.status_code, response.text[:1000], request_id)
        body = _json_object(response, request_id, "eligibility")
        raw_items = body.get("eligibilities")
        if not isinstance(raw_items, list):
            raise EtoroExecutionError(
                response.status_code,
                "eligibility response missing eligibilities",
                request_id,
            )
        item = next(
            (
                candidate
                for candidate in raw_items
                if isinstance(candidate, dict)
                and _optional_int(candidate.get("instrumentId")) == instrument_id
            ),
            None,
        )
        if item is None:
            raise EtoroExecutionError(
                response.status_code,
                f"eligibility response missing instrument {instrument_id}",
                request_id,
            )
        raw_configs = item.get("leverageConfigs")
        configs = (
            tuple(config for config in raw_configs if isinstance(config, dict))
            if isinstance(raw_configs, list)
            else ()
        )
        raw_minimum = item.get("minPositionExposure")
        return EtoroEligibilityResult(
            request_id=request_id,
            instrument_id=instrument_id,
            symbol=None if item.get("symbol") is None else str(item.get("symbol")),
            min_position_exposure=(None if raw_minimum is None else float(raw_minimum)),
            allow_open_position=bool(item.get("allowOpenPosition")),
            leverage_configs=configs,
            response=body,
        )

    async def what_if_open_costs(
        self,
        *,
        transaction: str,
        instrument_id: int,
        settlement_type: str,
        amount_usd: float,
        stop_loss_rate: float | None = None,
        take_profit_rate: float | None = None,
        leverage: int = 1,
    ) -> EtoroCostResult:
        request_id = str(uuid4())
        payload: dict[str, Any] = {
            "action": "open",
            "transaction": _normalize_open_transaction(transaction),
            "instrumentId": instrument_id,
            "settlementType": settlement_type,
            "orderType": "mkt",
            "leverage": leverage,
            "amount": amount_usd,
            "orderCurrency": "usd",
        }
        if stop_loss_rate is not None:
            payload["stopLossRate"] = stop_loss_rate
            payload["stopLossType"] = "fixed"
        if take_profit_rate is not None:
            payload["takeProfitRate"] = take_profit_rate

        try:
            async with httpx.AsyncClient(timeout=20, transport=self.transport) as client:
                response = await client.post(
                    f"{self.base_url}/trading/info/costs",
                    headers=self._headers(request_id, json_body=True),
                    json=payload,
                )
        except httpx.RequestError as exc:
            raise EtoroExecutionTransportError(repr(exc), request_id) from exc

        if response.is_error:
            raise EtoroExecutionError(response.status_code, response.text[:1000], request_id)
        body = _json_object(response, request_id, "cost")
        if _optional_int(body.get("instrumentId")) != instrument_id:
            raise EtoroExecutionError(
                response.status_code,
                "cost response instrument mismatch",
                request_id,
            )
        raw_costs = body.get("costs")
        if not isinstance(raw_costs, list):
            raise EtoroExecutionError(
                response.status_code,
                "cost response missing costs",
                request_id,
            )
        costs: list[EtoroCostComponent] = []
        for item in raw_costs:
            if not isinstance(item, dict):
                raise EtoroExecutionError(
                    response.status_code,
                    "invalid cost component",
                    request_id,
                )
            raw_value = item.get("value")
            if raw_value is None:
                raw_value = item.get("amount")
            if (
                item.get("costType") is None
                or raw_value is None
                or item.get("currency") is None
            ):
                raise EtoroExecutionError(
                    response.status_code,
                    "invalid cost component",
                    request_id,
                )
            costs.append(
                EtoroCostComponent(
                    cost_type=str(item["costType"]),
                    amount=float(raw_value),
                    currency=str(item["currency"]),
                )
            )
        raw_updated = body.get("lastUpdated")
        if raw_updated is None:
            raise EtoroExecutionError(
                response.status_code,
                "cost response missing lastUpdated",
                request_id,
            )
        return EtoroCostResult(
            request_id=request_id,
            instrument_id=instrument_id,
            symbol=None if body.get("symbol") is None else str(body.get("symbol")),
            costs=tuple(costs),
            last_updated=datetime.fromisoformat(str(raw_updated).replace("Z", "+00:00")),
            response=body,
        )

    async def open_market_order(
        self,
        *,
        transaction: str,
        instrument_id: int,
        amount_usd: float,
        stop_loss_rate: float,
        take_profit_rate: float | None = None,
        request_id: str | None = None,
        settlement_type: str | None = None,
        leverage: int = 1,
    ) -> EtoroOrderResult:
        submission_request_id = request_id or str(uuid4())
        payload = market_order_payload(
            transaction=transaction,
            instrument_id=instrument_id,
            amount_usd=amount_usd,
            stop_loss_rate=stop_loss_rate,
            take_profit_rate=take_profit_rate,
            settlement_type=settlement_type,
            leverage=leverage,
        )
        try:
            async with httpx.AsyncClient(timeout=20, transport=self.transport) as client:
                response = await client.post(
                    f"{self.base_url}/trading/execution/orders",
                    headers=self._headers(submission_request_id, json_body=True),
                    json=payload,
                )
        except httpx.RequestError as exc:
            raise EtoroExecutionTransportError(repr(exc), submission_request_id) from exc

        if response.is_error:
            raise EtoroExecutionError(
                response.status_code,
                response.text[:1000],
                submission_request_id,
            )

        body = _json_object(response, submission_request_id, "order")
        return EtoroOrderResult(
            request_id=submission_request_id,
            payload=payload,
            response=body,
        )

    async def lookup_order(
        self,
        *,
        order_id: str | int | None = None,
        reference_id: str | None = None,
    ) -> EtoroOrderLookupResult:
        if (order_id is None) == (reference_id is None):
            raise ValueError("Provide exactly one of order_id or reference_id")
        request_id = str(uuid4())
        params: dict[str, str] = {}
        if order_id is not None:
            params["orderId"] = str(order_id)
        else:
            params["referenceId"] = str(reference_id)

        try:
            async with httpx.AsyncClient(timeout=20, transport=self.transport) as client:
                response = await client.get(
                    f"{self.base_url}/trading/info/orders:lookup",
                    headers=self._headers(request_id),
                    params=params,
                )
        except httpx.RequestError as exc:
            raise EtoroExecutionTransportError(repr(exc), request_id) from exc

        if response.is_error:
            raise EtoroExecutionError(response.status_code, response.text[:1000], request_id)
        body = _json_object(response, request_id, "lookup")
        return EtoroOrderLookupResult(request_id=request_id, response=body)

    async def open_market_buy(
        self,
        *,
        instrument_id: int,
        amount_usd: float,
        stop_loss_rate: float,
        take_profit_rate: float | None = None,
        request_id: str | None = None,
        settlement_type: str | None = None,
        leverage: int = 1,
    ) -> EtoroOrderResult:
        return await self.open_market_order(
            transaction="buy",
            instrument_id=instrument_id,
            amount_usd=amount_usd,
            stop_loss_rate=stop_loss_rate,
            take_profit_rate=take_profit_rate,
            request_id=request_id,
            settlement_type=settlement_type,
            leverage=leverage,
        )


def _normalize_open_transaction(transaction: str) -> str:
    normalized = transaction.strip().replace("_", "").replace("-", "").lower()
    if normalized in {"buy", "long"}:
        return "buy"
    if normalized in {"sell", "sellshort", "short"}:
        return "sellShort"
    raise ValueError("transaction must describe an opening long or short")


def _json_object(response: httpx.Response, request_id: str, label: str) -> dict[str, Any]:
    try:
        body = response.json()
    except ValueError as exc:
        raise EtoroExecutionError(
            response.status_code,
            f"invalid JSON {label} response",
            request_id,
        ) from exc
    if not isinstance(body, dict):
        raise EtoroExecutionError(
            response.status_code,
            f"non-object {label} response",
            request_id,
        )
    return body


def _extract_id(response: dict[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = response.get(key)
        if value is not None and str(value).strip():
            return str(value)
    return None


def _optional_int(value: Any) -> int | None:
    return None if value is None else int(value)
