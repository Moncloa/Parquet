from __future__ import annotations

from dataclasses import dataclass
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
) -> dict[str, Any]:
    normalized = _normalize_open_transaction(transaction)
    payload: dict[str, Any] = {
        "action": "open",
        "transaction": normalized,
        "instrumentId": instrument_id,
        "orderType": "mkt",
        "amount": amount_usd,
        "orderCurrency": "usd",
        "leverage": 1,
        "stopLossRate": stop_loss_rate,
        "stopLossType": "fixed",
    }
    if take_profit_rate is not None:
        payload["takeProfitRate"] = take_profit_rate
    return payload


def market_buy_payload(
    *,
    instrument_id: int,
    amount_usd: float,
    stop_loss_rate: float,
    take_profit_rate: float | None = None,
) -> dict[str, Any]:
    return market_order_payload(
        transaction="buy",
        instrument_id=instrument_id,
        amount_usd=amount_usd,
        stop_loss_rate=stop_loss_rate,
        take_profit_rate=take_profit_rate,
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
            scopes=frozenset(str(item) for item in scopes) if isinstance(scopes, list) else frozenset(),
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
    ) -> EtoroOrderResult:
        submission_request_id = request_id or str(uuid4())
        payload = market_order_payload(
            transaction=transaction,
            instrument_id=instrument_id,
            amount_usd=amount_usd,
            stop_loss_rate=stop_loss_rate,
            take_profit_rate=take_profit_rate,
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
    ) -> EtoroOrderResult:
        return await self.open_market_order(
            transaction="buy",
            instrument_id=instrument_id,
            amount_usd=amount_usd,
            stop_loss_rate=stop_loss_rate,
            take_profit_rate=take_profit_rate,
            request_id=request_id,
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
