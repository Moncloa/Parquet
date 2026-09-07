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
class EtoroOrderResult:
    request_id: str
    payload: dict[str, Any]
    response: dict[str, Any]


def market_order_payload(
    *,
    transaction: str,
    instrument_id: int,
    amount_usd: float,
    stop_loss_rate: float,
    take_profit_rate: float | None = None,
) -> dict[str, Any]:
    normalized = transaction.lower()
    if normalized not in {"buy", "sell"}:
        raise ValueError("transaction must be buy or sell")
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
    """Minimal eToro live execution client. No retries are performed."""

    def __init__(
        self,
        *,
        api_key: str,
        user_key: str,
        base_url: str = "https://public-api.etoro.com/api/v2",
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.api_key = api_key
        self.user_key = user_key
        self.base_url = base_url.rstrip("/")
        self.transport = transport

    async def open_market_order(
        self,
        *,
        transaction: str,
        instrument_id: int,
        amount_usd: float,
        stop_loss_rate: float,
        take_profit_rate: float | None = None,
    ) -> EtoroOrderResult:
        request_id = str(uuid4())
        payload = market_order_payload(
            transaction=transaction,
            instrument_id=instrument_id,
            amount_usd=amount_usd,
            stop_loss_rate=stop_loss_rate,
            take_profit_rate=take_profit_rate,
        )
        headers = {
            "x-api-key": self.api_key,
            "x-user-key": self.user_key,
            "x-request-id": request_id,
            "content-type": "application/json",
            "accept": "application/json",
        }
        try:
            async with httpx.AsyncClient(timeout=20, transport=self.transport) as client:
                response = await client.post(
                    f"{self.base_url}/trading/execution/orders",
                    headers=headers,
                    json=payload,
                )
        except httpx.RequestError as exc:
            raise EtoroExecutionTransportError(repr(exc), request_id) from exc

        if response.is_error:
            raise EtoroExecutionError(response.status_code, response.text[:1000], request_id)

        body = response.json()
        if not isinstance(body, dict):
            raise EtoroExecutionError(response.status_code, "non-object response", request_id)
        return EtoroOrderResult(request_id=request_id, payload=payload, response=body)

    async def open_market_buy(
        self,
        *,
        instrument_id: int,
        amount_usd: float,
        stop_loss_rate: float,
        take_profit_rate: float | None = None,
    ) -> EtoroOrderResult:
        return await self.open_market_order(
            transaction="buy",
            instrument_id=instrument_id,
            amount_usd=amount_usd,
            stop_loss_rate=stop_loss_rate,
            take_profit_rate=take_profit_rate,
        )
