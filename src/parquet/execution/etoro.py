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


@dataclass(frozen=True)
class EtoroOrderResult:
    request_id: str
    payload: dict[str, Any]
    response: dict[str, Any]


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

    async def open_market_buy(
        self,
        *,
        instrument_id: int,
        amount_usd: float,
        stop_loss_rate: float,
        take_profit_rate: float | None = None,
    ) -> EtoroOrderResult:
        request_id = str(uuid4())
        payload: dict[str, Any] = {
            "action": "open",
            "transaction": "buy",
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

        headers = {
            "x-api-key": self.api_key,
            "x-user-key": self.user_key,
            "x-request-id": request_id,
            "content-type": "application/json",
            "accept": "application/json",
        }
        async with httpx.AsyncClient(timeout=20, transport=self.transport) as client:
            response = await client.post(
                f"{self.base_url}/trading/execution/orders",
                headers=headers,
                json=payload,
            )
        if response.is_error:
            raise EtoroExecutionError(response.status_code, response.text[:1000], request_id)

        body = response.json()
        if not isinstance(body, dict):
            raise EtoroExecutionError(response.status_code, "non-object response", request_id)
        return EtoroOrderResult(request_id=request_id, payload=payload, response=body)
