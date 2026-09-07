from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from parquet.market.etoro import EtoroMarketDataClient
from parquet.portfolio import BrokerOrder, BrokerPortfolioSnapshot, BrokerPosition


class EtoroPortfolioReader:
    """Read detailed broker state used for reconciliation.

    The reader deliberately performs no writes and reuses the authenticated
    read-only market client. eToro remains the source of truth.
    """

    def __init__(self, client: EtoroMarketDataClient) -> None:
        self.client = client

    async def snapshot(self, *, now: datetime | None = None) -> BrokerPortfolioSnapshot:
        body = await self.client._get("/trading/info/real/pnl", params={})
        portfolio = _client_portfolio(body)
        captured_at = (now or datetime.now(UTC)).astimezone(UTC)

        direct_positions = _dict_list(portfolio.get("positions"))
        mirrors = _dict_list(portfolio.get("mirrors"))
        mirror_positions = [
            position
            for mirror in mirrors
            for position in _dict_list(mirror.get("positions"))
        ]
        positions = _parse_positions([*direct_positions, *mirror_positions])

        raw_orders = _dict_list(portfolio.get("orders"))
        raw_orders_for_open = [
            item
            for item in _dict_list(portfolio.get("ordersForOpen"))
            if _mirror_id(item) == 0
        ]
        orders = _parse_orders(raw_orders)
        orders_for_open = _parse_orders(raw_orders_for_open)

        credit = _required_float(portfolio.get("credit"), "clientPortfolio.credit")
        open_order_amount = sum(_amount(item) for item in raw_orders_for_open)
        order_amount = sum(_amount(item) for item in raw_orders)
        available_cash = credit - open_order_amount - order_amount

        direct_invested = sum(_amount(position) for position in direct_positions)
        mirror_position_invested = sum(_amount(position) for position in mirror_positions)
        mirror_available_net = sum(
            _float_or_zero(mirror.get("availableAmount"))
            - _float_or_zero(mirror.get("closedPositionsNetProfit"))
            for mirror in mirrors
        )
        external_costs = sum(
            _float_or_zero(item.get("totalExternalCosts")) for item in raw_orders_for_open
        )
        invested = (
            direct_invested
            + mirror_position_invested
            + mirror_available_net
            + open_order_amount
            + order_amount
            + external_costs
        )

        nested_unrealized = sum(_position_pnl(position) for position in direct_positions)
        nested_unrealized += sum(_position_pnl(position) for position in mirror_positions)
        nested_unrealized += sum(
            _float_or_zero(mirror.get("closedPositionsNetProfit")) for mirror in mirrors
        )
        aggregate_unrealized = _optional_float(portfolio.get("unrealizedPnL"))
        unrealized = nested_unrealized if aggregate_unrealized is None else aggregate_unrealized
        equity = available_cash + invested + unrealized

        if equity < 0:
            raise ValueError("eToro portfolio equity cannot be negative")

        return BrokerPortfolioSnapshot(
            captured_at=captured_at,
            equity_usd=equity,
            available_cash_usd=available_cash,
            invested_usd=invested,
            unrealized_pnl_usd=unrealized,
            credit_usd=credit,
            positions=positions,
            orders=orders,
            orders_for_open=orders_for_open,
        )


def _parse_positions(items: list[dict[str, Any]]) -> list[BrokerPosition]:
    result: list[BrokerPosition] = []
    for item in items:
        position = _parse_position(item)
        if position is not None:
            result.append(position)
    return result


def _parse_orders(items: list[dict[str, Any]]) -> list[BrokerOrder]:
    result: list[BrokerOrder] = []
    for item in items:
        order = _parse_order(item)
        if order is not None:
            result.append(order)
    return result


def _parse_position(item: dict[str, Any]) -> BrokerPosition | None:
    raw_id = _first(item, "positionId", "positionID", "PositionID", "id", "ID")
    instrument_id = _optional_int(
        _first(item, "instrumentId", "instrumentID", "InstrumentID")
    )
    if raw_id is None or instrument_id is None:
        return None
    return BrokerPosition(
        position_id=str(raw_id),
        instrument_id=instrument_id,
        symbol=_optional_str(
            _first(item, "symbol", "internalSymbolFull", "instrumentSymbol", "Symbol")
        ),
        side=_normalise_side(_first(item, "isBuy", "transaction", "side")),
        amount_usd=_amount(item),
        leverage=_optional_float(_first(item, "leverage", "Leverage")),
        open_rate=_optional_float(_first(item, "openRate", "rate", "OpenRate")),
        stop_loss_rate=_optional_float(
            _first(item, "stopLossRate", "stopLoss", "StopLossRate")
        ),
        take_profit_rate=_optional_float(
            _first(item, "takeProfitRate", "takeProfit", "TakeProfitRate")
        ),
        unrealized_pnl_usd=_position_pnl(item),
    )


def _parse_order(item: dict[str, Any]) -> BrokerOrder | None:
    raw_id = _first(item, "orderId", "orderID", "OrderID", "id", "ID")
    if raw_id is None:
        return None
    return BrokerOrder(
        order_id=str(raw_id),
        instrument_id=_optional_int(
            _first(item, "instrumentId", "instrumentID", "InstrumentID")
        ),
        symbol=_optional_str(
            _first(item, "symbol", "internalSymbolFull", "instrumentSymbol", "Symbol")
        ),
        transaction=_normalise_side(_first(item, "isBuy", "transaction", "side")),
        amount_usd=_amount(item),
        order_type=_optional_str(_first(item, "orderType", "type")),
        status=_optional_str(_first(item, "status", "orderStatus")),
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


def _dict_list(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def _first(item: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = item.get(key)
        if value is not None:
            return value
    return None


def _normalise_side(value: Any) -> str | None:
    if isinstance(value, bool):
        return "BUY" if value else "SELL"
    if value is None:
        return None
    text = str(value).upper()
    if text in {"BUY", "SELL"}:
        return text
    return text


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
