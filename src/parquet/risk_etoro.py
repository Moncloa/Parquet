from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import BaseModel, Field

from parquet.market.etoro import EtoroMarketDataClient
from parquet.portfolio import BrokerPortfolioSnapshot

_HISTORY_PAGE_SIZE = 100
_HISTORY_MAX_PAGES = 10


class ClosedTrade(BaseModel):
    position_id: str = Field(min_length=1)
    open_timestamp: datetime
    close_timestamp: datetime
    net_profit_usd: float


class OpenRiskPosition(BaseModel):
    position_id: str = Field(min_length=1)
    opened_at: datetime
    unrealized_pnl_usd: float


class BrokerRiskMetrics(BaseModel):
    as_of: datetime
    timezone: str
    day_start: datetime
    week_start: datetime
    trades_today: int = Field(ge=0)
    daily_realized_pnl_usd: float
    weekly_realized_pnl_usd: float
    open_unrealized_pnl_usd: float
    daily_pnl_usd: float
    weekly_pnl_usd: float
    daily_start_equity_usd: float = Field(gt=0)
    weekly_start_equity_usd: float = Field(gt=0)
    daily_pnl_pct: float
    weekly_pnl_pct: float
    history_rows: int = Field(ge=0)


class EtoroRiskReader:
    """Rebuild daily/weekly risk counters from broker source-of-truth data.

    Trade history supplies realized P&L and opening timestamps for closed trades.
    The real P&L endpoint supplies currently open positions. If a position spans a
    risk-period boundary, its period-only P&L cannot be recovered from these
    snapshots, so reconstruction deliberately fails closed.
    """

    def __init__(self, client: EtoroMarketDataClient) -> None:
        self.client = client

    async def snapshot(
        self,
        broker_snapshot: BrokerPortfolioSnapshot,
        *,
        now: datetime | None = None,
        timezone: str,
    ) -> BrokerRiskMetrics:
        current = (now or datetime.now(UTC)).astimezone(UTC)
        try:
            local_tz = ZoneInfo(timezone)
        except ZoneInfoNotFoundError as exc:
            raise ValueError(f"unknown risk timezone: {timezone}") from exc

        local_now = current.astimezone(local_tz)
        day_start_local = local_now.replace(hour=0, minute=0, second=0, microsecond=0)
        week_start_local = day_start_local - timedelta(days=day_start_local.weekday())
        day_start = day_start_local.astimezone(UTC)
        week_start = week_start_local.astimezone(UTC)

        closed_trades = await self._closed_trades(week_start)
        open_positions = await self._open_positions()
        self._assert_same_open_positions(broker_snapshot, open_positions)

        for position in open_positions:
            if position.opened_at < day_start:
                raise ValueError(
                    "cannot reconstruct daily P&L exactly: open position "
                    f"{position.position_id} predates day start"
                )
            if position.opened_at < week_start:
                raise ValueError(
                    "cannot reconstruct weekly P&L exactly: open position "
                    f"{position.position_id} predates week start"
                )

        for trade in closed_trades:
            if trade.close_timestamp >= day_start and trade.open_timestamp < day_start:
                raise ValueError(
                    "cannot reconstruct daily P&L exactly: closed trade "
                    f"{trade.position_id} spans day start"
                )
            if trade.close_timestamp >= week_start and trade.open_timestamp < week_start:
                raise ValueError(
                    "cannot reconstruct weekly P&L exactly: closed trade "
                    f"{trade.position_id} spans week start"
                )

        daily_trade_ids = {
            trade.position_id for trade in closed_trades if trade.open_timestamp >= day_start
        }
        daily_trade_ids.update(
            position.position_id
            for position in open_positions
            if position.opened_at >= day_start
        )

        daily_realized = sum(
            trade.net_profit_usd
            for trade in closed_trades
            if trade.close_timestamp >= day_start
        )
        weekly_realized = sum(
            trade.net_profit_usd
            for trade in closed_trades
            if trade.close_timestamp >= week_start
        )
        open_unrealized = sum(position.unrealized_pnl_usd for position in open_positions)
        daily_pnl = daily_realized + open_unrealized
        weekly_pnl = weekly_realized + open_unrealized

        daily_start_equity, daily_pct = _period_return(
            broker_snapshot.equity_usd,
            daily_pnl,
            label="daily",
        )
        weekly_start_equity, weekly_pct = _period_return(
            broker_snapshot.equity_usd,
            weekly_pnl,
            label="weekly",
        )

        return BrokerRiskMetrics(
            as_of=current,
            timezone=timezone,
            day_start=day_start,
            week_start=week_start,
            trades_today=len(daily_trade_ids),
            daily_realized_pnl_usd=daily_realized,
            weekly_realized_pnl_usd=weekly_realized,
            open_unrealized_pnl_usd=open_unrealized,
            daily_pnl_usd=daily_pnl,
            weekly_pnl_usd=weekly_pnl,
            daily_start_equity_usd=daily_start_equity,
            weekly_start_equity_usd=weekly_start_equity,
            daily_pnl_pct=daily_pct,
            weekly_pnl_pct=weekly_pct,
            history_rows=len(closed_trades),
        )

    async def _closed_trades(self, week_start: datetime) -> list[ClosedTrade]:
        # minDate is date-only. Use the UTC calendar date containing the local
        # week boundary and filter exact timestamps below; fetching one extra
        # calendar day around DST/UTC offsets is safer than missing trades.
        min_date = week_start.astimezone(UTC).date().isoformat()
        collected: list[ClosedTrade] = []
        seen: set[tuple[str, datetime, float]] = set()

        for page in range(1, _HISTORY_MAX_PAGES + 1):
            body = await self.client._get(
                "/trading/info/trade/history",
                params={
                    "minDate": min_date,
                    "page": str(page),
                    "pageSize": str(_HISTORY_PAGE_SIZE),
                },
            )
            items = _history_items(body)
            for item in items:
                trade = _parse_closed_trade(item)
                key = (trade.position_id, trade.close_timestamp, trade.net_profit_usd)
                if key not in seen:
                    seen.add(key)
                    collected.append(trade)

            if len(items) < _HISTORY_PAGE_SIZE:
                break
        else:
            raise ValueError(
                "trade history exceeded safe pagination limit; risk reconstruction incomplete"
            )

        return [trade for trade in collected if trade.close_timestamp >= week_start]

    async def _open_positions(self) -> list[OpenRiskPosition]:
        body = await self.client._get("/trading/info/real/pnl", params={})
        portfolio = _client_portfolio(body)
        mirrors = _dict_list(portfolio.get("mirrors"))
        if mirrors:
            raise ValueError("copy-trading mirrors are unsupported for exact risk reconstruction")

        raw_positions = _dict_list(portfolio.get("positions"))
        result: list[OpenRiskPosition] = []
        for item in raw_positions:
            raw_id = _first(item, "positionId", "positionID", "PositionID", "id", "ID")
            if raw_id is None:
                raise ValueError("open eToro position is missing positionId")
            opened_raw = _first(item, "openDateTime", "openTimestamp", "openedAt")
            if opened_raw is None:
                raise ValueError(f"open eToro position {raw_id} is missing open timestamp")
            result.append(
                OpenRiskPosition(
                    position_id=str(raw_id),
                    opened_at=_parse_timestamp(opened_raw, "open position timestamp"),
                    unrealized_pnl_usd=_position_pnl_strict(item, str(raw_id)),
                )
            )
        return result

    @staticmethod
    def _assert_same_open_positions(
        broker_snapshot: BrokerPortfolioSnapshot,
        open_positions: list[OpenRiskPosition],
    ) -> None:
        reconciled_ids = {position.position_id for position in broker_snapshot.positions}
        risk_ids = {position.position_id for position in open_positions}
        if reconciled_ids != risk_ids:
            raise ValueError(
                "broker portfolio changed during risk reconstruction; refusing stale risk metrics"
            )


def _history_items(body: Any) -> list[dict[str, Any]]:
    if isinstance(body, list):
        if not all(isinstance(item, dict) for item in body):
            raise ValueError("eToro trade history contains a non-object item")
        return list(body)
    if isinstance(body, dict):
        candidate: Any = body.get("data")
        if candidate is None:
            candidate = body.get("items") or body.get("trades")
        if isinstance(candidate, list) and all(isinstance(item, dict) for item in candidate):
            return list(candidate)
    raise ValueError("eToro trade history response is not a trade list")


def _parse_closed_trade(item: dict[str, Any]) -> ClosedTrade:
    raw_id = _first(item, "positionId", "positionID", "PositionID")
    if raw_id is None:
        raise ValueError("closed eToro trade is missing positionId")
    open_raw = _first(item, "openTimestamp", "openDateTime")
    close_raw = _first(item, "closeTimestamp", "closeDateTime")
    if open_raw is None or close_raw is None:
        raise ValueError(f"closed eToro trade {raw_id} is missing timestamps")
    net_profit = _required_float(
        _first(item, "netProfit", "netPnL", "netPnl"),
        f"closed trade {raw_id} netProfit",
    )
    return ClosedTrade(
        position_id=str(raw_id),
        open_timestamp=_parse_timestamp(open_raw, "trade open timestamp"),
        close_timestamp=_parse_timestamp(close_raw, "trade close timestamp"),
        net_profit_usd=net_profit,
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
    if not all(isinstance(item, dict) for item in value):
        raise ValueError("eToro portfolio contains a non-object item")
    return list(value)


def _position_pnl_strict(item: dict[str, Any], position_id: str) -> float:
    value = item.get("unrealizedPnL")
    if isinstance(value, dict):
        value = _first(value, "pnL", "pnl", "PnL")
    return _required_float(value, f"open position {position_id} unrealizedPnL")


def _parse_timestamp(value: Any, label: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"invalid {label}")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"invalid {label}: {value}") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"{label} is missing timezone: {value}")
    return parsed.astimezone(UTC)


def _period_return(current_equity_usd: float, pnl_usd: float, *, label: str) -> tuple[float, float]:
    start_equity = current_equity_usd - pnl_usd
    if start_equity <= 0:
        raise ValueError(f"cannot derive positive {label} starting equity")
    return start_equity, (pnl_usd / start_equity) * 100.0


def _first(item: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = item.get(key)
        if value is not None:
            return value
    return None


def _required_float(value: Any, label: str) -> float:
    if isinstance(value, bool) or value is None:
        raise ValueError(f"missing numeric {label}")
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"invalid numeric {label}") from exc
