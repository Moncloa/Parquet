from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from pydantic import BaseModel, Field

from parquet.market.etoro import EtoroMarketDataClient
from parquet.portfolio import BrokerPortfolioSnapshot
from parquet.risk_ledger import EquityBoundaryBaseline, LocalEquityRiskLedger

_HISTORY_PAGE_SIZE = 100
_HISTORY_MAX_PAGES = 10
_RISK_TIMEZONE = "UTC"


class HistoricalTrade(BaseModel):
    position_id: str = Field(min_length=1)
    open_timestamp: datetime


class OpenRiskPosition(BaseModel):
    position_id: str = Field(min_length=1)
    opened_at: datetime


class BrokerRiskMetrics(BaseModel):
    as_of: datetime
    timezone: str = _RISK_TIMEZONE
    day_start: datetime
    week_start: datetime
    trades_today: int = Field(ge=0)
    current_equity_usd: float = Field(gt=0)
    daily_start_equity_usd: float = Field(gt=0)
    weekly_start_equity_usd: float = Field(gt=0)
    daily_pnl_usd: float
    weekly_pnl_usd: float
    daily_pnl_pct: float
    weekly_pnl_pct: float
    daily_baseline: EquityBoundaryBaseline
    weekly_baseline: EquityBoundaryBaseline
    history_rows: int = Field(ge=0)
    open_position_rows: int = Field(ge=0)
    source: str = "etoro_trade_history+local_equity_ledger+real_pnl"


class EtoroRiskReader:
    """Rebuild risk counters from live broker data plus a durable local equity ledger.

    eToro trading scopes provide current real equity/PnL and trade history, but a
    separate balance scope may be unavailable. Parquet therefore records its own
    broker-equity snapshots. Daily/weekly baselines use closely bracketing local
    snapshots: flat unchanged boundaries are exact, while overnight positions or
    boundary equity movement use the higher observed equity as a conservative
    upper bound. Missing or stale boundary observations still fail closed.
    """

    def __init__(
        self,
        client: EtoroMarketDataClient,
        storage: Any,
        *,
        boundary_tolerance_seconds: float = 120.0,
    ) -> None:
        self.client = client
        self.ledger = LocalEquityRiskLedger(
            storage,
            boundary_tolerance_seconds=boundary_tolerance_seconds,
        )

    def record_equity(self, broker_snapshot: BrokerPortfolioSnapshot) -> None:
        self.ledger.record(broker_snapshot)

    async def snapshot(
        self,
        broker_snapshot: BrokerPortfolioSnapshot,
        *,
        now: datetime | None = None,
    ) -> BrokerRiskMetrics:
        current = (now or datetime.now(UTC)).astimezone(UTC)
        day_start = current.replace(hour=0, minute=0, second=0, microsecond=0)
        week_start = day_start - timedelta(days=day_start.weekday())

        history = await self._trades_since(day_start)
        open_positions = await self._open_positions()
        self._assert_same_open_positions(broker_snapshot, open_positions)

        daily_baseline = self.ledger.baseline(day_start, label="daily")
        weekly_baseline = self.ledger.baseline(week_start, label="weekly")

        daily_trade_ids = {
            trade.position_id for trade in history if trade.open_timestamp >= day_start
        }
        daily_trade_ids.update(
            position.position_id
            for position in open_positions
            if position.opened_at >= day_start
        )

        current_equity = broker_snapshot.equity_usd
        if current_equity <= 0:
            raise ValueError("current broker equity must be positive")

        daily_pnl = current_equity - daily_baseline.equity_usd
        weekly_pnl = current_equity - weekly_baseline.equity_usd

        return BrokerRiskMetrics(
            as_of=current,
            day_start=day_start,
            week_start=week_start,
            trades_today=len(daily_trade_ids),
            current_equity_usd=current_equity,
            daily_start_equity_usd=daily_baseline.equity_usd,
            weekly_start_equity_usd=weekly_baseline.equity_usd,
            daily_pnl_usd=daily_pnl,
            weekly_pnl_usd=weekly_pnl,
            daily_pnl_pct=_return_pct(daily_pnl, daily_baseline.equity_usd, label="daily"),
            weekly_pnl_pct=_return_pct(
                weekly_pnl,
                weekly_baseline.equity_usd,
                label="weekly",
            ),
            daily_baseline=daily_baseline,
            weekly_baseline=weekly_baseline,
            history_rows=len(history),
            open_position_rows=len(open_positions),
        )

    async def _trades_since(self, day_start: datetime) -> list[HistoricalTrade]:
        min_date = day_start.date().isoformat()
        collected: list[HistoricalTrade] = []
        seen: set[tuple[str, datetime]] = set()

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
                trade = _parse_historical_trade(item)
                key = (trade.position_id, trade.open_timestamp)
                if key not in seen:
                    seen.add(key)
                    collected.append(trade)

            if len(items) < _HISTORY_PAGE_SIZE:
                break
        else:
            raise ValueError(
                "trade history exceeded safe pagination limit; risk reconstruction incomplete"
            )

        return collected

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
        candidate: Any = body
    elif isinstance(body, dict):
        candidate = body.get("data")
        if isinstance(candidate, dict):
            candidate = candidate.get("items") or candidate.get("trades") or candidate.get("data")
        if candidate is None:
            candidate = body.get("items") or body.get("trades")
    else:
        candidate = None

    if not isinstance(candidate, list) or not all(isinstance(item, dict) for item in candidate):
        raise ValueError("eToro trade history response is not a trade list")
    return list(candidate)


def _parse_historical_trade(item: dict[str, Any]) -> HistoricalTrade:
    raw_id = _first(item, "positionId", "positionID", "PositionID")
    if raw_id is None:
        raise ValueError("historical eToro trade is missing positionId")
    open_raw = _first(item, "openTimestamp", "openDateTime")
    if open_raw is None:
        raise ValueError(f"historical eToro trade {raw_id} is missing open timestamp")
    return HistoricalTrade(
        position_id=str(raw_id),
        open_timestamp=_parse_timestamp(open_raw, "trade open timestamp"),
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


def _return_pct(pnl_usd: float, start_equity_usd: float, *, label: str) -> float:
    if start_equity_usd <= 0:
        raise ValueError(f"cannot derive {label} return from non-positive starting equity")
    return (pnl_usd / start_equity_usd) * 100.0


def _first(item: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = item.get(key)
        if value is not None:
            return value
    return None
