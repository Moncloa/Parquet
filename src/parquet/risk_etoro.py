from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import Any

from pydantic import BaseModel, Field

from parquet.market.etoro import EtoroApiError, EtoroMarketDataClient
from parquet.portfolio import BrokerPortfolioSnapshot

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
    daily_baseline_date: date
    weekly_baseline_date: date
    history_rows: int = Field(ge=0)
    open_position_rows: int = Field(ge=0)


class EtoroRiskReader:
    """Rebuild broker-native daily/weekly risk counters.

    eToro historical balances are end-of-day snapshots keyed by UTC date. Risk
    accounting therefore uses UTC day/week boundaries so the baseline and the
    counting window refer to the same broker-native period.

    Trade history is used only to count unique positions opened today. Daily and
    weekly P&L are derived from current real-account equity versus the exact EOD
    balance immediately preceding the respective period. This remains correct for
    positions that were opened before the period and later closed or remain open.
    """

    def __init__(self, client: EtoroMarketDataClient) -> None:
        self.client = client

    async def snapshot(
        self,
        broker_snapshot: BrokerPortfolioSnapshot,
        *,
        now: datetime | None = None,
    ) -> BrokerRiskMetrics:
        current = (now or datetime.now(UTC)).astimezone(UTC)
        day_start = current.replace(hour=0, minute=0, second=0, microsecond=0)
        week_start = day_start - timedelta(days=day_start.weekday())
        daily_baseline_date = day_start.date() - timedelta(days=1)
        weekly_baseline_date = week_start.date() - timedelta(days=1)

        history = await self._trades_since(day_start)
        open_positions = await self._open_positions()
        self._assert_same_open_positions(broker_snapshot, open_positions)
        balances = await self._historical_balances(
            weekly_baseline_date,
            daily_baseline_date,
        )

        daily_start_equity = _required_baseline(
            balances,
            daily_baseline_date,
            "daily",
        )
        weekly_start_equity = _required_baseline(
            balances,
            weekly_baseline_date,
            "weekly",
        )

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

        daily_pnl = current_equity - daily_start_equity
        weekly_pnl = current_equity - weekly_start_equity

        return BrokerRiskMetrics(
            as_of=current,
            day_start=day_start,
            week_start=week_start,
            trades_today=len(daily_trade_ids),
            current_equity_usd=current_equity,
            daily_start_equity_usd=daily_start_equity,
            weekly_start_equity_usd=weekly_start_equity,
            daily_pnl_usd=daily_pnl,
            weekly_pnl_usd=weekly_pnl,
            daily_pnl_pct=_return_pct(daily_pnl, daily_start_equity, label="daily"),
            weekly_pnl_pct=_return_pct(weekly_pnl, weekly_start_equity, label="weekly"),
            daily_baseline_date=daily_baseline_date,
            weekly_baseline_date=weekly_baseline_date,
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

    async def _historical_balances(
        self,
        from_date: date,
        to_date: date,
    ) -> dict[date, float]:
        try:
            body = await self.client._get(
                "/balances/history",
                params={
                    "displayCurrency": "USD",
                    "fromDate": from_date.isoformat(),
                    "toDate": to_date.isoformat(),
                    "accountTypes": "Trading",
                },
            )
        except EtoroApiError as exc:
            if exc.status_code == 403:
                raise ValueError(
                    "historical balances are forbidden; eToro credentials may need "
                    "the etoro-public:money.balance:read scope"
                ) from exc
            raise

        snapshots, display_currency = _balance_snapshots(body)
        if display_currency is not None and display_currency.upper() != "USD":
            raise ValueError(
                f"historical balances returned unexpected display currency {display_currency}"
            )

        result: dict[date, float] = {}
        for snapshot in snapshots:
            snapshot_date = _parse_date(snapshot.get("date"), "historical balance date")
            equity = _historical_trading_equity(snapshot)
            existing = result.get(snapshot_date)
            if existing is not None and abs(existing - equity) > 1e-9:
                raise ValueError(
                    f"conflicting historical balance snapshots for {snapshot_date.isoformat()}"
                )
            result[snapshot_date] = equity
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


def _balance_snapshots(body: Any) -> tuple[list[dict[str, Any]], str | None]:
    if not isinstance(body, dict):
        raise ValueError("eToro historical balances response must be an object")
    candidate: Any = body
    data = body.get("data")
    if isinstance(data, dict):
        candidate = data
    snapshots = candidate.get("snapshots") if isinstance(candidate, dict) else None
    if snapshots is None:
        raise ValueError("eToro historical balances response is missing snapshots")
    if not isinstance(snapshots, list) or not all(isinstance(item, dict) for item in snapshots):
        raise ValueError("eToro historical balances snapshots must be a list of objects")
    display_currency = candidate.get("displayCurrency") if isinstance(candidate, dict) else None
    return list(snapshots), None if display_currency is None else str(display_currency)


def _historical_trading_equity(snapshot: dict[str, Any]) -> float:
    accounts = snapshot.get("accountSnapshots")
    if isinstance(accounts, list) and accounts:
        if not all(isinstance(item, dict) for item in accounts):
            raise ValueError("historical balance accountSnapshots contains a non-object item")
        trading = [
            item
            for item in accounts
            if str(item.get("accountType", "")).strip().lower() == "trading"
        ]
        if len(trading) != 1:
            raise ValueError(
                "historical balance must contain exactly one trading account snapshot"
            )
        return _positive_float(
            _first(trading[0], "displayTotal", "total"),
            "historical trading account equity",
        )

    return _positive_float(
        _first(snapshot, "displayTotalBalance", "totalBalance"),
        "historical trading equity",
    )


def _required_baseline(
    balances: dict[date, float],
    baseline_date: date,
    label: str,
) -> float:
    value = balances.get(baseline_date)
    if value is None:
        raise ValueError(
            f"missing exact {label} EOD balance for {baseline_date.isoformat()}"
        )
    if value <= 0:
        raise ValueError(f"{label} starting equity must be positive")
    return value


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


def _parse_date(value: Any, label: str) -> date:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"invalid {label}")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"invalid {label}: {value}") from exc


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


def _positive_float(value: Any, label: str) -> float:
    if isinstance(value, bool) or value is None:
        raise ValueError(f"missing numeric {label}")
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"invalid numeric {label}") from exc
    if parsed <= 0:
        raise ValueError(f"{label} must be positive")
    return parsed
