from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field


class ReconciliationState(StrEnum):
    SYNCED = "SYNCED"
    BLOCKED = "BLOCKED"
    ERROR = "ERROR"


class BrokerPosition(BaseModel):
    position_id: str = Field(min_length=1)
    instrument_id: int = Field(gt=0)
    symbol: str | None = None
    side: str | None = None
    amount_usd: float = 0.0
    leverage: float | None = None
    open_rate: float | None = None
    stop_loss_rate: float | None = None
    take_profit_rate: float | None = None
    unrealized_pnl_usd: float = 0.0


class BrokerOrder(BaseModel):
    order_id: str = Field(min_length=1)
    instrument_id: int | None = Field(default=None, gt=0)
    symbol: str | None = None
    transaction: str | None = None
    amount_usd: float = 0.0
    order_type: str | None = None
    status: str | None = None


class BrokerPortfolioSnapshot(BaseModel):
    captured_at: datetime
    equity_usd: float = Field(ge=0)
    available_cash_usd: float
    invested_usd: float
    unrealized_pnl_usd: float
    credit_usd: float
    positions: list[BrokerPosition] = Field(default_factory=list)
    orders: list[BrokerOrder] = Field(default_factory=list)
    orders_for_open: list[BrokerOrder] = Field(default_factory=list)

    @property
    def open_instrument_ids(self) -> list[int]:
        return sorted({position.instrument_id for position in self.positions})

    @property
    def open_symbols(self) -> list[str]:
        return sorted({position.symbol for position in self.positions if position.symbol})


class ManagedPosition(BaseModel):
    local_id: str = Field(min_length=1)
    broker_position_id: str = Field(min_length=1)
    proposal_id: str | None = None
    instrument_id: int = Field(gt=0)
    symbol: str = Field(min_length=1)
    side: str = Field(min_length=1)
    opened_at: datetime
    status: str = "OPEN"
    amount_usd: float = 0.0
    open_rate: float | None = None
    leverage: float | None = None
    stop_loss_rate: float | None = None
    take_profit_rate: float | None = None
    last_unrealized_pnl_usd: float = 0.0
    last_seen_at: datetime | None = None
    closed_at: datetime | None = None
    realized_pnl_usd: float | None = None
    pnl_estimated: bool = False


class ManagedOrder(BaseModel):
    local_id: str = Field(min_length=1)
    broker_order_id: str = Field(min_length=1)
    proposal_id: str | None = None
    instrument_id: int | None = Field(default=None, gt=0)
    symbol: str | None = None
    created_at: datetime
    status: str = "PENDING"


class ReconciliationIssue(BaseModel):
    code: str = Field(min_length=1)
    detail: str = Field(min_length=1)
    broker_id: str | None = None
    local_id: str | None = None


class ReconciliationReport(BaseModel):
    as_of: datetime
    state: ReconciliationState
    trading_enabled: bool
    broker_positions: int = 0
    broker_orders: int = 0
    managed_positions: int = 0
    managed_orders: int = 0
    issues: list[ReconciliationIssue] = Field(default_factory=list)


class PositionManager:
    """Reconciles Parquet's durable ledger against the broker source of truth.

    Autonomous execution must call ``assert_trading_enabled`` immediately before
    any broker write. A broker position/order unknown to Parquet is treated as a
    hard inconsistency because the intended production target is a dedicated
    agent portfolio.
    """

    def __init__(self, storage: Any) -> None:
        self.storage = storage

    def reconcile(self, snapshot: BrokerPortfolioSnapshot) -> ReconciliationReport:
        managed_positions = self.storage.active_managed_positions()
        managed_orders = self.storage.active_managed_orders()

        broker_positions = {position.position_id: position for position in snapshot.positions}
        broker_orders = {
            order.order_id: order for order in [*snapshot.orders, *snapshot.orders_for_open]
        }
        local_positions = {
            position.broker_position_id: position for position in managed_positions
        }
        local_orders = {order.broker_order_id: order for order in managed_orders}

        issues: list[ReconciliationIssue] = []

        for broker_id in sorted(set(broker_positions) - set(local_positions)):
            position = broker_positions[broker_id]
            instrument = position.symbol or str(position.instrument_id)
            issues.append(
                ReconciliationIssue(
                    code="UNMANAGED_BROKER_POSITION",
                    broker_id=broker_id,
                    detail=(
                        f"Broker position {broker_id} ({instrument}) "
                        "is not present in Parquet's managed ledger"
                    ),
                )
            )

        for broker_id in sorted(set(broker_orders) - set(local_orders)):
            order = broker_orders[broker_id]
            issues.append(
                ReconciliationIssue(
                    code="UNMANAGED_BROKER_ORDER",
                    broker_id=broker_id,
                    detail=(
                        f"Broker order {broker_id} ({order.symbol or order.instrument_id}) "
                        "is not present in Parquet's managed ledger"
                    ),
                )
            )

        for broker_id in sorted(set(local_positions) & set(broker_positions)):
            local = local_positions[broker_id]
            broker = broker_positions[broker_id]
            updated = local.model_copy(
                update={
                    "amount_usd": broker.amount_usd,
                    "open_rate": broker.open_rate,
                    "leverage": broker.leverage,
                    "stop_loss_rate": broker.stop_loss_rate,
                    "take_profit_rate": broker.take_profit_rate,
                    "last_unrealized_pnl_usd": broker.unrealized_pnl_usd,
                    "last_seen_at": snapshot.captured_at.astimezone(UTC),
                }
            )
            self.storage.save_managed_position(updated)

        for broker_id in sorted(set(local_positions) - set(broker_positions)):
            local = local_positions[broker_id]
            closed = local.model_copy(
                update={
                    "status": "CLOSED_AT_BROKER",
                    "closed_at": snapshot.captured_at.astimezone(UTC),
                    "realized_pnl_usd": local.last_unrealized_pnl_usd,
                    "pnl_estimated": True,
                }
            )
            self.storage.save_managed_position(closed)

        for broker_id in sorted(set(local_orders) - set(broker_orders)):
            self.storage.set_managed_order_status(
                local_orders[broker_id].local_id,
                "NOT_OPEN_AT_BROKER",
            )

        state = ReconciliationState.SYNCED if not issues else ReconciliationState.BLOCKED
        report = ReconciliationReport(
            as_of=snapshot.captured_at.astimezone(UTC),
            state=state,
            trading_enabled=state == ReconciliationState.SYNCED,
            broker_positions=len(snapshot.positions),
            broker_orders=len(broker_orders),
            managed_positions=len(managed_positions),
            managed_orders=len(managed_orders),
            issues=issues,
        )
        self.storage.set_broker_portfolio_snapshot(snapshot)
        self.storage.set_reconciliation_report(report)
        return report

    def record_error(
        self,
        error: Exception | str,
        *,
        now: datetime | None = None,
    ) -> ReconciliationReport:
        current = (now or datetime.now(UTC)).astimezone(UTC)
        detail = repr(error) if isinstance(error, Exception) else str(error)
        report = ReconciliationReport(
            as_of=current,
            state=ReconciliationState.ERROR,
            trading_enabled=False,
            issues=[
                ReconciliationIssue(
                    code="BROKER_RECONCILIATION_ERROR",
                    detail=detail,
                )
            ],
        )
        self.storage.set_reconciliation_report(report)
        return report

    def record_execution_position(self, position: ManagedPosition) -> None:
        self.storage.save_managed_position(position)

    def record_execution_order(self, order: ManagedOrder) -> None:
        self.storage.save_managed_order(order)

    def assert_trading_enabled(
        self,
        *,
        now: datetime | None = None,
        max_age_seconds: float = 30.0,
    ) -> None:
        report = self.storage.get_reconciliation_report()
        if report is None:
            raise RuntimeError("Autonomous execution blocked: no reconciliation report")
        current = (now or datetime.now(UTC)).astimezone(UTC)
        age = (current - report.as_of.astimezone(UTC)).total_seconds()
        if age < 0 or age > max_age_seconds:
            raise RuntimeError(
                f"Autonomous execution blocked: reconciliation is stale ({age:.1f}s)"
            )
        if not report.trading_enabled or report.state != ReconciliationState.SYNCED:
            codes = ",".join(issue.code for issue in report.issues) or report.state.value
            raise RuntimeError(f"Autonomous execution blocked: reconciliation={codes}")
