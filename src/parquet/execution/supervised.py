from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from parquet.config import Settings
from parquet.execution.autonomous import ExecutionAttempt, ExecutionAttemptState
from parquet.execution.etoro import (
    EtoroExecutionClient,
    EtoroExecutionError,
    EtoroExecutionTransportError,
)
from parquet.portfolio import ManagedOrder, ManagedPosition, PositionManager, ReconciliationState
from parquet.reconciliation import ReconciliationService
from parquet.storage import Storage


class RealSmallExecutionAdapter:
    """Supervised real-money adapter with durable state and no automatic retries."""

    def __init__(
        self,
        *,
        settings: Settings,
        storage: Storage,
        position_manager: PositionManager,
        reconciliation: ReconciliationService,
        client: EtoroExecutionClient,
    ) -> None:
        self.settings = settings
        self.storage = storage
        self.position_manager = position_manager
        self.reconciliation = reconciliation
        self.client = client

    async def execute(
        self,
        attempt: ExecutionAttempt,
        *,
        confirmation: str,
        now: datetime | None = None,
    ) -> ExecutionAttempt:
        current = (now or datetime.now(UTC)).astimezone(UTC)
        self._assert_supervised_allowed(attempt, confirmation)

        await self.reconciliation.poll_once(now=current, force=True)
        self._assert_no_uncertain_execution()
        self.position_manager.assert_trading_enabled(now=current)

        submitting = self._save_state(attempt, ExecutionAttemptState.SUBMITTING, current)
        try:
            result = await self.client.open_market_order(
                transaction=submitting.side,
                instrument_id=submitting.instrument_id,
                amount_usd=submitting.amount_usd,
                stop_loss_rate=submitting.stop_loss,
                take_profit_rate=submitting.take_profit,
            )
        except EtoroExecutionTransportError as exc:
            return self._mark_unknown(
                submitting,
                current,
                reason="transport_error_after_submission",
                request_id=exc.request_id,
            )
        except EtoroExecutionError as exc:
            rejected = submitting.model_copy(
                update={
                    "state": ExecutionAttemptState.REJECTED,
                    "broker_request_id": exc.request_id,
                    "reason": str(exc),
                    "updated_at": current,
                }
            )
            self.storage.save_execution_attempt(rejected)
            self.storage.add_event("real_small_execution_rejected", rejected.model_dump_json())
            return rejected

        broker_order_id = _extract_id(result.response, "orderId", "orderID", "order_id")
        broker_position_id = _extract_id(
            result.response,
            "positionId",
            "positionID",
            "position_id",
        )
        if broker_order_id is None and broker_position_id is None:
            return self._mark_unknown(
                submitting,
                current,
                reason="broker_ack_without_order_or_position_id",
                request_id=result.request_id,
            )

        acknowledged = submitting.model_copy(
            update={
                "state": ExecutionAttemptState.ACKNOWLEDGED,
                "broker_request_id": result.request_id,
                "broker_order_id": broker_order_id,
                "broker_position_id": broker_position_id,
                "updated_at": current,
            }
        )
        self.storage.save_execution_attempt(acknowledged)
        self._register_broker_identity(acknowledged, current)
        self.storage.add_event(
            "real_small_execution_acknowledged",
            json.dumps(
                {
                    "attempt": acknowledged.model_dump(mode="json"),
                    "response": result.response,
                },
                default=str,
            ),
        )

        reconciling = self._save_state(
            acknowledged,
            ExecutionAttemptState.RECONCILING,
            current,
        )
        await self.reconciliation.poll_once(force=True)
        report = self.storage.get_reconciliation_report()
        snapshot = self.storage.get_broker_portfolio_snapshot()

        if report is None or report.state != ReconciliationState.SYNCED:
            return self._mark_unknown(
                reconciling,
                datetime.now(UTC),
                reason="post_trade_reconciliation_not_synced",
                request_id=result.request_id,
            )

        if not _broker_identity_visible(reconciling, snapshot):
            return self._mark_unknown(
                reconciling,
                datetime.now(UTC),
                reason="acknowledged_identity_not_visible_after_reconciliation",
                request_id=result.request_id,
            )

        reconciled = self._save_state(
            reconciling,
            ExecutionAttemptState.RECONCILED,
            datetime.now(UTC),
        )
        self.storage.set("execution_uncertain", "0")
        self.storage.add_event("real_small_execution_reconciled", reconciled.model_dump_json())
        return reconciled

    def _assert_supervised_allowed(
        self,
        attempt: ExecutionAttempt,
        confirmation: str,
    ) -> None:
        config = self.settings.execution
        if not config.supervised_real_enabled:
            raise RuntimeError("Supervised real execution is disabled")
        if attempt.state != ExecutionAttemptState.PREPARED:
            raise RuntimeError(f"Execution attempt is not PREPARED: {attempt.state}")
        if attempt.amount_usd > config.supervised_real_max_amount_usd:
            raise RuntimeError(
                f"Amount {attempt.amount_usd:.2f} exceeds supervised real cap "
                f"{config.supervised_real_max_amount_usd:.2f}"
            )
        expected = f"REAL {attempt.attempt_id}"
        if confirmation != expected:
            raise RuntimeError(f"Explicit confirmation required: {expected}")

    def _assert_no_uncertain_execution(self) -> None:
        if self.storage.get("execution_uncertain") == "1":
            raise RuntimeError("Execution blocked: unresolved broker outcome")

    def _save_state(
        self,
        attempt: ExecutionAttempt,
        state: ExecutionAttemptState,
        now: datetime,
    ) -> ExecutionAttempt:
        updated = attempt.model_copy(update={"state": state, "updated_at": now})
        self.storage.save_execution_attempt(updated)
        return updated

    def _mark_unknown(
        self,
        attempt: ExecutionAttempt,
        now: datetime,
        *,
        reason: str,
        request_id: str | None,
    ) -> ExecutionAttempt:
        unknown = attempt.model_copy(
            update={
                "state": ExecutionAttemptState.OUTCOME_UNKNOWN,
                "reason": reason,
                "broker_request_id": request_id,
                "updated_at": now,
            }
        )
        self.storage.save_execution_attempt(unknown)
        self.storage.set("execution_uncertain", "1")
        self.storage.add_event("real_small_execution_outcome_unknown", unknown.model_dump_json())
        return unknown

    def _register_broker_identity(self, attempt: ExecutionAttempt, now: datetime) -> None:
        if attempt.broker_position_id is not None:
            self.position_manager.record_execution_position(
                ManagedPosition(
                    local_id=attempt.attempt_id,
                    broker_position_id=attempt.broker_position_id,
                    proposal_id=attempt.proposal_id,
                    instrument_id=attempt.instrument_id,
                    symbol=attempt.symbol,
                    side=attempt.side,
                    opened_at=now,
                )
            )
        if attempt.broker_order_id is not None:
            self.position_manager.record_execution_order(
                ManagedOrder(
                    local_id=attempt.attempt_id,
                    broker_order_id=attempt.broker_order_id,
                    proposal_id=attempt.proposal_id,
                    instrument_id=attempt.instrument_id,
                    symbol=attempt.symbol,
                    created_at=now,
                )
            )


def _extract_id(response: dict[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = response.get(key)
        if value is not None and str(value).strip():
            return str(value)
    return None


def _broker_identity_visible(attempt: ExecutionAttempt, snapshot: Any) -> bool:
    if snapshot is None:
        return False
    if attempt.broker_position_id is not None:
        if any(
            position.position_id == attempt.broker_position_id
            for position in snapshot.positions
        ):
            return True
    if attempt.broker_order_id is not None:
        orders = [*snapshot.orders, *snapshot.orders_for_open]
        if any(order.order_id == attempt.broker_order_id for order in orders):
            return True
    return False
