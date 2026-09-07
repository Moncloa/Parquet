from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from parquet.config import Settings
from parquet.execution.autonomous import ExecutionAttempt, ExecutionAttemptState
from parquet.execution.etoro import (
    EtoroExecutionClient,
    EtoroExecutionError,
    EtoroExecutionTransportError,
    EtoroIdentity,
    EtoroOrderLookupResult,
)
from parquet.portfolio import ManagedOrder, ManagedPosition, PositionManager, ReconciliationState
from parquet.reconciliation import ReconciliationService
from parquet.storage import Storage

_FILLED_STATUS_IDS = {3, 5}
_REJECTED_STATUS_IDS = {4}
_PARTIAL_REJECT_STATUS_IDS = {10}
_IN_FLIGHT_STATUS_IDS = {1, 2, 11, 12}
_AMBIGUOUS_SUBMISSION_HTTP_STATUS_IDS = {200, 408, 409, 425, 429}
_TRANSIENT_LOOKUP_HTTP_STATUS_IDS = {404, 408, 425, 429}


class RealSmallExecutionAdapter:
    """Supervised real-money adapter with durable state and no automatic write retries."""

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
        await self._assert_agent_portfolio_identity()

        submission_request_id = str(uuid4())
        submitting = attempt.model_copy(
            update={
                "state": ExecutionAttemptState.SUBMITTING,
                "broker_request_id": submission_request_id,
                "updated_at": current,
            }
        )
        self.storage.save_execution_attempt(submitting)

        broker_order_id: str | None = None
        lookup: EtoroOrderLookupResult | None = None

        try:
            result = await self.client.open_market_order(
                transaction=submitting.side,
                instrument_id=submitting.instrument_id,
                amount_usd=submitting.amount_usd,
                stop_loss_rate=submitting.stop_loss,
                take_profit_rate=submitting.take_profit,
                request_id=submission_request_id,
            )
            broker_order_id = _extract_id(result.response, "orderId", "orderID", "order_id")
            lookup = await self._lookup_until_settled(reference_id=submission_request_id)
        except EtoroExecutionTransportError as exc:
            # Never retry the POST. Read-only lookup by the durable request ID is safe.
            lookup = await self._lookup_until_settled(reference_id=submission_request_id)
            if lookup is None:
                return self._mark_unknown(
                    submitting,
                    datetime.now(UTC),
                    reason="transport_error_after_submission",
                    request_id=exc.request_id,
                )
            broker_order_id = lookup.order_id
        except EtoroExecutionError as exc:
            if _is_definite_submission_rejection(exc.status_code):
                return self._mark_rejected(
                    submitting,
                    datetime.now(UTC),
                    request_id=submission_request_id,
                    reason=str(exc),
                )
            lookup = await self._lookup_until_settled(reference_id=submission_request_id)
            if lookup is None:
                return self._mark_unknown(
                    submitting,
                    datetime.now(UTC),
                    reason=f"ambiguous_http_error_after_submission:{exc.status_code}",
                    request_id=submission_request_id,
                )
            broker_order_id = lookup.order_id

        if lookup is None:
            return self._mark_unknown(
                submitting,
                datetime.now(UTC),
                reason="broker_order_lookup_unresolved",
                request_id=submission_request_id,
                broker_order_id=broker_order_id,
            )

        broker_order_id = broker_order_id or lookup.order_id

        if lookup.status_id in _REJECTED_STATUS_IDS:
            return self._mark_rejected(
                submitting,
                datetime.now(UTC),
                request_id=submission_request_id,
                reason=_lookup_rejection_reason(lookup),
                broker_order_id=broker_order_id,
            )

        if lookup.status_id in _IN_FLIGHT_STATUS_IDS:
            return self._mark_unknown(
                submitting,
                datetime.now(UTC),
                reason=f"broker_order_still_in_flight:{lookup.status_name or lookup.status_id}",
                request_id=submission_request_id,
                broker_order_id=broker_order_id,
            )

        executed = lookup.status_id in _FILLED_STATUS_IDS
        partial_rejected = lookup.status_id in _PARTIAL_REJECT_STATUS_IDS
        if not executed and not partial_rejected:
            return self._mark_unknown(
                submitting,
                datetime.now(UTC),
                reason=f"unknown_broker_order_status:{lookup.status_id}:{lookup.status_name}",
                request_id=submission_request_id,
                broker_order_id=broker_order_id,
            )

        position_ids = lookup.position_ids
        if len(position_ids) != 1:
            reason = (
                "filled_order_missing_position_id"
                if not position_ids
                else "multiple_positions_from_single_open_order"
            )
            return self._mark_unknown(
                submitting,
                datetime.now(UTC),
                reason=reason,
                request_id=submission_request_id,
                broker_order_id=broker_order_id,
            )

        if broker_order_id is None:
            return self._mark_unknown(
                submitting,
                datetime.now(UTC),
                reason="broker_lookup_missing_order_id",
                request_id=submission_request_id,
            )

        acknowledged = submitting.model_copy(
            update={
                "state": ExecutionAttemptState.ACKNOWLEDGED,
                "broker_order_id": broker_order_id,
                "broker_position_id": position_ids[0],
                "reason": _lookup_rejection_reason(lookup) if partial_rejected else None,
                "updated_at": datetime.now(UTC),
            }
        )
        self.storage.save_execution_attempt(acknowledged)
        self._register_broker_identity(acknowledged, datetime.now(UTC))
        self.storage.add_event(
            "real_small_execution_acknowledged",
            json.dumps(
                {
                    "attempt": acknowledged.model_dump(mode="json"),
                    "lookup": lookup.response,
                },
                default=str,
            ),
        )

        reconciling = self._save_state(
            acknowledged,
            ExecutionAttemptState.RECONCILING,
            datetime.now(UTC),
        )
        await self.reconciliation.poll_once(force=True)
        report = self.storage.get_reconciliation_report()
        snapshot = self.storage.get_broker_portfolio_snapshot()

        if report is None or report.state != ReconciliationState.SYNCED:
            return self._mark_unknown(
                reconciling,
                datetime.now(UTC),
                reason="post_trade_reconciliation_not_synced",
                request_id=submission_request_id,
                broker_order_id=broker_order_id,
            )

        if not _broker_identity_visible(reconciling, snapshot):
            return self._mark_unknown(
                reconciling,
                datetime.now(UTC),
                reason="filled_position_not_visible_after_reconciliation",
                request_id=submission_request_id,
                broker_order_id=broker_order_id,
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
        if self.settings.etoro.expected_gcid is None:
            raise RuntimeError("Real execution blocked: etoro.expected_gcid is not configured")
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

    async def _assert_agent_portfolio_identity(self) -> None:
        expected_gcid = self.settings.etoro.expected_gcid
        if expected_gcid is None:
            raise RuntimeError("Real execution blocked: etoro.expected_gcid is not configured")
        try:
            identity = await self.client.identity()
        except (EtoroExecutionError, EtoroExecutionTransportError) as exc:
            message = f"Real execution blocked: unable to validate eToro identity: {exc}"
            raise RuntimeError(message) from exc

        if identity.gcid != expected_gcid:
            raise RuntimeError(
                f"Real execution blocked: eToro GCID {identity.gcid} does not match pinned "
                f"Agent Portfolio GCID {expected_gcid}"
            )
        required = set(self.settings.etoro.required_real_scopes)
        missing = sorted(required - set(identity.scopes))
        if missing:
            raise RuntimeError(
                "Real execution blocked: eToro token is missing required scopes: "
                + ", ".join(missing)
            )
        self._record_identity(identity)

    def _record_identity(self, identity: EtoroIdentity) -> None:
        self.storage.set("etoro_authenticated_gcid", str(identity.gcid))
        self.storage.set("etoro_authenticated_scopes", json.dumps(sorted(identity.scopes)))
        self.storage.add_event(
            "etoro_identity_validated",
            json.dumps(
                {
                    "gcid": identity.gcid,
                    "real_cid": identity.real_cid,
                    "demo_cid": identity.demo_cid,
                    "scopes": sorted(identity.scopes),
                }
            ),
        )

    async def _lookup_until_settled(
        self,
        *,
        order_id: str | None = None,
        reference_id: str | None = None,
    ) -> EtoroOrderLookupResult | None:
        if order_id is None and reference_id is None:
            return None
        attempts = self.settings.execution.broker_lookup_attempts
        interval = self.settings.execution.broker_lookup_interval_seconds
        last: EtoroOrderLookupResult | None = None
        for index in range(attempts):
            try:
                if order_id is not None:
                    last = await self.client.lookup_order(order_id=order_id)
                else:
                    last = await self.client.lookup_order(reference_id=reference_id)
            except EtoroExecutionError as exc:
                if not _is_transient_lookup_error(exc.status_code):
                    return None
            except EtoroExecutionTransportError:
                # Read-only status queries are safe to retry; the write is never retried.
                pass
            else:
                if last.status_id not in _IN_FLIGHT_STATUS_IDS:
                    return last
            if index + 1 < attempts and interval > 0:
                await asyncio.sleep(interval)
        return last

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

    def _mark_rejected(
        self,
        attempt: ExecutionAttempt,
        now: datetime,
        *,
        request_id: str | None,
        reason: str,
        broker_order_id: str | None = None,
    ) -> ExecutionAttempt:
        rejected = attempt.model_copy(
            update={
                "state": ExecutionAttemptState.REJECTED,
                "broker_request_id": request_id,
                "broker_order_id": broker_order_id,
                "reason": reason,
                "updated_at": now,
            }
        )
        self.storage.save_execution_attempt(rejected)
        self.storage.add_event("real_small_execution_rejected", rejected.model_dump_json())
        return rejected

    def _mark_unknown(
        self,
        attempt: ExecutionAttempt,
        now: datetime,
        *,
        reason: str,
        request_id: str | None,
        broker_order_id: str | None = None,
    ) -> ExecutionAttempt:
        unknown = attempt.model_copy(
            update={
                "state": ExecutionAttemptState.OUTCOME_UNKNOWN,
                "reason": reason,
                "broker_request_id": request_id,
                "broker_order_id": broker_order_id or attempt.broker_order_id,
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
                    amount_usd=attempt.amount_usd,
                    stop_loss_rate=attempt.stop_loss,
                    take_profit_rate=attempt.take_profit,
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


def _lookup_rejection_reason(lookup: EtoroOrderLookupResult) -> str:
    parts = [lookup.status_name or f"status={lookup.status_id}"]
    if lookup.error_code not in (None, "0"):
        parts.append(f"errorCode={lookup.error_code}")
    if lookup.error_message:
        parts.append(lookup.error_message)
    return ": ".join(parts)


def _is_definite_submission_rejection(status_code: int) -> bool:
    if status_code in _AMBIGUOUS_SUBMISSION_HTTP_STATUS_IDS:
        return False
    return 400 <= status_code < 500


def _is_transient_lookup_error(status_code: int) -> bool:
    return status_code in _TRANSIENT_LOOKUP_HTTP_STATUS_IDS or status_code >= 500


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
