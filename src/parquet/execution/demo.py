from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from parquet.config import Settings
from parquet.execution.autonomous import ExecutionAttempt, ExecutionAttemptState
from parquet.execution.etoro import (
    EtoroEligibilityResult,
    EtoroExecutionClient,
    EtoroExecutionError,
    EtoroExecutionTransportError,
    EtoroOrderLookupResult,
)
from parquet.storage import Storage

_FILLED_STATUS_IDS = {3, 5}
_REJECTED_STATUS_IDS = {4}
_PARTIAL_REJECT_STATUS_IDS = {10}
_IN_FLIGHT_STATUS_IDS = {1, 2, 11, 12}
_AMBIGUOUS_SUBMISSION_HTTP_STATUS_IDS = {200, 408, 409, 425, 429}
_TRANSIENT_LOOKUP_HTTP_STATUS_IDS = {404, 408, 425, 429}


class DemoExecutionAdapter:
    """Autonomous eToro demo executor with durable no-retry semantics."""

    def __init__(
        self,
        *,
        settings: Settings,
        storage: Storage,
        client: EtoroExecutionClient,
    ) -> None:
        self.settings = settings
        self.storage = storage
        self.client = client

    async def execute(
        self,
        attempt: ExecutionAttempt,
        *,
        now: datetime | None = None,
    ) -> ExecutionAttempt:
        current = (now or datetime.now(UTC)).astimezone(UTC)
        self._assert_demo_allowed(attempt)
        self._assert_no_uncertain_demo_execution()
        await self._assert_demo_identity()

        pnl = await self.client.account_pnl()
        if attempt.instrument_id in _open_instrument_ids_from_pnl(pnl):
            return self._mark_blocked(
                attempt,
                current,
                reason="duplicate_demo_instrument_position",
            )

        eligibility = await self.client.instrument_eligibility(
            instrument_id=attempt.instrument_id
        )
        prepared = self._apply_demo_terms(attempt, eligibility, current)

        submission_request_id = str(uuid4())
        submitting = prepared.model_copy(
            update={
                "state": ExecutionAttemptState.SUBMITTING,
                "broker_request_id": submission_request_id,
                "updated_at": current,
            }
        )
        self.storage.save_execution_attempt(submitting)
        self.storage.add_event(
            "demo_execution_submitting",
            submitting.model_dump_json(),
        )

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
                settlement_type=submitting.settlement_type,
                leverage=submitting.leverage,
            )
            broker_order_id = _extract_id(
                result.response,
                "orderId",
                "orderID",
                "order_id",
            )
            lookup = await self._lookup_until_settled(
                reference_id=submission_request_id
            )
        except EtoroExecutionTransportError as exc:
            lookup = await self._lookup_until_settled(
                reference_id=submission_request_id
            )
            if lookup is None:
                return self._mark_unknown(
                    submitting,
                    datetime.now(UTC),
                    reason="transport_error_after_demo_submission",
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
            lookup = await self._lookup_until_settled(
                reference_id=submission_request_id
            )
            if lookup is None:
                return self._mark_unknown(
                    submitting,
                    datetime.now(UTC),
                    reason=f"ambiguous_demo_http_error_after_submission:{exc.status_code}",
                    request_id=submission_request_id,
                )
            broker_order_id = lookup.order_id

        if lookup is None:
            return self._mark_unknown(
                submitting,
                datetime.now(UTC),
                reason="demo_broker_order_lookup_unresolved",
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
                reason=f"demo_order_still_in_flight:{lookup.status_name or lookup.status_id}",
                request_id=submission_request_id,
                broker_order_id=broker_order_id,
            )

        executed = lookup.status_id in _FILLED_STATUS_IDS
        partial_rejected = lookup.status_id in _PARTIAL_REJECT_STATUS_IDS
        if not executed and not partial_rejected:
            return self._mark_unknown(
                submitting,
                datetime.now(UTC),
                reason=f"unknown_demo_order_status:{lookup.status_id}:{lookup.status_name}",
                request_id=submission_request_id,
                broker_order_id=broker_order_id,
            )

        position_ids = lookup.position_ids
        if len(position_ids) != 1:
            reason = (
                "filled_demo_order_missing_position_id"
                if not position_ids
                else "multiple_demo_positions_from_single_open_order"
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
                reason="demo_lookup_missing_order_id",
                request_id=submission_request_id,
            )

        acknowledged = submitting.model_copy(
            update={
                "state": ExecutionAttemptState.ACKNOWLEDGED,
                "broker_order_id": broker_order_id,
                "broker_position_id": position_ids[0],
                "reason": (
                    _lookup_rejection_reason(lookup)
                    if partial_rejected
                    else None
                ),
                "updated_at": datetime.now(UTC),
            }
        )
        self.storage.save_execution_attempt(acknowledged)
        self.storage.add_event(
            "demo_execution_acknowledged",
            json.dumps(
                {
                    "attempt": acknowledged.model_dump(mode="json"),
                    "lookup": lookup.response,
                },
                default=str,
            ),
        )

        try:
            pnl_after = await self.client.account_pnl()
            visible_positions = _position_ids_from_pnl(pnl_after)
        except (EtoroExecutionError, EtoroExecutionTransportError, RuntimeError) as exc:
            return self._mark_unknown(
                acknowledged,
                datetime.now(UTC),
                reason=f"demo_position_verification_failed:{type(exc).__name__}",
                request_id=submission_request_id,
                broker_order_id=broker_order_id,
            )
        if acknowledged.broker_position_id not in visible_positions:
            return self._mark_unknown(
                acknowledged,
                datetime.now(UTC),
                reason="demo_position_not_visible_after_fill",
                request_id=submission_request_id,
                broker_order_id=broker_order_id,
            )

        reconciled = acknowledged.model_copy(
            update={
                "state": ExecutionAttemptState.RECONCILED,
                "updated_at": datetime.now(UTC),
            }
        )
        self.storage.save_execution_attempt(reconciled)
        self.storage.set("demo_execution_uncertain", "0")
        self.storage.add_event(
            "demo_execution_reconciled",
            reconciled.model_dump_json(),
        )
        return reconciled

    def _assert_demo_allowed(self, attempt: ExecutionAttempt) -> None:
        config = self.settings.execution
        if not config.autonomous_enabled:
            raise RuntimeError("Autonomous execution is disabled")
        if config.autonomous_mode != "demo":
            raise RuntimeError("Autonomous demo execution requires autonomous_mode=demo")
        if not config.autonomous_demo_enabled:
            raise RuntimeError("Autonomous demo execution is disabled")
        if attempt.state != ExecutionAttemptState.DEMO_PENDING:
            raise RuntimeError(f"Execution attempt is not DEMO_PENDING: {attempt.state}")

    def _assert_no_uncertain_demo_execution(self) -> None:
        if self.storage.get("demo_execution_uncertain") == "1":
            raise RuntimeError("Demo execution blocked: unresolved broker outcome")

    async def _assert_demo_identity(self) -> None:
        expected_gcid = self.settings.etoro.expected_gcid
        if expected_gcid is None:
            raise RuntimeError("Demo execution blocked: etoro.expected_gcid is not configured")
        try:
            identity = await self.client.identity()
        except (EtoroExecutionError, EtoroExecutionTransportError) as exc:
            raise RuntimeError(
                f"Demo execution blocked: unable to validate eToro identity: {exc}"
            ) from exc

        if identity.gcid != expected_gcid:
            raise RuntimeError(
                f"Demo execution blocked: eToro GCID {identity.gcid} does not match "
                f"pinned Agent Portfolio GCID {expected_gcid}"
            )
        if identity.demo_cid is None:
            raise RuntimeError("Demo execution blocked: authenticated identity has no demo CID")

        required = set(self.settings.etoro.required_demo_scopes)
        missing = sorted(required - set(identity.scopes))
        if missing:
            raise RuntimeError(
                "Demo execution blocked: eToro token is missing required scopes: "
                + ", ".join(missing)
            )
        self.storage.set("etoro_demo_authenticated_gcid", str(identity.gcid))
        self.storage.set("etoro_demo_authenticated_cid", str(identity.demo_cid))
        self.storage.set(
            "etoro_demo_authenticated_scopes",
            json.dumps(sorted(identity.scopes)),
        )

    def _apply_demo_terms(
        self,
        attempt: ExecutionAttempt,
        eligibility: EtoroEligibilityResult,
        now: datetime,
    ) -> ExecutionAttempt:
        if not eligibility.allow_open_position:
            return self._raise_blocked("eToro does not allow opening this demo instrument")

        direction = "LONG" if attempt.side.strip().upper() == "BUY" else "SHORT"
        allowed = [
            leverage
            for leverage in eligibility.allowed_leverages(direction=direction)
            if leverage <= self.settings.execution.autonomous_demo_max_leverage
        ]
        if not allowed:
            return self._raise_blocked(
                "No demo leverage is allowed within autonomous leverage cap"
            )

        gate_max_notional = attempt.amount_usd
        demo_cap = self.settings.execution.autonomous_demo_max_amount_usd
        diagnostics: list[str] = []
        for leverage in allowed:
            max_capital = min(demo_cap, gate_max_notional / leverage)
            if max_capital <= 0:
                continue
            minimum = eligibility.minimum_amount(
                direction=direction,
                leverage=leverage,
            )
            chosen = minimum if minimum is not None else min(10.0, max_capital)
            if minimum is not None and minimum > max_capital:
                diagnostics.append(
                    f"x{leverage}: minimum {minimum:.2f} > max {max_capital:.2f}"
                )
                continue
            settlement = eligibility.settlement_type(
                direction=direction,
                leverage=leverage,
            )
            updated = attempt.model_copy(
                update={
                    "amount_usd": chosen,
                    "leverage": leverage,
                    "settlement_type": settlement,
                    "updated_at": now,
                }
            )
            self.storage.save_execution_attempt(updated)
            return updated

        detail = "; ".join(diagnostics) or "no viable broker configuration"
        return self._raise_blocked(
            f"No demo execution terms satisfy risk and broker limits: {detail}"
        )

    @staticmethod
    def _raise_blocked(message: str) -> ExecutionAttempt:
        raise RuntimeError(message)

    async def _lookup_until_settled(
        self,
        *,
        reference_id: str,
    ) -> EtoroOrderLookupResult | None:
        attempts = self.settings.execution.broker_lookup_attempts
        interval = self.settings.execution.broker_lookup_interval_seconds
        last: EtoroOrderLookupResult | None = None
        for index in range(attempts):
            try:
                last = await self.client.lookup_order(reference_id=reference_id)
            except EtoroExecutionError as exc:
                if not _is_transient_lookup_error(exc.status_code):
                    return None
            except EtoroExecutionTransportError:
                pass
            else:
                if last.status_id not in _IN_FLIGHT_STATUS_IDS:
                    return last
            if index + 1 < attempts and interval > 0:
                await asyncio.sleep(interval)
        return last

    def _mark_blocked(
        self,
        attempt: ExecutionAttempt,
        now: datetime,
        *,
        reason: str,
    ) -> ExecutionAttempt:
        blocked = attempt.model_copy(
            update={
                "state": ExecutionAttemptState.BLOCKED,
                "reason": reason,
                "updated_at": now,
            }
        )
        self.storage.save_execution_attempt(blocked)
        self.storage.add_event("demo_execution_blocked", blocked.model_dump_json())
        return blocked

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
        self.storage.add_event("demo_execution_rejected", rejected.model_dump_json())
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
        self.storage.set("demo_execution_uncertain", "1")
        self.storage.add_event(
            "demo_execution_outcome_unknown",
            unknown.model_dump_json(),
        )
        return unknown


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


def _client_portfolio(body: dict[str, Any]) -> dict[str, Any]:
    candidate: Any = body.get("clientPortfolio")
    if candidate is None:
        data = body.get("data")
        if isinstance(data, dict):
            candidate = data.get("clientPortfolio") or data
    if not isinstance(candidate, dict):
        raise RuntimeError("eToro demo PnL response is missing clientPortfolio")
    return candidate


def _positions_from_pnl(body: dict[str, Any]) -> list[dict[str, Any]]:
    portfolio = _client_portfolio(body)
    direct = portfolio.get("positions")
    result = (
        [item for item in direct if isinstance(item, dict)]
        if isinstance(direct, list)
        else []
    )
    mirrors = portfolio.get("mirrors")
    if isinstance(mirrors, list):
        for mirror in mirrors:
            if not isinstance(mirror, dict):
                continue
            positions = mirror.get("positions")
            if isinstance(positions, list):
                result.extend(
                    item for item in positions if isinstance(item, dict)
                )
    return result


def _position_ids_from_pnl(body: dict[str, Any]) -> set[str]:
    result: set[str] = set()
    for position in _positions_from_pnl(body):
        raw = (
            position.get("positionId")
            or position.get("positionID")
            or position.get("PositionID")
            or position.get("id")
        )
        if raw is not None:
            result.add(str(raw))
    return result


def _open_instrument_ids_from_pnl(body: dict[str, Any]) -> set[int]:
    result: set[int] = set()
    for position in _positions_from_pnl(body):
        raw = (
            position.get("instrumentId")
            or position.get("instrumentID")
            or position.get("InstrumentID")
        )
        if raw is not None:
            result.add(int(raw))
    return result
