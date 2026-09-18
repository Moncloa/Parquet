from __future__ import annotations

import asyncio
from datetime import UTC, datetime

from parquet.execution.autonomous import ExecutionAttempt, ExecutionAttemptState
from parquet.execution.supervised import RealSmallExecutionAdapter
from parquet.portfolio import ReconciliationState


class AutonomousRealExecutionAdapter(RealSmallExecutionAdapter):
    """Real broker adapter for autonomous execution with a separate hard safety envelope.

    The broker submission/reconciliation implementation is deliberately inherited from
    the supervised adapter so both paths keep the same durable request-id, no-blind-retry,
    identity, broker-terms and post-trade reconciliation guarantees.
    """

    async def execute(
        self,
        attempt: ExecutionAttempt,
        *,
        confirmation: str = "",
        now: datetime | None = None,
    ) -> ExecutionAttempt:
        result = await super().execute(attempt, confirmation=confirmation, now=now)
        if (
            result.state == ExecutionAttemptState.OUTCOME_UNKNOWN
            and result.reason == "filled_position_not_visible_after_reconciliation"
            and result.broker_position_id is not None
        ):
            return await self._recover_delayed_position_visibility(result)
        return result

    async def _recover_delayed_position_visibility(
        self,
        attempt: ExecutionAttempt,
    ) -> ExecutionAttempt:
        """Retry read-only reconciliation when a confirmed fill propagates slowly.

        eToro can acknowledge a filled order before the new position is visible in the
        portfolio endpoint.  Never repeat the write: only poll broker state.  If the
        position becomes visible, resolve the uncertainty and persist RECONCILED;
        otherwise preserve OUTCOME_UNKNOWN so execution remains fail-closed.
        """

        attempts = self.settings.execution.broker_lookup_attempts
        interval = self.settings.execution.broker_lookup_interval_seconds
        for index in range(attempts):
            if index > 0 and interval > 0:
                await asyncio.sleep(interval)
            await self.reconciliation.poll_once(force=True)
            report = self.storage.get_reconciliation_report()
            snapshot = self.storage.get_broker_portfolio_snapshot()
            if (
                report is not None
                and report.state == ReconciliationState.SYNCED
                and snapshot is not None
                and any(
                    position.position_id == attempt.broker_position_id
                    for position in snapshot.positions
                )
            ):
                reconciled = attempt.model_copy(
                    update={
                        "state": ExecutionAttemptState.RECONCILED,
                        "reason": None,
                        "updated_at": datetime.now(UTC),
                    }
                )
                self.storage.save_execution_attempt(reconciled)
                self.storage.set("execution_uncertain", "0")
                self.storage.add_event(
                    "autonomous_real_delayed_reconciliation_recovered",
                    reconciled.model_dump_json(),
                )
                return reconciled
        return attempt

    def _assert_supervised_allowed(
        self,
        attempt: ExecutionAttempt,
        confirmation: str,
    ) -> None:
        del confirmation
        config = self.settings.execution
        if not config.autonomous_enabled:
            raise RuntimeError("Autonomous execution is disabled")
        if config.autonomous_mode != "real":
            raise RuntimeError("Autonomous real execution requires autonomous_mode=real")
        if not config.autonomous_real_enabled:
            raise RuntimeError("Autonomous real execution is disabled")
        if self.settings.etoro.expected_gcid is None:
            raise RuntimeError("Real execution blocked: etoro.expected_gcid is not configured")
        if attempt.state != ExecutionAttemptState.PREPARED:
            raise RuntimeError(f"Execution attempt is not PREPARED: {attempt.state}")
        if attempt.settlement_type is None or not attempt.settlement_type.strip():
            raise RuntimeError("Real execution blocked: prepared ticket has no settlement_type")

        if attempt.leverage > config.autonomous_real_max_leverage:
            raise RuntimeError(
                f"Leverage x{attempt.leverage} exceeds autonomous real cap "
                f"x{config.autonomous_real_max_leverage}"
            )

        snapshot = self.storage.get_risk_snapshot()
        if snapshot is None or snapshot.equity_usd is None:
            raise RuntimeError("Real execution blocked: risk equity is unavailable")
        minimum_capital = (
            snapshot.equity_usd * config.autonomous_real_min_position_pct / 100
        )
        if attempt.amount_usd + 1e-9 < minimum_capital:
            raise RuntimeError(
                f"Amount {attempt.amount_usd:.2f} is below autonomous real minimum "
                f"{minimum_capital:.2f} ({config.autonomous_real_min_position_pct:.2f}% equity)"
            )
        maximum_capital = (
            snapshot.equity_usd * config.autonomous_real_max_position_pct / 100
        )
        if attempt.amount_usd > maximum_capital + 1e-9:
            raise RuntimeError(
                f"Amount {attempt.amount_usd:.2f} exceeds autonomous real cap "
                f"{maximum_capital:.2f} ({config.autonomous_real_max_position_pct:.2f}% equity)"
            )

