from __future__ import annotations

from datetime import datetime

from parquet.execution.autonomous import ExecutionAttempt, ExecutionAttemptState
from parquet.execution.supervised import RealSmallExecutionAdapter


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
        now: datetime | None = None,
    ) -> ExecutionAttempt:
        return await super().execute(attempt, confirmation="", now=now)

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

        snapshot = self.storage.get_risk_snapshot()
        if snapshot is None or snapshot.equity_usd is None:
            raise RuntimeError("Real execution blocked: risk equity is unavailable")
        maximum_capital = (
            snapshot.equity_usd * config.autonomous_real_max_position_pct / 100
        )
        if attempt.amount_usd > maximum_capital + 1e-9:
            raise RuntimeError(
                f"Amount {attempt.amount_usd:.2f} exceeds autonomous real cap "
                f"{maximum_capital:.2f} ({config.autonomous_real_max_position_pct:.2f}% equity)"
            )
        if attempt.leverage > config.autonomous_real_max_leverage:
            raise RuntimeError(
                f"Leverage x{attempt.leverage} exceeds autonomous real cap "
                f"x{config.autonomous_real_max_leverage}"
            )
