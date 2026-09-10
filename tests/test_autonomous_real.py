from datetime import UTC, datetime

import pytest

from parquet.config import EtoroConfig, ExecutionConfig, Settings
from parquet.execution.autonomous import ExecutionAttempt, ExecutionAttemptState
from parquet.execution.autonomous_real import AutonomousRealExecutionAdapter
from parquet.models import RiskSnapshot
from parquet.portfolio import PositionManager
from parquet.storage import Storage


def _attempt(amount_usd: float, leverage: int = 2) -> ExecutionAttempt:
    now = datetime(2026, 9, 10, 6, 0, tzinfo=UTC)
    return ExecutionAttempt(
        attempt_id="attempt-real-1",
        proposal_id="proposal-real-1",
        watch_id="direct-proposal:proposal-real-1",
        symbol="OIL",
        instrument_id=1001,
        side="SELL",
        amount_usd=amount_usd,
        leverage=leverage,
        settlement_type="CFD",
        stop_loss=95.0,
        take_profit=94.0,
        created_at=now,
        updated_at=now,
        state=ExecutionAttemptState.PREPARED,
    )


def _adapter(tmp_path, execution: ExecutionConfig) -> AutonomousRealExecutionAdapter:  # type: ignore[no-untyped-def]
    settings = Settings(
        state_db=tmp_path / "state.db",
        etoro=EtoroConfig(expected_gcid=49462743),
        execution=execution,
    )
    storage = Storage(settings.state_db)
    storage.set_risk_snapshot(
        RiskSnapshot(
            as_of=datetime(2026, 9, 10, 6, 0, tzinfo=UTC),
            equity_usd=10_000.0,
        )
    )
    return AutonomousRealExecutionAdapter(
        settings=settings,
        storage=storage,
        position_manager=PositionManager(storage),
        reconciliation=object(),  # type: ignore[arg-type]
        client=object(),  # type: ignore[arg-type]
    )


def test_autonomous_real_is_disabled_by_default(tmp_path) -> None:  # type: ignore[no-untyped-def]
    adapter = _adapter(
        tmp_path,
        ExecutionConfig(autonomous_enabled=True, autonomous_mode="real"),
    )
    with pytest.raises(RuntimeError, match="Autonomous real execution is disabled"):
        adapter._assert_supervised_allowed(_attempt(10.0), "")


def test_autonomous_real_accepts_amount_at_percentage_cap(tmp_path) -> None:  # type: ignore[no-untyped-def]
    adapter = _adapter(
        tmp_path,
        ExecutionConfig(
            autonomous_enabled=True,
            autonomous_mode="real",
            autonomous_real_enabled=True,
            autonomous_real_max_position_pct=12.5,
            autonomous_real_max_leverage=2,
        ),
    )
    adapter._assert_supervised_allowed(_attempt(1250.0, leverage=2), "")


def test_autonomous_real_rejects_amount_above_percentage_cap(tmp_path) -> None:  # type: ignore[no-untyped-def]
    adapter = _adapter(
        tmp_path,
        ExecutionConfig(
            autonomous_enabled=True,
            autonomous_mode="real",
            autonomous_real_enabled=True,
            autonomous_real_max_position_pct=12.5,
            autonomous_real_max_leverage=2,
        ),
    )
    with pytest.raises(RuntimeError, match="exceeds autonomous real cap"):
        adapter._assert_supervised_allowed(_attempt(1250.01, leverage=2), "")


def test_autonomous_real_rejects_excess_leverage(tmp_path) -> None:  # type: ignore[no-untyped-def]
    adapter = _adapter(
        tmp_path,
        ExecutionConfig(
            autonomous_enabled=True,
            autonomous_mode="real",
            autonomous_real_enabled=True,
            autonomous_real_max_position_pct=12.5,
            autonomous_real_max_leverage=2,
        ),
    )
    with pytest.raises(RuntimeError, match="Leverage x3 exceeds autonomous real cap x2"):
        adapter._assert_supervised_allowed(_attempt(100.0, leverage=3), "")
