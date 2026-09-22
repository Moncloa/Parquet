from datetime import UTC, datetime

import pytest

from parquet.config import EtoroConfig, ExecutionConfig, Settings
from parquet.execution.autonomous import ExecutionAttempt, ExecutionAttemptState
from parquet.execution.autonomous_real import AutonomousRealExecutionAdapter
from parquet.execution.etoro import EtoroEligibilityResult
from parquet.execution.sizing import choose_autonomous_real_terms
from parquet.models import RiskSnapshot
from parquet.portfolio import PositionManager
from parquet.storage import Storage


def _eligibility(*, leverages: list[int] | None = None) -> EtoroEligibilityResult:
    return EtoroEligibilityResult(
        request_id="eligibility-autonomous-real",
        instrument_id=1001,
        symbol="TEST",
        min_position_exposure=10.0,
        allow_open_position=True,
        leverage_configs=(
            {
                "settlementType": "CFD",
                "direction": "LONG",
                "leverageValues": leverages or [1, 2],
                "minPositionAmount": 10.0,
            },
        ),
        response={},
    )


def test_autonomous_real_percentage_defaults() -> None:
    config = ExecutionConfig()

    assert config.autonomous_real_min_position_pct == 10.0
    assert config.autonomous_real_max_position_pct == 50.0
    assert config.autonomous_real_max_leverage == 2


def test_autonomous_real_sizing_accepts_risk_amount_within_range() -> None:
    leverage, settlement, minimum, amount, maximum = choose_autonomous_real_terms(
        _eligibility(),
        direction="LONG",
        gate_maximum_notional_usd=2_500.0,
        minimum_capital_usd=1_000.0,
        maximum_capital_usd=5_000.0,
        max_leverage=2,
    )

    assert leverage == 1
    assert settlement == "CFD"
    assert minimum == 10.0
    assert amount == 2_500.0
    assert maximum == 2_500.0


def test_autonomous_real_sizing_uses_leverage_only_to_reach_risk_exposure() -> None:
    leverage, _, _, capital, capital_needed = choose_autonomous_real_terms(
        _eligibility(),
        direction="LONG",
        gate_maximum_notional_usd=8_000.0,
        minimum_capital_usd=1_000.0,
        maximum_capital_usd=5_000.0,
        max_leverage=2,
    )

    # x1 would expose only 5k because of the capital cap. x2 can express the
    # full 8k risk-approved exposure using 4k capital, so x2 is selected.
    assert leverage == 2
    assert capital == 4_000.0
    assert capital * leverage == 8_000.0
    assert capital_needed == 4_000.0


def test_autonomous_real_sizing_prefers_lowest_leverage_for_same_exposure() -> None:
    leverage, _, _, capital, _ = choose_autonomous_real_terms(
        _eligibility(),
        direction="LONG",
        gate_maximum_notional_usd=4_000.0,
        minimum_capital_usd=1_000.0,
        maximum_capital_usd=5_000.0,
        max_leverage=2,
    )

    assert leverage == 1
    assert capital == 4_000.0
    assert capital * leverage == 4_000.0


def test_autonomous_real_sizing_never_exceeds_risk_approved_exposure() -> None:
    leverage, _, _, capital, _ = choose_autonomous_real_terms(
        _eligibility(),
        direction="LONG",
        gate_maximum_notional_usd=3_500.0,
        minimum_capital_usd=1_000.0,
        maximum_capital_usd=5_000.0,
        max_leverage=2,
    )

    assert capital * leverage <= 3_500.0 + 1e-9


def test_autonomous_real_sizing_does_not_inflate_below_minimum() -> None:
    with pytest.raises(RuntimeError, match="below autonomous minimum"):
        choose_autonomous_real_terms(
            _eligibility(leverages=[2]),
            direction="LONG",
            gate_maximum_notional_usd=1_900.0,
            minimum_capital_usd=1_000.0,
            maximum_capital_usd=5_000.0,
            max_leverage=2,
        )


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


def test_autonomous_real_accepts_amount_within_percentage_range(tmp_path) -> None:  # type: ignore[no-untyped-def]
    adapter = _adapter(
        tmp_path,
        ExecutionConfig(
            autonomous_enabled=True,
            autonomous_mode="real",
            autonomous_real_enabled=True,
        ),
    )
    adapter._assert_supervised_allowed(_attempt(2500.0, leverage=2), "")


def test_autonomous_real_rejects_amount_below_percentage_minimum(tmp_path) -> None:  # type: ignore[no-untyped-def]
    adapter = _adapter(
        tmp_path,
        ExecutionConfig(
            autonomous_enabled=True,
            autonomous_mode="real",
            autonomous_real_enabled=True,
        ),
    )
    with pytest.raises(RuntimeError, match="below autonomous real minimum"):
        adapter._assert_supervised_allowed(_attempt(999.99, leverage=2), "")


def test_autonomous_real_rejects_amount_above_percentage_cap(tmp_path) -> None:  # type: ignore[no-untyped-def]
    adapter = _adapter(
        tmp_path,
        ExecutionConfig(
            autonomous_enabled=True,
            autonomous_mode="real",
            autonomous_real_enabled=True,
        ),
    )
    with pytest.raises(RuntimeError, match="exceeds autonomous real cap"):
        adapter._assert_supervised_allowed(_attempt(5000.01, leverage=2), "")


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


def test_execution_attempt_cost_basis_defaults_to_zero() -> None:
    attempt = _attempt(1000.0)
    assert attempt.estimated_open_cost_usd == 0.0
    assert attempt.initial_net_risk_usd is None
