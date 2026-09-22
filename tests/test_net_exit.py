from parquet.execution.net_exit import calculate_protective_stop, evaluate_net_exit


def test_net_exit_holds_before_cost_adjusted_threshold() -> None:
    decision = evaluate_net_exit(
        gross_pnl_usd=80.0,
        estimated_open_cost_usd=5.0,
        estimated_close_cost_usd=5.0,
        initial_net_risk_usd=100.0,
        take_profit_net_r=1.5,
        protect_profit_net_r=0.75,
    )
    assert decision.action == "HOLD"
    assert decision.net_pnl_usd == 70.0
    assert decision.net_r_multiple == 0.7


def test_net_exit_protects_only_after_net_profit_threshold() -> None:
    decision = evaluate_net_exit(
        gross_pnl_usd=90.0,
        estimated_open_cost_usd=5.0,
        estimated_close_cost_usd=5.0,
        initial_net_risk_usd=100.0,
        take_profit_net_r=1.5,
        protect_profit_net_r=0.75,
    )
    assert decision.action == "PROTECT"
    assert decision.net_r_multiple == 0.8


def test_net_exit_closes_at_net_r_target() -> None:
    decision = evaluate_net_exit(
        gross_pnl_usd=160.0,
        estimated_open_cost_usd=5.0,
        estimated_close_cost_usd=5.0,
        initial_net_risk_usd=100.0,
        take_profit_net_r=1.5,
        protect_profit_net_r=0.75,
    )
    assert decision.action == "CLOSE"
    assert decision.net_pnl_usd == 150.0
    assert decision.net_r_multiple == 1.5


def test_net_exit_does_not_mistake_gross_profit_for_net_target() -> None:
    decision = evaluate_net_exit(
        gross_pnl_usd=150.0,
        estimated_open_cost_usd=5.0,
        estimated_close_cost_usd=5.0,
        initial_net_risk_usd=100.0,
        take_profit_net_r=1.5,
        protect_profit_net_r=0.75,
    )
    assert decision.action == "PROTECT"
    assert decision.net_r_multiple == 1.4



def test_protective_stop_long_covers_round_trip_costs() -> None:
    stop = calculate_protective_stop(
        side="BUY",
        open_rate=100.0,
        current_executable_price=102.0,
        current_stop_rate=99.0,
        exposure_usd=1000.0,
        estimated_open_cost_usd=2.0,
        estimated_close_cost_usd=2.0,
        initial_net_risk_usd=20.0,
    )
    assert stop.approved
    assert stop.stop_rate == 100.4
    assert stop.locked_net_profit_usd == 0.0


def test_protective_stop_can_lock_positive_net_r() -> None:
    stop = calculate_protective_stop(
        side="BUY",
        open_rate=100.0,
        current_executable_price=103.0,
        current_stop_rate=100.0,
        exposure_usd=1000.0,
        estimated_open_cost_usd=2.0,
        estimated_close_cost_usd=2.0,
        initial_net_risk_usd=20.0,
        lock_net_r=0.5,
    )
    assert stop.approved
    assert stop.stop_rate == 101.4
    assert stop.locked_net_profit_usd == 10.0


def test_protective_stop_never_worsens_existing_long_stop() -> None:
    stop = calculate_protective_stop(
        side="BUY",
        open_rate=100.0,
        current_executable_price=103.0,
        current_stop_rate=101.5,
        exposure_usd=1000.0,
        estimated_open_cost_usd=2.0,
        estimated_close_cost_usd=2.0,
        initial_net_risk_usd=20.0,
        lock_net_r=0.5,
    )
    assert not stop.approved
    assert stop.reason == "protective_stop_would_not_improve"


def test_protective_stop_short_is_above_executable_and_improves_stop() -> None:
    stop = calculate_protective_stop(
        side="SELL",
        open_rate=100.0,
        current_executable_price=97.0,
        current_stop_rate=101.0,
        exposure_usd=1000.0,
        estimated_open_cost_usd=2.0,
        estimated_close_cost_usd=2.0,
        initial_net_risk_usd=20.0,
        lock_net_r=0.5,
    )
    assert stop.approved
    assert stop.stop_rate == 98.6


def test_protective_stop_rejects_level_crossing_current_price() -> None:
    stop = calculate_protective_stop(
        side="BUY",
        open_rate=100.0,
        current_executable_price=100.3,
        current_stop_rate=99.0,
        exposure_usd=1000.0,
        estimated_open_cost_usd=2.0,
        estimated_close_cost_usd=2.0,
        initial_net_risk_usd=20.0,
    )
    assert not stop.approved
    assert stop.reason == "protective_stop_not_below_executable_price"
