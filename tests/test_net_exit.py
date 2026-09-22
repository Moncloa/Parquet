from parquet.execution.net_exit import evaluate_net_exit


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
