from parquet.operations_dashboard import render_operations_dashboard


def test_operations_dashboard_renders_core_sections() -> None:
    html = render_operations_dashboard(
        {
            "runtime": {
                "version": "0.11.0",
                "branch": "feat/operations-dashboard",
                "commit": "abcdef123456",
                "execution_mode": "real",
                "strategy_provider": "local_ollama",
                "position_min_pct": 10.0,
                "position_max_pct": 50.0,
                "max_leverage": 2,
            },
            "overview": {
                "equity_usd": 1000.0,
                "daily_pnl_pct": 1.0,
                "weekly_pnl_pct": 2.0,
                "open_positions": 1,
                "available_cash_usd": 800.0,
                "invested_usd": 200.0,
                "unrealized_pnl_usd": 5.0,
                "managed_closed_pnl_usd": 12.0,
                "managed_closed_pnl_estimated": True,
            },
            "pending_reviews": [
                {
                    "at": "2026-09-19T14:31:00+00:00",
                    "reason": "market_open:wall_street_open",
                    "source": "structural",
                }
            ],
            "reviews": [
                {
                    "generated_at": "2026-09-19T09:30:00+00:00",
                    "analysis_id": "local-analysis-1",
                    "reason": "manual_opportunity_scan",
                    "market_regime": "mixed",
                    "summary": "No trade after candidate comparison",
                    "no_trade": True,
                    "trade_proposals": [],
                    "watch": [],
                }
            ],
            "decisions": [
                {
                    "at": "2026-09-19T09:30:00+00:00",
                    "outcome": "REJECTED",
                    "symbol": "GOLD",
                    "proposal_id": "p1",
                    "reasons": ["spread_too_wide"],
                    "gate": {"spread_bps": 40.0},
                }
            ],
            "positions": {
                "open": [
                    {
                        "symbol": "OIL",
                        "side": "BUY",
                        "opened_at": "2026-09-19T09:00:00+00:00",
                        "amount_usd": 100.0,
                        "open_rate": 90.0,
                        "stop_loss_rate": 88.0,
                        "take_profit_rate": 94.0,
                        "pnl_usd": 2.0,
                        "pnl_pct": 2.0,
                    }
                ]
            },
            "watches": [],
            "system": {
                "reconciliation_state": "SYNCED",
                "identity_verified": True,
                "execution_uncertain": False,
                "strategy_pending_requests": 0,
                "reconciliation_issues": [],
            },
        }
    )

    assert "Parquet · Operations" in html
    assert "Reviews" in html
    assert "Decisions" in html
    assert "GOLD" in html
    assert "spread_too_wide" in html
    assert "OIL" in html
    assert "10–50%" in html
    assert "abcdef12" in html
