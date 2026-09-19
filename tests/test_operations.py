from datetime import UTC, datetime, timedelta

from parquet.config import ExecutionConfig, Settings
from parquet.models import MarketAnalysis, NextReview, RiskSnapshot
from parquet.operations import build_operations_snapshot
from parquet.portfolio import BrokerPortfolioSnapshot, ManagedPosition
from parquet.scheduler import ReviewQueue, ScheduledReview
from parquet.storage import Storage


class FakeOrchestrator:
    def __init__(self, storage: Storage) -> None:
        self.storage = storage
        self.reviews = ReviewQueue()


def test_operations_snapshot_connects_reviews_decisions_and_positions(tmp_path) -> None:
    storage = Storage(tmp_path / "state.db")
    orchestrator = FakeOrchestrator(storage)
    now = datetime(2026, 9, 19, 9, 30, tzinfo=UTC)

    analysis = MarketAnalysis(
        analysis_id="local-analysis-1",
        review_request_id="request-1",
        generated_at=now,
        market_regime="mixed",
        summary="GER40 closest; spread too wide. GOLD lacks confirmation.",
        next_review=NextReview(at=now + timedelta(hours=1), reason="reassess"),
    )
    storage.save_analysis(analysis.analysis_id, now.isoformat(), analysis.model_dump_json())
    storage.add_event(
        "review_request",
        '{"schema_version":1,"request_id":"request-1","requested_at":"2026-09-19T09:29:00Z","reason":"manual_opportunity_scan","symbols":[],"context":{}}',
    )
    storage.add_event(
        "direct_proposal_execution_rejected",
        '{"proposal_id":"p1","symbol":"GOLD","reasons":["spread_too_wide"],"gate":{"approved":false,"spread_bps":42.0}}',
    )
    storage.set_risk_snapshot(
        RiskSnapshot(
            as_of=now,
            equity_usd=1000.0,
            daily_pnl_pct=1.2,
            weekly_pnl_pct=-0.5,
        )
    )
    storage.set_broker_portfolio_snapshot(
        BrokerPortfolioSnapshot(
            captured_at=now,
            equity_usd=1000.0,
            available_cash_usd=900.0,
            invested_usd=100.0,
            unrealized_pnl_usd=3.0,
            credit_usd=1000.0,
        )
    )
    storage.save_managed_position(
        ManagedPosition(
            local_id="a1",
            broker_position_id="b1",
            proposal_id="p-open",
            instrument_id=1,
            symbol="OIL",
            side="BUY",
            opened_at=now,
            amount_usd=100.0,
            last_unrealized_pnl_usd=2.0,
        )
    )
    orchestrator.reviews.add(
        ScheduledReview(
            at=now + timedelta(minutes=30),
            reason="market_open:wall_street_open",
            source="structural",
        )
    )
    settings = Settings(
        execution=ExecutionConfig(
            autonomous_real_min_position_pct=10.0,
            autonomous_real_max_position_pct=50.0,
        )
    )

    snapshot = build_operations_snapshot(settings, orchestrator)

    assert snapshot["overview"]["equity_usd"] == 1000.0
    assert snapshot["runtime"]["position_min_pct"] == 10.0
    assert snapshot["runtime"]["position_max_pct"] == 50.0
    assert snapshot["reviews"][0]["reason"] == "manual_opportunity_scan"
    assert snapshot["reviews"][0]["no_trade"] is True
    outcomes = {item["outcome"] for item in snapshot["decisions"]}
    assert "NO_TRADE" in outcomes
    assert "REJECTED" in outcomes
    assert snapshot["positions"]["open"][0]["symbol"] == "OIL"
    assert snapshot["pending_reviews"][0]["source"] == "structural"
