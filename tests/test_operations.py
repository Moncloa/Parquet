from datetime import UTC, datetime, timedelta

from parquet.config import ExecutionConfig, Settings
from parquet.models import (
    Bias,
    MarketAnalysis,
    NextReview,
    RiskSnapshot,
    Trigger,
    TriggerAction,
    TriggerType,
    WatchItem,
)
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
    watch = WatchItem(
        watch_id="watch-1",
        symbol="GOLD",
        bias=Bias.LONG,
        trigger=Trigger(type=TriggerType.PRICE_ABOVE, price=2500.0),
        expires_at=now + timedelta(hours=2),
        on_trigger=TriggerAction.REASSESS,
        rationale="wait for breakout",
    )
    storage.save_watch(analysis.analysis_id, watch)
    storage.add_event(
        "watch_event",
        (
            '{"watch_id":"watch-1","symbol":"GOLD","event":"TRIGGERED",'
            '"observed_at":"2026-09-19T09:35:00Z","observed_price":2501.0,'
            '"action":"REASSESS","reason":"price_above"}'
        ),
    )
    storage.add_event(
        "review_request",
        (
            '{"schema_version":1,"request_id":"request-1",'
            '"requested_at":"2026-09-19T09:29:00Z",'
            '"reason":"manual_opportunity_scan","symbols":[],"context":{}}'
        ),
    )
    storage.add_event(
        "direct_proposal_execution_rejected",
        (
            '{"proposal_id":"p1","symbol":"GOLD",'
            '"reasons":["spread_too_wide"],'
            '"gate":{"approved":false,"spread_bps":42.0}}'
        ),
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
    storage.save_managed_position(
        ManagedPosition(
            local_id="a2",
            broker_position_id="b2",
            proposal_id="p-closed",
            instrument_id=2,
            symbol="GOLD",
            side="SELL",
            opened_at=now - timedelta(hours=1),
            status="CLOSED_AT_BROKER",
            amount_usd=200.0,
            closed_at=now + timedelta(minutes=10),
            realized_pnl_usd=4.0,
            pnl_estimated=True,
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
    families = {item["family"] for item in snapshot["timeline"]}
    assert {"review", "decision", "watch", "position_open", "position_close"} <= families
    close_events = [
        item for item in snapshot["timeline"] if item["event"] == "POSITION_CLOSED"
    ]
    assert close_events[0]["symbol"] == "GOLD"
    assert snapshot["watch_history"][0]["watch_id"] == "watch-1"
    assert snapshot["watch_events"][0]["event"] == "TRIGGERED"
