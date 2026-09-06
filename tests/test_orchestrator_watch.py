from datetime import UTC, datetime, timedelta

from parquet.config import Settings
from parquet.models import (
    Bias,
    MarketAnalysis,
    MarketObservation,
    Trigger,
    TriggerAction,
    TriggerType,
    WatchEventType,
    WatchItem,
)
from parquet.orchestrator import Orchestrator


def test_triggered_watch_schedules_reassessment(tmp_path) -> None:
    settings = Settings(state_db=tmp_path / "parquet.db")
    orchestrator = Orchestrator(settings)
    now = datetime.now(UTC)
    analysis = MarketAnalysis(
        analysis_id="a-1",
        generated_at=now,
        watch=[
            WatchItem(
                watch_id="w-1",
                symbol="NSDQ100",
                bias=Bias.LONG,
                trigger=Trigger(type=TriggerType.PRICE_ABOVE, price=100),
                expires_at=now + timedelta(hours=1),
                on_trigger=TriggerAction.REASSESS,
            )
        ],
    )
    orchestrator.process_analysis(analysis)

    events = orchestrator.process_observation(
        MarketObservation(symbol="NSDQ100", price=101, observed_at=now + timedelta(minutes=1))
    )
    assert events[0].event == WatchEventType.TRIGGERED
    assert any(review.reason == "watch_trigger:w-1" for review in orchestrator.reviews.pending())
    assert orchestrator.storage.active_watches() == []
