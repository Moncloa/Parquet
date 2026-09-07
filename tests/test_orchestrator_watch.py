from datetime import UTC, datetime, timedelta

import httpx
import pytest

from parquet.config import EtoroConfig, Settings
from parquet.market.etoro import EtoroMarketDataClient
from parquet.models import (
    Bias,
    MarketAnalysis,
    MarketObservation,
    ReviewRequest,
    Trigger,
    TriggerAction,
    TriggerType,
    WatchEventType,
    WatchItem,
)
from parquet.orchestrator import Orchestrator
from parquet.scheduler import ScheduledReview


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


class FakeBridge:
    def __init__(self) -> None:
        self.requests: list[ReviewRequest] = []

    async def post_review_request(self, request: ReviewRequest) -> None:
        self.requests.append(request)


@pytest.mark.asyncio
async def test_due_review_contains_etoro_quote_context(tmp_path) -> None:
    now = datetime(2026, 9, 7, 8, 31, tzinfo=UTC)

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.params["instrumentIds"] == "1001"
        return httpx.Response(
            200,
            json={
                "rates": [
                    {
                        "instrumentId": 1001,
                        "symbol": "GER40",
                        "bid": 24500.0,
                        "ask": 24502.0,
                        "lastPrice": 24501.0,
                        "change": 0.2,
                        "timestamp": "2026-09-07T08:30:59Z",
                    }
                ]
            },
        )

    settings = Settings(
        state_db=tmp_path / "parquet.db",
        etoro=EtoroConfig(
            review_symbols=["GER40"],
            instrument_ids={"GER40": 1001},
        ),
    )
    client = EtoroMarketDataClient(
        api_key="api",
        user_key="user",
        transport=httpx.MockTransport(handler),
    )
    orchestrator = Orchestrator(settings, market_client=client)
    fake_bridge = FakeBridge()
    orchestrator.bridge = fake_bridge  # type: ignore[assignment]
    orchestrator.add_review(ScheduledReview(at=now, reason="macro_release", source="chatgpt"))

    posted = await orchestrator.post_due_reviews(now)

    assert posted == 1
    request = fake_bridge.requests[0]
    assert request.symbols == ["GER40"]
    market_data = request.context["market_data"]
    assert isinstance(market_data, dict)
    quotes = market_data["quotes"]
    assert isinstance(quotes, dict)
    quote = quotes["GER40"]
    assert isinstance(quote, dict)
    assert quote["last_price"] == 24501.0
    assert quote["stale"] is False


@pytest.mark.asyncio
async def test_market_poll_evaluates_active_watch(tmp_path) -> None:
    now = datetime(2026, 9, 7, 9, 0, tzinfo=UTC)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "rates": [
                    {
                        "instrumentId": 1002,
                        "symbol": "NSDQ100",
                        "bid": 100.9,
                        "ask": 101.1,
                        "lastPrice": 101.0,
                        "timestamp": "2026-09-07T09:00:00Z",
                    }
                ]
            },
        )

    settings = Settings(
        state_db=tmp_path / "parquet.db",
        etoro=EtoroConfig(
            review_symbols=[],
            instrument_ids={"NSDQ100": 1002},
        ),
    )
    client = EtoroMarketDataClient(
        api_key="api",
        user_key="user",
        transport=httpx.MockTransport(handler),
    )
    orchestrator = Orchestrator(settings, market_client=client)
    orchestrator.process_analysis(
        MarketAnalysis(
            analysis_id="a-market",
            generated_at=now,
            watch=[
                WatchItem(
                    watch_id="w-market",
                    symbol="NSDQ100",
                    bias=Bias.LONG,
                    trigger=Trigger(type=TriggerType.PRICE_ABOVE, price=100),
                    expires_at=now + timedelta(hours=1),
                    on_trigger=TriggerAction.REASSESS,
                )
            ],
        )
    )

    processed = await orchestrator.poll_market_once(now)

    assert processed == 1
    assert orchestrator.storage.active_watches(now + timedelta(seconds=1)) == []
    assert any(
        review.reason == "watch_trigger:w-market" for review in orchestrator.reviews.pending()
    )
