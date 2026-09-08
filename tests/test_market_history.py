from datetime import UTC, datetime, timedelta

from parquet.market.history import MarketHistoryStore
from parquet.models import MarketObservation
from parquet.storage import Storage


def test_history_persists_and_exposes_window_metrics(tmp_path) -> None:
    storage = Storage(tmp_path / "parquet.db")
    history = MarketHistoryStore(storage)
    now = datetime(2026, 9, 8, 7, 0, tzinfo=UTC)

    for minutes_ago, price in ((60, 100.0), (15, 102.0), (5, 103.0), (0, 104.0)):
        history.record(
            MarketObservation(
                symbol="GER40",
                price=price,
                observed_at=now - timedelta(minutes=minutes_ago),
                instrument_id=32,
            )
        )

    reloaded = MarketHistoryStore(Storage(tmp_path / "parquet.db"))
    context = reloaded.context("GER40", now=now)

    assert context["sample_count"] == 4
    assert context["span_minutes"] == 60.0
    metrics = context["metrics"]
    assert isinstance(metrics, dict)
    assert metrics["change_pct_5m"] == 0.97087
    assert metrics["change_pct_15m"] == 1.96078
    assert metrics["change_pct_60m"] == 4.0
    assert metrics["high_60m"] == 104.0
    assert metrics["low_60m"] == 100.0
    assert metrics["range_pct_60m"] == 4.0


def test_history_does_not_invent_missing_windows(tmp_path) -> None:
    storage = Storage(tmp_path / "parquet.db")
    history = MarketHistoryStore(storage)
    now = datetime(2026, 9, 8, 7, 0, tzinfo=UTC)

    history.record(
        MarketObservation(
            symbol="GOLD",
            price=4500.0,
            observed_at=now - timedelta(minutes=4),
        )
    )
    history.record(
        MarketObservation(symbol="GOLD", price=4510.0, observed_at=now)
    )

    metrics = history.context("GOLD", now=now)["metrics"]
    assert isinstance(metrics, dict)
    assert metrics["change_pct_5m"] is None
    assert metrics["change_pct_15m"] is None
    assert metrics["change_pct_60m"] is None


def test_history_context_downsamples_without_losing_latest_point(tmp_path) -> None:
    storage = Storage(tmp_path / "parquet.db")
    history = MarketHistoryStore(storage)
    now = datetime(2026, 9, 8, 7, 0, tzinfo=UTC)

    for index in range(40):
        history.record(
            MarketObservation(
                symbol="EURUSD",
                price=1.10 + index / 10000,
                observed_at=now - timedelta(minutes=39 - index),
            )
        )

    context = history.context("EURUSD", now=now, max_points=10)
    points = context["points"]
    assert isinstance(points, list)
    assert len(points) <= 10
    assert points[-1]["p"] == 1.1039
