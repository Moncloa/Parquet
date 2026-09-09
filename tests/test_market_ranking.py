from datetime import UTC, datetime, timedelta

from parquet.market.ranking import rank_stream_candidates


def _candidate(prices: list[float], *, spread_bps: float | None = 5.0) -> dict[str, object]:
    start = datetime(2026, 9, 9, 10, 0, tzinfo=UTC)
    points = [
        {"t": (start + timedelta(minutes=index)).isoformat(), "p": price}
        for index, price in enumerate(prices)
    ]
    return {
        "instrument_id": 1,
        "price": prices[-1],
        "spread_bps": spread_bps,
        "sample_count": len(points),
        "first_at": points[0]["t"],
        "last_at": points[-1]["t"],
        "points": points,
        "score": 0.0,
    }


def test_ranking_exposes_time_window_metrics() -> None:
    ranked = rank_stream_candidates(
        [_candidate([100, 100.1, 100.2, 100.3, 100.5, 100.7, 100.9])],
        max_spread_bps=30.0,
    )
    item = ranked[0]
    assert item["change_pct_2m"] is not None
    assert item["change_pct_5m"] is not None
    assert item["change_pct_15m"] is None
    assert item["persistence"] > 0.9
    assert item["directional_efficiency"] > 0.9
    assert item["tick_rate_per_min"] == 1.0
    assert item["score"] > 0


def test_ranking_penalizes_single_spike_vs_persistent_move() -> None:
    persistent = _candidate([100, 100.1, 100.2, 100.3, 100.4, 100.5, 100.6])
    persistent["instrument_id"] = 1
    spike = _candidate([100, 100, 100, 100, 100, 100, 100.6])
    spike["instrument_id"] = 2
    ranked = rank_stream_candidates([spike, persistent], max_spread_bps=30.0)
    assert ranked[0]["instrument_id"] == 1
    assert ranked[0]["spike_ratio"] < ranked[1]["spike_ratio"]


def test_ranking_penalizes_wide_known_spread() -> None:
    narrow = _candidate([100, 100.1, 100.2, 100.3, 100.4, 100.5])
    narrow["instrument_id"] = 1
    wide = _candidate([100, 100.1, 100.2, 100.3, 100.4, 100.5], spread_bps=30.0)
    wide["instrument_id"] = 2
    ranked = rank_stream_candidates([wide, narrow], max_spread_bps=30.0)
    assert ranked[0]["instrument_id"] == 1
