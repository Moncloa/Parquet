from __future__ import annotations

from parquet.autonomous_orchestrator import _filter_stream_candidates


def candidate(
    instrument_id: int,
    *,
    samples: int = 20,
    first_at: str = "2026-09-09T08:00:00Z",
    last_at: str = "2026-09-09T08:05:00Z",
    spread_bps: float | None = 5.0,
) -> dict[str, object]:
    return {
        "instrument_id": instrument_id,
        "sample_count": samples,
        "first_at": first_at,
        "last_at": last_at,
        "spread_bps": spread_bps,
        "score": 1.0,
    }


def test_filter_stream_candidates_rejects_sparse_short_and_wide_spread() -> None:
    items = [
        candidate(1),
        candidate(2, samples=4),
        candidate(3, last_at="2026-09-09T08:00:30Z"),
        candidate(4, spread_bps=31.0),
        candidate(5, spread_bps=None),
    ]

    filtered = _filter_stream_candidates(items, limit=20, max_spread_bps=30.0)

    assert [item["instrument_id"] for item in filtered] == [1, 5]


def test_filter_stream_candidates_preserves_rank_order_and_limit() -> None:
    items = [candidate(1), candidate(2), candidate(3)]

    filtered = _filter_stream_candidates(items, limit=2, max_spread_bps=30.0)

    assert [item["instrument_id"] for item in filtered] == [1, 2]
