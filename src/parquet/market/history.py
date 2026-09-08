from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from math import ceil
from statistics import pstdev
from typing import Any

from pydantic import BaseModel

from parquet.models import MarketObservation
from parquet.storage import Storage


class MarketHistoryPoint(BaseModel):
    observed_at: datetime
    price: float
    instrument_id: int | None = None
    bid: float | None = None
    ask: float | None = None


class MarketHistoryStore:
    """Small persistent rolling history backed by the existing key/value store."""

    def __init__(self, storage: Storage) -> None:
        self.storage = storage

    def record(
        self,
        observation: MarketObservation,
        *,
        retention_minutes: int = 120,
        max_samples: int = 1000,
    ) -> None:
        symbol = observation.symbol.upper()
        points = self._load(symbol)
        point = MarketHistoryPoint(
            observed_at=observation.observed_at.astimezone(UTC),
            price=observation.price,
            instrument_id=observation.instrument_id,
            bid=observation.bid,
            ask=observation.ask,
        )
        if not points or points[-1].observed_at != point.observed_at:
            points.append(point)
        else:
            points[-1] = point
        cutoff = point.observed_at - timedelta(minutes=retention_minutes)
        points = [item for item in points if item.observed_at >= cutoff]
        if len(points) > max_samples:
            points = points[-max_samples:]
        self.storage.set(
            self._key(symbol),
            json.dumps([item.model_dump(mode="json") for item in points]),
        )

    def context(
        self,
        symbol: str,
        *,
        now: datetime,
        retention_minutes: int = 120,
        max_points: int = 30,
    ) -> dict[str, Any]:
        current = now.astimezone(UTC)
        cutoff = current - timedelta(minutes=retention_minutes)
        points = [
            item
            for item in self._load(symbol.upper())
            if cutoff <= item.observed_at <= current
        ]
        if not points:
            return {
                "sample_count": 0,
                "span_minutes": 0.0,
                "points": [],
                "metrics": {},
            }
        compact = _downsample(points, max_points)
        return {
            "sample_count": len(points),
            "span_minutes": round(
                (points[-1].observed_at - points[0].observed_at).total_seconds()
                / 60.0,
                2,
            ),
            "first_at": points[0].observed_at.isoformat(),
            "last_at": points[-1].observed_at.isoformat(),
            "points": [
                {"t": item.observed_at.isoformat(), "p": item.price}
                for item in compact
            ],
            "metrics": _metrics(points, current),
        }

    def _load(self, symbol: str) -> list[MarketHistoryPoint]:
        raw = self.storage.get(self._key(symbol))
        if not raw:
            return []
        try:
            payload = json.loads(raw)
            if not isinstance(payload, list):
                return []
            points = [MarketHistoryPoint.model_validate(item) for item in payload]
        except (json.JSONDecodeError, ValueError, TypeError):
            return []
        return sorted(points, key=lambda item: item.observed_at)

    @staticmethod
    def _key(symbol: str) -> str:
        return f"market_history:{symbol.upper()}"


def _downsample(
    points: list[MarketHistoryPoint], max_points: int
) -> list[MarketHistoryPoint]:
    if len(points) <= max_points:
        return points
    stride = ceil((len(points) - 1) / (max_points - 1))
    sampled = points[::stride]
    if sampled[-1].observed_at != points[-1].observed_at:
        sampled.append(points[-1])
    return sampled[-max_points:]


def _metrics(
    points: list[MarketHistoryPoint], current: datetime
) -> dict[str, float | None]:
    latest = points[-1]
    metrics: dict[str, float | None] = {}
    for minutes in (5, 15, 60):
        baseline = _baseline(points, current - timedelta(minutes=minutes))
        metrics[f"change_pct_{minutes}m"] = (
            None if baseline is None else _pct_change(baseline.price, latest.price)
        )

    recent_60 = [
        item
        for item in points
        if item.observed_at >= current - timedelta(minutes=60)
    ]
    prices = [item.price for item in recent_60]
    if prices:
        high = max(prices)
        low = min(prices)
        metrics["high_60m"] = high
        metrics["low_60m"] = low
        metrics["range_pct_60m"] = _pct_change(low, high) if low else None
    else:
        metrics["high_60m"] = None
        metrics["low_60m"] = None
        metrics["range_pct_60m"] = None

    returns_bps = [
        ((right.price / left.price) - 1.0) * 10_000.0
        for left, right in zip(recent_60, recent_60[1:], strict=False)
        if left.price != 0
    ]
    metrics["step_volatility_bps_60m"] = (
        round(pstdev(returns_bps), 4) if len(returns_bps) >= 2 else None
    )
    return metrics


def _baseline(
    points: list[MarketHistoryPoint], target: datetime
) -> MarketHistoryPoint | None:
    candidates = [item for item in points if item.observed_at <= target]
    return candidates[-1] if candidates else None


def _pct_change(start: float, end: float) -> float | None:
    if start == 0:
        return None
    return round(((end / start) - 1.0) * 100.0, 5)
