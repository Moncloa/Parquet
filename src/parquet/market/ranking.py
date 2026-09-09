from __future__ import annotations

from datetime import UTC, datetime, timedelta
from statistics import pstdev
from typing import Any


def rank_stream_candidates(
    candidates: list[dict[str, Any]],
    *,
    max_spread_bps: float,
) -> list[dict[str, Any]]:
    """Re-rank scanner candidates using time-window momentum and move quality."""

    ranked: list[dict[str, Any]] = []
    for item in candidates:
        points = _parse_points(item.get("points"))
        if len(points) < 2:
            continue
        first_at, first_price = points[0]
        last_at, last_price = points[-1]
        span_minutes = max(0.0, (last_at - first_at).total_seconds() / 60.0)
        if first_price == 0 or span_minutes <= 0:
            continue

        overall_change = ((last_price / first_price) - 1.0) * 100.0
        change_2m = _window_change(points, minutes=2)
        change_5m = _window_change(points, minutes=5)
        change_15m = _window_change(points, minutes=15)

        step_returns_bps = [
            ((right[1] / left[1]) - 1.0) * 10_000.0
            for left, right in zip(points, points[1:], strict=False)
            if left[1] != 0
        ]
        volatility_bps = pstdev(step_returns_bps) if len(step_returns_bps) >= 2 else 0.0

        absolute_path = sum(
            abs(right[1] - left[1])
            for left, right in zip(points, points[1:], strict=False)
        )
        directional_efficiency = (
            0.0 if absolute_path == 0 else min(1.0, abs(last_price - first_price) / absolute_path)
        )

        direction = 1 if overall_change > 0 else -1 if overall_change < 0 else 0
        aligned_steps = (
            sum(
                1
                for value in step_returns_bps
                if value != 0 and (value > 0) == (direction > 0)
            )
            if direction != 0
            else 0
        )
        # Flat steps matter: a single jump after a long flat period must not look
        # as persistent as a move that advances in the same direction repeatedly.
        persistence = (
            0.0 if not step_returns_bps else aligned_steps / len(step_returns_bps)
        )

        absolute_steps = [abs(value) for value in step_returns_bps]
        total_absolute_bps = sum(absolute_steps)
        spike_ratio = (
            0.0
            if total_absolute_bps == 0
            else min(1.0, max(absolute_steps, default=0.0) / total_absolute_bps)
        )
        tick_rate_per_min = max(0.0, (len(points) - 1) / span_minutes)

        momentum = _weighted_momentum(change_2m, change_5m, change_15m, overall_change)
        acceleration_pct_per_min = _acceleration(change_2m, change_5m)
        activity = min(1.0, tick_rate_per_min / 5.0)
        direction_quality = (persistence + directional_efficiency) / 2.0

        score = momentum * (0.45 + 0.40 * direction_quality + 0.15 * activity)
        # Volatility is useful context, but it must not make a single jump rank
        # above a sustained move of similar magnitude.
        score += min(volatility_bps, 30.0) / 600.0
        score += min(max(acceleration_pct_per_min, 0.0), 1.0) * 0.10
        score += persistence * 0.10
        score -= spike_ratio * 0.65

        spread_raw = item.get("spread_bps")
        spread_bps = _optional_float(spread_raw)
        if spread_bps is not None and max_spread_bps > 0:
            score -= min(spread_bps / max_spread_bps, 2.0) * 0.15

        ranked.append(
            {
                **item,
                "change_pct_stream": round(overall_change, 5),
                "change_pct_2m": _round_optional(change_2m, 5),
                "change_pct_5m": _round_optional(change_5m, 5),
                "change_pct_15m": _round_optional(change_15m, 5),
                "directional_efficiency": round(directional_efficiency, 4),
                "persistence": round(persistence, 4),
                "spike_ratio": round(spike_ratio, 4),
                "tick_rate_per_min": round(tick_rate_per_min, 3),
                "acceleration_pct_per_min": round(acceleration_pct_per_min, 5),
                "step_volatility_bps": round(volatility_bps, 4),
                "span_minutes": round(span_minutes, 3),
                "score": round(max(score, 0.0), 5),
            }
        )

    ranked.sort(key=lambda item: float(item.get("score") or 0.0), reverse=True)
    return ranked


def _parse_points(value: Any) -> list[tuple[datetime, float]]:
    if not isinstance(value, list):
        return []
    result: list[tuple[datetime, float]] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        raw_time = item.get("t")
        raw_price = item.get("p")
        if raw_time is None or raw_price is None:
            continue
        try:
            timestamp = datetime.fromisoformat(str(raw_time).replace("Z", "+00:00"))
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=UTC)
            result.append((timestamp.astimezone(UTC), float(raw_price)))
        except (TypeError, ValueError):
            continue
    result.sort(key=lambda point: point[0])
    return result


def _window_change(points: list[tuple[datetime, float]], *, minutes: int) -> float | None:
    last_at, last_price = points[-1]
    cutoff = last_at - timedelta(minutes=minutes)
    eligible = [point for point in points if point[0] <= cutoff]
    if not eligible:
        return None
    baseline = eligible[-1][1]
    if baseline == 0:
        return None
    return ((last_price / baseline) - 1.0) * 100.0


def _weighted_momentum(
    change_2m: float | None,
    change_5m: float | None,
    change_15m: float | None,
    fallback: float,
) -> float:
    weighted = [(change_2m, 0.5), (change_5m, 0.3), (change_15m, 0.2)]
    available = [(abs(value), weight) for value, weight in weighted if value is not None]
    if not available:
        return abs(fallback)
    total_weight = sum(weight for _, weight in available)
    return sum(value * weight for value, weight in available) / total_weight


def _acceleration(change_2m: float | None, change_5m: float | None) -> float:
    if change_2m is None or change_5m is None:
        return 0.0
    recent_velocity = abs(change_2m) / 2.0
    earlier_move = abs(change_5m - change_2m)
    earlier_velocity = earlier_move / 3.0
    return recent_velocity - earlier_velocity


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _round_optional(value: float | None, digits: int) -> float | None:
    return None if value is None else round(value, digits)
