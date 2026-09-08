from __future__ import annotations

import asyncio
import json
from collections import defaultdict, deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from math import ceil
from statistics import pstdev
from typing import Any

import websockets


@dataclass(frozen=True)
class StreamTick:
    instrument_id: int
    observed_at: datetime
    price: float
    bid: float | None = None
    ask: float | None = None


class EtoroWebSocketScanner:
    """Wide, read-only eToro stream scanner with reconnect and local ranking."""

    def __init__(
        self,
        *,
        api_key: str,
        user_key: str,
        url: str = "wss://ws.etoro.com/ws",
        max_points_per_instrument: int = 240,
        on_tick: Callable[[StreamTick], Awaitable[None]] | None = None,
    ) -> None:
        self.api_key = api_key
        self.user_key = user_key
        self.url = url
        self.max_points_per_instrument = max_points_per_instrument
        self.on_tick = on_tick
        self.instrument_ids: list[int] = []
        self.series: dict[int, deque[StreamTick]] = defaultdict(
            lambda: deque(maxlen=self.max_points_per_instrument)
        )
        self.connected = False
        self.last_message_at: datetime | None = None
        self.last_error: str | None = None

    def set_universe(self, instrument_ids: list[int]) -> None:
        self.instrument_ids = sorted(set(instrument_ids))

    async def run_forever(self) -> None:
        backoff = 1.0
        while True:
            try:
                await self._run_once()
                backoff = 1.0
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.connected = False
                self.last_error = repr(exc)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2.0, 30.0)

    async def _run_once(self) -> None:
        async with websockets.connect(self.url, ping_interval=20, ping_timeout=20) as socket:
            await socket.send(
                json.dumps(
                    {
                        "operation": "Authenticate",
                        "data": {"userKey": self.user_key, "apiKey": self.api_key},
                    }
                )
            )
            for start in range(0, len(self.instrument_ids), 100):
                batch = self.instrument_ids[start : start + 100]
                await socket.send(
                    json.dumps(
                        {
                            "operation": "Subscribe",
                            "data": {
                                "topics": [f"instrument:{value}" for value in batch],
                                "snapshot": False,
                            },
                        }
                    )
                )
            self.connected = True
            self.last_error = None
            async for raw in socket:
                tick = parse_stream_tick(raw)
                if tick is None:
                    continue
                self.series[tick.instrument_id].append(tick)
                self.last_message_at = datetime.now(UTC)
                if self.on_tick is not None:
                    await self.on_tick(tick)

    def shortlist(self, limit: int = 20, history_points: int = 20) -> list[dict[str, Any]]:
        ranked: list[dict[str, Any]] = []
        active_ids = set(self.instrument_ids)
        for instrument_id, points in self.series.items():
            if instrument_id not in active_ids or len(points) < 2:
                continue
            first = points[0]
            last = points[-1]
            if first.price == 0:
                continue
            change_pct = ((last.price / first.price) - 1.0) * 100.0
            prices = [point.price for point in points]
            returns_bps = [
                ((right / left) - 1.0) * 10_000.0
                for left, right in zip(prices, prices[1:], strict=False)
                if left != 0
            ]
            volatility = pstdev(returns_bps) if len(returns_bps) >= 2 else 0.0
            spread_bps = None
            if last.bid is not None and last.ask is not None and last.price:
                spread_bps = ((last.ask - last.bid) / last.price) * 10_000.0
            score = abs(change_pct) + min(volatility / 100.0, 5.0)
            compact = _downsample(list(points), history_points)
            ranked.append(
                {
                    "instrument_id": instrument_id,
                    "price": last.price,
                    "change_pct_stream": round(change_pct, 5),
                    "step_volatility_bps": round(volatility, 4),
                    "spread_bps": None if spread_bps is None else round(spread_bps, 3),
                    "sample_count": len(points),
                    "first_at": first.observed_at.isoformat(),
                    "last_at": last.observed_at.isoformat(),
                    "score": round(score, 5),
                    "points": [
                        {"t": point.observed_at.isoformat(), "p": point.price}
                        for point in compact
                    ],
                }
            )
        ranked.sort(key=lambda item: float(item["score"] or 0.0), reverse=True)
        return ranked[:limit]


def parse_stream_tick(raw: str | bytes) -> StreamTick | None:
    try:
        payload = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return None
    if not isinstance(payload, dict):
        return None

    data = payload.get("data") if isinstance(payload.get("data"), dict) else payload
    topic = payload.get("topic") or payload.get("Topic")
    instrument_id = _int_value(data, "instrumentId", "instrumentID", "InstrumentID")
    if instrument_id is None and isinstance(topic, str) and topic.startswith("instrument:"):
        try:
            instrument_id = int(topic.split(":", 1)[1])
        except ValueError:
            return None
    if instrument_id is None:
        return None

    bid = _float_value(data, "bid", "Bid")
    ask = _float_value(data, "ask", "Ask")
    price = _float_value(data, "lastPrice", "lastExecution", "rate", "price", "Price")
    if price is None and bid is not None and ask is not None:
        price = (bid + ask) / 2.0
    if price is None:
        return None

    timestamp = _timestamp(data.get("timestamp") or data.get("date") or data.get("Timestamp"))
    return StreamTick(
        instrument_id=instrument_id,
        observed_at=timestamp,
        price=price,
        bid=bid,
        ask=ask,
    )


def _downsample(points: list[StreamTick], max_points: int) -> list[StreamTick]:
    if len(points) <= max_points:
        return points
    stride = max(1, ceil((len(points) - 1) / (max_points - 1)))
    sampled = points[::stride]
    if sampled[-1].observed_at != points[-1].observed_at:
        sampled.append(points[-1])
    return sampled[-max_points:]


def _int_value(data: dict[str, Any], *keys: str) -> int | None:
    for key in keys:
        if data.get(key) is not None:
            return int(data[key])
    return None


def _float_value(data: dict[str, Any], *keys: str) -> float | None:
    for key in keys:
        if data.get(key) is not None:
            return float(data[key])
    return None


def _timestamp(value: Any) -> datetime:
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
        except ValueError:
            pass
    return datetime.now(UTC)
