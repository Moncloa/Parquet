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
from uuid import uuid4

import websockets


@dataclass(frozen=True)
class StreamTick:
    instrument_id: int
    observed_at: datetime
    price: float
    bid: float | None = None
    ask: float | None = None


def build_websocket_request(operation: str, data: dict[str, Any]) -> dict[str, Any]:
    """Build one eToro WebSocket command with the documented correlation GUID."""

    return {
        "id": str(uuid4()),
        "operation": operation,
        "data": data,
    }


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
        self.universe_started_at: datetime | None = None
        self.series: dict[int, deque[StreamTick]] = defaultdict(
            lambda: deque(maxlen=self.max_points_per_instrument)
        )
        self.connected = False
        # Wire-level observability: this advances for every application frame received,
        # even if the current tick parser does not recognize its schema.
        self.last_message_at: datetime | None = None
        self.last_tick_at: datetime | None = None
        self.incoming_message_count = 0
        self.parsed_tick_count = 0
        self.last_error: str | None = None

    def set_universe(self, instrument_ids: list[int], *, now: datetime | None = None) -> None:
        self.instrument_ids = sorted(set(instrument_ids))
        self.universe_started_at = (now or datetime.now(UTC)).astimezone(UTC)

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
                    build_websocket_request(
                        "Authenticate",
                        {"userKey": self.user_key, "apiKey": self.api_key},
                    )
                )
            )
            for start in range(0, len(self.instrument_ids), 100):
                batch = self.instrument_ids[start : start + 100]
                await socket.send(
                    json.dumps(
                        build_websocket_request(
                            "Subscribe",
                            {
                                "topics": [f"instrument:{value}" for value in batch],
                                "snapshot": False,
                            },
                        )
                    )
                )
            self.connected = True
            self.last_error = None
            async for raw in socket:
                received_at = datetime.now(UTC)
                self.last_message_at = received_at
                self.incoming_message_count += 1
                wire_error = parse_stream_error(raw)
                if wire_error is not None:
                    self.last_error = wire_error
                tick = parse_stream_tick(raw)
                if tick is None:
                    continue
                self.series[tick.instrument_id].append(tick)
                self.last_tick_at = received_at
                self.parsed_tick_count += 1
                if self.on_tick is not None:
                    await self.on_tick(tick)

    def shortlist(self, limit: int = 20, history_points: int = 20) -> list[dict[str, Any]]:
        ranked: list[dict[str, Any]] = []
        active_ids = set(self.instrument_ids)
        started_at = self.universe_started_at
        for instrument_id, stored_points in self.series.items():
            points = [
                point
                for point in stored_points
                if started_at is None or point.observed_at >= started_at
            ]
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
            compact = _downsample(points, history_points)
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


def parse_stream_error(raw: str | bytes) -> str | None:
    """Return a redacted server-side error summary, if the frame clearly represents one."""

    payload = _json_dict(raw)
    if payload is None:
        return None

    data = _nested_dict(payload.get("data"))
    content = _content_dict(payload, data)
    sources = (payload, data, content)

    operation = _first_scalar(sources, "operation", "type", "event")
    success = _first_value(sources, "success", "isSucceeded", "isSuccess")
    status = _first_scalar(sources, "status")
    explicitly_failed = success is False or (
        isinstance(status, str) and status.lower() in {"error", "failed", "failure", "rejected"}
    ) or (isinstance(operation, str) and "error" in operation.lower())
    if not explicitly_failed:
        return None

    operation_text = "websocket" if operation is None else str(operation)
    safe_parts: list[str] = []
    if status is not None:
        safe_parts.append(f"status={str(status)[:80]}")
    code = _first_scalar(sources, "code", "errorCode", "error_code")
    if code is not None:
        safe_parts.append(f"code={str(code)[:80]}")
    detail = _first_scalar(sources, "error", "message", "reason", "description")
    if detail is not None:
        safe_parts.append(f"message={str(detail)[:240]}")

    suffix = "" if not safe_parts else ": " + ", ".join(safe_parts)
    return f"eToro WebSocket {operation_text} failed{suffix}"


def parse_stream_tick(raw: str | bytes) -> StreamTick | None:
    """Parse both legacy/direct and documented JSON-in-`content` instrument pushes."""

    payload = _json_dict(raw)
    if payload is None:
        return None

    data = _nested_dict(payload.get("data"))
    content = _content_dict(payload, data)
    sources = (content, data, payload)

    topic = _first_scalar(sources, "topic", "Topic")
    instrument_id = _first_int(
        sources,
        "instrumentId",
        "instrumentID",
        "InstrumentID",
        "InstrumentId",
    )
    if instrument_id is None and isinstance(topic, str) and topic.startswith("instrument:"):
        try:
            instrument_id = int(topic.split(":", 1)[1])
        except ValueError:
            return None
    if instrument_id is None:
        return None

    bid = _first_float(sources, "bid", "Bid")
    ask = _first_float(sources, "ask", "Ask")
    price = _first_float(
        sources,
        "lastPrice",
        "LastPrice",
        "lastExecution",
        "LastExecution",
        "rate",
        "Rate",
        "price",
        "Price",
    )
    if price is None and bid is not None and ask is not None:
        price = (bid + ask) / 2.0
    if price is None:
        return None

    timestamp_value = _first_value(sources, "timestamp", "Timestamp", "date", "Date")
    return StreamTick(
        instrument_id=instrument_id,
        observed_at=_timestamp(timestamp_value),
        price=price,
        bid=bid,
        ask=ask,
    )


def _json_dict(raw: str | bytes) -> dict[str, Any] | None:
    try:
        payload = json.loads(raw)
    except (json.JSONDecodeError, TypeError, UnicodeDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    return {str(key): value for key, value in payload.items()}


def _nested_dict(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    return {str(key): item for key, item in value.items()}


def _content_dict(payload: dict[str, Any], data: dict[str, Any]) -> dict[str, Any]:
    raw_content = payload.get("content", data.get("content"))
    if isinstance(raw_content, dict):
        return {str(key): value for key, value in raw_content.items()}
    if isinstance(raw_content, str):
        try:
            decoded = json.loads(raw_content)
        except json.JSONDecodeError:
            return {}
        if isinstance(decoded, dict):
            return {str(key): value for key, value in decoded.items()}
    return {}


def _first_value(sources: tuple[dict[str, Any], ...], *keys: str) -> Any:
    for source in sources:
        for key in keys:
            if key in source:
                return source[key]
    return None


def _first_scalar(
    sources: tuple[dict[str, Any], ...],
    *keys: str,
) -> str | int | float | bool | None:
    value = _first_value(sources, *keys)
    if isinstance(value, (str, int, float, bool)):
        return value
    return None


def _first_int(sources: tuple[dict[str, Any], ...], *keys: str) -> int | None:
    value = _first_value(sources, *keys)
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _first_float(sources: tuple[dict[str, Any], ...], *keys: str) -> float | None:
    value = _first_value(sources, *keys)
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _downsample(points: list[StreamTick], max_points: int) -> list[StreamTick]:
    if len(points) <= max_points:
        return points
    stride = max(1, ceil((len(points) - 1) / (max_points - 1)))
    sampled = points[::stride]
    if sampled[-1].observed_at != points[-1].observed_at:
        sampled.append(points[-1])
    return sampled[-max_points:]


def _timestamp(value: Any) -> datetime:
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
        except ValueError:
            pass
    return datetime.now(UTC)
