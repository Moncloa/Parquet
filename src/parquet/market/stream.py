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

from parquet.market.universe import EtoroUniverseClient


@dataclass(frozen=True)
class StreamTick:
    instrument_id: int
    observed_at: datetime
    price: float
    bid: float | None = None
    ask: float | None = None


def build_websocket_request(operation: str, data: dict[str, Any]) -> dict[str, Any]:
    """Build one eToro WebSocket command with a correlation GUID."""

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
        market_base_url: str = "https://public-api.etoro.com/api/v1",
        max_points_per_instrument: int = 240,
        on_tick: Callable[[StreamTick], Awaitable[None]] | None = None,
    ) -> None:
        self.api_key = api_key
        self.user_key = user_key
        self.url = url
        self.market_base_url = market_base_url
        self.max_points_per_instrument = max_points_per_instrument
        self.on_tick = on_tick
        self.instrument_ids: list[int] = []
        self.symbol_by_id: dict[int, str] = {}
        self.id_by_symbol: dict[str, int] = {}
        self.universe_started_at: datetime | None = None
        self.series: dict[int, deque[StreamTick]] = defaultdict(
            lambda: deque(maxlen=self.max_points_per_instrument)
        )
        self.connected = False
        self.last_message_at: datetime | None = None
        self.last_tick_at: datetime | None = None
        self.incoming_message_count = 0
        self.parsed_tick_count = 0
        self.frame_type_counts: dict[str, int] = defaultdict(int)
        self.last_error: str | None = None
        self._metadata_client = EtoroUniverseClient(
            api_key=api_key,
            user_key=user_key,
            base_url=market_base_url,
        )

    def set_universe(
        self,
        instrument_ids: list[int],
        *,
        symbol_by_id: dict[int, str] | None = None,
        now: datetime | None = None,
    ) -> None:
        self.instrument_ids = sorted(set(instrument_ids))
        supplied = symbol_by_id or {}
        active_ids = set(self.instrument_ids)
        self.symbol_by_id = {
            instrument_id: symbol
            for instrument_id, symbol in supplied.items()
            if instrument_id in active_ids and symbol
        }
        self.id_by_symbol = {
            symbol.upper(): instrument_id
            for instrument_id, symbol in self.symbol_by_id.items()
        }
        self.universe_started_at = (now or datetime.now(UTC)).astimezone(UTC)
        self.incoming_message_count = 0
        self.parsed_tick_count = 0
        self.frame_type_counts = defaultdict(int)
        self.last_message_at = None
        self.last_tick_at = None
        self.last_error = None

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

    async def _ensure_symbol_map(self) -> None:
        missing = [value for value in self.instrument_ids if value not in self.symbol_by_id]
        if not missing:
            return
        try:
            metadata = await self._metadata_client.metadata(missing)
        except Exception as exc:
            # Topic-based streaming can still work without symbol metadata.
            self.last_error = f"eToro WebSocket metadata lookup failed: {exc!r}"
            return
        for instrument_id, item in metadata.items():
            if item.symbol:
                self.symbol_by_id[instrument_id] = item.symbol
                self.id_by_symbol[item.symbol.upper()] = instrument_id

    async def _run_once(self) -> None:
        await self._ensure_symbol_map()
        headers = {
            "x-api-key": self.api_key,
            "x-user-key": self.user_key,
        }
        async with websockets.connect(
            self.url,
            additional_headers=headers,
            ping_interval=20,
            ping_timeout=20,
        ) as socket:
            # Topic-based protocol documented in the API quick reference.
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

            # Channel-based protocol documented in the current WebSocket guide.
            symbols = [
                self.symbol_by_id[instrument_id]
                for instrument_id in self.instrument_ids
                if instrument_id in self.symbol_by_id
            ]
            for start in range(0, len(symbols), 100):
                batch = symbols[start : start + 100]
                await socket.send(
                    json.dumps(
                        {
                            "action": "subscribe",
                            "channels": ["quotes"],
                            "instruments": batch,
                        }
                    )
                )

            self.connected = True
            if self.last_error and "metadata lookup" not in self.last_error:
                self.last_error = None
            active_ids = set(self.instrument_ids)
            async for raw in socket:
                received_at = datetime.now(UTC)
                self.last_message_at = received_at
                self.incoming_message_count += 1
                self.frame_type_counts[classify_stream_frame(raw)] += 1

                wire_error = parse_stream_error(raw)
                if wire_error is not None:
                    self.last_error = wire_error

                tick = parse_stream_tick(raw, self.id_by_symbol)
                if tick is None or tick.instrument_id not in active_ids:
                    continue
                self.series[tick.instrument_id].append(tick)
                self.last_tick_at = received_at
                self.parsed_tick_count += 1
                self.last_error = None
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
                    "symbol": self.symbol_by_id.get(instrument_id),
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


def classify_stream_frame(raw: str | bytes) -> str:
    """Classify a frame using only safe protocol metadata."""

    payload = _json_dict(raw)
    if payload is None:
        return "non_json"
    message_type = _first_scalar((payload,), "type")
    if isinstance(message_type, str):
        return f"type:{message_type.lower()[:40]}"
    operation = _first_scalar((payload,), "operation")
    if isinstance(operation, str):
        return f"operation:{operation.lower()[:40]}"
    action = _first_scalar((payload,), "action")
    if isinstance(action, str):
        return f"action:{action.lower()[:40]}"
    topic = _first_scalar((payload,), "topic", "Topic")
    if isinstance(topic, str):
        return "topic:instrument" if topic.startswith("instrument:") else "topic:other"
    if "content" in payload:
        return "content"
    return "other"


def parse_stream_error(raw: str | bytes) -> str | None:
    """Return a redacted server-side error summary, if the frame clearly represents one."""

    payload = _json_dict(raw)
    if payload is None:
        return None

    data = _nested_dict(payload.get("data"))
    content = _content_dict(payload, data)
    sources = (payload, data, content)

    operation = _first_scalar(sources, "operation", "type", "event", "action")
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


def parse_stream_tick(
    raw: str | bytes,
    symbol_to_instrument_id: dict[str, int] | None = None,
) -> StreamTick | None:
    """Parse topic/content pushes and channel-based quote messages."""

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
    instrument = _first_scalar(sources, "instrument", "Instrument", "symbol", "Symbol")
    if instrument_id is None and instrument is not None:
        try:
            instrument_id = int(instrument)
        except (TypeError, ValueError):
            mapping = symbol_to_instrument_id or {}
            instrument_id = mapping.get(str(instrument).upper())
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

    timestamp_value = _first_value(
        sources,
        "timestamp",
        "Timestamp",
        "date",
        "Date",
        "time",
        "Time",
    )
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
    if isinstance(value, (int, float)):
        seconds = float(value)
        if seconds > 10_000_000_000:
            seconds /= 1000.0
        try:
            return datetime.fromtimestamp(seconds, tz=UTC)
        except (OverflowError, OSError, ValueError):
            pass
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
        except ValueError:
            pass
    return datetime.now(UTC)
