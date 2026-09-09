from __future__ import annotations

import asyncio
import json
import re
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
        self.frame_shape_counts: dict[str, int] = defaultdict(int)
        self.last_error: str | None = None

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
        self.frame_shape_counts = defaultdict(int)
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

    async def _run_once(self) -> None:
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
            await socket.send(
                json.dumps(
                    build_websocket_request(
                        "Authenticate",
                        {"userKey": self.user_key, "apiKey": self.api_key},
                    )
                )
            )
            for start in range(0, len(self.instrument_ids), 100):
                id_batch = self.instrument_ids[start : start + 100]
                await socket.send(
                    json.dumps(
                        build_websocket_request(
                            "Subscribe",
                            {
                                "topics": [f"instrument:{value}" for value in id_batch],
                                "snapshot": False,
                            },
                        )
                    )
                )

            self.connected = True
            self.last_error = None
            active_ids = set(self.instrument_ids)
            async for raw in socket:
                received_at = datetime.now(UTC)
                self.last_message_at = received_at
                self.incoming_message_count += 1
                self._count_label(self.frame_type_counts, classify_stream_frame(raw))
                self._count_label(self.frame_shape_counts, classify_stream_shape(raw))

                wire_error = parse_stream_error(raw)
                if wire_error is not None:
                    self.last_error = wire_error

                for tick in parse_stream_ticks(raw, self.id_by_symbol):
                    if tick.instrument_id not in active_ids:
                        continue
                    self.series[tick.instrument_id].append(tick)
                    self.last_tick_at = received_at
                    self.parsed_tick_count += 1
                    self.last_error = None
                    if self.on_tick is not None:
                        await self.on_tick(tick)

    @staticmethod
    def _count_label(counter: dict[str, int], label: str, *, max_labels: int = 24) -> None:
        if label not in counter and len(counter) >= max_labels:
            label = "other_shapes"
        counter[label] += 1

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

    payload = _json_value(raw)
    if payload is _INVALID_JSON:
        return "non_json"
    if isinstance(payload, list):
        return "json_list"
    if not isinstance(payload, dict):
        return f"json_{type(payload).__name__}"
    normalized = _normalize_dict(payload)
    if isinstance(normalized.get("messages"), list):
        return "messages_batch"
    message_type = _first_scalar((normalized,), "type")
    if isinstance(message_type, str):
        return f"type:{message_type.lower()[:40]}"
    operation = _first_scalar((normalized,), "operation")
    if isinstance(operation, str):
        return f"operation:{operation.lower()[:40]}"
    action = _first_scalar((normalized,), "action")
    if isinstance(action, str):
        return f"action:{action.lower()[:40]}"
    topic = _first_scalar((normalized,), "topic", "Topic")
    if isinstance(topic, str):
        return "topic:instrument" if topic.startswith("instrument:") else "topic:other"
    if "content" in normalized:
        return "content"
    return "other"


def classify_stream_shape(raw: str | bytes) -> str:
    """Describe only container types and key names; never include frame values."""

    payload = _json_value(raw)
    if payload is _INVALID_JSON:
        return "non_json:bytes" if isinstance(raw, bytes) else "non_json:text"
    return _shape_for_value(payload)


def parse_stream_error(raw: str | bytes) -> str | None:
    """Return a redacted server-side error summary, including messages batches."""

    payload = _json_value(raw)
    if payload is _INVALID_JSON:
        return None
    for candidate in _frame_candidates(payload):
        result = _parse_stream_error_payload(candidate)
        if result is not None:
            return result
    return None


def _parse_stream_error_payload(payload: dict[str, Any]) -> str | None:
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


def parse_stream_ticks(
    raw: str | bytes,
    symbol_to_instrument_id: dict[str, int] | None = None,
) -> list[StreamTick]:
    """Parse ticks from single, list, or {messages:[...]} WebSocket frames."""

    payload = _json_value(raw)
    if payload is _INVALID_JSON:
        return []

    ticks: list[StreamTick] = []
    for candidate in _frame_candidates(payload):
        tick = _parse_stream_tick_payload(candidate, symbol_to_instrument_id)
        if tick is not None:
            ticks.append(tick)
    return ticks


def parse_stream_tick(
    raw: str | bytes,
    symbol_to_instrument_id: dict[str, int] | None = None,
) -> StreamTick | None:
    """Compatibility wrapper returning the first tick found in a frame."""

    ticks = parse_stream_ticks(raw, symbol_to_instrument_id)
    return None if not ticks else ticks[0]


def _frame_candidates(value: Any, *, depth: int = 0) -> list[dict[str, Any]]:
    """Flatten only known WebSocket batch containers, with a small recursion bound."""

    if depth > 4:
        return []
    if isinstance(value, dict):
        normalized = _normalize_dict(value)
        result = [normalized]
        messages = normalized.get("messages")
        if isinstance(messages, (list, dict)):
            result.extend(_frame_candidates(messages, depth=depth + 1))
        elif isinstance(messages, str):
            decoded = _json_value(messages)
            if decoded is not _INVALID_JSON:
                result.extend(_frame_candidates(decoded, depth=depth + 1))
        return result
    if isinstance(value, list):
        list_result: list[dict[str, Any]] = []
        for item in value:
            list_result.extend(_frame_candidates(item, depth=depth + 1))
        return list_result
    if isinstance(value, str):
        decoded = _json_value(value)
        if decoded is not _INVALID_JSON:
            return _frame_candidates(decoded, depth=depth + 1)
    return []


def _parse_stream_tick_payload(
    payload: dict[str, Any],
    symbol_to_instrument_id: dict[str, int] | None,
) -> StreamTick | None:
    data = _nested_dict(payload.get("data"))
    message = _nested_dict(payload.get("message"))
    nested_payload = _nested_dict(payload.get("payload"))
    content = _content_dict(payload, data)
    sources = (content, data, message, nested_payload, payload)

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


_INVALID_JSON = object()
_SAFE_KEY_RE = re.compile(r"[^A-Za-z0-9_.:-]+")
_DOTNET_DATE_RE = re.compile(r"^/Date\(([-+]?\d+)(?:[-+]\d{4})?\)/$")


def _json_value(raw: str | bytes) -> Any:
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError, UnicodeDecodeError):
        return _INVALID_JSON


def _normalize_dict(value: dict[Any, Any]) -> dict[str, Any]:
    return {str(key): item for key, item in value.items()}


def _nested_dict(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    return _normalize_dict(value)


def _content_dict(payload: dict[str, Any], data: dict[str, Any]) -> dict[str, Any]:
    raw_content = payload.get("content", data.get("content"))
    if isinstance(raw_content, dict):
        return _normalize_dict(raw_content)
    if isinstance(raw_content, str):
        decoded = _json_value(raw_content)
        if isinstance(decoded, dict):
            return _normalize_dict(decoded)
    return {}


def _shape_for_value(value: Any) -> str:
    if isinstance(value, dict):
        normalized = _normalize_dict(value)
        parts = [f"dict[{_safe_key_signature(normalized)}]"]
        for key in ("messages", "data", "content", "payload", "message"):
            nested = normalized.get(key)
            if isinstance(nested, dict):
                parts.append(f"{key}[{_safe_key_signature(_normalize_dict(nested))}]")
            elif isinstance(nested, list):
                parts.append(f"{key}:list[len={len(nested)};{_list_item_shape(nested)}]")
            elif isinstance(nested, str):
                decoded = _json_value(nested)
                if isinstance(decoded, dict):
                    parts.append(f"{key}:json[{_safe_key_signature(_normalize_dict(decoded))}]")
                elif isinstance(decoded, list):
                    parts.append(f"{key}:json_list[{_list_item_shape(decoded)}]")
        return "/".join(parts)[:480]
    if isinstance(value, list):
        return f"list[len={len(value)};{_list_item_shape(value)}]"[:480]
    if value is None:
        return "json:null"
    return f"json:{type(value).__name__}"


def _list_item_shape(items: list[Any]) -> str:
    if not items:
        return "empty"
    kinds: list[str] = []
    for item in items[:3]:
        if isinstance(item, dict):
            kinds.append(f"dict[{_safe_key_signature(_normalize_dict(item))}]")
        elif isinstance(item, list):
            kinds.append("list")
        elif item is None:
            kinds.append("null")
        else:
            kinds.append(type(item).__name__)
    return "items=" + ",".join(kinds)


def _safe_key_signature(value: dict[str, Any], *, limit: int = 10) -> str:
    keys = sorted(_safe_key_name(key) for key in value)[:limit]
    suffix = ",..." if len(value) > limit else ""
    return ",".join(keys) + suffix


def _safe_key_name(key: str) -> str:
    lowered = key.lower()
    if any(token in lowered for token in ("apikey", "userkey", "token", "secret", "password")):
        return "<credential-field>"
    cleaned = _SAFE_KEY_RE.sub("?", key)[:40]
    return cleaned or "<empty-key>"


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
        return _timestamp_from_number(float(value))
    if isinstance(value, str):
        match = _DOTNET_DATE_RE.match(value)
        if match is not None:
            try:
                return _timestamp_from_number(float(match.group(1)))
            except ValueError:
                pass
        try:
            numeric = float(value)
        except ValueError:
            numeric = None
        if numeric is not None:
            return _timestamp_from_number(numeric)
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
        except ValueError:
            pass
    return datetime.now(UTC)


def _timestamp_from_number(value: float) -> datetime:
    seconds = value / 1000.0 if abs(value) > 10_000_000_000 else value
    try:
        return datetime.fromtimestamp(seconds, tz=UTC)
    except (OverflowError, OSError, ValueError):
        return datetime.now(UTC)
