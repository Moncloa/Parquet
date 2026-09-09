from __future__ import annotations

from datetime import UTC, datetime

from parquet.market.stream import (
    classify_stream_frame,
    classify_stream_shape,
    parse_stream_error,
    parse_stream_ticks,
)


def test_parse_stream_ticks_accepts_messages_envelope() -> None:
    raw = (
        '{"messages":['
        '{"topic":"instrument:32","content":"{\\"InstrumentID\\":32,'
        '\\"Bid\\":25900.1,\\"Ask\\":25901.1,\\"LastExecution\\":25900.6,'
        '\\"Date\\":\\"2026-09-09T08:05:00Z\\"}"},'
        '{"topic":"instrument:33","content":"{\\"InstrumentID\\":33,'
        '\\"Bid\\":100,\\"Ask\\":102,\\"Date\\":\\"2026-09-09T08:05:01Z\\"}"}'
        ']}'
    )

    ticks = parse_stream_ticks(raw)

    assert [tick.instrument_id for tick in ticks] == [32, 33]
    assert ticks[0].price == 25900.6
    assert ticks[0].observed_at == datetime(2026, 9, 9, 8, 5, tzinfo=UTC)
    assert ticks[1].price == 101.0


def test_parse_stream_ticks_accepts_json_string_messages() -> None:
    raw = (
        '{"messages":"[{\\"topic\\":\\"instrument:32\\",'
        '\\"data\\":{\\"bid\\":99,\\"ask\\":101}}]"}'
    )

    ticks = parse_stream_ticks(raw)

    assert len(ticks) == 1
    assert ticks[0].instrument_id == 32
    assert ticks[0].price == 100.0


def test_messages_envelope_shape_is_safe_and_informative() -> None:
    raw = (
        '{"messages":[{"topic":"instrument:32","content":"secret-value"},'
        '{"topic":"instrument:33","data":{"bid":1,"ask":2}}]}'
    )

    assert classify_stream_frame(raw) == "messages_batch"
    shape = classify_stream_shape(raw)
    assert shape.startswith("dict[messages]/messages:list")
    assert "topic" in shape
    assert "secret-value" not in shape
    assert "instrument:32" not in shape


def test_messages_envelope_scans_nested_errors() -> None:
    raw = (
        '{"messages":[{"operation":"Subscribe","success":false,'
        '"message":"not allowed","apiKey":"secret-value"}]}'
    )

    error = parse_stream_error(raw)

    assert error == "eToro WebSocket Subscribe failed: message=not allowed"
    assert "secret-value" not in error
