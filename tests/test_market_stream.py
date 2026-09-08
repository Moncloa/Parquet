from __future__ import annotations

from datetime import UTC, datetime, timedelta

import httpx
import pytest

from parquet.market.stream import (
    EtoroWebSocketScanner,
    StreamTick,
    parse_stream_error,
    parse_stream_tick,
)
from parquet.market.universe import EtoroUniverseClient, rotate_universe


def test_parse_stream_tick_accepts_nested_payload() -> None:
    tick = parse_stream_tick(
        '{"topic":"instrument:123","data":{"bid":99,"ask":101,'
        '"timestamp":"2026-09-08T07:00:00Z"}}'
    )
    assert tick is not None
    assert tick.instrument_id == 123
    assert tick.price == 100.0
    assert tick.observed_at == datetime(2026, 9, 8, 7, 0, tzinfo=UTC)


def test_parse_stream_error_reports_failure_without_echoing_payload() -> None:
    error = parse_stream_error(
        '{"operation":"Authenticate","success":false,'
        '"message":"not allowed","apiKey":"secret-value"}'
    )

    assert error == "eToro WebSocket Authenticate failed: not allowed"
    assert "secret-value" not in error


def test_parse_stream_error_ignores_successful_control_message() -> None:
    assert parse_stream_error('{"operation":"Authenticate","success":true}') is None


def test_scanner_shortlist_ranks_current_universe_and_includes_history() -> None:
    scanner = EtoroWebSocketScanner(api_key="api", user_key="user")
    now = datetime(2026, 9, 8, 7, 0, tzinfo=UTC)
    scanner.set_universe([1, 2], now=now)
    scanner.series[1].extend(
        [
            StreamTick(1, now, 100.0, 99.9, 100.1),
            StreamTick(1, now + timedelta(seconds=20), 102.0, 101.9, 102.1),
        ]
    )
    scanner.series[2].extend(
        [
            StreamTick(2, now, 100.0, 99.9, 100.1),
            StreamTick(2, now + timedelta(seconds=20), 100.2, 100.1, 100.3),
        ]
    )
    scanner.series[3].extend(
        [
            StreamTick(3, now, 100.0),
            StreamTick(3, now + timedelta(seconds=20), 110.0),
        ]
    )

    shortlist = scanner.shortlist(limit=2)

    assert [item["instrument_id"] for item in shortlist] == [1, 2]
    assert shortlist[0]["change_pct_stream"] == 2.0
    assert len(shortlist[0]["points"]) == 2


def test_scanner_does_not_join_discontinuous_rotation_history() -> None:
    scanner = EtoroWebSocketScanner(api_key="api", user_key="user")
    old = datetime(2026, 9, 8, 6, 0, tzinfo=UTC)
    new = datetime(2026, 9, 8, 7, 0, tzinfo=UTC)
    scanner.series[1].extend(
        [
            StreamTick(1, old, 90.0),
            StreamTick(1, old + timedelta(seconds=20), 95.0),
        ]
    )
    scanner.set_universe([1], now=new)
    scanner.series[1].extend(
        [
            StreamTick(1, new, 100.0),
            StreamTick(1, new + timedelta(seconds=20), 101.0),
        ]
    )

    shortlist = scanner.shortlist(limit=1)

    assert shortlist[0]["sample_count"] == 2
    assert shortlist[0]["change_pct_stream"] == 1.0


def test_rotate_universe_keeps_pinned_and_rotates() -> None:
    first, offset = rotate_universe(
        list(range(1, 11)),
        offset=0,
        limit=5,
        pinned=[10],
    )
    second, _ = rotate_universe(
        list(range(1, 11)),
        offset=offset,
        limit=5,
        pinned=[10],
    )

    assert first[0] == 10
    assert second[0] == 10
    assert set(first[1:]).isdisjoint(set(second[1:]))


@pytest.mark.asyncio
async def test_universe_client_discovers_open_ids_and_metadata() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/history/closing-price"):
            return httpx.Response(
                200,
                json=[
                    {"instrumentId": 1, "isMarketOpen": True},
                    {"instrumentId": 2, "isMarketOpen": False},
                    {"instrumentId": 3, "isMarketOpen": True},
                ],
            )
        assert request.url.params["instrumentIds"] == "1,3"
        return httpx.Response(
            200,
            json={
                "instruments": [
                    {
                        "instrumentId": 1,
                        "internalSymbolFull": "AAA",
                        "displayName": "Asset A",
                        "instrumentTypeId": 5,
                        "exchangeId": 8,
                    },
                    {
                        "instrumentId": 3,
                        "internalSymbolFull": "BBB",
                        "displayName": "Asset B",
                        "instrumentTypeId": 5,
                        "exchangeId": 8,
                    },
                ]
            },
        )

    client = EtoroUniverseClient(
        api_key="api",
        user_key="user",
        transport=httpx.MockTransport(handler),
    )

    ids = await client.open_instrument_ids()
    metadata = await client.metadata(ids)

    assert ids == [1, 3]
    assert metadata[1].symbol == "AAA"
    assert metadata[3].name == "Asset B"
