import httpx
import pytest

from parquet.market.universe import EtoroUniverseClient


@pytest.mark.asyncio
async def test_metadata_parses_official_instrument_display_data_shape() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path.endswith("/market-data/instruments")
        assert request.url.params["instrumentIds"] == "2694,3534"
        return httpx.Response(
            200,
            json={
                "instrumentDisplayDatas": [
                    {
                        "instrumentID": 2694,
                        "instrumentDisplayName": "Example Asset",
                        "instrumentTypeID": 5,
                        "exchangeID": 12,
                        "symbolFull": "EXAMPLE",
                    },
                    {
                        "instrumentID": 3534,
                        "instrumentDisplayName": "Second Asset",
                        "instrumentTypeID": 4,
                        "exchangeID": 7,
                        "symbolFull": "SECOND",
                    },
                ]
            },
        )

    client = EtoroUniverseClient(
        api_key="api",
        user_key="user",
        transport=httpx.MockTransport(handler),
    )
    metadata = await client.metadata([2694, 3534])

    assert metadata[2694].symbol == "EXAMPLE"
    assert metadata[2694].name == "Example Asset"
    assert metadata[2694].instrument_type_id == 5
    assert metadata[2694].exchange_id == 12
    assert metadata[3534].symbol == "SECOND"


@pytest.mark.asyncio
async def test_metadata_keeps_legacy_instruments_shape_compatible() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "items": [
                    {
                        "instrumentId": 1001,
                        "internalSymbolFull": "AAPL",
                        "displayName": "Apple",
                        "instrumentTypeId": 5,
                        "exchangeId": 1,
                    }
                ]
            },
        )

    client = EtoroUniverseClient(
        api_key="api",
        user_key="user",
        transport=httpx.MockTransport(handler),
    )
    metadata = await client.metadata([1001])

    assert metadata[1001].symbol == "AAPL"
    assert metadata[1001].name == "Apple"
