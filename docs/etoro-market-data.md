# eToro market-data integration

Parquet V0.4 can use the eToro Public API as a **read-only market-data source** for both ChatGPT review requests and deterministic WATCH evaluation.

Official eToro documentation currently specifies:

- REST base: `https://public-api.etoro.com/api/v1`
- instrument search: `GET /market-data/search?search=...`
- rates: `GET /market-data/instruments/rates?instrumentIds=...`
- WebSocket: `wss://ws.etoro.com/ws`
- authenticated request headers: `x-api-key`, `x-user-key`, and a unique `x-request-id`
- `429` responses should respect `Retry-After`

References:

- https://builders.etoro.com/products/market-data-realtime
- https://builders.etoro.com/reference
- https://builders.etoro.com/faq

## Credentials

The ChatGPT eToro connector and the LXC are separate trust boundaries. Parquet cannot reuse or extract credentials held by ChatGPT. The LXC therefore uses API credentials generated in the eToro API Portal.

Store them only on the host:

```text
/etc/parquet/etoro_api_key
/etc/parquet/etoro_user_key
```

Recommended permissions:

```bash
install -m 0640 -o root -g parquet /dev/null /etc/parquet/etoro_api_key
install -m 0640 -o root -g parquet /dev/null /etc/parquet/etoro_user_key
```

Do not put either value in Git, `parquet.yaml`, logs, issue comments, or PR comments.

## Configuration

```yaml
etoro:
  enabled: true
  api_key_file: /etc/parquet/etoro_api_key
  user_key_file: /etc/parquet/etoro_user_key
  base_url: https://public-api.etoro.com/api/v1
  max_quote_age_seconds: 120
  review_symbols:
    - GER40
    - NSDQ100
    - SPX500
    - GOLD
    - OIL
    - EURUSD
    - USDJPY
  instrument_ids: {}
```

`review_symbols` is the default cross-market universe supplied to ChatGPT on every due review. Active WATCH symbols are added automatically.

`instrument_ids` is an optional explicit symbol-to-eToro-ID cache. If a symbol is absent, Parquet uses eToro instrument search and accepts only an exact symbol match. Resolved IDs are cached in memory for the lifetime of the service.

## Review-request snapshot

Before publishing a due `[PARQUET:REVIEW_REQUEST]`, Parquet fetches current rates and adds a market-data block to `context`:

```json
{
  "market_data": {
    "provider": "etoro",
    "captured_at": "2026-09-07T08:31:00+00:00",
    "quotes": {
      "GER40": {
        "instrument_id": 1234,
        "bid": 24500.0,
        "ask": 24502.0,
        "last_price": 24501.0,
        "change": 0.2,
        "timestamp": "2026-09-07T08:30:59+00:00",
        "age_seconds": 1.0,
        "stale": false
      }
    },
    "unresolved_symbols": []
  }
}
```

If eToro is unavailable, the review request is still posted. Its context contains the market-data error so ChatGPT can fall back to its own connected market sources rather than losing the scheduled analysis.

## WATCH polling

While at least one WATCH is active, the LXC polls eToro rates every normal Parquet loop (`poll_seconds`, currently 20 seconds) and feeds observations into the Watch Engine. This makes `price_above` and `price_below` triggers independent of ChatGPT.

`close_above` and `close_below` still require candle-close observations. The REST rate polling implemented in V0.4 does not synthesize candles, so those trigger types need the later candle/WebSocket integration before they can be evaluated continuously from eToro data.

## Safety boundary

This integration is read-only. It does not add broker order execution. `EXECUTE` watch actions remain deferred and all deployment should remain in `shadow` until demo execution and reconciliation are implemented and tested.
