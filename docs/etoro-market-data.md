# eToro market-data integration

Parquet V0.3 contains a **read-only** Public API client in `parquet.market.etoro`.

Official eToro documentation currently specifies:

- REST base: `https://public-api.etoro.com/api/v1`
- instrument search: `GET /market-data/search?search=...`
- rates: `GET /market-data/instruments/rates?instrumentIds=...`
- WebSocket: `wss://ws.etoro.com/ws`
- request headers: `x-api-key`, optional/required-by-user-context `x-user-key`, and a unique `x-request-id`
- `429` responses should respect `Retry-After`

References:

- https://builders.etoro.com/products/market-data-realtime
- https://builders.etoro.com/reference
- https://builders.etoro.com/faq

## Credential boundary

The market-data client accepts `api_key` and `user_key` as constructor arguments but deliberately does **not** read `/etc/parquet/private_key.pem` or `/etc/parquet/public_key.pem` itself.

This is intentional: the host-level private/public key convention was agreed before confirming the exact eToro credential type. The standard Public API documentation calls its values **API Key** and **User Key**, not private/public PEM keys. The mapping must be explicitly configured once the actual credentials are available; Parquet must not guess which secret belongs in which HTTP header.

## V0.3 behavior

- Search instruments by symbol/name.
- Fetch current rates for multiple instrument IDs in one request.
- Convert usable rates to a `MarketObservation`.
- Surface 429 rate limiting, including parsed `Retry-After`.
- Feed observations to `Orchestrator.process_observation()`.
- The Watch Engine can trigger, invalidate or expire a stored watch.
- `REASSESS` triggers create an immediate persistent review request.
- `EXECUTE` watch actions are explicitly deferred; no broker order is emitted.

## Next step

Once credentials are validated on the LXC, add a continuous market-data service:

1. resolve configured symbols to instrument IDs;
2. subscribe via WebSocket for live updates (REST polling as fallback);
3. normalize updates to `MarketObservation`;
4. call `Orchestrator.process_observation()`;
5. reconnect with backoff and use REST reconciliation after disconnects.
