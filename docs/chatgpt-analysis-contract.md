# ChatGPT analysis contract

Parquet treats ChatGPT as an analyst, not as the final execution authority.

A ChatGPT task may use current web/news context and the market snapshot supplied by Parquet. Its machine-readable result must be posted to the configured private runtime channel with the marker:

```text
[PARQUET:ANALYSIS]
```

followed by one JSON object.

## Required output shape

```json
{
  "schema_version": 1,
  "analysis_id": "20260907-europe-open-001",
  "generated_at": "2026-09-07T09:01:20+02:00",
  "market_regime": "risk_on",
  "summary": "Short human-readable market summary",
  "watch": [
    {
      "watch_id": "nsdq-breakout-001",
      "symbol": "NSDQ100",
      "bias": "LONG",
      "trigger": {
        "type": "close_above",
        "price": 29510.0,
        "timeframe": "5m"
      },
      "invalidation": 29440.0,
      "expires_at": "2026-09-07T12:00:00+02:00",
      "on_trigger": "REASSESS",
      "proposal_id": null,
      "rationale": "Breakout only if confirmed by a 5-minute close"
    },
    {
      "watch_id": "gold-execute-001",
      "symbol": "GOLD",
      "bias": "LONG",
      "trigger": {
        "type": "price_above",
        "price": 4510.0
      },
      "expires_at": "2026-09-07T09:16:20+02:00",
      "on_trigger": "EXECUTE",
      "proposal_id": "gold-long-001",
      "rationale": "Execute only after the deterministic trigger"
    }
  ],
  "trade_proposals": [
    {
      "proposal_id": "gold-long-001",
      "symbol": "GOLD",
      "side": "BUY",
      "entry": 4510.0,
      "stop_loss": 4495.0,
      "take_profit": 4540.0,
      "confidence": 0.74,
      "generated_at": "2026-09-07T09:01:20+02:00",
      "expires_at": "2026-09-07T09:16:20+02:00",
      "thesis": ["reason 1", "reason 2"],
      "risks": ["risk 1"]
    }
  ],
  "next_review": {
    "at": "2026-09-07T14:29:00+02:00",
    "reason": "US macro release at 14:30"
  }
}
```

## Trigger vocabulary

Only deterministic triggers are accepted:

- `price_above`
- `price_below`
- `close_above`
- `close_below`

A `close_*` trigger should specify the timeframe when relevant.

## Semantics

- `WATCH`: Parquet monitors the condition locally. `on_trigger=REASSESS` requests another ChatGPT analysis.
- `EXECUTE`: the watch must reference the exact `trade_proposals[].proposal_id`. Parquet never infers a proposal by symbol alone.
- `TRADE_PROPOSAL`: this is never a broker order. Parquet persists the proposal and validates it again when the linked trigger fires.
- The execution gate checks signal expiry/age, mandatory stop-loss, position/trade/loss limits, risk snapshot freshness, equity availability, duplicate-symbol exposure, quote freshness, spread, adverse entry slippage and deterministic position sizing.
- Position sizing is controlled by Parquet from account equity, stop distance and configured risk limits. ChatGPT must not choose final account exposure.
- `mode=shadow` never places an order. A gate-approved setup is recorded as `execution_shadow_approved` only.
- Non-shadow broker execution remains blocked until a dedicated execution adapter and reconciliation loop are implemented.
- `next_review`: ChatGPT may request an extraordinary future review. Structural reviews remain controlled by Parquet.
- `NO TRADE`: use empty `watch` and `trade_proposals` arrays. This is a valid and expected result.

## Prompt guidance for the ChatGPT task

The task should explicitly:

1. Use current market/news information and identify the relevant event time, not merely article publication time.
2. Consider cross-market context and whether a move is already consumed.
3. Prefer `NO TRADE` to a low-quality setup.
4. Never invent prices or indicators that were not obtained from current data or supplied by Parquet.
5. Always attach an expiry to watches and proposals.
6. Always include a stop-loss on trade proposals.
7. For `on_trigger=EXECUTE`, always link the watch with `proposal_id` to one exact proposal in the same analysis.
8. Request `next_review` only for a concrete catalyst or unresolved market condition.
9. Never choose final account exposure or override Parquet risk limits.
