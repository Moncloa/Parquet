from __future__ import annotations

import argparse
import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path

from parquet.config import Settings, load_settings
from parquet.execution.etoro import EtoroExecutionClient
from parquet.market.etoro import EtoroMarketDataClient

CONFIRM_TEXT = "REAL-MONEY"


def _read_secret(path: Path, label: str) -> str:
    value = path.read_text(encoding="utf-8").strip()
    if not value:
        raise RuntimeError(f"Empty {label}: {path}")
    return value


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Submit one deliberately small, manually confirmed eToro real-money test order."
    )
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--amount", required=True, type=float)
    parser.add_argument("--stop-loss", required=True, type=float)
    parser.add_argument("--take-profit", type=float)
    parser.add_argument("--confirm", required=True)
    return parser


async def run_live_test(
    settings: Settings,
    *,
    symbol: str,
    amount_usd: float,
    stop_loss_rate: float,
    take_profit_rate: float | None,
    confirmation: str,
) -> dict[str, object]:
    if not settings.etoro.enabled:
        raise RuntimeError("eToro is disabled")
    if not settings.execution.live_test_enabled:
        raise RuntimeError("execution.live_test_enabled is false")
    if confirmation != CONFIRM_TEXT:
        raise RuntimeError(f"confirmation must be exactly {CONFIRM_TEXT!r}")
    if amount_usd <= 0:
        raise RuntimeError("amount must be positive")
    if amount_usd > settings.execution.live_test_max_amount_usd:
        raise RuntimeError(
            f"amount {amount_usd} exceeds live-test cap "
            f"{settings.execution.live_test_max_amount_usd}"
        )

    api_key = _read_secret(settings.etoro.api_key_file, "eToro API key")
    user_key = _read_secret(settings.etoro.user_key_file, "eToro user key")
    market = EtoroMarketDataClient(
        api_key=api_key,
        user_key=user_key,
        base_url=settings.etoro.base_url,
    )

    normalized_symbol = symbol.upper()
    hits = await market.search(normalized_symbol)
    exact = next(
        (
            hit
            for hit in hits
            if hit.symbol is not None and hit.symbol.upper() == normalized_symbol
        ),
        None,
    )
    if exact is None:
        raise RuntimeError(f"exact eToro symbol not found: {normalized_symbol}")

    rates = await market.rates([exact.instrument_id])
    if len(rates) != 1:
        raise RuntimeError(f"no unique live quote for {normalized_symbol}")
    rate = rates[0]
    now = datetime.now(UTC)
    quote_age = max(0.0, (now - rate.timestamp.astimezone(UTC)).total_seconds())
    if quote_age > settings.etoro.max_quote_age_seconds:
        raise RuntimeError(f"quote is stale: {quote_age:.1f}s")
    if rate.ask is None or rate.bid is None:
        raise RuntimeError("bid/ask unavailable")
    if stop_loss_rate >= rate.ask:
        raise RuntimeError("BUY stop loss must be below current ask")
    if take_profit_rate is not None and take_profit_rate <= rate.ask:
        raise RuntimeError("BUY take profit must be above current ask")

    before = await market.account_snapshot(now=now)
    if before.open_positions != 0:
        raise RuntimeError(
            "live-test CLI requires zero open positions; use normal execution logic after bootstrap"
        )

    execution = EtoroExecutionClient(
        api_key=api_key,
        user_key=user_key,
        base_url=settings.etoro.execution_base_url,
    )
    order = await execution.open_market_buy(
        instrument_id=exact.instrument_id,
        amount_usd=amount_usd,
        stop_loss_rate=stop_loss_rate,
        take_profit_rate=take_profit_rate,
    )

    await asyncio.sleep(2)
    after = await market.account_snapshot()
    return {
        "symbol": normalized_symbol,
        "instrument_id": exact.instrument_id,
        "quote": {
            "bid": rate.bid,
            "ask": rate.ask,
            "timestamp": rate.timestamp.isoformat(),
        },
        "request_id": order.request_id,
        "order_response": order.response,
        "before": {
            "equity_usd": before.equity_usd,
            "open_positions": before.open_positions,
        },
        "after": {
            "equity_usd": after.equity_usd,
            "open_positions": after.open_positions,
            "open_instrument_ids": after.open_instrument_ids,
        },
    }


async def _async_main() -> None:
    args = _parser().parse_args()
    result = await run_live_test(
        load_settings(),
        symbol=args.symbol,
        amount_usd=args.amount,
        stop_loss_rate=args.stop_loss,
        take_profit_rate=args.take_profit,
        confirmation=args.confirm,
    )
    print(json.dumps(result, indent=2))


def main() -> None:
    asyncio.run(_async_main())


if __name__ == "__main__":
    main()
