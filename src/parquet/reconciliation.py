from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime

from parquet.config import Settings
from parquet.models import RiskSnapshot
from parquet.orchestrator import Orchestrator
from parquet.portfolio import PositionManager
from parquet.portfolio_etoro import EtoroPortfolioReader
from parquet.storage import Storage


class ReconciliationService:
    """Polls the broker, reconciles the durable ledger and refreshes risk state."""

    def __init__(
        self,
        settings: Settings,
        storage: Storage,
        market_client: object | None,
    ) -> None:
        self.settings = settings
        self.storage = storage
        self.position_manager = PositionManager(storage)
        self.reader = (
            EtoroPortfolioReader(market_client)  # type: ignore[arg-type]
            if market_client is not None
            else None
        )
        self._last_poll_at: datetime | None = None
        self._reverse_ids = {
            instrument_id: symbol.upper()
            for symbol, instrument_id in settings.etoro.instrument_ids.items()
        }

    async def poll_once(
        self,
        now: datetime | None = None,
        *,
        force: bool = False,
    ) -> int:
        if self.reader is None:
            return 0
        current = (now or datetime.now(UTC)).astimezone(UTC)
        if (
            not force
            and self._last_poll_at is not None
            and (current - self._last_poll_at).total_seconds()
            < self.settings.etoro.account_poll_seconds
        ):
            return 0
        self._last_poll_at = current

        try:
            snapshot = await self.reader.snapshot(now=current)
            if snapshot.equity_usd <= 0:
                raise ValueError("eToro equity must be positive for execution sizing")
            report = self.position_manager.reconcile(snapshot)
        except Exception as exc:
            report = self.position_manager.record_error(exc, now=current)
            self.storage.add_event(
                "reconciliation_error",
                json.dumps(
                    {
                        "state": report.state.value,
                        "error": repr(exc),
                    }
                ),
            )
            return 0

        open_symbols = set(snapshot.open_symbols)
        for instrument_id in snapshot.open_instrument_ids:
            symbol = self._reverse_ids.get(instrument_id)
            if symbol is not None:
                open_symbols.add(symbol)

        previous = self.storage.get_risk_snapshot()
        risk_snapshot = RiskSnapshot(
            as_of=snapshot.captured_at,
            equity_usd=snapshot.equity_usd,
            open_positions=len(snapshot.positions),
            trades_today=0 if previous is None else previous.trades_today,
            daily_pnl_pct=0.0 if previous is None else previous.daily_pnl_pct,
            weekly_pnl_pct=0.0 if previous is None else previous.weekly_pnl_pct,
            open_symbols=sorted(open_symbols),
            open_instrument_ids=snapshot.open_instrument_ids,
        )
        self.storage.set_risk_snapshot(risk_snapshot)
        self.storage.set(
            "account_snapshot_components",
            json.dumps(
                {
                    "as_of": snapshot.captured_at.isoformat(),
                    "equity_usd": snapshot.equity_usd,
                    "available_cash_usd": snapshot.available_cash_usd,
                    "invested_usd": snapshot.invested_usd,
                    "unrealized_pnl_usd": snapshot.unrealized_pnl_usd,
                    "open_positions": len(snapshot.positions),
                    "reconciliation_state": report.state.value,
                    "autonomous_trading_enabled": report.trading_enabled,
                }
            ),
        )
        self.storage.add_event("reconciliation", report.model_dump_json())
        return 1


async def run_with_reconciliation(
    orchestrator: Orchestrator,
    service: ReconciliationService,
) -> None:
    """Main runtime loop with reconciliation replacing the legacy account poll."""

    while True:
        try:
            await orchestrator.poll_github_once()
            orchestrator.ensure_structural_reviews()
            await service.poll_once()
            await orchestrator.poll_market_once()
            await orchestrator.post_due_reviews()
        except Exception as exc:
            orchestrator.storage.add_event(
                "orchestrator_error",
                json.dumps({"error": repr(exc)}),
            )
        await asyncio.sleep(orchestrator.settings.poll_seconds)
