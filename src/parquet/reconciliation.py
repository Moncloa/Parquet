from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from typing import Any

from parquet.config import Settings
from parquet.market.etoro import EtoroMarketDataClient
from parquet.models import RiskSnapshot
from parquet.orchestrator import Orchestrator
from parquet.portfolio import PositionManager
from parquet.portfolio_etoro import EtoroPortfolioReader
from parquet.storage import Storage


class ReconciliationService:
    """Polls the broker, verifies identity and refreshes durable portfolio/risk state."""

    def __init__(
        self,
        settings: Settings,
        storage: Storage,
        market_client: EtoroMarketDataClient | None,
    ) -> None:
        self.settings = settings
        self.storage = storage
        self.position_manager = PositionManager(storage)
        self.market_client = market_client
        self.reader = EtoroPortfolioReader(market_client) if market_client is not None else None
        self._last_poll_at: datetime | None = None
        self._reverse_ids = {
            instrument_id: symbol.upper()
            for symbol, instrument_id in settings.etoro.instrument_ids.items()
        }

    async def verify_identity_once(self, *, now: datetime | None = None) -> bool:
        """Verify the authenticated eToro identity and persist the result.

        A configured ``expected_gcid`` is a hard account boundary: if the token
        resolves to another GCID, or required real scopes disappear, reconciliation
        is put into ERROR so every execution gate fails closed.
        """

        if self.market_client is None:
            return False
        current = (now or datetime.now(UTC)).astimezone(UTC)
        checked_at = current.isoformat()

        try:
            body = await self.market_client._get("/me", params={})
            identity = _parse_identity(body)
            self.storage.set("etoro_authenticated_gcid", str(identity["gcid"]))
            self.storage.set("etoro_authenticated_real_cid", _optional_text(identity["real_cid"]))
            self.storage.set("etoro_authenticated_demo_cid", _optional_text(identity["demo_cid"]))
            self.storage.set("etoro_authenticated_scopes", json.dumps(sorted(identity["scopes"])))
            self.storage.set("etoro_identity_checked_at", checked_at)

            expected_gcid = self.settings.etoro.expected_gcid
            if expected_gcid is None:
                self.storage.set("etoro_identity_verified", "0")
                self.storage.set("etoro_identity_error", "Agent Portfolio GCID is not pinned")
                return True

            if identity["gcid"] != expected_gcid:
                raise ValueError(
                    f"eToro authenticated GCID {identity['gcid']} does not match pinned "
                    f"Agent Portfolio GCID {expected_gcid}"
                )

            missing = sorted(
                set(self.settings.etoro.required_real_scopes) - set(identity["scopes"])
            )
            if missing:
                raise ValueError(
                    "eToro authenticated token is missing required scopes: " + ", ".join(missing)
                )
        except Exception as exc:
            self.storage.set("etoro_identity_verified", "0")
            self.storage.set("etoro_identity_checked_at", checked_at)
            self.storage.set("etoro_identity_error", str(exc))
            report = self.position_manager.record_error(
                f"eToro identity verification failed: {exc}",
                now=current,
            )
            self.storage.add_event(
                "etoro_identity_verification_failed",
                json.dumps(
                    {
                        "as_of": checked_at,
                        "state": report.state.value,
                        "error": str(exc),
                    }
                ),
            )
            return False

        self.storage.set("etoro_identity_verified", "1")
        self.storage.set("etoro_identity_error", "")
        self.storage.add_event(
            "etoro_identity_verified",
            json.dumps(
                {
                    "as_of": checked_at,
                    "gcid": identity["gcid"],
                    "real_cid": identity["real_cid"],
                    "demo_cid": identity["demo_cid"],
                    "scopes": sorted(identity["scopes"]),
                }
            ),
        )
        return True

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

        if not await self.verify_identity_once(now=current):
            return 0

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
            # Broker identity/reconciliation is checked before external review work so
            # the execution gate fails closed as early as possible after each interval.
            await service.poll_once()
            await orchestrator.poll_github_once()
            orchestrator.ensure_structural_reviews()
            await orchestrator.poll_market_once()
            await orchestrator.post_due_reviews()
        except Exception as exc:
            orchestrator.storage.add_event(
                "orchestrator_error",
                json.dumps({"error": repr(exc)}),
            )
        await asyncio.sleep(orchestrator.settings.poll_seconds)


def _parse_identity(body: Any) -> dict[str, Any]:
    if not isinstance(body, dict):
        raise ValueError("eToro /me response must be an object")
    raw_gcid = body.get("gcid")
    if raw_gcid is None:
        raise ValueError("eToro /me response is missing gcid")
    raw_scopes = body.get("scopes")
    if raw_scopes is None:
        raw_scopes = []
    if not isinstance(raw_scopes, list):
        raise ValueError("eToro /me response scopes must be a list")
    return {
        "gcid": int(raw_gcid),
        "real_cid": _optional_int(body.get("realCid")),
        "demo_cid": _optional_int(body.get("demoCid")),
        "scopes": frozenset(str(scope) for scope in raw_scopes),
    }


def _optional_int(value: Any) -> int | None:
    return None if value is None else int(value)


def _optional_text(value: Any) -> str:
    return "" if value is None else str(value)
