from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from datetime import UTC, datetime

from fastapi import FastAPI
from fastapi.responses import HTMLResponse

from parquet.config import Settings
from parquet.dashboard import position_payload, render_positions_dashboard
from parquet.orchestrator import Orchestrator
from parquet.strategy import StrategyDispatcher, StrategyQueue


def create_app(settings: Settings, orchestrator: Orchestrator) -> FastAPI:
    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        task: asyncio.Task[None] | None = None
        if settings.strategy.enabled:
            if orchestrator.bridge is None:
                orchestrator.storage.set(
                    "strategy_last_error",
                    "Strategy is enabled but GitHub bridge is disabled",
                )
                orchestrator.storage.set(
                    "strategy_last_error_at", datetime.now(UTC).isoformat()
                )
            else:
                dispatcher = StrategyDispatcher(
                    queue_dir=settings.strategy.queue_dir,
                    state_db=settings.state_db,
                    storage=orchestrator.storage,
                    bridge=orchestrator.bridge,
                )
                task = asyncio.create_task(
                    dispatcher.run_forever(), name="parquet-strategy-dispatcher"
                )
        try:
            yield
        finally:
            if task is not None:
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task

    app = FastAPI(title="Parquet", version="0.11.0", lifespan=lifespan)

    @app.get("/positions", response_class=HTMLResponse)
    def positions_page() -> HTMLResponse:
        return HTMLResponse(render_positions_dashboard(orchestrator.storage.managed_positions()))

    @app.get("/positions.json")
    def positions_data() -> dict[str, object]:
        positions = orchestrator.storage.managed_positions()
        return {
            "open": [position_payload(item) for item in positions if item.status == "OPEN"],
            "closed": [position_payload(item) for item in positions if item.status != "OPEN"],
        }

    @app.get("/health")
    def health() -> dict[str, object]:
        reconciliation = orchestrator.storage.get_reconciliation_report()
        authenticated_gcid = orchestrator.storage.get("etoro_authenticated_gcid")
        identity_verified = orchestrator.storage.get("etoro_identity_verified") == "1"
        execution_uncertain = orchestrator.storage.get("execution_uncertain") == "1"
        reconciliation_ready = reconciliation is not None and reconciliation.trading_enabled
        broker_execution_ready = (
            settings.execution.supervised_real_enabled
            and identity_verified
            and reconciliation_ready
            and not execution_uncertain
        )
        strategy = _strategy_state(settings, orchestrator)
        return {
            "status": "ok",
            "mode": settings.mode,
            "github": settings.github.enabled,
            "etoro": settings.etoro.enabled,
            "etoro_expected_gcid": settings.etoro.expected_gcid,
            "etoro_authenticated_gcid": authenticated_gcid,
            "etoro_agent_portfolio_pinned": settings.etoro.expected_gcid is not None,
            "etoro_identity_verified": identity_verified,
            "etoro_identity_checked_at": orchestrator.storage.get("etoro_identity_checked_at"),
            "etoro_identity_error": orchestrator.storage.get("etoro_identity_error") or None,
            "strategy_enabled": settings.strategy.enabled,
            "strategy_provider": settings.strategy.provider,
            "strategy_worker_ready": strategy["worker_ready"],
            "strategy_worker_heartbeat_at": strategy["heartbeat_at"],
            "strategy_pending_requests": strategy["pending_requests"],
            "strategy_last_analysis_id": orchestrator.storage.get("strategy_last_analysis_id"),
            "strategy_last_error": orchestrator.storage.get("strategy_last_error") or None,
            "execution_gate": True,
            "position_manager": True,
            "reconciliation_engine": True,
            "positions_dashboard": True,
            "reconciliation_state": None if reconciliation is None else reconciliation.state.value,
            "autonomous_trading_enabled": reconciliation_ready,
            "autonomous_execution_configured": settings.execution.autonomous_enabled,
            "autonomous_execution_mode": settings.execution.autonomous_mode,
            "supervised_real_execution": settings.execution.supervised_real_enabled,
            "supervised_real_max_amount_usd": (
                settings.execution.supervised_real_max_amount_usd
            ),
            "execution_uncertain": execution_uncertain,
            "broker_execution": broker_execution_ready,
            "live_test_execution": (
                settings.execution.live_test_enabled and settings.etoro.expected_gcid is None
            ),
            "live_test_dry_run": settings.execution.live_test_enabled,
        }

    @app.get("/status")
    def status() -> dict[str, object]:
        active_watches = orchestrator.storage.active_watches()
        risk_snapshot = orchestrator.storage.get_risk_snapshot()
        reconciliation = orchestrator.storage.get_reconciliation_report()
        broker_portfolio = orchestrator.storage.get_broker_portfolio_snapshot()
        attempts = orchestrator.storage.latest_execution_attempts()
        positions = orchestrator.storage.managed_positions()
        authenticated_gcid = orchestrator.storage.get("etoro_authenticated_gcid")
        authenticated_scopes = orchestrator.storage.get("etoro_authenticated_scopes")
        identity_verified = orchestrator.storage.get("etoro_identity_verified") == "1"
        execution_uncertain = orchestrator.storage.get("execution_uncertain") == "1"
        reconciliation_ready = reconciliation is not None and reconciliation.trading_enabled
        broker_execution_ready = (
            settings.execution.supervised_real_enabled
            and identity_verified
            and reconciliation_ready
            and not execution_uncertain
        )
        strategy = _strategy_state(settings, orchestrator)
        return {
            "mode": settings.mode,
            "latest_analysis_id": orchestrator.storage.get("latest_analysis_id"),
            "active_watches": len(active_watches),
            "watch_symbols": sorted({watch.symbol for watch in active_watches}),
            "risk_snapshot_available": risk_snapshot is not None,
            "risk_snapshot_as_of": None if risk_snapshot is None else risk_snapshot.as_of,
            "risk_equity_usd": None if risk_snapshot is None else risk_snapshot.equity_usd,
            "risk_open_positions": None if risk_snapshot is None else risk_snapshot.open_positions,
            "reconciliation_state": None if reconciliation is None else reconciliation.state.value,
            "reconciliation_as_of": None if reconciliation is None else reconciliation.as_of,
            "autonomous_trading_enabled": reconciliation_ready,
            "reconciliation_issues": (
                []
                if reconciliation is None
                else [issue.model_dump(mode="json") for issue in reconciliation.issues]
            ),
            "broker_positions": (
                None if broker_portfolio is None else len(broker_portfolio.positions)
            ),
            "broker_orders": (
                None
                if broker_portfolio is None
                else len(broker_portfolio.orders) + len(broker_portfolio.orders_for_open)
            ),
            "managed_positions": len(orchestrator.storage.active_managed_positions()),
            "managed_closed_positions": len([item for item in positions if item.status != "OPEN"]),
            "managed_orders": len(orchestrator.storage.active_managed_orders()),
            "etoro_expected_gcid": settings.etoro.expected_gcid,
            "etoro_authenticated_gcid": authenticated_gcid,
            "etoro_authenticated_real_cid": orchestrator.storage.get(
                "etoro_authenticated_real_cid"
            ),
            "etoro_authenticated_demo_cid": orchestrator.storage.get(
                "etoro_authenticated_demo_cid"
            ),
            "etoro_authenticated_scopes": authenticated_scopes,
            "etoro_agent_portfolio_pinned": settings.etoro.expected_gcid is not None,
            "etoro_identity_verified": identity_verified,
            "etoro_identity_checked_at": orchestrator.storage.get("etoro_identity_checked_at"),
            "etoro_identity_error": orchestrator.storage.get("etoro_identity_error") or None,
            "strategy_enabled": settings.strategy.enabled,
            "strategy_provider": settings.strategy.provider,
            "strategy_worker_ready": strategy["worker_ready"],
            "strategy_worker_heartbeat_at": strategy["heartbeat_at"],
            "strategy_codex_authenticated": strategy["codex_authenticated"],
            "strategy_pending_requests": strategy["pending_requests"],
            "strategy_last_analysis_id": orchestrator.storage.get("strategy_last_analysis_id"),
            "strategy_last_success_at": orchestrator.storage.get("strategy_last_success_at"),
            "strategy_last_error": orchestrator.storage.get("strategy_last_error") or None,
            "strategy_last_error_at": orchestrator.storage.get("strategy_last_error_at"),
            "autonomous_execution_configured": settings.execution.autonomous_enabled,
            "autonomous_execution_mode": settings.execution.autonomous_mode,
            "supervised_real_execution": settings.execution.supervised_real_enabled,
            "supervised_real_max_amount_usd": (
                settings.execution.supervised_real_max_amount_usd
            ),
            "broker_execution": broker_execution_ready,
            "execution_uncertain": execution_uncertain,
            "execution_attempts": [attempt.model_dump(mode="json") for attempt in attempts],
            "live_test_execution": (
                settings.execution.live_test_enabled and settings.etoro.expected_gcid is None
            ),
            "live_test_dry_run": settings.execution.live_test_enabled,
            "live_test_max_amount_usd": settings.execution.live_test_max_amount_usd,
            "pending_reviews": [
                {
                    "at": review.at.isoformat(),
                    "reason": review.reason,
                    "source": review.source,
                }
                for review in orchestrator.reviews.pending()
            ],
        }

    return app


def _strategy_state(settings: Settings, orchestrator: Orchestrator) -> dict[str, object]:
    if not settings.strategy.enabled:
        return {
            "worker_ready": False,
            "heartbeat_at": None,
            "codex_authenticated": False,
            "pending_requests": 0,
        }
    queue = StrategyQueue(settings.strategy.queue_dir)
    worker = queue.worker_status()
    heartbeat_at = None if worker is None else worker.get("heartbeat_at")
    codex_authenticated = bool(
        worker is not None and worker.get("codex_authenticated") is True
    )
    worker_ready = codex_authenticated and _heartbeat_fresh(heartbeat_at)
    try:
        pending = queue.pending_count()
    except OSError:
        pending = 0
    return {
        "worker_ready": worker_ready,
        "heartbeat_at": heartbeat_at,
        "codex_authenticated": codex_authenticated,
        "pending_requests": pending,
    }


def _heartbeat_fresh(value: object) -> bool:
    if not isinstance(value, str):
        return False
    try:
        timestamp = datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
    except ValueError:
        return False
    return (datetime.now(UTC) - timestamp).total_seconds() <= 30
