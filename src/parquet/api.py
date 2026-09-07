from __future__ import annotations

from fastapi import FastAPI
from fastapi.responses import HTMLResponse

from parquet.config import Settings
from parquet.dashboard import position_payload, render_positions_dashboard
from parquet.orchestrator import Orchestrator


def create_app(settings: Settings, orchestrator: Orchestrator) -> FastAPI:
    app = FastAPI(title="Parquet", version="0.10.0")

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
        return {
            "status": "ok",
            "mode": settings.mode,
            "github": settings.github.enabled,
            "etoro": settings.etoro.enabled,
            "etoro_expected_gcid": settings.etoro.expected_gcid,
            "etoro_authenticated_gcid": authenticated_gcid,
            "etoro_agent_portfolio_pinned": settings.etoro.expected_gcid is not None,
            "execution_gate": True,
            "position_manager": True,
            "reconciliation_engine": True,
            "positions_dashboard": True,
            "reconciliation_state": None if reconciliation is None else reconciliation.state.value,
            "autonomous_trading_enabled": (
                False if reconciliation is None else reconciliation.trading_enabled
            ),
            "autonomous_execution_configured": settings.execution.autonomous_enabled,
            "autonomous_execution_mode": settings.execution.autonomous_mode,
            "supervised_real_execution": settings.execution.supervised_real_enabled,
            "supervised_real_max_amount_usd": (
                settings.execution.supervised_real_max_amount_usd
            ),
            "execution_uncertain": orchestrator.storage.get("execution_uncertain") == "1",
            "broker_execution": False,
            "live_test_execution": settings.execution.live_test_enabled,
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
            "autonomous_trading_enabled": (
                False if reconciliation is None else reconciliation.trading_enabled
            ),
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
            "etoro_authenticated_scopes": authenticated_scopes,
            "etoro_agent_portfolio_pinned": settings.etoro.expected_gcid is not None,
            "autonomous_execution_configured": settings.execution.autonomous_enabled,
            "autonomous_execution_mode": settings.execution.autonomous_mode,
            "supervised_real_execution": settings.execution.supervised_real_enabled,
            "supervised_real_max_amount_usd": (
                settings.execution.supervised_real_max_amount_usd
            ),
            "execution_uncertain": orchestrator.storage.get("execution_uncertain") == "1",
            "execution_attempts": [attempt.model_dump(mode="json") for attempt in attempts],
            "live_test_execution": settings.execution.live_test_enabled,
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
