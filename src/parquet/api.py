from __future__ import annotations

from fastapi import FastAPI

from parquet.config import Settings
from parquet.orchestrator import Orchestrator


def create_app(settings: Settings, orchestrator: Orchestrator) -> FastAPI:
    app = FastAPI(title="Parquet", version="0.7.0")

    @app.get("/health")
    def health() -> dict[str, object]:
        reconciliation = orchestrator.storage.get_reconciliation_report()
        return {
            "status": "ok",
            "mode": settings.mode,
            "github": settings.github.enabled,
            "etoro": settings.etoro.enabled,
            "execution_gate": True,
            "position_manager": True,
            "reconciliation_engine": True,
            "reconciliation_state": None if reconciliation is None else reconciliation.state.value,
            "autonomous_trading_enabled": (
                False if reconciliation is None else reconciliation.trading_enabled
            ),
            "broker_execution": False,
            "live_test_execution": settings.execution.live_test_enabled,
        }

    @app.get("/status")
    def status() -> dict[str, object]:
        active_watches = orchestrator.storage.active_watches()
        risk_snapshot = orchestrator.storage.get_risk_snapshot()
        reconciliation = orchestrator.storage.get_reconciliation_report()
        broker_portfolio = orchestrator.storage.get_broker_portfolio_snapshot()
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
            "managed_orders": len(orchestrator.storage.active_managed_orders()),
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
