from __future__ import annotations

from fastapi import FastAPI

from parquet.config import Settings
from parquet.orchestrator import Orchestrator


def create_app(settings: Settings, orchestrator: Orchestrator) -> FastAPI:
    app = FastAPI(title="Parquet", version="0.5.0")

    @app.get("/health")
    def health() -> dict[str, object]:
        return {
            "status": "ok",
            "mode": settings.mode,
            "github": settings.github.enabled,
            "etoro": settings.etoro.enabled,
            "execution_gate": True,
            "broker_execution": False,
        }

    @app.get("/status")
    def status() -> dict[str, object]:
        active_watches = orchestrator.storage.active_watches()
        risk_snapshot = orchestrator.storage.get_risk_snapshot()
        return {
            "mode": settings.mode,
            "latest_analysis_id": orchestrator.storage.get("latest_analysis_id"),
            "active_watches": len(active_watches),
            "watch_symbols": sorted({watch.symbol for watch in active_watches}),
            "risk_snapshot_available": risk_snapshot is not None,
            "risk_snapshot_as_of": None if risk_snapshot is None else risk_snapshot.as_of,
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
