from __future__ import annotations

from fastapi import FastAPI

from parquet.config import Settings
from parquet.orchestrator import Orchestrator


def create_app(settings: Settings, orchestrator: Orchestrator) -> FastAPI:
    app = FastAPI(title="Parquet", version="0.1.0")

    @app.get("/health")
    def health() -> dict[str, object]:
        return {"status": "ok", "mode": settings.mode, "github": settings.github.enabled}

    @app.get("/status")
    def status() -> dict[str, object]:
        return {
            "mode": settings.mode,
            "latest_analysis_id": orchestrator.storage.get("latest_analysis_id"),
            "pending_reviews": [
                {"at": r.at.isoformat(), "reason": r.reason} for r in orchestrator.reviews.pending()
            ],
        }

    return app
