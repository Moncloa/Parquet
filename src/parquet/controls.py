from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from parquet.config import Settings
from parquet.models import MarketAnalysis
from parquet.scheduler import ScheduledReview
from parquet.storage import Storage
from parquet.strategy import StrategyQueue

_ALLOWED_PROVIDERS = {"local_ollama", "codex_cli"}


def begin_manual_review(
    settings: Settings,
    orchestrator: Any,
    *,
    provider: str,
) -> str:
    normalized = provider.strip().lower()
    if normalized not in _ALLOWED_PROVIDERS:
        raise ValueError(f"Unsupported manual review provider: {provider}")
    if not settings.strategy.enabled:
        raise RuntimeError("Strategy worker is disabled")
    if orchestrator.bridge is None:
        raise RuntimeError("GitHub bridge is disabled")

    request_id = str(uuid4())
    orchestrator.storage.add_event(
        "manual_review_requested",
        json.dumps(
            {
                "request_id": request_id,
                "provider": normalized,
                "requested_at": datetime.now(UTC).isoformat(),
                "analysis_only": True,
                "source": "web",
            }
        ),
    )
    return request_id


async def execute_manual_review(
    settings: Settings,
    orchestrator: Any,
    *,
    request_id: str,
    provider: str,
) -> None:
    normalized = provider.strip().lower()
    try:
        report = orchestrator.storage.get_reconciliation_report()
        if report is None or not report.trading_enabled:
            state = None if report is None else report.state.value
            raise RuntimeError(f"Broker reconciliation is not ready ({state})")
        if orchestrator.storage.get("etoro_identity_verified") != "1":
            raise RuntimeError("eToro identity is not verified")

        current = datetime.now(UTC)
        review = ScheduledReview(
            at=current,
            reason=f"manual_web:{normalized}",
            source="manual_web",
        )
        orchestrator.add_review(review)
        posted = await orchestrator.post_due_reviews(
            now=current,
            source="manual_web",
            review_key=review.key,
            request_id=request_id,
            request_context={
                "_parquet_control": {
                    "strategy_provider": normalized,
                    "analysis_only": True,
                    "source": "web",
                }
            },
        )
        if posted != 1:
            raise RuntimeError(f"Expected exactly one manual review request, got {posted}")
    except Exception as exc:
        orchestrator.storage.add_event(
            "manual_review_request_error",
            json.dumps(
                {
                    "request_id": request_id,
                    "provider": normalized,
                    "failed_at": datetime.now(UTC).isoformat(),
                    "error": str(exc)[:1000],
                }
            ),
        )


def manual_review_status(
    storage: Storage,
    queue_dir: Path,
    request_id: str,
) -> dict[str, object]:
    events = _events_for_request(storage, request_id)
    requested = _last_event(events, "manual_review_requested")
    if requested is None:
        return {
            "request_id": request_id,
            "state": "not_found",
            "progress_pct": 0,
            "provider": None,
            "message": "Review request not found",
        }

    provider = str(requested.get("provider") or "unknown")
    analysis = _analysis_for_request(storage, request_id)
    if analysis is not None:
        return {
            "request_id": request_id,
            "state": "completed",
            "progress_pct": 100,
            "provider": provider,
            "message": "Analysis completed",
            "analysis": {
                "analysis_id": analysis.analysis_id,
                "generated_at": analysis.generated_at.isoformat(),
                "market_regime": analysis.market_regime,
                "summary": analysis.summary,
                "trade_proposals": [
                    proposal.model_dump(mode="json")
                    for proposal in analysis.trade_proposals
                ],
                "watch": [item.model_dump(mode="json") for item in analysis.watch],
                "next_review": (
                    None
                    if analysis.next_review is None
                    else analysis.next_review.model_dump(mode="json")
                ),
            },
        }

    failed = (
        _last_event(events, "manual_review_request_error")
        or _last_event(events, "strategy_analysis_error")
        or _last_event(events, "strategy_result_rejected")
    )
    if failed is not None:
        return {
            "request_id": request_id,
            "state": "failed",
            "progress_pct": 100,
            "provider": provider,
            "message": str(failed.get("error") or "Analysis failed"),
        }

    worker: dict[str, object] = {}
    try:
        worker = StrategyQueue(queue_dir).worker_status() or {}
    except OSError:
        worker = {}

    if worker.get("active_request_id") == request_id:
        stage = str(worker.get("active_stage") or "analysing")
        return {
            "request_id": request_id,
            "state": "analysing",
            "progress_pct": 65,
            "provider": provider,
            "message": f"{provider} · {stage}",
            "started_at": worker.get("active_started_at"),
        }

    if _last_event(events, "strategy_analysis_published") is not None:
        return {
            "request_id": request_id,
            "state": "publishing",
            "progress_pct": 90,
            "provider": provider,
            "message": "Analysis published; waiting for local ingest",
        }

    if _last_event(events, "strategy_request_queued") is not None:
        return {
            "request_id": request_id,
            "state": "queued",
            "progress_pct": 40,
            "provider": provider,
            "message": f"Queued for {provider}",
        }

    if _last_event(events, "review_request") is not None:
        return {
            "request_id": request_id,
            "state": "queued",
            "progress_pct": 30,
            "provider": provider,
            "message": "Market context ready; waiting for strategy dispatcher",
        }

    return {
        "request_id": request_id,
        "state": "collecting_context",
        "progress_pct": 15,
        "provider": provider,
        "message": "Collecting current market context",
    }


def is_analysis_only_request(storage: Storage, request_id: str | None) -> bool:
    if not request_id:
        return False
    events = _events_for_request(storage, request_id, kinds={"review_request"})
    for _, payload in reversed(events):
        control = payload.get("context")
        if not isinstance(control, dict):
            continue
        metadata = control.get("_parquet_control")
        if isinstance(metadata, dict) and metadata.get("analysis_only") is True:
            return True
    return False


def request_control_metadata(request_context: dict[str, object]) -> dict[str, object]:
    value = request_context.get("_parquet_control")
    return value if isinstance(value, dict) else {}


def _events_for_request(
    storage: Storage,
    request_id: str,
    *,
    kinds: set[str] | None = None,
) -> list[tuple[str, dict[str, Any]]]:
    with storage._lock:
        rows = storage.conn.execute(
            "SELECT kind, payload FROM events WHERE payload LIKE ? ORDER BY id",
            (f"%{request_id}%",),
        ).fetchall()

    result: list[tuple[str, dict[str, Any]]] = []
    for kind, raw in rows:
        name = str(kind)
        if kinds is not None and name not in kinds:
            continue
        try:
            payload = json.loads(str(raw))
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict) and payload.get("request_id") == request_id:
            result.append((name, payload))
    return result


def _last_event(
    events: list[tuple[str, dict[str, Any]]],
    kind: str,
) -> dict[str, Any] | None:
    for name, payload in reversed(events):
        if name == kind:
            return payload
    return None


def _analysis_for_request(storage: Storage, request_id: str) -> MarketAnalysis | None:
    with storage._lock:
        rows = storage.conn.execute(
            "SELECT payload FROM analyses ORDER BY generated_at DESC LIMIT 100"
        ).fetchall()
    for row in rows:
        try:
            analysis = MarketAnalysis.model_validate_json(str(row[0]))
        except Exception:
            continue
        if analysis.review_request_id == request_id:
            return analysis
    return None
