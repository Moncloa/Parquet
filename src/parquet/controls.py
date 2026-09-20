from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from parquet.config import Settings
from parquet.models import MarketAnalysis, TriggerAction
from parquet.scheduler import ScheduledReview
from parquet.storage import Storage
from parquet.strategy import StrategyQueue

_ALLOWED_PROVIDERS = {"local_ollama", "codex_cli"}
_TERMINAL_EXECUTION_STATES = {
    "SHADOW_EXECUTED",
    "REJECTED",
    "BLOCKED",
    "PROTECTION_MISMATCH",
    "RECONCILED",
}
_TERMINAL_EXECUTION_EVENTS = {
    "direct_proposal_execution_rejected",
    "direct_proposal_execution_blocked",
    "direct_proposal_execution_error",
    "direct_proposal_execution_shadow",
    "direct_proposal_execution_real",
}


def begin_manual_review(
    settings: Settings,
    orchestrator: Any,
    *,
    provider: str,
    allow_execution: bool = False,
) -> str:
    normalized = provider.strip().lower()
    if normalized not in _ALLOWED_PROVIDERS:
        raise ValueError(f"Unsupported manual review provider: {provider}")
    if not settings.strategy.enabled:
        raise RuntimeError("Strategy worker is disabled")
    if orchestrator.bridge is None:
        raise RuntimeError("GitHub bridge is disabled")
    if allow_execution:
        _validate_operational_mode(settings, orchestrator.storage)

    request_id = str(uuid4())
    orchestrator.storage.add_event(
        "manual_review_requested",
        json.dumps(
            {
                "request_id": request_id,
                "provider": normalized,
                "requested_at": datetime.now(UTC).isoformat(),
                "analysis_only": not allow_execution,
                "allow_execution": allow_execution,
                "execution_mode": settings.execution.autonomous_mode,
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
    allow_execution: bool = False,
) -> None:
    normalized = provider.strip().lower()
    try:
        report = orchestrator.storage.get_reconciliation_report()
        if report is None or not report.trading_enabled:
            state = None if report is None else report.state.value
            raise RuntimeError(f"Broker reconciliation is not ready ({state})")
        if orchestrator.storage.get("etoro_identity_verified") != "1":
            raise RuntimeError("eToro identity is not verified")
        if allow_execution:
            _validate_operational_mode(settings, orchestrator.storage)

        current = datetime.now(UTC)
        review_kind = "operational" if allow_execution else "analysis"
        review = ScheduledReview(
            at=current,
            reason=f"manual_web:{normalized}:{review_kind}",
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
                    "analysis_only": not allow_execution,
                    "allow_execution": allow_execution,
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
            "analysis_only": True,
            "allow_execution": False,
            "message": "Review request not found",
        }

    provider = str(requested.get("provider") or "unknown")
    analysis_only = requested.get("analysis_only") is not False
    allow_execution = not analysis_only
    base: dict[str, object] = {
        "request_id": request_id,
        "provider": provider,
        "analysis_only": analysis_only,
        "allow_execution": allow_execution,
        "execution_mode": requested.get("execution_mode"),
    }

    analysis = _analysis_for_request(storage, request_id)
    if analysis is not None:
        analysis_payload = _analysis_payload(analysis)
        if analysis_only:
            return {
                **base,
                "state": "completed",
                "progress_pct": 100,
                "message": "Analysis completed",
                "analysis": analysis_payload,
            }
        return {
            **base,
            **_operational_analysis_status(storage, analysis),
            "analysis": analysis_payload,
        }

    failed = (
        _last_event(events, "manual_review_request_error")
        or _last_event(events, "strategy_analysis_error")
        or _last_event(events, "strategy_result_rejected")
    )
    if failed is not None:
        return {
            **base,
            "state": "failed",
            "progress_pct": 100,
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
            **base,
            "state": "analysing",
            "progress_pct": 65,
            "message": f"{provider} · {stage}",
            "started_at": worker.get("active_started_at"),
        }

    if _last_event(events, "strategy_analysis_published") is not None:
        return {
            **base,
            "state": "publishing",
            "progress_pct": 80 if allow_execution else 90,
            "message": "Analysis published; waiting for local ingest",
        }

    if _last_event(events, "strategy_request_queued") is not None:
        return {
            **base,
            "state": "queued",
            "progress_pct": 40,
            "message": f"Queued for {provider}",
        }

    if _last_event(events, "review_request") is not None:
        return {
            **base,
            "state": "queued",
            "progress_pct": 30,
            "message": "Market context ready; waiting for strategy dispatcher",
        }

    return {
        **base,
        "state": "collecting_context",
        "progress_pct": 15,
        "message": "Collecting current market context",
    }


def latest_manual_review_status(
    storage: Storage,
    queue_dir: Path,
) -> dict[str, object] | None:
    with storage._lock:
        row = storage.conn.execute(
            "SELECT payload FROM events WHERE kind = 'manual_review_requested' "
            "ORDER BY id DESC LIMIT 1"
        ).fetchone()
    if row is None:
        return None
    try:
        payload = json.loads(str(row[0]))
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict):
        return None
    request_id = payload.get("request_id")
    if not isinstance(request_id, str) or not request_id:
        return None
    return manual_review_status(storage, queue_dir, request_id)


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


def _validate_operational_mode(settings: Settings, storage: Storage) -> None:
    execution = settings.execution
    if not execution.autonomous_enabled:
        raise RuntimeError("Autonomous execution is disabled")
    mode = execution.autonomous_mode
    if mode == "real":
        if settings.mode.lower() != "real":
            raise RuntimeError("Operational REAL review requires global mode=real")
        if not execution.autonomous_real_enabled:
            raise RuntimeError("Autonomous real execution is disabled")
        if storage.get("execution_uncertain") == "1":
            raise RuntimeError("Execution blocked: unresolved broker outcome")
    elif mode == "demo":
        if not execution.autonomous_demo_enabled:
            raise RuntimeError("Autonomous demo execution is disabled")
    elif mode != "shadow":
        raise RuntimeError(f"Unsupported autonomous execution mode: {mode}")


def _analysis_payload(analysis: MarketAnalysis) -> dict[str, object]:
    return {
        "analysis_id": analysis.analysis_id,
        "generated_at": analysis.generated_at.isoformat(),
        "market_regime": analysis.market_regime,
        "summary": analysis.summary,
        "trade_proposals": [
            proposal.model_dump(mode="json") for proposal in analysis.trade_proposals
        ],
        "watch": [item.model_dump(mode="json") for item in analysis.watch],
        "next_review": (
            None
            if analysis.next_review is None
            else analysis.next_review.model_dump(mode="json")
        ),
    }


def _operational_analysis_status(
    storage: Storage,
    analysis: MarketAnalysis,
) -> dict[str, object]:
    if not analysis.trade_proposals:
        return {
            "state": "completed",
            "progress_pct": 100,
            "message": "Operational analysis completed: no trade proposal",
            "execution": [],
        }

    execute_watch_proposals = {
        item.proposal_id
        for item in analysis.watch
        if item.on_trigger == TriggerAction.EXECUTE and item.proposal_id is not None
    }
    outcomes: list[dict[str, object]] = []
    waiting = False
    executing = False

    for proposal in analysis.trade_proposals:
        proposal_id = proposal.proposal_id
        if proposal_id in execute_watch_proposals:
            outcomes.append(
                {
                    "proposal_id": proposal_id,
                    "symbol": proposal.symbol,
                    "state": "WATCHING",
                    "message": "Execution deferred until watch trigger",
                }
            )
            continue

        attempt = storage.get_active_execution_attempt_for_proposal(proposal_id)
        if attempt is not None:
            state = attempt.state.value
            outcomes.append(
                {
                    "proposal_id": proposal_id,
                    "symbol": proposal.symbol,
                    "state": state,
                    "attempt_id": attempt.attempt_id,
                    "broker_position_id": attempt.broker_position_id,
                    "reason": attempt.reason,
                }
            )
            if state not in _TERMINAL_EXECUTION_STATES:
                executing = True
            continue

        event = _execution_event_for_proposal(storage, proposal_id)
        if event is not None:
            kind, payload = event
            outcomes.append(
                {
                    "proposal_id": proposal_id,
                    "symbol": proposal.symbol,
                    "state": _execution_state_for_event(kind),
                    "reason": _event_reason(payload),
                }
            )
            continue

        if proposal.expires_at.astimezone(UTC) <= datetime.now(UTC):
            outcomes.append(
                {
                    "proposal_id": proposal_id,
                    "symbol": proposal.symbol,
                    "state": "EXPIRED",
                    "message": "Proposal expired before execution",
                }
            )
            continue

        waiting = True
        outcomes.append(
            {
                "proposal_id": proposal_id,
                "symbol": proposal.symbol,
                "state": "AWAITING_GATE",
            }
        )

    if executing:
        return {
            "state": "executing",
            "progress_pct": 95,
            "message": "Execution attempt in progress",
            "execution": outcomes,
        }
    if waiting:
        return {
            "state": "awaiting_execution",
            "progress_pct": 90,
            "message": "Analysis accepted; waiting for deterministic execution gate",
            "execution": outcomes,
        }
    return {
        "state": "completed",
        "progress_pct": 100,
        "message": "Operational review completed",
        "execution": outcomes,
    }


def _execution_event_for_proposal(
    storage: Storage,
    proposal_id: str,
) -> tuple[str, dict[str, Any]] | None:
    placeholders = ",".join("?" for _ in _TERMINAL_EXECUTION_EVENTS)
    with storage._lock:
        rows = storage.conn.execute(
            f"SELECT kind, payload FROM events WHERE kind IN ({placeholders}) "
            "AND payload LIKE ? ORDER BY id DESC LIMIT 20",
            (*sorted(_TERMINAL_EXECUTION_EVENTS), f"%{proposal_id}%"),
        ).fetchall()
    for kind, raw in rows:
        try:
            payload = json.loads(str(raw))
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict) and payload.get("proposal_id") == proposal_id:
            return str(kind), payload
    return None


def _execution_state_for_event(kind: str) -> str:
    return {
        "direct_proposal_execution_rejected": "REJECTED",
        "direct_proposal_execution_blocked": "BLOCKED",
        "direct_proposal_execution_error": "ERROR",
        "direct_proposal_execution_shadow": "SHADOW_EXECUTED",
        "direct_proposal_execution_real": "REAL_EXECUTION",
    }.get(kind, kind.upper())


def _event_reason(payload: dict[str, Any]) -> str | None:
    reasons = payload.get("reasons")
    if isinstance(reasons, list) and reasons:
        return " · ".join(str(item) for item in reasons)
    gate = payload.get("gate")
    if isinstance(gate, dict):
        gate_reasons = gate.get("reasons")
        if isinstance(gate_reasons, list) and gate_reasons:
            return " · ".join(str(item) for item in gate_reasons)
    return None


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
