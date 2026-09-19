from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from importlib.metadata import PackageNotFoundError, version
from typing import Any

from parquet.config import Settings
from parquet.controls import latest_manual_review_status
from parquet.dashboard import position_payload
from parquet.models import MarketAnalysis
from parquet.strategy import StrategyQueue

_DECISION_KINDS = (
    "direct_proposal_execution_rejected",
    "direct_proposal_execution_blocked",
    "direct_proposal_execution_error",
    "direct_proposal_execution_shadow",
    "direct_proposal_execution_demo_pending",
    "direct_proposal_execution_real",
)

_OUTCOME_BY_KIND = {
    "direct_proposal_execution_rejected": "REJECTED",
    "direct_proposal_execution_blocked": "BLOCKED",
    "direct_proposal_execution_error": "ERROR",
    "direct_proposal_execution_shadow": "SHADOW_EXECUTED",
    "direct_proposal_execution_demo_pending": "DEMO_PENDING",
    "direct_proposal_execution_real": "REAL_EXECUTION",
}


def build_operations_snapshot(settings: Settings, orchestrator: Any) -> dict[str, Any]:
    storage = orchestrator.storage
    analyses = _recent_analyses(storage, limit=10)
    review_requests = _recent_review_requests(storage, limit=100)
    decisions = _recent_decisions(storage, limit=30)
    decisions.extend(_no_trade_decisions(analyses, review_requests))
    decisions.sort(key=lambda item: str(item.get("at") or ""), reverse=True)

    positions = storage.managed_positions()
    open_positions = [position_payload(item) for item in positions if item.status == "OPEN"]
    closed_positions = [
        position_payload(item) for item in positions if item.status != "OPEN"
    ][:20]

    risk = storage.get_risk_snapshot()
    broker = storage.get_broker_portfolio_snapshot()
    reconciliation = storage.get_reconciliation_report()
    watches = storage.active_watches()
    attempts = storage.latest_execution_attempts(limit=20)

    worker: dict[str, Any] = {}
    pending_strategy_requests = 0
    try:
        queue = StrategyQueue(settings.strategy.queue_dir)
        worker = queue.worker_status() or {}
        pending_strategy_requests = queue.pending_count()
    except OSError:
        worker = {}

    closed_pnl_values = [
        item.realized_pnl_usd
        for item in positions
        if item.status != "OPEN" and item.realized_pnl_usd is not None
    ]
    closed_pnl_estimated = any(
        item.pnl_estimated for item in positions if item.status != "OPEN"
    )

    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "runtime": {
            "version": _package_version(),
            "branch": os.getenv("PARQUET_RUNTIME_BRANCH", "unknown"),
            "commit": os.getenv("PARQUET_RUNTIME_COMMIT", "unknown"),
            "mode": settings.mode,
            "execution_mode": settings.execution.autonomous_mode,
            "strategy_provider": settings.strategy.provider,
            "position_min_pct": settings.execution.autonomous_real_min_position_pct,
            "position_max_pct": settings.execution.autonomous_real_max_position_pct,
            "max_leverage": settings.execution.autonomous_real_max_leverage,
        },
        "overview": {
            "equity_usd": None if risk is None else risk.equity_usd,
            "daily_pnl_pct": None if risk is None else risk.daily_pnl_pct,
            "weekly_pnl_pct": None if risk is None else risk.weekly_pnl_pct,
            "open_positions": len(open_positions),
            "available_cash_usd": None if broker is None else broker.available_cash_usd,
            "invested_usd": None if broker is None else broker.invested_usd,
            "unrealized_pnl_usd": None if broker is None else broker.unrealized_pnl_usd,
            "managed_closed_pnl_usd": (
                None if not closed_pnl_values else sum(closed_pnl_values)
            ),
            "managed_closed_pnl_estimated": closed_pnl_estimated,
            "commission_status": "exact_closed_trade_costs_not_available",
        },
        "controls": {
            "providers": (
                worker.get("providers")
                if isinstance(worker.get("providers"), dict)
                else {}
            ),
            "latest_review": latest_manual_review_status(
                storage,
                settings.strategy.queue_dir,
            ),
        },
        "pending_reviews": [
            {
                "at": review.at.isoformat(),
                "reason": review.reason,
                "source": review.source,
            }
            for review in orchestrator.reviews.pending()
        ],
        "reviews": [
            _analysis_payload(analysis, review_requests.get(analysis.review_request_id or ""))
            for analysis in analyses
        ],
        "decisions": decisions[:30],
        "positions": {
            "open": open_positions,
            "closed": closed_positions,
        },
        "watches": [watch.model_dump(mode="json") for watch in watches],
        "attempts": [attempt.model_dump(mode="json") for attempt in attempts],
        "system": {
            "reconciliation_state": None if reconciliation is None else reconciliation.state.value,
            "reconciliation_as_of": (
                None if reconciliation is None else reconciliation.as_of.isoformat()
            ),
            "reconciliation_issues": (
                []
                if reconciliation is None
                else [issue.model_dump(mode="json") for issue in reconciliation.issues]
            ),
            "identity_verified": storage.get("etoro_identity_verified") == "1",
            "execution_uncertain": storage.get("execution_uncertain") == "1",
            "strategy_enabled": settings.strategy.enabled,
            "strategy_pending_requests": pending_strategy_requests,
            "strategy_worker": worker,
            "strategy_last_success_at": storage.get("strategy_last_success_at"),
            "strategy_last_error": storage.get("strategy_last_error") or None,
            "local_screener_last_at": storage.get("local_screener_last_at"),
            "local_screener_last_error": storage.get("local_screener_last_error") or None,
            "websocket_last_message_at": storage.get("websocket_last_message_at"),
            "websocket_last_error": storage.get("websocket_last_error") or None,
        },
    }


def _recent_analyses(storage: Any, *, limit: int) -> list[MarketAnalysis]:
    with storage._lock:
        rows = storage.conn.execute(
            "SELECT payload FROM analyses ORDER BY generated_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
    analyses: list[MarketAnalysis] = []
    for row in rows:
        try:
            analyses.append(MarketAnalysis.model_validate_json(str(row[0])))
        except Exception:
            continue
    return analyses


def _recent_review_requests(storage: Any, *, limit: int) -> dict[str, dict[str, Any]]:
    with storage._lock:
        rows = storage.conn.execute(
            "SELECT payload FROM events WHERE kind = 'review_request' "
            "ORDER BY id DESC LIMIT ?",
            (limit,),
        ).fetchall()
    requests: dict[str, dict[str, Any]] = {}
    for row in rows:
        try:
            payload = json.loads(str(row[0]))
        except json.JSONDecodeError:
            continue
        if not isinstance(payload, dict):
            continue
        request_id = payload.get("request_id")
        if isinstance(request_id, str) and request_id:
            requests[request_id] = payload
    return requests


def _analysis_payload(
    analysis: MarketAnalysis,
    request: dict[str, Any] | None,
) -> dict[str, Any]:
    request = request or {}
    return {
        "analysis_id": analysis.analysis_id,
        "review_request_id": analysis.review_request_id,
        "generated_at": analysis.generated_at.isoformat(),
        "reason": request.get("reason"),
        "market_regime": analysis.market_regime,
        "summary": analysis.summary,
        "sources": list(analysis.sources),
        "no_trade": not analysis.trade_proposals,
        "trade_proposals": [
            proposal.model_dump(mode="json") for proposal in analysis.trade_proposals
        ],
        "watch": [watch.model_dump(mode="json") for watch in analysis.watch],
        "next_review": (
            None
            if analysis.next_review is None
            else analysis.next_review.model_dump(mode="json")
        ),
    }


def _recent_decisions(storage: Any, *, limit: int) -> list[dict[str, Any]]:
    placeholders = ",".join("?" for _ in _DECISION_KINDS)
    with storage._lock:
        rows = storage.conn.execute(
            f"SELECT created_at, kind, payload FROM events "
            f"WHERE kind IN ({placeholders}) ORDER BY id DESC LIMIT ?",
            (*_DECISION_KINDS, limit),
        ).fetchall()

    decisions: list[dict[str, Any]] = []
    for created_at, kind, raw_payload in rows:
        try:
            payload = json.loads(str(raw_payload))
        except json.JSONDecodeError:
            payload = {}
        if not isinstance(payload, dict):
            payload = {}

        gate = payload.get("gate")
        gate_payload = gate if isinstance(gate, dict) else {}
        reasons = payload.get("reasons")
        if not isinstance(reasons, list):
            reasons = gate_payload.get("reasons")
        normalized_reasons = (
            [str(item) for item in reasons]
            if isinstance(reasons, list)
            else []
        )
        attempt = payload.get("attempt")
        attempt_payload = attempt if isinstance(attempt, dict) else {}
        preflight = payload.get("real_preflight")
        preflight_payload = preflight if isinstance(preflight, dict) else {}

        decisions.append(
            {
                "at": str(created_at),
                "kind": str(kind),
                "outcome": _OUTCOME_BY_KIND.get(str(kind), str(kind).upper()),
                "symbol": payload.get("symbol") or attempt_payload.get("symbol"),
                "proposal_id": payload.get("proposal_id") or attempt_payload.get("proposal_id"),
                "reasons": normalized_reasons,
                "gate": gate_payload,
                "attempt": attempt_payload,
                "preflight": preflight_payload,
            }
        )
    return decisions


def _no_trade_decisions(
    analyses: list[MarketAnalysis],
    review_requests: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for analysis in analyses:
        if analysis.trade_proposals:
            continue
        request = review_requests.get(analysis.review_request_id or "", {})
        result.append(
            {
                "at": analysis.generated_at.isoformat(),
                "kind": "strategy_no_trade",
                "outcome": "NO_TRADE",
                "symbol": None,
                "proposal_id": None,
                "analysis_id": analysis.analysis_id,
                "reason": request.get("reason"),
                "reasons": [analysis.summary] if analysis.summary else [],
                "gate": {},
                "attempt": {},
                "preflight": {},
            }
        )
    return result


def _package_version() -> str:
    try:
        return version("parquet-trader")
    except PackageNotFoundError:
        return "dev"
