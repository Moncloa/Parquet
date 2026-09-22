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
    ][:50]
    watch_history = _watch_history(storage, analyses=analyses, limit=100)
    watch_events = _recent_watch_events(storage, limit=100)
    timeline = _timeline_events(
        analyses=analyses,
        review_requests=review_requests,
        decisions=decisions,
        positions=positions,
        watch_history=watch_history,
        watch_events=watch_events,
    )

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
            "operational": {
                "enabled": _operational_controls_enabled(settings),
                "mode": settings.execution.autonomous_mode,
            },
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
        "watch_history": watch_history,
        "watch_events": watch_events,
        "timeline": timeline[:150],
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
                "at": _iso_timestamp(created_at),
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


def _operational_controls_enabled(settings: Settings) -> bool:
    execution = settings.execution
    if not execution.autonomous_enabled:
        return False
    if execution.autonomous_mode == "real":
        return settings.mode.lower() == "real" and execution.autonomous_real_enabled
    if execution.autonomous_mode == "demo":
        return execution.autonomous_demo_enabled
    return execution.autonomous_mode == "shadow"



def _watch_history(
    storage: Any,
    *,
    analyses: list[MarketAnalysis],
    limit: int,
) -> list[dict[str, Any]]:
    analysis_times = {
        analysis.analysis_id: analysis.generated_at.astimezone(UTC).isoformat()
        for analysis in analyses
    }
    with storage._lock:
        rows = storage.conn.execute(
            "SELECT watch_id, analysis_id, status, payload "
            "FROM watches ORDER BY rowid DESC LIMIT ?",
            (limit,),
        ).fetchall()

    result: list[dict[str, Any]] = []
    for watch_id, analysis_id, status, raw_payload in rows:
        try:
            payload = json.loads(str(raw_payload))
        except json.JSONDecodeError:
            payload = {}
        if not isinstance(payload, dict):
            payload = {}
        payload = dict(payload)
        payload["watch_id"] = str(watch_id)
        payload["analysis_id"] = str(analysis_id)
        payload["status"] = str(status)
        payload["created_at"] = analysis_times.get(str(analysis_id))
        result.append(payload)
    return result


def _recent_watch_events(storage: Any, *, limit: int) -> list[dict[str, Any]]:
    with storage._lock:
        rows = storage.conn.execute(
            "SELECT created_at, payload FROM events WHERE kind = 'watch_event' "
            "ORDER BY id DESC LIMIT ?",
            (limit,),
        ).fetchall()

    result: list[dict[str, Any]] = []
    for created_at, raw_payload in rows:
        try:
            payload = json.loads(str(raw_payload))
        except json.JSONDecodeError:
            payload = {}
        if not isinstance(payload, dict):
            payload = {}
        item = dict(payload)
        item["at"] = _iso_timestamp(payload.get("observed_at") or created_at)
        result.append(item)
    return result


def _timeline_events(
    *,
    analyses: list[MarketAnalysis],
    review_requests: dict[str, dict[str, Any]],
    decisions: list[dict[str, Any]],
    positions: list[Any],
    watch_history: list[dict[str, Any]],
    watch_events: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []

    for request_id, request in review_requests.items():
        requested_at = request.get("requested_at")
        if requested_at is None:
            continue
        items.append(
            {
                "id": f"review-request:{request_id}",
                "at": _iso_timestamp(requested_at),
                "family": "review",
                "event": "REVIEW_REQUESTED",
                "title": "Revisión solicitada",
                "symbol": None,
                "summary": str(request.get("reason") or "review"),
                "detail": str(request_id),
            }
        )

    for analysis in analyses:
        request = review_requests.get(analysis.review_request_id or "", {})
        proposal_symbols = ", ".join(
            proposal.symbol for proposal in analysis.trade_proposals
        )
        items.append(
            {
                "id": f"review:{analysis.analysis_id}",
                "at": analysis.generated_at.astimezone(UTC).isoformat(),
                "family": "review",
                "event": "REVIEW_COMPLETED",
                "title": "Revisión completada",
                "symbol": None,
                "summary": analysis.summary or "Sin resumen",
                "detail": (
                    f"{request.get('reason') or analysis.market_regime}"
                    + (f" · propuestas: {proposal_symbols}" if proposal_symbols else "")
                ),
                "analysis_id": analysis.analysis_id,
            }
        )

    for decision in decisions:
        outcome = str(decision.get("outcome") or "UNKNOWN")
        reasons = decision.get("reasons")
        reason_text = (
            " · ".join(str(reason) for reason in reasons if reason)
            if isinstance(reasons, list)
            else ""
        )
        items.append(
            {
                "id": (
                    "decision:"
                    + str(
                        decision.get("proposal_id")
                        or decision.get("analysis_id")
                        or decision.get("at")
                    )
                    + ":"
                    + outcome
                ),
                "at": _iso_timestamp(decision.get("at")),
                "family": "decision",
                "event": outcome,
                "title": "Decisión",
                "symbol": decision.get("symbol"),
                "summary": reason_text or outcome,
                "detail": str(
                    decision.get("proposal_id")
                    or decision.get("analysis_id")
                    or ""
                ),
                "gate": decision.get("gate") or {},
                "preflight": decision.get("preflight") or {},
            }
        )

    for watch in watch_history:
        created_at = watch.get("created_at")
        if created_at is None:
            continue
        trigger = watch.get("trigger")
        trigger_payload = trigger if isinstance(trigger, dict) else {}
        trigger_text = (
            f"{trigger_payload.get('type') or 'trigger'} "
            f"{trigger_payload.get('price') or ''}"
        ).strip()
        items.append(
            {
                "id": f"watch-created:{watch.get('watch_id')}",
                "at": _iso_timestamp(created_at),
                "family": "watch",
                "event": "WATCH_CREATED",
                "title": "Watch creado",
                "symbol": watch.get("symbol"),
                "summary": str(watch.get("rationale") or trigger_text or "Watch"),
                "detail": (
                    f"{trigger_text} → {watch.get('on_trigger') or 'REASSESS'}"
                ),
            }
        )

    for event in watch_events:
        event_name = str(event.get("event") or "WATCH_EVENT")
        items.append(
            {
                "id": (
                    f"watch-event:{event.get('watch_id')}:"
                    f"{event_name}:{event.get('at')}"
                ),
                "at": _iso_timestamp(event.get("at")),
                "family": "watch",
                "event": event_name,
                "title": "Watch " + event_name.lower().replace("_", " "),
                "symbol": event.get("symbol"),
                "summary": str(event.get("reason") or event_name),
                "detail": (
                    f"price {event.get('observed_price')}"
                    + (
                        f" → {event.get('action')}"
                        if event.get("action") is not None
                        else ""
                    )
                ),
            }
        )

    for position in positions:
        opened = position.opened_at.astimezone(UTC).isoformat()
        items.append(
            {
                "id": f"position-open:{position.local_id}",
                "at": opened,
                "family": "position_open",
                "event": "POSITION_OPENED",
                "title": "Posición abierta",
                "symbol": position.symbol,
                "summary": (
                    f"{position.side.upper()} · capital "
                    f"{position.amount_usd:,.2f} USD"
                ),
                "detail": (
                    f"open {position.open_rate if position.open_rate is not None else '—'}"
                    f" · SL {position.stop_loss_rate if position.stop_loss_rate is not None else '—'}"
                    f" · TP {position.take_profit_rate if position.take_profit_rate is not None else '—'}"
                ),
                "position_id": position.broker_position_id,
            }
        )
        if position.closed_at is not None:
            pnl = (
                "—"
                if position.realized_pnl_usd is None
                else f"{position.realized_pnl_usd:+,.2f} USD"
            )
            items.append(
                {
                    "id": f"position-close:{position.local_id}",
                    "at": position.closed_at.astimezone(UTC).isoformat(),
                    "family": "position_close",
                    "event": "POSITION_CLOSED",
                    "title": "Posición cerrada",
                    "symbol": position.symbol,
                    "summary": f"{position.side.upper()} · P/L {pnl}",
                    "detail": (
                        "resultado estimado"
                        if position.pnl_estimated
                        else "resultado registrado"
                    ),
                    "position_id": position.broker_position_id,
                }
            )

    return sorted(
        items,
        key=lambda item: _timeline_sort_key(item.get("at")),
        reverse=True,
    )


def _timeline_sort_key(value: object) -> datetime:
    text_value = str(value or "")
    try:
        parsed = datetime.fromisoformat(text_value.replace("Z", "+00:00"))
    except ValueError:
        return datetime.min.replace(tzinfo=UTC)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _iso_timestamp(value: object) -> str:
    if value is None:
        return ""
    parsed = _timeline_sort_key(value)
    if parsed == datetime.min.replace(tzinfo=UTC):
        return str(value)
    return parsed.isoformat()
