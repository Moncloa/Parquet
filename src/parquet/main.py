from __future__ import annotations

import argparse
import asyncio
import threading
from datetime import UTC, datetime
from pathlib import Path

import uvicorn

from parquet.api import create_app
from parquet.config import Settings, load_settings
from parquet.enhanced_orchestrator import AutonomousOrchestrator
from parquet.execution.etoro import EtoroExecutionClient
from parquet.execution.supervised import RealSmallExecutionAdapter
from parquet.reconciliation import ReconciliationService, run_with_reconciliation
from parquet.scheduler import ScheduledReview
from parquet.storage import Storage
from parquet.tickets import prepare_real_small_ticket, recent_proposals


def validate_keys(settings_path: Path | None) -> int:
    settings = load_settings(settings_path)
    key_paths = [settings.keys.private_key, settings.keys.public_key]
    if settings.etoro.enabled:
        key_paths.extend([settings.etoro.api_key_file, settings.etoro.user_key_file])
    missing = [path for path in key_paths if not path.exists()]
    if missing:
        print("Missing key files:")
        for path in missing:
            print(f"  - {path}")
        return 2
    if settings.execution.supervised_real_enabled and settings.etoro.expected_gcid is None:
        print("Invalid configuration: supervised real execution requires etoro.expected_gcid")
        return 2
    print("Configuration and key paths are valid")
    return 0


def serve(settings_path: Path | None) -> None:
    settings = load_settings(settings_path)
    orchestrator = AutonomousOrchestrator(settings)
    reconciliation = ReconciliationService(
        settings,
        orchestrator.storage,
        orchestrator.market_client,
    )

    # Populate identity and reconciliation state before exposing /health. Failure is
    # persisted as a blocked reconciliation state; it does not prevent diagnostics
    # from starting.
    asyncio.run(reconciliation.poll_once(force=True))

    def worker() -> None:
        asyncio.run(run_with_reconciliation(orchestrator, reconciliation))

    threading.Thread(target=worker, name="parquet-orchestrator", daemon=True).start()
    uvicorn.run(create_app(settings, orchestrator), host=settings.host, port=settings.port)


def _execution_client(settings_path: Path | None) -> tuple[Settings, EtoroExecutionClient]:
    settings = load_settings(settings_path)
    client = EtoroExecutionClient(
        api_key=_read_secret(settings.etoro.api_key_file, "eToro API key"),
        user_key=_read_secret(settings.etoro.user_key_file, "eToro User key"),
        base_url=settings.etoro.execution_base_url,
        identity_base_url=settings.etoro.base_url,
    )
    return settings, client


def run_etoro_check(settings_path: Path | None) -> int:
    settings, client = _execution_client(settings_path)
    identity = asyncio.run(client.identity())

    print("eToro authenticated identity")
    print(f"  gcid: {identity.gcid}")
    print(f"  real_cid: {identity.real_cid}")
    print(f"  demo_cid: {identity.demo_cid}")
    print("  scopes:")
    for scope in sorted(identity.scopes):
        print(f"    - {scope}")

    expected_gcid = settings.etoro.expected_gcid
    if expected_gcid is None:
        print("FAIL: etoro.expected_gcid is not configured")
        return 3
    if identity.gcid != expected_gcid:
        print(f"FAIL: authenticated GCID {identity.gcid} != expected {expected_gcid}")
        return 4
    missing = sorted(set(settings.etoro.required_real_scopes) - set(identity.scopes))
    if missing:
        print("FAIL: missing required scopes:")
        for scope in missing:
            print(f"  - {scope}")
        return 5
    print("OK: Agent Portfolio identity and scopes match configuration")
    return 0


def run_proposals(settings_path: Path | None, limit: int) -> int:
    settings = load_settings(settings_path)
    storage = Storage(settings.state_db)
    records = recent_proposals(storage, limit=limit)
    if not records:
        print("No trade proposals stored")
        return 0

    now = datetime.now(UTC)
    print("Recent trade proposals")
    for record in records:
        proposal = record.proposal
        state = "ACTIVE" if proposal.expires_at.astimezone(UTC) > now else "EXPIRED"
        print(
            f"{proposal.proposal_id}  {state}  {proposal.symbol} {proposal.side.value}  "
            f"entry={proposal.entry}  SL={proposal.stop_loss}  TP={proposal.take_profit}  "
            f"expires={proposal.expires_at.astimezone(UTC).isoformat()}  "
            f"analysis={record.analysis_id}"
        )
    return 0


def run_request_review_now(settings_path: Path | None, reason: str) -> int:
    normalized_reason = reason.strip()
    if not normalized_reason:
        raise RuntimeError("Manual review reason must not be empty")

    settings = load_settings(settings_path)
    orchestrator = AutonomousOrchestrator(settings)
    if orchestrator.bridge is None:
        raise RuntimeError("Cannot request review now: GitHub bridge is disabled")

    reconciliation = ReconciliationService(
        settings,
        orchestrator.storage,
        orchestrator.market_client,
    )

    async def request() -> tuple[int, datetime]:
        await reconciliation.poll_once(force=True)
        report = orchestrator.storage.get_reconciliation_report()
        if report is None or not report.trading_enabled:
            state = None if report is None else report.state.value
            raise RuntimeError(
                f"Cannot request review now: broker reconciliation is not ready ({state})"
            )
        if orchestrator.storage.get("etoro_identity_verified") != "1":
            raise RuntimeError("Cannot request review now: eToro identity is not verified")

        current = datetime.now(UTC)
        orchestrator.add_review(
            ScheduledReview(
                at=current,
                reason=normalized_reason,
                source="manual",
            )
        )
        posted = await orchestrator.post_due_reviews(now=current)
        return posted, current

    posted, requested_at = asyncio.run(request())
    if posted != 1:
        raise RuntimeError(f"Expected one manual review request to be posted, got {posted}")

    print("Manual review request posted")
    print(f"  reason: {normalized_reason}")
    print(f"  requested_at: {requested_at.isoformat()}")
    print(f"  repository: {settings.github.repository}")
    print(f"  runtime_pr: {settings.github.runtime_pr}")
    print("Current eToro market context and risk snapshot were attached.")
    print("No broker order was sent.")
    return 0


def run_prepare_real_small(
    settings_path: Path | None,
    proposal_id: str,
    amount_usd: float | None,
) -> int:
    settings = load_settings(settings_path)
    ticket = asyncio.run(
        prepare_real_small_ticket(
            settings,
            proposal_id=proposal_id,
            requested_amount_usd=amount_usd,
        )
    )
    attempt = ticket.attempt
    decision = ticket.gate_decision
    observation = ticket.observation

    print("Prepared supervised real-small ticket -- NO BROKER ORDER SENT")
    print(f"  attempt: {attempt.attempt_id}")
    print(f"  proposal: {attempt.proposal_id}")
    print(f"  symbol: {attempt.symbol}")
    print(f"  instrument_id: {attempt.instrument_id}")
    print(f"  side: {attempt.side}")
    print(f"  quote_bid: {observation.bid}")
    print(f"  quote_ask: {observation.ask}")
    print(f"  quote_at: {observation.observed_at.astimezone(UTC).isoformat()}")
    print(f"  broker_minimum_usd: {ticket.broker_minimum_usd}")
    print(f"  gate_maximum_usd: {decision.amount_usd}")
    print(f"  supervised_safe_maximum_usd: {ticket.maximum_safe_amount_usd:.2f}")
    print(f"  prepared_amount_usd: {attempt.amount_usd:.2f}")
    print(f"  stop_loss: {attempt.stop_loss}")
    print(f"  take_profit: {attempt.take_profit}")
    print(f"  spread_bps: {decision.spread_bps}")
    print(f"  adverse_slippage_bps: {decision.adverse_slippage_bps}")
    print("No execution POST has been sent.")
    return 0


def run_real_small(settings_path: Path | None, attempt_id: str) -> int:
    settings = load_settings(settings_path)
    orchestrator = AutonomousOrchestrator(settings)
    attempt = orchestrator.storage.get_execution_attempt(attempt_id)
    if attempt is None:
        print(f"Execution attempt not found: {attempt_id}")
        return 2

    print("Supervised real-money execution ticket")
    print(f"  attempt: {attempt.attempt_id}")
    print(f"  symbol: {attempt.symbol}")
    print(f"  side: {attempt.side}")
    print(f"  amount_usd: {attempt.amount_usd:.2f}")
    print(f"  stop_loss: {attempt.stop_loss}")
    print(f"  take_profit: {attempt.take_profit}")
    expected = f"REAL {attempt.attempt_id}"
    confirmation = input(f"Type exactly '{expected}' to submit: ").strip()

    reconciliation = ReconciliationService(
        settings,
        orchestrator.storage,
        orchestrator.market_client,
    )
    _, client = _execution_client(settings_path)
    adapter = RealSmallExecutionAdapter(
        settings=settings,
        storage=orchestrator.storage,
        position_manager=reconciliation.position_manager,
        reconciliation=reconciliation,
        client=client,
    )
    result = asyncio.run(adapter.execute(attempt, confirmation=confirmation))
    print(
        f"Execution attempt {result.attempt_id}: {result.state.value}; "
        f"request={result.broker_request_id}; order={result.broker_order_id}; "
        f"position={result.broker_position_id}"
    )
    return 0 if result.state.value == "RECONCILED" else 3


def main() -> None:
    parser = argparse.ArgumentParser(prog="parquet")
    parser.add_argument("--config", type=Path, default=None)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("serve")
    sub.add_parser("validate")
    sub.add_parser("once")
    sub.add_parser("etoro-check")

    proposals = sub.add_parser("proposals")
    proposals.add_argument("--limit", type=int, default=20)

    request_review_now = sub.add_parser("request-review-now")
    request_review_now.add_argument(
        "--reason",
        default="manual_opportunity_scan",
        help="Reason recorded in the Parquet review request",
    )

    prepare_real_small = sub.add_parser("prepare-real-small")
    prepare_real_small.add_argument("proposal_id")
    prepare_real_small.add_argument("--amount", type=float, default=None)

    real_small = sub.add_parser("real-small")
    real_small.add_argument("attempt_id")
    args = parser.parse_args()

    if args.command == "validate":
        raise SystemExit(validate_keys(args.config))
    if args.command == "serve":
        serve(args.config)
        return
    if args.command == "etoro-check":
        raise SystemExit(run_etoro_check(args.config))
    if args.command == "proposals":
        raise SystemExit(run_proposals(args.config, int(args.limit)))
    if args.command == "request-review-now":
        raise SystemExit(run_request_review_now(args.config, str(args.reason)))
    if args.command == "prepare-real-small":
        amount = None if args.amount is None else float(args.amount)
        raise SystemExit(
            run_prepare_real_small(args.config, str(args.proposal_id), amount)
        )
    if args.command == "real-small":
        raise SystemExit(run_real_small(args.config, str(args.attempt_id)))
    if args.command == "once":
        settings = load_settings(args.config)
        orchestrator = AutonomousOrchestrator(settings)
        reconciliation = ReconciliationService(
            settings,
            orchestrator.storage,
            orchestrator.market_client,
        )

        async def run_once() -> tuple[int, int]:
            reconciled = await reconciliation.poll_once(force=True)
            processed = await orchestrator.poll_github_once()
            return reconciled, processed

        reconciled, processed = asyncio.run(run_once())
        print(
            f"Reconciled broker state: {reconciled}; "
            f"processed {processed} new analysis comment(s)"
        )
        return


def _read_secret(path: Path, label: str) -> str:
    try:
        value = path.read_text(encoding="utf-8").strip()
    except FileNotFoundError as exc:
        raise RuntimeError(f"Missing {label} file: {path}") from exc
    if not value:
        raise RuntimeError(f"Empty {label} file: {path}")
    return value
