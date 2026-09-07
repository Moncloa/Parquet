from __future__ import annotations

import argparse
import asyncio
import threading
from pathlib import Path

import uvicorn

from parquet.api import create_app
from parquet.autonomous_orchestrator import AutonomousOrchestrator
from parquet.config import Settings, load_settings
from parquet.execution.etoro import EtoroExecutionClient
from parquet.execution.supervised import RealSmallExecutionAdapter
from parquet.reconciliation import ReconciliationService, run_with_reconciliation


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
