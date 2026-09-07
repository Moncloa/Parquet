from __future__ import annotations

import argparse
import asyncio
import threading
from pathlib import Path

import uvicorn

from parquet.api import create_app
from parquet.autonomous_orchestrator import AutonomousOrchestrator
from parquet.config import load_settings
from parquet.reconciliation import ReconciliationService, run_with_reconciliation


def validate_keys(settings_path: Path | None) -> int:
    settings = load_settings(settings_path)
    key_paths = (settings.keys.private_key, settings.keys.public_key)
    missing = [path for path in key_paths if not path.exists()]
    if missing:
        print("Missing key files:")
        for path in missing:
            print(f"  - {path}")
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

    def worker() -> None:
        asyncio.run(run_with_reconciliation(orchestrator, reconciliation))

    threading.Thread(target=worker, name="parquet-orchestrator", daemon=True).start()
    uvicorn.run(create_app(settings, orchestrator), host=settings.host, port=settings.port)


def main() -> None:
    parser = argparse.ArgumentParser(prog="parquet")
    parser.add_argument("--config", type=Path, default=None)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("serve")
    sub.add_parser("validate")
    sub.add_parser("once")
    args = parser.parse_args()

    if args.command == "validate":
        raise SystemExit(validate_keys(args.config))
    if args.command == "serve":
        serve(args.config)
        return
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
