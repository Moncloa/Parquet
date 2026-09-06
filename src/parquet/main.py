from __future__ import annotations

import argparse
import asyncio
import threading
from pathlib import Path

import uvicorn

from parquet.api import create_app
from parquet.config import load_settings
from parquet.orchestrator import Orchestrator


def validate_keys(settings_path: Path | None) -> int:
    settings = load_settings(settings_path)
    missing = [path for path in (settings.keys.private_key, settings.keys.public_key) if not path.exists()]
    if missing:
        print("Missing key files:")
        for path in missing:
            print(f"  - {path}")
        return 2
    print("Configuration and key paths are valid")
    return 0


def serve(settings_path: Path | None) -> None:
    settings = load_settings(settings_path)
    orchestrator = Orchestrator(settings)

    def worker() -> None:
        asyncio.run(orchestrator.run_forever())

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
        orchestrator = Orchestrator(settings)
        processed = asyncio.run(orchestrator.poll_github_once())
        print(f"Processed {processed} new analysis comment(s)")
        return
