from __future__ import annotations

import argparse
import asyncio
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

import httpx

from parquet.autonomous_orchestrator import AutonomousOrchestrator
from parquet.config import Settings, load_settings
from parquet.execution.autonomous import ExecutionAttemptState
from parquet.execution.etoro import EtoroExecutionClient
from parquet.execution.supervised import RealSmallExecutionAdapter
from parquet.models import MarketObservation, Side, TradeProposal
from parquet.reconciliation import ReconciliationService
from parquet.risk_ledger import LocalEquityRiskLedger
from parquet.storage import Storage

_MAX_REAL_FRACTION = 0.30
_MAX_VIRTUAL_EXPOSURE_PCT = 150.0
_EXPECTED_STOCK_CFD_FEE_PER_SIDE = 0.0015


def _read_secret(path: Path, label: str) -> str:
    value = path.read_text(encoding="utf-8").strip()
    if not value:
        raise RuntimeError(f"Empty {label}: {path}")
    return value


def _execution_client(settings: Settings) -> EtoroExecutionClient:
    return EtoroExecutionClient(
        api_key=_read_secret(settings.etoro.api_key_file, "eToro API key"),
        user_key=_read_secret(settings.etoro.user_key_file, "eToro User key"),
        base_url=settings.etoro.execution_base_url,
        identity_base_url=settings.etoro.base_url,
    )


def _parse_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timestamp must include timezone")
    return parsed.astimezone(UTC)


def seed_baseline(
    settings: Settings,
    *,
    period: str,
    boundary: datetime,
    equity_usd: float,
    mode: str,
    source: str,
) -> None:
    storage = Storage(settings.state_db)
    ledger = LocalEquityRiskLedger(storage)
    baseline = ledger.seed_manual_baseline(
        period=period,
        boundary=boundary,
        equity_usd=equity_usd,
        mode=mode,
        source=source,
    )
    print("Manual risk baseline stored")
    print(f"  period: {period}")
    print(f"  boundary: {baseline.boundary.isoformat()}")
    print(f"  equity_usd: {baseline.equity_usd:.5f}")
    print(f"  mode: {baseline.mode}")
    print(f"  source: {baseline.source}")


async def _resolve_instrument(orchestrator: AutonomousOrchestrator, symbol: str) -> tuple[int, Any]:
    market = orchestrator.market_client
    if market is None:
        raise RuntimeError("eToro market client unavailable")
    normalized = symbol.upper()
    configured = orchestrator.settings.etoro.instrument_ids.get(normalized)
    if configured is not None:
        instrument_id = configured
    else:
        hits = await market.search(normalized)
        exact = sorted(
            {
                hit.instrument_id
                for hit in hits
                if hit.symbol is not None and hit.symbol.upper() == normalized
            }
        )
        if len(exact) != 1:
            raise RuntimeError(
                f"Expected one exact eToro instrument for {normalized}, found {len(exact)}"
            )
        instrument_id = exact[0]
    rates = await market.rates([instrument_id])
    matching = [rate for rate in rates if rate.instrument_id == instrument_id]
    if len(matching) != 1 or matching[0].bid is None or matching[0].ask is None:
        raise RuntimeError(f"No unique bid/ask for {normalized} ({instrument_id})")
    return instrument_id, matching[0]


async def prepare_experiment(
    settings: Settings,
    *,
    symbol: str,
    real_eur: float,
    eurusd: float,
    funded_usd: float,
    leverage: int,
    stop_pct: float,
) -> None:
    if symbol.upper() != "AAPL":
        raise RuntimeError("This controlled experiment is intentionally restricted to AAPL")
    if real_eur <= 0 or eurusd <= 0 or funded_usd <= 0:
        raise RuntimeError("real_eur, eurusd and funded_usd must be positive")
    if leverage != 5:
        raise RuntimeError("This controlled experiment is intentionally fixed at x5")
    if not 0 < stop_pct <= 2.0:
        raise RuntimeError("stop_pct must be >0 and <=2.0 for this experiment")

    orchestrator = AutonomousOrchestrator(settings)
    reconciliation = ReconciliationService(
        settings,
        orchestrator.storage,
        orchestrator.market_client,
    )
    await reconciliation.poll_once(force=True)
    reconciliation.position_manager.assert_trading_enabled()
    if orchestrator.storage.get("etoro_identity_verified") != "1":
        raise RuntimeError("eToro Agent Portfolio identity is not verified")
    if orchestrator.storage.get("execution_uncertain") == "1":
        raise RuntimeError("Unresolved broker outcome blocks the experiment")

    snapshot = orchestrator.storage.get_risk_snapshot()
    if snapshot is None or snapshot.equity_usd is None:
        raise RuntimeError("Fresh broker risk snapshot unavailable")
    if snapshot.open_positions != 0:
        raise RuntimeError("Experiment requires a flat Agent Portfolio before opening")

    real_usd = real_eur * eurusd
    real_fraction = real_usd / funded_usd
    if real_fraction > _MAX_REAL_FRACTION:
        raise RuntimeError(
            f"Target uses {real_fraction:.2%} of funded capital; experiment cap is "
            f"{_MAX_REAL_FRACTION:.0%}"
        )
    virtual_margin_usd = snapshot.equity_usd * real_fraction
    virtual_exposure_usd = virtual_margin_usd * leverage
    virtual_exposure_pct = virtual_exposure_usd / snapshot.equity_usd * 100.0
    if virtual_exposure_pct > _MAX_VIRTUAL_EXPOSURE_PCT:
        raise RuntimeError(
            f"Virtual exposure {virtual_exposure_pct:.2f}% exceeds experiment cap "
            f"{_MAX_VIRTUAL_EXPOSURE_PCT:.0f}%"
        )

    instrument_id, rate = await _resolve_instrument(orchestrator, symbol)
    entry = float(rate.ask)
    bid = float(rate.bid)
    stop_loss = round(entry * (1.0 - stop_pct / 100.0), 2)
    now = datetime.now(UTC)
    proposal = TradeProposal(
        proposal_id=f"cost-exp-aapl-{now.strftime('%Y%m%dT%H%M%S')}",
        symbol="AAPL",
        side=Side.BUY,
        entry=entry,
        stop_loss=stop_loss,
        take_profit=None,
        confidence=1.0,
        generated_at=now,
        expires_at=now + timedelta(minutes=10),
        thesis=["controlled transaction-cost experiment; no directional thesis"],
        risks=["AAPL market movement", "CFD leverage x5", "spread and execution costs"],
    )
    observation = MarketObservation(
        symbol="AAPL",
        price=float(rate.last_price or ((bid + entry) / 2.0)),
        observed_at=rate.timestamp.astimezone(UTC),
        instrument_id=instrument_id,
        bid=bid,
        ask=entry,
    )
    decision = orchestrator.execution_gate.evaluate(
        proposal,
        snapshot,
        observation,
        now=now,
    )
    if not decision.approved:
        raise RuntimeError(
            "Normal execution checks rejected the experiment before the explicit notional "
            f"override: {', '.join(decision.reasons)}"
        )

    client = _execution_client(settings)
    eligibility = await client.instrument_eligibility(instrument_id=instrument_id)
    if not eligibility.allow_open_position:
        raise RuntimeError("eToro currently disallows opening AAPL")
    settlement_type = eligibility.settlement_type(direction="LONG", leverage=leverage)
    if settlement_type.lower() != "cfd":
        raise RuntimeError(
            f"Expected AAPL x5 to be CFD, broker returned settlementType={settlement_type}"
        )
    broker_minimum = eligibility.minimum_amount(direction="LONG", leverage=leverage)
    if broker_minimum is not None and virtual_margin_usd + 1e-9 < broker_minimum:
        raise RuntimeError(
            f"Virtual margin {virtual_margin_usd:.2f} is below broker minimum "
            f"{broker_minimum:.2f}"
        )

    costs = await client.what_if_open_costs(
        transaction="buy",
        instrument_id=instrument_id,
        settlement_type=settlement_type,
        amount_usd=virtual_margin_usd,
        stop_loss_rate=stop_loss,
        take_profit_rate=None,
        leverage=leverage,
    )

    # The normal gate remains authoritative for every safety reason. Only its
    # calculated notional amount is overridden for this one explicitly-labelled
    # experiment, within the hard experiment caps above.
    experiment_decision = replace(decision, amount_usd=virtual_margin_usd)
    orchestrator.storage.save_proposal("cost-experiment", proposal)
    attempt = orchestrator.autonomous_execution.prepare(
        proposal=proposal,
        watch_id=f"cost-experiment:{proposal.proposal_id}",
        observation=observation,
        decision=experiment_decision,
        now=now,
        leverage=leverage,
        settlement_type=settlement_type,
    )

    expected_round_trip_virtual = (
        virtual_exposure_usd * _EXPECTED_STOCK_CFD_FEE_PER_SIDE * 2.0
    )
    copy_scale = funded_usd / snapshot.equity_usd
    payload = {
        "attempt_id": attempt.attempt_id,
        "proposal_id": proposal.proposal_id,
        "prepared_at": now.isoformat(),
        "symbol": "AAPL",
        "instrument_id": instrument_id,
        "side": "BUY",
        "leverage": leverage,
        "settlement_type": settlement_type,
        "real_eur": real_eur,
        "eurusd": eurusd,
        "target_real_usd": real_usd,
        "funded_usd": funded_usd,
        "agent_virtual_equity_usd": snapshot.equity_usd,
        "copy_scale_real_per_virtual": copy_scale,
        "virtual_margin_usd": virtual_margin_usd,
        "virtual_exposure_usd": virtual_exposure_usd,
        "virtual_exposure_pct": virtual_exposure_pct,
        "bid": bid,
        "ask": entry,
        "stop_loss": stop_loss,
        "stop_pct": stop_pct,
        "what_if": costs.response,
        "what_if_totals_by_currency": costs.totals_by_currency,
        "documented_stock_cfd_fee_per_side": _EXPECTED_STOCK_CFD_FEE_PER_SIDE,
        "expected_round_trip_virtual_usd_at_flat_price": expected_round_trip_virtual,
        "expected_round_trip_real_usd_at_flat_price": expected_round_trip_virtual * copy_scale,
        "normal_gate_amount_usd": decision.amount_usd,
        "notional_override_reason": "explicit controlled cost experiment",
    }
    orchestrator.storage.set("cost_experiment_active", json.dumps(payload))
    orchestrator.storage.add_event("cost_experiment_prepared", json.dumps(payload))

    print("AAPL x5 cost experiment PREPARED -- NO BROKER ORDER SENT")
    print(f"  attempt_id: {attempt.attempt_id}")
    print(f"  instrument_id: {instrument_id}")
    print(f"  quote: bid={bid:.4f} ask={entry:.4f}")
    print(f"  stop_loss: {stop_loss:.2f} ({stop_pct:.2f}% below ask)")
    print(f"  target real capital: EUR {real_eur:.2f} ~= USD {real_usd:.2f}")
    print(f"  virtual margin: USD {virtual_margin_usd:.2f}")
    print(f"  leverage: x{leverage}")
    print(f"  virtual exposure: USD {virtual_exposure_usd:.2f} ({virtual_exposure_pct:.2f}%)")
    print(f"  settlement_type: {settlement_type}")
    print(f"  what-if totals: {costs.totals_by_currency}")
    print("  raw what-if:")
    print(json.dumps(costs.response, indent=2, default=str))
    print("\nTo submit, run:")
    print(
        f"  /opt/parquet/venv/bin/python -m parquet.cost_experiment execute "
        f"{attempt.attempt_id}"
    )


async def execute_experiment(settings: Settings, attempt_id: str) -> None:
    orchestrator = AutonomousOrchestrator(settings)
    attempt = orchestrator.storage.get_execution_attempt(attempt_id)
    if attempt is None:
        raise RuntimeError(f"Execution attempt not found: {attempt_id}")
    if attempt.state != ExecutionAttemptState.PREPARED:
        raise RuntimeError(f"Attempt is not PREPARED: {attempt.state}")
    active_raw = orchestrator.storage.get("cost_experiment_active")
    if not active_raw:
        raise RuntimeError("No active cost experiment metadata")
    active = json.loads(active_raw)
    if active.get("attempt_id") != attempt_id:
        raise RuntimeError("Attempt does not match active cost experiment")
    if attempt.symbol != "AAPL" or attempt.leverage != 5:
        raise RuntimeError("Experiment execution is restricted to prepared AAPL x5 ticket")

    cap = max(settings.execution.supervised_real_max_amount_usd, attempt.amount_usd * 1.01)
    experiment_execution = settings.execution.model_copy(
        update={
            "supervised_real_enabled": True,
            "supervised_real_max_amount_usd": cap,
            "supervised_real_max_leverage": max(
                settings.execution.supervised_real_max_leverage,
                attempt.leverage,
            ),
        }
    )
    experiment_settings = settings.model_copy(update={"execution": experiment_execution})
    reconciliation = ReconciliationService(
        experiment_settings,
        orchestrator.storage,
        orchestrator.market_client,
    )
    client = _execution_client(experiment_settings)
    adapter = RealSmallExecutionAdapter(
        settings=experiment_settings,
        storage=orchestrator.storage,
        position_manager=reconciliation.position_manager,
        reconciliation=reconciliation,
        client=client,
    )

    expected = f"REAL {attempt.attempt_id}"
    print("REAL-MONEY AAPL x5 COST EXPERIMENT")
    print(f"  virtual margin: USD {attempt.amount_usd:.2f}")
    print(f"  virtual exposure: USD {attempt.exposure_usd:.2f}")
    print(f"  stop_loss: {attempt.stop_loss}")
    confirmation = input(f"Type exactly '{expected}' to submit: ").strip()
    result = await adapter.execute(attempt, confirmation=confirmation)
    print(
        f"Execution result: {result.state.value}; request={result.broker_request_id}; "
        f"order={result.broker_order_id}; position={result.broker_position_id}"
    )
    active["execution_result"] = result.model_dump(mode="json")
    active["executed_at"] = datetime.now(UTC).isoformat()
    orchestrator.storage.set("cost_experiment_active", json.dumps(active, default=str))
    orchestrator.storage.add_event(
        "cost_experiment_execution_result",
        json.dumps(active, default=str),
    )


async def close_experiment(settings: Settings, position_id: str | None) -> None:
    orchestrator = AutonomousOrchestrator(settings)
    active_raw = orchestrator.storage.get("cost_experiment_active")
    if not active_raw:
        raise RuntimeError("No active cost experiment metadata")
    active = json.loads(active_raw)
    if position_id is None:
        execution_result = active.get("execution_result") or {}
        position_id = execution_result.get("broker_position_id")
    if not position_id:
        raise RuntimeError("No experiment position ID available")

    reconciliation = ReconciliationService(
        settings,
        orchestrator.storage,
        orchestrator.market_client,
    )
    await reconciliation.poll_once(force=True)
    snapshot = orchestrator.storage.get_broker_portfolio_snapshot()
    visible = {
        str(position.position_id)
        for position in (snapshot.positions if snapshot is not None else [])
    }
    if str(position_id) not in visible:
        raise RuntimeError(f"Position {position_id} is not currently visible at broker")

    client = _execution_client(settings)
    identity = await client.identity()
    if settings.etoro.expected_gcid is None or identity.gcid != settings.etoro.expected_gcid:
        raise RuntimeError("Pinned Agent Portfolio identity check failed")

    expected = f"CLOSE REAL {position_id}"
    confirmation = input(f"Type exactly '{expected}' to close the full position: ").strip()
    if confirmation != expected:
        raise RuntimeError(f"Explicit confirmation required: {expected}")

    request_id = str(uuid4())
    url = (
        f"{settings.etoro.base_url.rstrip('/')}/trading/execution/"
        f"market-close-orders/positions/{position_id}"
    )
    headers = {
        "x-api-key": _read_secret(settings.etoro.api_key_file, "eToro API key"),
        "x-user-key": _read_secret(settings.etoro.user_key_file, "eToro User key"),
        "x-request-id": request_id,
        "accept": "application/json",
        "content-type": "application/json",
    }
    orchestrator.storage.set("execution_uncertain", "1")
    try:
        async with httpx.AsyncClient(timeout=20) as http:
            response = await http.post(url, headers=headers, json={"UnitsToDeduct": None})
    except httpx.RequestError as exc:
        orchestrator.storage.add_event(
            "cost_experiment_close_transport_error",
            json.dumps({"position_id": position_id, "request_id": request_id, "error": repr(exc)}),
        )
        raise RuntimeError(
            "Close POST transport outcome is unknown; execution_uncertain remains set"
        ) from exc
    if response.is_error:
        orchestrator.storage.add_event(
            "cost_experiment_close_http_error",
            json.dumps(
                {
                    "position_id": position_id,
                    "request_id": request_id,
                    "status": response.status_code,
                    "body": response.text[:2000],
                }
            ),
        )
        raise RuntimeError(
            f"Close returned HTTP {response.status_code}; execution_uncertain remains set"
        )

    try:
        body: Any = response.json()
    except ValueError:
        body = {"raw": response.text[:2000]}
    orchestrator.storage.add_event(
        "cost_experiment_close_submitted",
        json.dumps(
            {"position_id": position_id, "request_id": request_id, "response": body},
            default=str,
        ),
    )

    gone = False
    for _ in range(15):
        await asyncio.sleep(1)
        await reconciliation.poll_once(force=True)
        broker = orchestrator.storage.get_broker_portfolio_snapshot()
        ids = {
            str(position.position_id)
            for position in (broker.positions if broker is not None else [])
        }
        if str(position_id) not in ids:
            gone = True
            break
    if gone:
        orchestrator.storage.set("execution_uncertain", "0")
        active["closed_at"] = datetime.now(UTC).isoformat()
        active["close_request_id"] = request_id
        active["close_response"] = body
        orchestrator.storage.set("cost_experiment_active", json.dumps(active, default=str))
        orchestrator.storage.add_event(
            "cost_experiment_close_reconciled",
            json.dumps(active, default=str),
        )
        print(f"Position {position_id} no longer visible; close reconciled")
    else:
        print(
            f"Close accepted but position {position_id} is still visible after 15s; "
            "execution_uncertain remains set"
        )
    print(json.dumps(body, indent=2, default=str))


async def report_experiment(settings: Settings) -> None:
    storage = Storage(settings.state_db)
    active_raw = storage.get("cost_experiment_active")
    if not active_raw:
        print("No active cost experiment metadata")
        return
    active = json.loads(active_raw)
    client = EtoroExecutionClient(
        api_key=_read_secret(settings.etoro.api_key_file, "eToro API key"),
        user_key=_read_secret(settings.etoro.user_key_file, "eToro User key"),
        base_url=settings.etoro.execution_base_url,
        identity_base_url=settings.etoro.base_url,
    )
    # Use the market-data client shape through a lightweight authenticated GET.
    headers = {
        "x-api-key": client.api_key,
        "x-user-key": client.user_key,
        "x-request-id": str(uuid4()),
        "accept": "application/json",
    }
    async with httpx.AsyncClient(timeout=20) as http:
        pnl_resp = await http.get(
            f"{settings.etoro.base_url.rstrip('/')}/trading/info/real/pnl",
            headers=headers,
        )
        pnl_resp.raise_for_status()
        prepared_at = _parse_utc(str(active["prepared_at"]))
        history_resp = await http.get(
            f"{settings.etoro.base_url.rstrip('/')}/trading/info/trade/history",
            headers={**headers, "x-request-id": str(uuid4())},
            params={"minDate": prepared_at.date().isoformat(), "page": "1", "pageSize": "100"},
        )
        history_resp.raise_for_status()
    history = history_resp.json()
    execution_result = active.get("execution_result") or {}
    position_id = execution_result.get("broker_position_id")
    rows = history if isinstance(history, list) else history.get("items", history.get("data", []))
    matches = [
        row
        for row in (rows if isinstance(rows, list) else [])
        if isinstance(row, dict) and str(row.get("positionId")) == str(position_id)
    ]
    print("=== EXPERIMENT METADATA ===")
    print(json.dumps(active, indent=2, default=str))
    print("\n=== CURRENT PNL RESPONSE ===")
    print(json.dumps(pnl_resp.json(), indent=2, default=str))
    print("\n=== MATCHING TRADE HISTORY ===")
    print(json.dumps(matches, indent=2, default=str))


def main() -> None:
    parser = argparse.ArgumentParser(prog="python -m parquet.cost_experiment")
    parser.add_argument("--config", type=Path, default=None)
    sub = parser.add_subparsers(dest="command", required=True)

    seed = sub.add_parser("seed")
    seed.add_argument("--period", choices=("daily", "weekly"), required=True)
    seed.add_argument("--boundary", required=True)
    seed.add_argument("--equity", type=float, required=True)
    seed.add_argument(
        "--mode",
        choices=("exact", "conservative_upper_bound"),
        required=True,
    )
    seed.add_argument("--source", required=True)

    prepare = sub.add_parser("prepare")
    prepare.add_argument("--symbol", default="AAPL")
    prepare.add_argument("--real-eur", type=float, required=True)
    prepare.add_argument("--eurusd", type=float, required=True)
    prepare.add_argument("--funded-usd", type=float, required=True)
    prepare.add_argument("--leverage", type=int, default=5)
    prepare.add_argument("--stop-pct", type=float, default=1.5)

    execute = sub.add_parser("execute")
    execute.add_argument("attempt_id")

    close = sub.add_parser("close")
    close.add_argument("--position-id", default=None)

    sub.add_parser("report")

    args = parser.parse_args()
    settings = load_settings(args.config)
    if args.command == "seed":
        seed_baseline(
            settings,
            period=args.period,
            boundary=_parse_utc(args.boundary),
            equity_usd=float(args.equity),
            mode=args.mode,
            source=args.source,
        )
        return
    if args.command == "prepare":
        asyncio.run(
            prepare_experiment(
                settings,
                symbol=args.symbol,
                real_eur=float(args.real_eur),
                eurusd=float(args.eurusd),
                funded_usd=float(args.funded_usd),
                leverage=int(args.leverage),
                stop_pct=float(args.stop_pct),
            )
        )
        return
    if args.command == "execute":
        asyncio.run(execute_experiment(settings, str(args.attempt_id)))
        return
    if args.command == "close":
        asyncio.run(close_experiment(settings, args.position_id))
        return
    if args.command == "report":
        asyncio.run(report_experiment(settings))
        return


if __name__ == "__main__":
    main()
