from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

from parquet.autonomous_orchestrator import AutonomousOrchestrator
from parquet.config import Settings
from parquet.execution.autonomous import ExecutionAttempt
from parquet.execution.etoro import EtoroCostResult, EtoroEligibilityResult, EtoroExecutionClient
from parquet.execution.gate import ExecutionDecision
from parquet.market.etoro import EtoroMarketDataClient, InstrumentRate
from parquet.models import MarketObservation, Side, TradeProposal
from parquet.reconciliation import ReconciliationService
from parquet.storage import Storage


@dataclass(frozen=True)
class ProposalRecord:
    analysis_id: str
    proposal: TradeProposal


@dataclass(frozen=True)
class PreparedRealSmallTicket:
    attempt: ExecutionAttempt
    proposal: TradeProposal
    observation: MarketObservation
    gate_decision: ExecutionDecision
    eligibility: EtoroEligibilityResult
    costs: EtoroCostResult
    settlement_type: str
    broker_minimum_usd: float | None
    maximum_safe_amount_usd: float


def recent_proposals(storage: Storage, *, limit: int = 20) -> list[ProposalRecord]:
    rows = storage.conn.execute(
        "SELECT analysis_id, payload FROM proposals ORDER BY rowid DESC LIMIT ?",
        (limit,),
    ).fetchall()
    return [
        ProposalRecord(
            analysis_id=str(row[0]),
            proposal=TradeProposal.model_validate_json(str(row[1])),
        )
        for row in rows
    ]


def choose_real_small_amount(
    *,
    gate_maximum_usd: float,
    supervised_cap_usd: float,
    broker_minimum_usd: float | None,
    requested_amount_usd: float | None = None,
) -> tuple[float, float]:
    if gate_maximum_usd <= 0 or supervised_cap_usd <= 0:
        raise RuntimeError("No positive amount remains after supervised and risk caps")
    maximum_safe = min(gate_maximum_usd, supervised_cap_usd)

    if broker_minimum_usd is not None:
        if broker_minimum_usd <= 0:
            raise RuntimeError("eToro returned a non-positive minimum position amount")
        if broker_minimum_usd > maximum_safe:
            raise RuntimeError(
                f"eToro minimum {broker_minimum_usd:.2f} USD exceeds the safe supervised "
                f"maximum {maximum_safe:.2f} USD for this proposal"
            )

    if requested_amount_usd is None:
        chosen_amount = (
            broker_minimum_usd
            if broker_minimum_usd is not None
            else min(10.0, maximum_safe)
        )
    else:
        if requested_amount_usd <= 0:
            raise RuntimeError("Requested amount must be positive")
        chosen_amount = requested_amount_usd

    if broker_minimum_usd is not None and chosen_amount < broker_minimum_usd:
        raise RuntimeError(
            f"Requested amount {chosen_amount:.2f} USD is below eToro minimum "
            f"{broker_minimum_usd:.2f} USD"
        )
    if chosen_amount > maximum_safe:
        raise RuntimeError(
            f"Requested amount {chosen_amount:.2f} USD exceeds safe maximum "
            f"{maximum_safe:.2f} USD"
        )
    return chosen_amount, maximum_safe


def validate_what_if_costs(
    costs: EtoroCostResult,
    *,
    instrument_id: int,
    amount_usd: float,
    now: datetime,
    max_age: timedelta = timedelta(minutes=2),
) -> None:
    if costs.instrument_id != instrument_id:
        raise RuntimeError("eToro what-if cost response does not match the ticket instrument")
    updated = costs.last_updated.astimezone(UTC)
    age = now - updated
    if age < timedelta(seconds=-5) or age > max_age:
        raise RuntimeError(
            f"eToro what-if costs are stale or future-dated: age={age.total_seconds():.1f}s"
        )
    total = costs.total_usd
    if total < 0:
        raise RuntimeError("eToro what-if cost total is negative")
    if total >= amount_usd:
        raise RuntimeError(
            f"eToro what-if costs {total:.2f} USD consume the full "
            f"{amount_usd:.2f} USD ticket"
        )


async def prepare_real_small_ticket(
    settings: Settings,
    *,
    proposal_id: str,
    requested_amount_usd: float | None = None,
) -> PreparedRealSmallTicket:
    if not settings.etoro.enabled:
        raise RuntimeError("eToro is disabled")
    if settings.etoro.expected_gcid is None:
        raise RuntimeError("Cannot prepare real ticket: Agent Portfolio GCID is not pinned")

    orchestrator = AutonomousOrchestrator(settings)
    reconciliation = ReconciliationService(
        settings,
        orchestrator.storage,
        orchestrator.market_client,
    )
    await reconciliation.poll_once(force=True)
    reconciliation.position_manager.assert_trading_enabled()
    if orchestrator.storage.get("etoro_identity_verified") != "1":
        raise RuntimeError("Cannot prepare real ticket: eToro Agent Portfolio identity is not verified")
    if orchestrator.storage.get("execution_uncertain") == "1":
        raise RuntimeError("Cannot prepare real ticket: unresolved broker outcome")

    proposal = orchestrator.storage.get_proposal(proposal_id)
    if proposal is None:
        raise RuntimeError(f"Proposal not found: {proposal_id}")
    snapshot = orchestrator.storage.get_risk_snapshot()
    if snapshot is None:
        raise RuntimeError("Risk snapshot unavailable after reconciliation")
    market = orchestrator.market_client
    if market is None:
        raise RuntimeError("eToro market client is unavailable")

    instrument_id = await _resolve_instrument_id(settings, market, proposal.symbol)
    rate = await _current_rate(market, instrument_id)
    observation = _observation(proposal, rate, instrument_id)
    now = datetime.now(UTC)
    decision = orchestrator.execution_gate.evaluate(
        proposal,
        snapshot,
        observation,
        now=now,
    )
    if not decision.approved:
        reasons = ", ".join(decision.reasons) or "unknown"
        raise RuntimeError(f"Execution gate rejected proposal: {reasons}")
    if decision.amount_usd is None or decision.amount_usd <= 0:
        raise RuntimeError("Execution gate did not produce a positive maximum amount")

    execution_client = _execution_client(settings)
    eligibility = await execution_client.instrument_eligibility(instrument_id=instrument_id)
    if not eligibility.allow_open_position:
        raise RuntimeError(f"eToro does not allow opening instrument {instrument_id}")

    direction = "LONG" if proposal.side == Side.BUY else "SHORT"
    broker_minimum = eligibility.minimum_amount(direction=direction, leverage=1)
    settlement_type = eligibility.settlement_type(direction=direction, leverage=1)
    chosen_amount, maximum_safe = choose_real_small_amount(
        gate_maximum_usd=decision.amount_usd,
        supervised_cap_usd=settings.execution.supervised_real_max_amount_usd,
        broker_minimum_usd=broker_minimum,
        requested_amount_usd=requested_amount_usd,
    )

    costs = await execution_client.what_if_open_costs(
        transaction="buy" if proposal.side == Side.BUY else "sellShort",
        instrument_id=instrument_id,
        settlement_type=settlement_type,
        amount_usd=chosen_amount,
        stop_loss_rate=proposal.stop_loss,
        take_profit_rate=proposal.take_profit,
        leverage=1,
    )
    validate_what_if_costs(
        costs,
        instrument_id=instrument_id,
        amount_usd=chosen_amount,
        now=datetime.now(UTC),
    )

    capped_decision = replace(decision, amount_usd=chosen_amount)
    attempt = orchestrator.autonomous_execution.prepare(
        proposal=proposal,
        watch_id=f"manual-real-small:{proposal.proposal_id}",
        observation=observation,
        decision=capped_decision,
        now=now,
    )
    return PreparedRealSmallTicket(
        attempt=attempt,
        proposal=proposal,
        observation=observation,
        gate_decision=decision,
        eligibility=eligibility,
        costs=costs,
        settlement_type=settlement_type,
        broker_minimum_usd=broker_minimum,
        maximum_safe_amount_usd=maximum_safe,
    )


async def _resolve_instrument_id(
    settings: Settings,
    market: EtoroMarketDataClient,
    symbol: str,
) -> int:
    normalized = symbol.upper()
    configured = settings.etoro.instrument_ids.get(normalized)
    if configured is not None:
        return configured

    hits = await market.search(normalized)
    exact_ids = sorted(
        {
            hit.instrument_id
            for hit in hits
            if hit.symbol is not None and hit.symbol.upper() == normalized
        }
    )
    if len(exact_ids) != 1:
        raise RuntimeError(
            f"Expected one exact eToro instrument for {normalized}, found {len(exact_ids)}"
        )
    return exact_ids[0]


async def _current_rate(market: EtoroMarketDataClient, instrument_id: int) -> InstrumentRate:
    rates = await market.rates([instrument_id])
    matching = [rate for rate in rates if rate.instrument_id == instrument_id]
    if len(matching) != 1:
        raise RuntimeError(f"No unique live quote for instrument {instrument_id}")
    rate = matching[0]
    if rate.bid is None or rate.ask is None:
        raise RuntimeError(f"Bid/ask unavailable for instrument {instrument_id}")
    return rate


def _observation(
    proposal: TradeProposal,
    rate: InstrumentRate,
    instrument_id: int,
) -> MarketObservation:
    if rate.bid is None or rate.ask is None:
        raise RuntimeError("Bid/ask unavailable")
    price = rate.last_price
    if price is None:
        price = (rate.bid + rate.ask) / 2
    return MarketObservation(
        symbol=proposal.symbol,
        price=price,
        observed_at=rate.timestamp.astimezone(UTC),
        instrument_id=instrument_id,
        bid=rate.bid,
        ask=rate.ask,
    )


def _execution_client(settings: Settings) -> EtoroExecutionClient:
    return EtoroExecutionClient(
        api_key=_read_secret(settings.etoro.api_key_file, "eToro API key"),
        user_key=_read_secret(settings.etoro.user_key_file, "eToro User key"),
        base_url=settings.etoro.execution_base_url,
        identity_base_url=settings.etoro.base_url,
    )


def _read_secret(path: Path, label: str) -> str:
    value = path.read_text(encoding="utf-8").strip()
    if not value:
        raise RuntimeError(f"Empty {label}: {path}")
    return value
