from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime

from parquet.enhanced_orchestrator import (
    AutonomousOrchestrator as EnhancedAutonomousOrchestrator,
)
from parquet.execution.autonomous_real import AutonomousRealExecutionAdapter
from parquet.execution.etoro import EtoroExecutionClient
from parquet.models import MarketObservation, Side, TradeProposal, TriggerAction
from parquet.reconciliation import ReconciliationService
from parquet.tickets import choose_leverage_terms, validate_what_if_costs

_EXCLUDED_AUTONOMOUS_SYMBOLS = {"AIR", "AIR.PA"}


class AutonomousOrchestrator(EnhancedAutonomousOrchestrator):
    """Enhanced orchestrator that also consumes immediate trade proposals."""

    def __init__(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
        super().__init__(*args, **kwargs)
        self._real_reconciliation: ReconciliationService | None = None
        self._real_execution_client: EtoroExecutionClient | None = None
        self._real_execution_adapter: AutonomousRealExecutionAdapter | None = None
        if self.settings.etoro.enabled:
            self._real_reconciliation = ReconciliationService(
                self.settings,
                self.storage,
                self.market_client,
            )
            api_key = self.settings.etoro.api_key_file.read_text(encoding="utf-8").strip()
            user_key = self.settings.etoro.user_key_file.read_text(encoding="utf-8").strip()
            if not api_key or not user_key:
                raise RuntimeError("Empty eToro credentials for autonomous real execution")
            self._real_execution_client = EtoroExecutionClient(
                api_key=api_key,
                user_key=user_key,
                base_url=self.settings.etoro.execution_base_url,
                identity_base_url=self.settings.etoro.base_url,
            )
            self._real_execution_adapter = AutonomousRealExecutionAdapter(
                settings=self.settings,
                storage=self.storage,
                position_manager=self._real_reconciliation.position_manager,
                reconciliation=self._real_reconciliation,
                client=self._real_execution_client,
            )

    async def poll_market_once(self, now: datetime | None = None) -> int:
        processed = await super().poll_market_once(now)
        processed += await self.poll_active_proposals_once(now)
        return processed

    async def poll_active_proposals_once(self, now: datetime | None = None) -> int:
        if not self.settings.execution.autonomous_enabled or self.market_client is None:
            return 0

        current = (now or datetime.now(UTC)).astimezone(UTC)
        snapshot = self.storage.get_risk_snapshot()
        if snapshot is None:
            self.storage.add_event(
                "direct_proposal_execution_blocked",
                json.dumps({"reasons": ["risk_snapshot_unavailable"]}),
            )
            return 0

        active_execute_proposals = {
            watch.proposal_id
            for watch in self.storage.active_watches(current)
            if watch.on_trigger == TriggerAction.EXECUTE and watch.proposal_id is not None
        }
        processed = 0
        for proposal in _latest_active_proposals(self.storage, current):
            if proposal.proposal_id in active_execute_proposals:
                continue
            if proposal.symbol.upper() in _EXCLUDED_AUTONOMOUS_SYMBOLS:
                self.storage.add_event(
                    "direct_proposal_execution_rejected",
                    json.dumps(
                        {
                            "proposal_id": proposal.proposal_id,
                            "symbol": proposal.symbol,
                            "reasons": ["symbol_excluded_from_autonomous_execution"],
                        }
                    ),
                )
                continue
            if self.storage.get_active_execution_attempt_for_proposal(proposal.proposal_id):
                continue

            payload: dict[str, object] = {
                "source": "direct_proposal",
                "proposal_id": proposal.proposal_id,
                "symbol": proposal.symbol,
                "evaluated_at": current.isoformat(),
            }
            try:
                mode = self.settings.execution.autonomous_mode
                if mode == "real":
                    await self._execute_real_proposal(proposal, current, payload)
                    processed += 1
                    continue

                observation = await self._proposal_observation(proposal)
                decision = self.execution_gate.evaluate(
                    proposal,
                    snapshot,
                    observation,
                    now=current,
                )
                payload["observation"] = observation.model_dump(mode="json")
                payload["gate"] = decision.as_dict()
                if not decision.approved:
                    self.storage.add_event(
                        "direct_proposal_execution_rejected",
                        json.dumps(payload, default=str),
                    )
                    processed += 1
                    continue

                attempt = self.autonomous_execution.prepare(
                    proposal=proposal,
                    watch_id=f"direct-proposal:{proposal.proposal_id}",
                    observation=observation,
                    decision=decision,
                    now=current,
                )
                if mode == "shadow":
                    attempt = self.autonomous_execution.execute_shadow(attempt, now=current)
                    payload["attempt"] = attempt.model_dump(mode="json")
                    self.storage.add_event(
                        "direct_proposal_execution_shadow",
                        json.dumps(payload, default=str),
                    )
                else:
                    attempt = self.autonomous_execution.mark_demo_pending(attempt, now=current)
                    payload["attempt"] = attempt.model_dump(mode="json")
                    payload["reasons"] = ["demo_broker_adapter_not_implemented"]
                    self.storage.add_event(
                        "direct_proposal_execution_demo_pending",
                        json.dumps(payload, default=str),
                    )
            except Exception as exc:
                payload["reasons"] = [str(exc)]
                self.storage.add_event(
                    "direct_proposal_execution_error",
                    json.dumps(payload, default=str),
                )
            processed += 1
        return processed

    async def _execute_real_proposal(
        self,
        proposal: TradeProposal,
        current: datetime,
        payload: dict[str, object],
    ) -> None:
        config = self.settings.execution
        if not config.autonomous_real_enabled:
            payload["reasons"] = ["autonomous_real_execution_disabled"]
            self.storage.add_event(
                "direct_proposal_execution_blocked",
                json.dumps(payload, default=str),
            )
            return
        if (
            self._real_reconciliation is None
            or self._real_execution_client is None
            or self._real_execution_adapter is None
        ):
            raise RuntimeError("Autonomous real broker services are unavailable")

        await self._real_reconciliation.poll_once(now=current, force=True)
        self._real_reconciliation.position_manager.assert_trading_enabled(now=current)
        if self.storage.get("etoro_identity_verified") != "1":
            raise RuntimeError("eToro Agent Portfolio identity is not verified")
        if self.storage.get("execution_uncertain") == "1":
            raise RuntimeError("Execution blocked: unresolved broker outcome")

        snapshot = self.storage.get_risk_snapshot()
        if snapshot is None or snapshot.equity_usd is None:
            raise RuntimeError("Risk snapshot unavailable after forced reconciliation")

        observation = await self._proposal_observation(proposal)
        decision = self.execution_gate.evaluate(
            proposal,
            snapshot,
            observation,
            now=datetime.now(UTC),
        )
        payload["observation"] = observation.model_dump(mode="json")
        payload["gate"] = decision.as_dict()
        if not decision.approved or decision.amount_usd is None or decision.amount_usd <= 0:
            reasons = list(decision.reasons) or ["execution_gate_no_positive_amount"]
            payload["reasons"] = reasons
            self.storage.add_event(
                "direct_proposal_execution_rejected",
                json.dumps(payload, default=str),
            )
            return

        instrument_id = observation.instrument_id
        if instrument_id is None:
            raise RuntimeError("Live quote has no eToro instrument id")
        eligibility = await self._real_execution_client.instrument_eligibility(
            instrument_id=instrument_id
        )
        if not eligibility.allow_open_position:
            raise RuntimeError(f"eToro does not allow opening instrument {instrument_id}")

        direction = "LONG" if proposal.side == Side.BUY else "SHORT"
        virtual_capital_cap = (
            snapshot.equity_usd * config.autonomous_real_max_position_pct / 100
        )
        leverage, settlement_type, broker_minimum, chosen_amount, maximum_safe = (
            choose_leverage_terms(
                eligibility,
                direction=direction,
                gate_maximum_notional_usd=decision.amount_usd,
                supervised_cap_usd=virtual_capital_cap,
                max_leverage=config.autonomous_real_max_leverage,
            )
        )
        exposure_usd = chosen_amount * leverage

        costs = await self._real_execution_client.what_if_open_costs(
            transaction="buy" if proposal.side == Side.BUY else "sellShort",
            instrument_id=instrument_id,
            settlement_type=settlement_type,
            amount_usd=chosen_amount,
            stop_loss_rate=proposal.stop_loss,
            take_profit_rate=proposal.take_profit,
            leverage=leverage,
        )
        validate_what_if_costs(
            costs,
            instrument_id=instrument_id,
            amount_usd=chosen_amount,
            now=datetime.now(UTC),
        )

        payload["real_preflight"] = {
            "virtual_equity_usd": snapshot.equity_usd,
            "max_position_pct": config.autonomous_real_max_position_pct,
            "maximum_virtual_capital_usd": virtual_capital_cap,
            "broker_minimum_usd": broker_minimum,
            "maximum_safe_virtual_capital_usd": maximum_safe,
            "chosen_virtual_capital_usd": chosen_amount,
            "leverage": leverage,
            "virtual_exposure_usd": exposure_usd,
            "settlement_type": settlement_type,
            "what_if_total_cost_usd": costs.total_usd,
        }

        capped_decision = replace(decision, amount_usd=chosen_amount)
        attempt = self.autonomous_execution.prepare(
            proposal=proposal,
            watch_id=f"direct-proposal:{proposal.proposal_id}",
            observation=observation,
            decision=capped_decision,
            now=datetime.now(UTC),
            leverage=leverage,
            settlement_type=settlement_type,
        )
        result = await self._real_execution_adapter.execute(
            attempt,
            now=datetime.now(UTC),
        )
        payload["attempt"] = result.model_dump(mode="json")
        self.storage.add_event(
            "direct_proposal_execution_real",
            json.dumps(payload, default=str),
        )

    async def _proposal_observation(self, proposal: TradeProposal) -> MarketObservation:
        resolved = await self._resolve_instrument_ids([proposal.symbol])
        instrument_id = resolved.get(proposal.symbol)
        if instrument_id is None:
            raise RuntimeError(f"Unable to resolve eToro instrument for {proposal.symbol}")
        rates = await self.market_client.rates([instrument_id])  # type: ignore[union-attr]
        matching = [rate for rate in rates if rate.instrument_id == instrument_id]
        if len(matching) != 1:
            raise RuntimeError(f"No unique live quote for instrument {instrument_id}")
        rate = matching[0]
        if rate.bid is None or rate.ask is None:
            raise RuntimeError(f"Bid/ask unavailable for instrument {instrument_id}")
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


def _latest_active_proposals(storage, now: datetime) -> list[TradeProposal]:  # type: ignore[no-untyped-def]
    analysis_id = storage.get("latest_analysis_id")
    if not analysis_id:
        return []
    rows = storage.conn.execute(
        "SELECT payload FROM proposals "
        "WHERE analysis_id = ? AND expires_at > ? ORDER BY rowid",
        (analysis_id, now.astimezone(UTC).isoformat()),
    ).fetchall()
    return [TradeProposal.model_validate_json(str(row[0])) for row in rows]
