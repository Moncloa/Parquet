from __future__ import annotations

import json
from datetime import UTC, datetime

from parquet.enhanced_orchestrator import (
    AutonomousOrchestrator as EnhancedAutonomousOrchestrator,
)
from parquet.models import MarketObservation, TradeProposal, TriggerAction

_EXCLUDED_AUTONOMOUS_SYMBOLS = {"AIR", "AIR.PA"}


class AutonomousOrchestrator(EnhancedAutonomousOrchestrator):
    """Enhanced orchestrator that also consumes immediate trade proposals."""

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
                mode = self.settings.execution.autonomous_mode
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
