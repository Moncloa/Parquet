from __future__ import annotations

from uuid import uuid4

from parquet.execution.base import ExecutionResult
from parquet.models import TradeProposal
from parquet.storage import Storage


class ShadowExecutor:
    def __init__(self, storage: Storage) -> None:
        self.storage = storage

    async def submit(self, proposal: TradeProposal) -> ExecutionResult:
        ref = f"shadow-{uuid4()}"
        self.storage.add_event("shadow_trade", proposal.model_dump_json())
        return ExecutionResult(True, ref, "Recorded only; no broker order was sent")
