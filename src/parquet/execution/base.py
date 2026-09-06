from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from parquet.models import TradeProposal


@dataclass(frozen=True)
class ExecutionResult:
    accepted: bool
    reference: str
    detail: str


class Executor(Protocol):
    async def submit(self, proposal: TradeProposal) -> ExecutionResult: ...
