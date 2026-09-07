"""Execution adapters and deterministic pre-trade gate."""

from parquet.execution.gate import ExecutionDecision, ExecutionGate
from parquet.execution.supervised import RealSmallExecutionAdapter

__all__ = ["ExecutionDecision", "ExecutionGate", "RealSmallExecutionAdapter"]
