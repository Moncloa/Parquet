from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from pydantic import BaseModel, Field

from parquet.portfolio import BrokerPortfolioSnapshot


class EquityBoundaryBaseline(BaseModel):
    boundary: datetime
    equity_usd: float = Field(gt=0)
    before_at: datetime
    after_at: datetime
    before_gap_seconds: float = Field(ge=0)
    after_gap_seconds: float = Field(ge=0)


class LocalEquityRiskLedger:
    """Durable broker-equity snapshots for risk-period baselines.

    Historical eToro balance scope is not always available. This ledger records
    every broker reconciliation locally and establishes a period baseline only
    when Parquet has snapshots on both sides of the UTC boundary, both snapshots
    are flat, and their equities agree to the cent. Otherwise it fails closed.
    """

    def __init__(self, storage: Any, *, boundary_tolerance_seconds: float = 120.0) -> None:
        self.storage = storage
        self.boundary_tolerance_seconds = boundary_tolerance_seconds
        with self.storage._lock:
            self.storage.conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS risk_equity_snapshots (
                    captured_at TEXT PRIMARY KEY,
                    equity_usd REAL NOT NULL,
                    open_positions INTEGER NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_risk_equity_snapshots_captured_at
                ON risk_equity_snapshots(captured_at);
                """
            )
            self.storage.conn.commit()

    def record(self, snapshot: BrokerPortfolioSnapshot) -> None:
        captured_at = snapshot.captured_at.astimezone(UTC)
        with self.storage._lock:
            self.storage.conn.execute(
                "INSERT OR REPLACE INTO risk_equity_snapshots"
                "(captured_at, equity_usd, open_positions) VALUES(?, ?, ?)",
                (
                    captured_at.isoformat(),
                    float(snapshot.equity_usd),
                    len(snapshot.positions),
                ),
            )
            cutoff = (captured_at - timedelta(days=35)).isoformat()
            self.storage.conn.execute(
                "DELETE FROM risk_equity_snapshots WHERE captured_at < ?",
                (cutoff,),
            )
            self.storage.conn.commit()

    def baseline(self, boundary: datetime, *, label: str) -> EquityBoundaryBaseline:
        boundary_utc = boundary.astimezone(UTC)
        boundary_text = boundary_utc.isoformat()
        with self.storage._lock:
            before = self.storage.conn.execute(
                "SELECT captured_at, equity_usd, open_positions "
                "FROM risk_equity_snapshots WHERE captured_at <= ? "
                "ORDER BY captured_at DESC LIMIT 1",
                (boundary_text,),
            ).fetchone()
            after = self.storage.conn.execute(
                "SELECT captured_at, equity_usd, open_positions "
                "FROM risk_equity_snapshots WHERE captured_at >= ? "
                "ORDER BY captured_at ASC LIMIT 1",
                (boundary_text,),
            ).fetchone()

        if before is None or after is None:
            raise ValueError(
                f"missing local {label} equity baseline around {boundary_utc.isoformat()}"
            )

        before_at = datetime.fromisoformat(str(before[0])).astimezone(UTC)
        after_at = datetime.fromisoformat(str(after[0])).astimezone(UTC)
        before_gap = (boundary_utc - before_at).total_seconds()
        after_gap = (after_at - boundary_utc).total_seconds()
        if before_gap < 0 or after_gap < 0:
            raise ValueError(f"invalid local {label} equity baseline ordering")
        if (
            before_gap > self.boundary_tolerance_seconds
            or after_gap > self.boundary_tolerance_seconds
        ):
            raise ValueError(
                f"local {label} equity baseline is too far from boundary "
                f"(before={before_gap:.1f}s after={after_gap:.1f}s)"
            )

        before_open = int(before[2])
        after_open = int(after[2])
        if before_open != 0 or after_open != 0:
            raise ValueError(
                f"cannot establish exact local {label} baseline: "
                "portfolio was not flat across boundary"
            )

        before_equity = float(before[1])
        after_equity = float(after[1])
        if before_equity <= 0 or after_equity <= 0:
            raise ValueError(f"local {label} equity baseline must be positive")
        if abs(before_equity - after_equity) > 0.01:
            raise ValueError(
                f"cannot establish exact local {label} baseline: equity changed "
                f"across boundary ({before_equity:.2f} -> {after_equity:.2f})"
            )

        return EquityBoundaryBaseline(
            boundary=boundary_utc,
            equity_usd=after_equity,
            before_at=before_at,
            after_at=after_at,
            before_gap_seconds=before_gap,
            after_gap_seconds=after_gap,
        )
