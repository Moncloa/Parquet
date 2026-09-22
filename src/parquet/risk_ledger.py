from __future__ import annotations

import json
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
    provenance: str = "local_bracket"
    mode: str = "exact"
    source: str | None = None


class LocalEquityRiskLedger:
    """Durable broker-equity snapshots for risk-period baselines.

    Historical eToro balance scope is not always available. This ledger records
    every broker reconciliation locally and establishes a period baseline from
    snapshots bracketing the UTC boundary. When the portfolio is flat and equity
    agrees to the cent, the baseline is exact. Otherwise, if both observations
    are still close enough to the boundary, Parquet uses the higher observed
    equity as a conservative upper-bound baseline so overnight positions remain
    supported without understating losses. A manual baseline may be seeded
    explicitly for bootstrap/recovery and is stored separately with provenance.
    """

    _MANUAL_MODES = {"exact", "conservative_upper_bound"}
    _MANUAL_PERIODS = {"daily", "weekly"}

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

                CREATE TABLE IF NOT EXISTS risk_manual_baselines (
                    period TEXT NOT NULL,
                    boundary TEXT NOT NULL,
                    equity_usd REAL NOT NULL,
                    mode TEXT NOT NULL,
                    source TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY(period, boundary)
                );
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

    def seed_manual_baseline(
        self,
        *,
        period: str,
        boundary: datetime,
        equity_usd: float,
        mode: str,
        source: str,
        created_at: datetime | None = None,
    ) -> EquityBoundaryBaseline:
        normalized_period = period.strip().lower()
        normalized_mode = mode.strip().lower().replace("-", "_")
        normalized_source = source.strip()
        if normalized_period not in self._MANUAL_PERIODS:
            raise ValueError("manual baseline period must be daily or weekly")
        if normalized_mode not in self._MANUAL_MODES:
            raise ValueError(
                "manual baseline mode must be exact or conservative_upper_bound"
            )
        if equity_usd <= 0:
            raise ValueError("manual baseline equity must be positive")
        if not normalized_source:
            raise ValueError("manual baseline source must not be empty")

        boundary_utc = boundary.astimezone(UTC)
        created_utc = (created_at or datetime.now(UTC)).astimezone(UTC)
        with self.storage._lock:
            self.storage.conn.execute(
                "INSERT OR REPLACE INTO risk_manual_baselines"
                "(period, boundary, equity_usd, mode, source, created_at) "
                "VALUES(?, ?, ?, ?, ?, ?)",
                (
                    normalized_period,
                    boundary_utc.isoformat(),
                    float(equity_usd),
                    normalized_mode,
                    normalized_source,
                    created_utc.isoformat(),
                ),
            )
            self.storage.conn.commit()

        if hasattr(self.storage, "add_event"):
            self.storage.add_event(
                "manual_risk_baseline_seeded",
                json.dumps(
                    {
                        "period": normalized_period,
                        "boundary": boundary_utc.isoformat(),
                        "equity_usd": float(equity_usd),
                        "mode": normalized_mode,
                        "source": normalized_source,
                        "created_at": created_utc.isoformat(),
                    }
                ),
            )

        baseline = self._manual_baseline(
            period=normalized_period,
            boundary=boundary_utc,
        )
        if baseline is None:
            raise RuntimeError("manual risk baseline disappeared after persistence")
        return baseline

    def baseline(self, boundary: datetime, *, label: str) -> EquityBoundaryBaseline:
        boundary_utc = boundary.astimezone(UTC)
        manual = self._manual_baseline(period=label, boundary=boundary_utc, required=False)
        if manual is not None:
            return manual

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
        before_equity = float(before[1])
        after_equity = float(after[1])
        if before_equity <= 0 or after_equity <= 0:
            raise ValueError(f"local {label} equity baseline must be positive")

        flat_exact = (
            before_open == 0
            and after_open == 0
            and abs(before_equity - after_equity) <= 0.01
        )
        if flat_exact:
            return EquityBoundaryBaseline(
                boundary=boundary_utc,
                equity_usd=after_equity,
                before_at=before_at,
                after_at=after_at,
                before_gap_seconds=before_gap,
                after_gap_seconds=after_gap,
            )

        conservative_equity = max(before_equity, after_equity)
        reasons: list[str] = []
        if before_open != 0 or after_open != 0:
            reasons.append(
                f"open_positions={before_open}->{after_open}"
            )
        if abs(before_equity - after_equity) > 0.01:
            reasons.append(
                f"equity={before_equity:.2f}->{after_equity:.2f}"
            )

        return EquityBoundaryBaseline(
            boundary=boundary_utc,
            equity_usd=conservative_equity,
            before_at=before_at,
            after_at=after_at,
            before_gap_seconds=before_gap,
            after_gap_seconds=after_gap,
            provenance="local_bracket",
            mode="conservative_upper_bound",
            source=(
                "automatic conservative boundary recovery: "
                + ", ".join(reasons)
            ),
        )

    def _manual_baseline(
        self,
        *,
        period: str,
        boundary: datetime,
        required: bool = True,
    ) -> EquityBoundaryBaseline | None:
        normalized_period = period.strip().lower()
        boundary_utc = boundary.astimezone(UTC)
        with self.storage._lock:
            row = self.storage.conn.execute(
                "SELECT equity_usd, mode, source FROM risk_manual_baselines "
                "WHERE period = ? AND boundary = ?",
                (normalized_period, boundary_utc.isoformat()),
            ).fetchone()
        if row is None:
            if required:
                raise ValueError(
                    f"manual {normalized_period} equity baseline not found at "
                    f"{boundary_utc.isoformat()}"
                )
            return None
        equity = float(row[0])
        if equity <= 0:
            raise ValueError("stored manual baseline equity must be positive")
        return EquityBoundaryBaseline(
            boundary=boundary_utc,
            equity_usd=equity,
            before_at=boundary_utc,
            after_at=boundary_utc,
            before_gap_seconds=0.0,
            after_gap_seconds=0.0,
            provenance="manual",
            mode=str(row[1]),
            source=str(row[2]),
        )
