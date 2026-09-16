from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from parquet.portfolio import BrokerPortfolioSnapshot
from parquet.risk_ledger import LocalEquityRiskLedger
from parquet.storage import Storage

BOUNDARY = datetime(2026, 9, 16, 0, 0, tzinfo=UTC)


def _snapshot(at: datetime, equity: float) -> BrokerPortfolioSnapshot:
    return BrokerPortfolioSnapshot(
        captured_at=at,
        equity_usd=equity,
        available_cash_usd=equity,
        invested_usd=0.0,
        unrealized_pnl_usd=0.0,
        credit_usd=equity,
        positions=[],
    )


def test_manual_baseline_is_auditable_and_preferred(tmp_path) -> None:
    storage = Storage(tmp_path / "state.db")
    ledger = LocalEquityRiskLedger(storage)
    ledger.record(_snapshot(BOUNDARY - timedelta(seconds=30), 9990.0))
    ledger.record(_snapshot(BOUNDARY + timedelta(seconds=30), 9990.0))

    seeded = ledger.seed_manual_baseline(
        period="daily",
        boundary=BOUNDARY,
        equity_usd=9999.98,
        mode="exact",
        source="operator:flat account; no trades since boundary",
        created_at=BOUNDARY + timedelta(hours=12),
    )
    result = ledger.baseline(BOUNDARY, label="daily")

    assert seeded.equity_usd == pytest.approx(9999.98)
    assert result.equity_usd == pytest.approx(9999.98)
    assert result.provenance == "manual"
    assert result.mode == "exact"
    assert result.source == "operator:flat account; no trades since boundary"
    event = storage.conn.execute(
        "SELECT kind, payload FROM events ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert event is not None
    assert event[0] == "manual_risk_baseline_seeded"
    assert "9999.98" in str(event[1])


def test_manual_conservative_upper_bound_is_preserved(tmp_path) -> None:
    storage = Storage(tmp_path / "state.db")
    ledger = LocalEquityRiskLedger(storage)

    result = ledger.seed_manual_baseline(
        period="weekly",
        boundary=datetime(2026, 9, 14, 0, 0, tzinfo=UTC),
        equity_usd=10000.17,
        mode="conservative_upper_bound",
        source="CRV position TP-derived upper bound",
    )

    assert result.provenance == "manual"
    assert result.mode == "conservative_upper_bound"
    assert result.equity_usd == pytest.approx(10000.17)


def test_manual_baseline_rejects_unlabelled_or_invalid_values(tmp_path) -> None:
    storage = Storage(tmp_path / "state.db")
    ledger = LocalEquityRiskLedger(storage)

    with pytest.raises(ValueError, match="source must not be empty"):
        ledger.seed_manual_baseline(
            period="daily",
            boundary=BOUNDARY,
            equity_usd=9999.98,
            mode="exact",
            source="",
        )
    with pytest.raises(ValueError, match="mode must be"):
        ledger.seed_manual_baseline(
            period="daily",
            boundary=BOUNDARY,
            equity_usd=9999.98,
            mode="guess",
            source="test",
        )
