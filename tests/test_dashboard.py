from datetime import UTC, datetime, timedelta

from parquet.dashboard import position_payload, render_positions_dashboard
from parquet.portfolio import (
    BrokerPortfolioSnapshot,
    BrokerPosition,
    ManagedPosition,
    PositionManager,
)
from parquet.storage import Storage


def _snapshot(now: datetime, positions: list[BrokerPosition]) -> BrokerPortfolioSnapshot:
    return BrokerPortfolioSnapshot(
        captured_at=now,
        equity_usd=10_000.0,
        available_cash_usd=9_900.0,
        invested_usd=100.0,
        unrealized_pnl_usd=sum(item.unrealized_pnl_usd for item in positions),
        credit_usd=10_000.0,
        positions=positions,
    )


def test_reconciliation_tracks_open_then_closed_pnl(tmp_path) -> None:
    storage = Storage(tmp_path / "state.db")
    manager = PositionManager(storage)
    opened = datetime(2026, 9, 7, 12, 0, tzinfo=UTC)
    storage.save_managed_position(
        ManagedPosition(
            local_id="attempt-1",
            broker_position_id="position-1",
            proposal_id="proposal-1",
            instrument_id=42,
            symbol="GOLD",
            side="BUY",
            opened_at=opened,
        )
    )

    manager.reconcile(
        _snapshot(
            opened + timedelta(minutes=5),
            [
                BrokerPosition(
                    position_id="position-1",
                    instrument_id=42,
                    symbol="GOLD",
                    side="BUY",
                    amount_usd=25.0,
                    open_rate=3500.0,
                    stop_loss_rate=3480.0,
                    take_profit_rate=3540.0,
                    unrealized_pnl_usd=1.25,
                )
            ],
        )
    )
    active = storage.active_managed_positions()[0]
    assert active.amount_usd == 25.0
    assert active.last_unrealized_pnl_usd == 1.25

    manager.reconcile(_snapshot(opened + timedelta(minutes=10), []))
    closed = storage.managed_positions()[0]
    assert closed.status == "CLOSED_AT_BROKER"
    assert closed.realized_pnl_usd == 1.25
    assert closed.pnl_estimated is True
    assert closed.closed_at == opened + timedelta(minutes=10)


def test_dashboard_shows_git_graph_and_absolute_and_percentage_pnl() -> None:
    now = datetime(2026, 9, 7, 12, 0, tzinfo=UTC)
    closed = ManagedPosition(
        local_id="attempt-2",
        broker_position_id="position-2",
        instrument_id=7,
        symbol="NSDQ100",
        side="SELL",
        opened_at=now - timedelta(hours=1),
        status="CLOSED_AT_BROKER",
        amount_usd=25.0,
        closed_at=now,
        realized_pnl_usd=2.5,
        pnl_estimated=True,
    )
    html = render_positions_dashboard([closed])
    assert "git graph" in html
    assert "NSDQ100" in html
    assert "+2.50 USD" in html
    assert "+10.00%" in html
    assert "resultado estimado" in html


def test_position_json_payload_has_pnl_percentage() -> None:
    now = datetime(2026, 9, 7, 12, 0, tzinfo=UTC)
    position = ManagedPosition(
        local_id="attempt-3",
        broker_position_id="position-3",
        instrument_id=8,
        symbol="OIL",
        side="BUY",
        opened_at=now,
        amount_usd=20.0,
        last_unrealized_pnl_usd=-1.0,
    )
    payload = position_payload(position)
    assert payload["pnl_usd"] == -1.0
    assert payload["pnl_pct"] == -5.0
