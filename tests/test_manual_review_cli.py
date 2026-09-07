from __future__ import annotations

from types import SimpleNamespace

from parquet.main import run_request_review_now


class FakeStorage:
    def get_reconciliation_report(self):
        return SimpleNamespace(trading_enabled=True, state=SimpleNamespace(value="SYNCED"))

    def get(self, key: str):
        if key == "etoro_identity_verified":
            return "1"
        return None


class FakeOrchestrator:
    def __init__(self) -> None:
        self.bridge = object()
        self.storage = FakeStorage()
        self.market_client = object()
        self.reviews = []

    def add_review(self, review) -> None:
        self.reviews.append(review)

    async def post_due_reviews(self, now=None) -> int:
        assert len(self.reviews) == 1
        assert self.reviews[0].source == "manual"
        assert self.reviews[0].reason == "manual_opportunity_scan"
        return 1


class FakeReconciliation:
    def __init__(self, settings, storage, market_client) -> None:
        self.calls = 0

    async def poll_once(self, *, force: bool = False) -> int:
        assert force is True
        self.calls += 1
        return 1


def test_request_review_now_posts_one_safe_review(monkeypatch, capsys) -> None:
    settings = SimpleNamespace(
        github=SimpleNamespace(
            repository="Moncloa/Parquet",
            runtime_pr=2,
        )
    )
    orchestrator = FakeOrchestrator()

    monkeypatch.setattr("parquet.main.load_settings", lambda path: settings)
    monkeypatch.setattr("parquet.main.AutonomousOrchestrator", lambda settings: orchestrator)
    monkeypatch.setattr("parquet.main.ReconciliationService", FakeReconciliation)

    result = run_request_review_now(None, "manual_opportunity_scan")

    assert result == 0
    out = capsys.readouterr().out
    assert "Manual review request posted" in out
    assert "No broker order was sent." in out
