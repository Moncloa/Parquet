from datetime import UTC, datetime

from parquet.api import _strategy_state
from parquet.config import Settings, StrategyConfig
from parquet.strategy import StrategyQueue


class FakeStorage:
    def __init__(self, values: dict[str, str]) -> None:
        self.values = values

    def get(self, key: str) -> str | None:
        return self.values.get(key)


class FakeOrchestrator:
    def __init__(self, storage: FakeStorage) -> None:
        self.storage = storage


def test_strategy_state_marks_usage_limit_unavailable(tmp_path) -> None:  # type: ignore[no-untyped-def]
    queue_dir = tmp_path / "exchange"
    queue = StrategyQueue(queue_dir)
    queue.write_worker_status(
        {
            "heartbeat_at": datetime.now(UTC).isoformat(),
            "codex_authenticated": True,
        }
    )
    settings = Settings(strategy=StrategyConfig(enabled=True, queue_dir=queue_dir))
    orchestrator = FakeOrchestrator(
        FakeStorage(
            {
                "strategy_usage_limited": "1",
                "strategy_usage_retry_at": "2026-09-09T15:24:13+00:00",
            }
        )
    )

    state = _strategy_state(settings, orchestrator)  # type: ignore[arg-type]

    assert state["worker_alive"] is True
    assert state["worker_ready"] is False
    assert state["usage_limited"] is True
    assert state["retry_at"] == "2026-09-09T15:24:13+00:00"
