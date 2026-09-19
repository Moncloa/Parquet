from pathlib import Path


def test_broker_service_can_write_only_state_and_strategy_exchange() -> None:
    unit = Path("deploy/parquet.service").read_text(encoding="utf-8")
    assert "ProtectSystem=strict" in unit
    assert "ReadWritePaths=/var/lib/parquet /var/lib/parquet-exchange" in unit
    assert "ReadOnlyPaths=/etc/parquet" in unit


def test_strategy_service_cannot_access_broker_credentials() -> None:
    unit = Path("deploy/parquet-strategy.service").read_text(encoding="utf-8")
    assert "User=parquet-strategy" in unit
    assert "InaccessiblePaths=/etc/parquet" in unit
    assert "ReadWritePaths=/var/lib/parquet-exchange /var/lib/parquet-strategy" in unit


def test_update_script_refuses_non_main_without_explicit_override() -> None:
    script = Path("scripts/update.sh").read_text(encoding="utf-8")
    assert 'CURRENT_BRANCH="$(git branch --show-current)"' in script
    assert '"$CURRENT_BRANCH" != "main"' in script
    assert "--allow-non-main" in script
    assert "refusing to update non-main branch" in script
