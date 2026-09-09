from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field, model_validator


class KeyConfig(BaseModel):
    private_key: Path = Path("/etc/parquet/private_key.pem")
    public_key: Path = Path("/etc/parquet/public_key.pem")


class GitHubConfig(BaseModel):
    enabled: bool = False
    repository: str = "Moncloa/Parquet"
    runtime_pr: int = 1
    token_file: Path = Path("/etc/parquet/github_token")


class EtoroConfig(BaseModel):
    enabled: bool = False
    api_key_file: Path = Path("/etc/parquet/etoro_api_key")
    user_key_file: Path = Path("/etc/parquet/etoro_user_key")
    base_url: str = "https://public-api.etoro.com/api/v1"
    execution_base_url: str = "https://public-api.etoro.com/api/v2"
    expected_gcid: int | None = Field(default=None, gt=0)
    required_real_scopes: list[str] = Field(
        default_factory=lambda: [
            "etoro-public:real:read",
            "etoro-public:real:write",
            "etoro-public:trade.real:read",
            "etoro-public:trade.real:write",
        ]
    )
    review_symbols: list[str] = Field(
        default_factory=lambda: [
            "GER40",
            "NSDQ100",
            "SPX500",
            "GOLD",
            "OIL",
            "EURUSD",
            "USDJPY",
        ]
    )
    instrument_ids: dict[str, int] = Field(default_factory=dict)
    max_quote_age_seconds: int = Field(default=120, ge=1, le=3600)
    account_poll_seconds: int = Field(default=60, ge=10, le=3600)
    history_sample_seconds: int = Field(default=20, ge=10, le=300)
    history_retention_minutes: int = Field(default=120, ge=15, le=1440)
    history_context_points: int = Field(default=30, ge=10, le=100)
    websocket_enabled: bool = True
    websocket_url: str = "wss://ws.etoro.com/ws"
    websocket_universe_size: int = Field(default=500, ge=20, le=5000)
    websocket_shortlist_size: int = Field(default=20, ge=5, le=100)
    websocket_rotation_minutes: int = Field(default=15, ge=5, le=240)
    websocket_points_per_instrument: int = Field(default=1000, ge=20, le=5000)


class StrategyConfig(BaseModel):
    enabled: bool = False
    provider: str = "codex_cli"
    queue_dir: Path = Path("/var/lib/parquet-exchange")

    @model_validator(mode="after")
    def validate_strategy(self) -> StrategyConfig:
        if self.provider != "codex_cli":
            raise ValueError("strategy.provider must be codex_cli")
        return self


class ExecutionConfig(BaseModel):
    live_test_enabled: bool = False
    live_test_max_amount_usd: float = Field(default=25.0, gt=0, le=100.0)
    autonomous_enabled: bool = False
    autonomous_mode: str = "shadow"
    supervised_real_enabled: bool = False
    supervised_real_max_amount_usd: float = Field(default=25.0, gt=0, le=100.0)
    supervised_real_max_leverage: int = Field(default=20, ge=1, le=100)
    broker_lookup_attempts: int = Field(default=10, ge=1, le=60)
    broker_lookup_interval_seconds: float = Field(default=0.5, ge=0.0, le=5.0)

    @model_validator(mode="after")
    def validate_autonomous_mode(self) -> ExecutionConfig:
        if self.autonomous_mode not in {"shadow", "demo"}:
            raise ValueError("autonomous_mode must be shadow or demo")
        return self


class RiskConfig(BaseModel):
    stop_loss_required: bool = True
    max_signal_age_minutes: int = 15
    max_open_positions: int = 2
    max_trades_per_day: int = 5
    max_daily_loss_pct: float = 3.0
    max_weekly_loss_pct: float = 6.0
    max_risk_per_trade_pct: float = Field(default=1.0, gt=0, le=100)
    max_position_notional_pct: float = Field(default=100.0, gt=0, le=100)
    max_spread_bps: float = Field(default=30.0, gt=0)
    max_entry_slippage_bps: float = Field(default=20.0, ge=0)
    max_risk_snapshot_age_seconds: int = Field(default=120, ge=1, le=3600)
    allow_duplicate_symbol_positions: bool = False


class StructuralReview(BaseModel):
    name: str
    timezone: str
    hour: int = Field(ge=0, le=23)
    minute: int = Field(ge=0, le=59)
    offset_minutes: int = Field(default=1, ge=0, le=60)
    weekdays: list[int] = Field(default_factory=lambda: [0, 1, 2, 3, 4])
    calendar: str | None = None


class ScheduleConfig(BaseModel):
    timezone: str = "Europe/Madrid"
    structural_reviews: list[StructuralReview] = Field(default_factory=list)


class Settings(BaseModel):
    mode: str = "shadow"
    state_db: Path = Path("/var/lib/parquet/parquet.db")
    host: str = "127.0.0.1"
    port: int = 8787
    poll_seconds: int = 20
    keys: KeyConfig = Field(default_factory=KeyConfig)
    github: GitHubConfig = Field(default_factory=GitHubConfig)
    etoro: EtoroConfig = Field(default_factory=EtoroConfig)
    strategy: StrategyConfig = Field(default_factory=StrategyConfig)
    execution: ExecutionConfig = Field(default_factory=ExecutionConfig)
    risk: RiskConfig = Field(default_factory=RiskConfig)
    schedule: ScheduleConfig = Field(default_factory=ScheduleConfig)


def _deep_set(data: dict[str, Any], path: list[str], value: Any) -> None:
    cursor = data
    for key in path[:-1]:
        cursor = cursor.setdefault(key, {})
    cursor[path[-1]] = value


def load_settings(path: Path | None = None) -> Settings:
    config_path = path or Path(os.getenv("PARQUET_CONFIG", "/etc/parquet/parquet.yaml"))
    raw: dict[str, Any] = {}
    if config_path.exists():
        loaded = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
        if not isinstance(loaded, dict):
            raise ValueError(f"Config root must be a mapping: {config_path}")
        raw = loaded

    env_overrides = {
        "PARQUET_PRIVATE_KEY": (["keys", "private_key"], str),
        "PARQUET_PUBLIC_KEY": (["keys", "public_key"], str),
        "PARQUET_GITHUB_TOKEN_FILE": (["github", "token_file"], str),
        "PARQUET_ETORO_API_KEY_FILE": (["etoro", "api_key_file"], str),
        "PARQUET_ETORO_USER_KEY_FILE": (["etoro", "user_key_file"], str),
    }
    for env_name, (keys, cast) in env_overrides.items():
        if env_name in os.environ:
            _deep_set(raw, keys, cast(os.environ[env_name]))

    return Settings.model_validate(raw)
