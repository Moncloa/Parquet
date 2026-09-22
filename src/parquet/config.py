from __future__ import annotations

import os
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

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
    demo_user_key_file: Path = Path("/etc/parquet/etoro_demo_user_key")
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
    required_demo_scopes: list[str] = Field(
        default_factory=lambda: [
            "etoro-public:demo:read",
            "etoro-public:demo:write",
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


class LocalScreenerConfig(BaseModel):
    enabled: bool = True
    base_url: str = "http://127.0.0.1:11434"
    model: str = "qwen3.5:4b"
    input_candidates: int = Field(default=10, ge=3, le=30)
    output_candidates: int = Field(default=3, ge=1, le=5)
    min_deterministic_score: float = Field(default=0.05, ge=0.0, le=10.0)
    timeout_seconds: int = Field(default=45, ge=5, le=180)
    keep_alive: str = "15m"
    context_length: int = Field(default=4096, ge=2048, le=32768)
    max_output_tokens: int = Field(default=128, ge=64, le=512)

    @model_validator(mode="after")
    def validate_local_endpoint(self) -> LocalScreenerConfig:
        parsed = urlparse(self.base_url)
        if parsed.scheme != "http" or parsed.hostname not in {"127.0.0.1", "localhost", "::1"}:
            raise ValueError("local_screener.base_url must use HTTP on loopback")
        if self.output_candidates > self.input_candidates:
            raise ValueError("local_screener.output_candidates cannot exceed input_candidates")
        if not self.model.strip():
            raise ValueError("local_screener.model cannot be empty")
        return self


class StrategyConfig(BaseModel):
    enabled: bool = False
    provider: str = "codex_cli"
    queue_dir: Path = Path("/var/lib/parquet-exchange")

    @model_validator(mode="after")
    def validate_strategy(self) -> StrategyConfig:
        if self.provider not in {"local_ollama", "codex_cli", "openai_api"}:
            raise ValueError(
                "strategy.provider must be local_ollama, codex_cli or openai_api"
            )
        return self


class ExecutionConfig(BaseModel):
    live_test_enabled: bool = False
    live_test_max_amount_usd: float = Field(default=25.0, gt=0, le=100.0)
    autonomous_enabled: bool = False
    autonomous_mode: str = "shadow"
    autonomous_demo_enabled: bool = False
    autonomous_demo_max_amount_usd: float = Field(default=25.0, gt=0, le=1000.0)
    autonomous_demo_max_leverage: int = Field(default=2, ge=1, le=100)
    autonomous_real_enabled: bool = False
    autonomous_real_min_position_pct: float = Field(default=10.0, gt=0, le=100)
    autonomous_real_max_position_pct: float = Field(default=50.0, gt=0, le=100)
    autonomous_real_max_leverage: int = Field(default=2, ge=1, le=100)
    supervised_real_enabled: bool = False
    supervised_real_max_amount_usd: float = Field(default=25.0, gt=0, le=100.0)
    supervised_real_max_leverage: int = Field(default=20, ge=1, le=100)
    broker_lookup_attempts: int = Field(default=30, ge=1, le=60)
    broker_lookup_interval_seconds: float = Field(default=0.5, ge=0.0, le=5.0)

    @model_validator(mode="after")
    def validate_autonomous_mode(self) -> ExecutionConfig:
        if self.autonomous_mode not in {"shadow", "demo", "real"}:
            raise ValueError("autonomous_mode must be shadow, demo or real")
        if self.autonomous_real_min_position_pct > self.autonomous_real_max_position_pct:
            raise ValueError(
                "autonomous_real_min_position_pct cannot exceed "
                "autonomous_real_max_position_pct"
            )
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
    min_stop_distance_bps: float = Field(default=25.0, ge=0)
    min_stop_spread_multiple: float = Field(default=3.0, ge=0)
    min_stop_volatility_multiple: float = Field(default=4.0, ge=0)
    min_stop_range_60m_fraction: float = Field(default=0.15, ge=0, le=1)
    min_stop_recent_move_fraction: float = Field(default=0.25, ge=0, le=1)
    max_risk_snapshot_age_seconds: int = Field(default=120, ge=1, le=3600)
    allow_duplicate_symbol_positions: bool = False
    # Cost-aware entry hurdle. The broker what-if endpoint provides opening
    # costs; estimate a round trip conservatively until exact close costs exist.
    net_edge_enabled: bool = True
    estimated_round_trip_cost_multiplier: float = Field(default=2.0, ge=1.0, le=5.0)
    min_net_reward_risk: float = Field(default=1.20, ge=0.0, le=10.0)
    min_gross_reward_to_cost: float = Field(default=3.0, ge=0.0, le=100.0)
    # Net-P&L exit management. Automated closes are opt-in until validated in
    # production; evaluation/journaling can be enabled independently.
    net_exit_enabled: bool = True
    net_exit_real_close_enabled: bool = False
    net_exit_take_profit_r: float = Field(default=1.50, gt=0.0, le=20.0)
    net_exit_protect_profit_r: float = Field(default=0.75, gt=0.0, le=20.0)


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
    local_screener: LocalScreenerConfig = Field(default_factory=LocalScreenerConfig)
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
        "PARQUET_ETORO_DEMO_USER_KEY_FILE": (["etoro", "demo_user_key_file"], str),
        "PARQUET_LOCAL_SCREENER_URL": (["local_screener", "base_url"], str),
        "PARQUET_LOCAL_SCREENER_MODEL": (["local_screener", "model"], str),
    }
    for env_name, (keys, cast) in env_overrides.items():
        if env_name in os.environ:
            _deep_set(raw, keys, cast(os.environ[env_name]))

    return Settings.model_validate(raw)
