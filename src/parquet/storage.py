from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from threading import RLock

from parquet.models import RiskSnapshot, TradeProposal, WatchItem
from parquet.scheduler import ScheduledReview

SCHEMA = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS kv (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS analyses (
    analysis_id TEXT PRIMARY KEY,
    generated_at TEXT NOT NULL,
    payload TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    kind TEXT NOT NULL,
    payload TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS scheduled_reviews (
    review_key TEXT PRIMARY KEY,
    due_at TEXT NOT NULL,
    reason TEXT NOT NULL,
    source TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS watches (
    watch_id TEXT PRIMARY KEY,
    analysis_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    status TEXT NOT NULL,
    payload TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS proposals (
    proposal_id TEXT PRIMARY KEY,
    analysis_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    payload TEXT NOT NULL
);
"""


class Storage:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = RLock()
        self.conn = sqlite3.connect(path, check_same_thread=False)
        with self._lock:
            self.conn.executescript(SCHEMA)
            self.conn.commit()

    def get(self, key: str) -> str | None:
        with self._lock:
            row = self.conn.execute("SELECT value FROM kv WHERE key = ?", (key,)).fetchone()
        return None if row is None else str(row[0])

    def set(self, key: str, value: str) -> None:
        with self._lock:
            self.conn.execute(
                "INSERT INTO kv(key, value) VALUES(?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value),
            )
            self.conn.commit()

    def save_analysis(self, analysis_id: str, generated_at: str, payload: str) -> None:
        with self._lock:
            self.conn.execute(
                "INSERT OR IGNORE INTO analyses(analysis_id, generated_at, payload) "
                "VALUES(?, ?, ?)",
                (analysis_id, generated_at, payload),
            )
            self.conn.commit()

    def add_event(self, kind: str, payload: str) -> None:
        with self._lock:
            self.conn.execute(
                "INSERT INTO events(kind, payload) VALUES(?, ?)",
                (kind, payload),
            )
            self.conn.commit()

    def schedule_review(self, review: ScheduledReview) -> None:
        due_at = review.at.astimezone(UTC).isoformat()
        with self._lock:
            self.conn.execute(
                "INSERT OR REPLACE INTO scheduled_reviews(review_key, due_at, reason, source) "
                "VALUES(?, ?, ?, ?)",
                (review.key, due_at, review.reason, review.source),
            )
            self.conn.commit()

    def delete_review(self, review: ScheduledReview) -> None:
        with self._lock:
            self.conn.execute(
                "DELETE FROM scheduled_reviews WHERE review_key = ?",
                (review.key,),
            )
            self.conn.commit()

    def pending_reviews(self) -> list[ScheduledReview]:
        with self._lock:
            rows = self.conn.execute(
                "SELECT due_at, reason, source FROM scheduled_reviews ORDER BY due_at"
            ).fetchall()
        return [
            ScheduledReview(
                at=datetime.fromisoformat(str(row[0])),
                reason=str(row[1]),
                source=str(row[2]),
            )
            for row in rows
        ]

    def save_watch(self, analysis_id: str, watch: WatchItem) -> None:
        with self._lock:
            self.conn.execute(
                "INSERT OR REPLACE INTO watches"
                "(watch_id, analysis_id, symbol, expires_at, status, payload) "
                "VALUES(?, ?, ?, ?, 'ACTIVE', ?)",
                (
                    watch.watch_id,
                    analysis_id,
                    watch.symbol,
                    watch.expires_at.astimezone(UTC).isoformat(),
                    watch.model_dump_json(),
                ),
            )
            self.conn.commit()

    def active_watches(self, now: datetime | None = None) -> list[WatchItem]:
        current = (now or datetime.now(UTC)).astimezone(UTC).isoformat()
        with self._lock:
            rows = self.conn.execute(
                "SELECT payload FROM watches WHERE status = 'ACTIVE' AND expires_at > ? "
                "ORDER BY symbol, watch_id",
                (current,),
            ).fetchall()
        return [WatchItem.model_validate_json(str(row[0])) for row in rows]

    def set_watch_status(self, watch_id: str, status: str) -> None:
        with self._lock:
            self.conn.execute(
                "UPDATE watches SET status = ? WHERE watch_id = ?",
                (status, watch_id),
            )
            self.conn.commit()

    def save_proposal(self, analysis_id: str, proposal: TradeProposal) -> None:
        with self._lock:
            self.conn.execute(
                "INSERT OR REPLACE INTO proposals"
                "(proposal_id, analysis_id, symbol, expires_at, payload) VALUES(?, ?, ?, ?, ?)",
                (
                    proposal.proposal_id,
                    analysis_id,
                    proposal.symbol,
                    proposal.expires_at.astimezone(UTC).isoformat(),
                    proposal.model_dump_json(),
                ),
            )
            self.conn.commit()

    def get_proposal(self, proposal_id: str) -> TradeProposal | None:
        with self._lock:
            row = self.conn.execute(
                "SELECT payload FROM proposals WHERE proposal_id = ?",
                (proposal_id,),
            ).fetchone()
        if row is None:
            return None
        return TradeProposal.model_validate_json(str(row[0]))

    def set_risk_snapshot(self, snapshot: RiskSnapshot) -> None:
        self.set("risk_snapshot", snapshot.model_dump_json())

    def get_risk_snapshot(self) -> RiskSnapshot | None:
        payload = self.get("risk_snapshot")
        if payload is None:
            return None
        return RiskSnapshot.model_validate_json(payload)
