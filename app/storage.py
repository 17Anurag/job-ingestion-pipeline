"""
Storage layer. SQLite is a deliberate choice for a take-home demo — durable
across restarts (unlike an in-memory dict, which would make "resilience"
claims fake), zero external infra, trivial to inspect.

Three tables carry the resilience story:
  - jobs: deduped listings (the product)
  - quarantine: items that failed schema validation — proof that drift
    doesn't crash the pipeline, it gets set aside for triage
  - source_state: per-adapter circuit breaker + checkpoint state, survives
    process restarts so a redeploy doesn't forget a source was misbehaving
"""
from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Iterator, Optional

DB_PATH = Path(__file__).parent.parent / "data" / "pipeline.db"
DB_PATH.parent.mkdir(exist_ok=True)

SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    dedupe_key TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    source_id TEXT NOT NULL,
    title TEXT NOT NULL,
    company TEXT NOT NULL,
    location TEXT,
    url TEXT NOT NULL,
    posted_at TEXT,
    tags TEXT,
    fetched_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS quarantine (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    raw_payload TEXT NOT NULL,
    error TEXT NOT NULL,
    quarantined_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS source_state (
    name TEXT PRIMARY KEY,
    state TEXT NOT NULL DEFAULT 'closed',
    consecutive_failures INTEGER NOT NULL DEFAULT 0,
    last_success_at TEXT,
    last_attempt_at TEXT,
    last_error TEXT,
    jobs_last_run INTEGER NOT NULL DEFAULT 0,
    opened_at TEXT
);
"""


@contextmanager
def get_conn() -> Iterator[sqlite3.Connection]:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db() -> None:
    with get_conn() as conn:
        conn.executescript(SCHEMA)


def upsert_job(job) -> bool:
    """Returns True if this was a new job, False if it already existed."""
    with get_conn() as conn:
        existing = conn.execute(
            "SELECT 1 FROM jobs WHERE dedupe_key = ?", (job.dedupe_key(),)
        ).fetchone()
        conn.execute(
            """INSERT INTO jobs
               (dedupe_key, source, source_id, title, company, location, url,
                posted_at, tags, fetched_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(dedupe_key) DO UPDATE SET
                 title=excluded.title, company=excluded.company,
                 location=excluded.location, url=excluded.url,
                 fetched_at=excluded.fetched_at""",
            (
                job.dedupe_key(), job.source, job.source_id, job.title,
                job.company, job.location, job.url,
                job.posted_at.isoformat() if job.posted_at else None,
                json.dumps(job.tags), job.fetched_at.isoformat(),
            ),
        )
        return existing is None


def quarantine_item(source: str, raw_payload: dict, error: str) -> None:
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO quarantine (source, raw_payload, error, quarantined_at) VALUES (?, ?, ?, ?)",
            (source, json.dumps(raw_payload, default=str), error, datetime.utcnow().isoformat()),
        )


def list_jobs(limit: int = 100, source: Optional[str] = None) -> list[dict]:
    with get_conn() as conn:
        if source:
            rows = conn.execute(
                "SELECT * FROM jobs WHERE source = ? ORDER BY fetched_at DESC LIMIT ?",
                (source, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM jobs ORDER BY fetched_at DESC LIMIT ?", (limit,)
            ).fetchall()
        return [dict(r) for r in rows]


def count_jobs(source: Optional[str] = None) -> int:
    with get_conn() as conn:
        if source:
            row = conn.execute("SELECT COUNT(*) c FROM jobs WHERE source = ?", (source,)).fetchone()
        else:
            row = conn.execute("SELECT COUNT(*) c FROM jobs").fetchone()
        return row["c"]


def count_quarantine(source: Optional[str] = None) -> int:
    with get_conn() as conn:
        if source:
            row = conn.execute("SELECT COUNT(*) c FROM quarantine WHERE source = ?", (source,)).fetchone()
        else:
            row = conn.execute("SELECT COUNT(*) c FROM quarantine").fetchone()
        return row["c"]


def get_source_state(name: str) -> Optional[dict]:
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM source_state WHERE name = ?", (name,)).fetchone()
        return dict(row) if row else None


def upsert_source_state(**fields) -> None:
    name = fields["name"]
    existing = get_source_state(name)
    with get_conn() as conn:
        if existing is None:
            cols = ", ".join(fields.keys())
            placeholders = ", ".join("?" for _ in fields)
            conn.execute(
                f"INSERT INTO source_state ({cols}) VALUES ({placeholders})",
                tuple(fields.values()),
            )
        else:
            set_clause = ", ".join(f"{k} = ?" for k in fields.keys() if k != "name")
            values = [v for k, v in fields.items() if k != "name"] + [name]
            conn.execute(f"UPDATE source_state SET {set_clause} WHERE name = ?", values)


def all_source_states() -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute("SELECT * FROM source_state").fetchall()
        return [dict(r) for r in rows]
