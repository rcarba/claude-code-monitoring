"""Parse Claude Code JSONL transcripts into a SQLite database.

Idempotent: dedups by message id. Safe to run on a loop.
"""

from __future__ import annotations

import json
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from pricing import cost_usd

PROJECTS_DIR = Path(os.environ.get("CLAUDE_PROJECTS_DIR", "/claude-projects"))
DB_PATH = Path(os.environ.get("DB_PATH", "/data/tokens.db"))
INTERVAL = int(os.environ.get("INTERVAL_SECONDS", "300"))

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    message_id        TEXT PRIMARY KEY,
    session_id        TEXT NOT NULL,
    ts                TEXT NOT NULL,
    ts_epoch          INTEGER NOT NULL,
    project           TEXT NOT NULL,
    cwd               TEXT,
    model             TEXT,
    role              TEXT,
    input_tokens      INTEGER DEFAULT 0,
    output_tokens     INTEGER DEFAULT 0,
    cache_5m          INTEGER DEFAULT 0,
    cache_1h          INTEGER DEFAULT 0,
    cache_read        INTEGER DEFAULT 0,
    total_tokens      INTEGER DEFAULT 0,
    cost_usd          REAL DEFAULT 0,
    git_branch        TEXT,
    cc_version        TEXT,
    source_file       TEXT
);
CREATE INDEX IF NOT EXISTS idx_events_ts_epoch    ON events(ts_epoch);
CREATE INDEX IF NOT EXISTS idx_events_project_ts  ON events(project, ts_epoch);
CREATE INDEX IF NOT EXISTS idx_events_session     ON events(session_id);
CREATE INDEX IF NOT EXISTS idx_events_model       ON events(model);

CREATE TABLE IF NOT EXISTS files (
    path        TEXT PRIMARY KEY,
    size        INTEGER NOT NULL,
    mtime       REAL    NOT NULL,
    ingested_at TEXT    NOT NULL
);
"""


def project_name_from_dir(dirname: str) -> str:
    """`-home-raul-Projectes-foo` -> `foo` (last segment, more readable)."""
    parts = dirname.lstrip("-").split("-")
    return parts[-1] if parts else dirname


def open_db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.executescript(SCHEMA)
    conn.execute("PRAGMA journal_mode=DELETE")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def parse_line(raw: str, project: str, source: str) -> dict | None:
    try:
        rec = json.loads(raw)
    except json.JSONDecodeError:
        return None
    msg = rec.get("message")
    if not isinstance(msg, dict):
        return None
    usage = msg.get("usage")
    if not isinstance(usage, dict):
        return None
    msg_id = msg.get("id")
    if not msg_id:
        return None

    cache_create = usage.get("cache_creation") or {}
    cache_5m = int(cache_create.get("ephemeral_5m_input_tokens") or 0)
    cache_1h = int(cache_create.get("ephemeral_1h_input_tokens") or 0)
    if cache_5m == 0 and cache_1h == 0:
        cache_5m = int(usage.get("cache_creation_input_tokens") or 0)

    input_tokens = int(usage.get("input_tokens") or 0)
    output_tokens = int(usage.get("output_tokens") or 0)
    cache_read = int(usage.get("cache_read_input_tokens") or 0)
    model = msg.get("model") or ""
    total = input_tokens + output_tokens + cache_5m + cache_1h + cache_read

    ts_iso = rec.get("timestamp") or ""
    try:
        ts_epoch = int(datetime.fromisoformat(ts_iso.replace("Z", "+00:00")).timestamp())
    except ValueError:
        ts_epoch = 0

    return {
        "message_id":    msg_id,
        "session_id":    rec.get("sessionId") or "",
        "ts":            ts_iso,
        "ts_epoch":      ts_epoch,
        "project":       project,
        "cwd":           rec.get("cwd"),
        "model":         model,
        "role":          msg.get("role"),
        "input_tokens":  input_tokens,
        "output_tokens": output_tokens,
        "cache_5m":      cache_5m,
        "cache_1h":      cache_1h,
        "cache_read":    cache_read,
        "total_tokens":  total,
        "cost_usd":      cost_usd(model, input_tokens, output_tokens, cache_5m, cache_1h, cache_read),
        "git_branch":    rec.get("gitBranch"),
        "cc_version":    rec.get("version"),
        "source_file":   source,
    }


INSERT_SQL = """
INSERT OR IGNORE INTO events (
    message_id, session_id, ts, ts_epoch, project, cwd, model, role,
    input_tokens, output_tokens, cache_5m, cache_1h, cache_read,
    total_tokens, cost_usd, git_branch, cc_version, source_file
) VALUES (
    :message_id, :session_id, :ts, :ts_epoch, :project, :cwd, :model, :role,
    :input_tokens, :output_tokens, :cache_5m, :cache_1h, :cache_read,
    :total_tokens, :cost_usd, :git_branch, :cc_version, :source_file
)
"""


def ingest_file(conn: sqlite3.Connection, path: Path, project: str) -> int:
    rows = []
    with path.open("r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            row = parse_line(line, project, str(path))
            if row:
                rows.append(row)
    if not rows:
        return 0
    cur = conn.executemany(INSERT_SQL, rows)
    return cur.rowcount


def file_already_ingested(conn: sqlite3.Connection, path: Path) -> bool:
    stat = path.stat()
    cur = conn.execute(
        "SELECT size, mtime FROM files WHERE path = ?", (str(path),)
    )
    r = cur.fetchone()
    if r is None:
        return False
    return r[0] == stat.st_size and abs(r[1] - stat.st_mtime) < 0.001


def mark_file_ingested(conn: sqlite3.Connection, path: Path) -> None:
    stat = path.stat()
    conn.execute(
        "INSERT OR REPLACE INTO files (path, size, mtime, ingested_at) VALUES (?, ?, ?, datetime('now'))",
        (str(path), stat.st_size, stat.st_mtime),
    )


def run_once(conn: sqlite3.Connection) -> tuple[int, int]:
    files_processed = 0
    rows_inserted = 0
    for project_dir in sorted(PROJECTS_DIR.iterdir()):
        if not project_dir.is_dir():
            continue
        project = project_name_from_dir(project_dir.name)
        for jsonl in project_dir.glob("*.jsonl"):
            if file_already_ingested(conn, jsonl):
                continue
            inserted = ingest_file(conn, jsonl, project)
            mark_file_ingested(conn, jsonl)
            conn.commit()
            files_processed += 1
            rows_inserted += inserted
    return files_processed, rows_inserted


def main() -> int:
    conn = open_db(DB_PATH)
    one_shot = "--once" in sys.argv

    while True:
        if not PROJECTS_DIR.exists():
            print(
                f"[ingest] projects dir not found: {PROJECTS_DIR} — "
                f"will retry. Run Claude Code at least once to create it.",
                flush=True,
            )
            if one_shot:
                return 1
            time.sleep(INTERVAL)
            continue

        start = time.time()
        files_n, rows_n = run_once(conn)
        elapsed = time.time() - start
        print(
            f"[ingest] {files_n} files processed, {rows_n} rows inserted "
            f"({elapsed:.1f}s)",
            flush=True,
        )
        if one_shot:
            return 0
        time.sleep(INTERVAL)


if __name__ == "__main__":
    sys.exit(main())
