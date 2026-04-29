"""Parse Claude Code JSONL transcripts into a SQLite database.

Idempotent: dedups by message id. Safe to run on a loop.
"""

from __future__ import annotations

import json
import os
import sqlite3
import subprocess
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

CREATE TABLE IF NOT EXISTS blocks (
    id                       TEXT PRIMARY KEY,
    start_ts                 INTEGER NOT NULL,
    end_ts                   INTEGER NOT NULL,
    actual_end_ts            INTEGER,
    is_active                INTEGER NOT NULL,
    is_gap                   INTEGER NOT NULL,
    entries                  INTEGER,
    input_tokens             INTEGER,
    output_tokens            INTEGER,
    cache_create             INTEGER,
    cache_read               INTEGER,
    total_tokens             INTEGER,
    cost_usd                 REAL,
    models                   TEXT,
    burn_tokens_per_min      REAL,
    burn_cost_per_hour       REAL,
    projected_total_cost     REAL,
    projected_remaining_min  INTEGER,
    token_limit              INTEGER,
    projected_percent_used   REAL,
    limit_status             TEXT,
    updated_at               INTEGER
);
CREATE INDEX IF NOT EXISTS idx_blocks_active ON blocks(is_active);
CREATE INDEX IF NOT EXISTS idx_blocks_start  ON blocks(start_ts);
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
    _migrate_blocks(conn)
    return conn


def _migrate_blocks(conn: sqlite3.Connection) -> None:
    """Add columns to the blocks table if missing (idempotent)."""
    existing = {r[1] for r in conn.execute("PRAGMA table_info(blocks)")}
    additions = [
        ("token_limit",            "INTEGER"),
        ("projected_percent_used", "REAL"),
        ("limit_status",           "TEXT"),
    ]
    for name, typ in additions:
        if name not in existing:
            conn.execute(f"ALTER TABLE blocks ADD COLUMN {name} {typ}")
    conn.commit()


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


def _iso_to_epoch(s: str | None) -> int | None:
    if not s:
        return None
    try:
        return int(datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp())
    except ValueError:
        return None


def fetch_ccusage_blocks() -> list[dict]:
    try:
        r = subprocess.run(
            ["ccusage", "blocks", "--json", "--token-limit", "max"],
            capture_output=True, text=True, timeout=120, check=True,
        )
    except (subprocess.SubprocessError, FileNotFoundError) as e:
        print(f"[ccusage] failed to run: {e}", flush=True)
        return []
    try:
        return json.loads(r.stdout).get("blocks", [])
    except json.JSONDecodeError as e:
        print(f"[ccusage] bad json: {e}", flush=True)
        return []


BLOCKS_INSERT_SQL = """
INSERT OR REPLACE INTO blocks (
    id, start_ts, end_ts, actual_end_ts, is_active, is_gap, entries,
    input_tokens, output_tokens, cache_create, cache_read, total_tokens,
    cost_usd, models, burn_tokens_per_min, burn_cost_per_hour,
    projected_total_cost, projected_remaining_min,
    token_limit, projected_percent_used, limit_status, updated_at
) VALUES (
    :id, :start_ts, :end_ts, :actual_end_ts, :is_active, :is_gap, :entries,
    :input_tokens, :output_tokens, :cache_create, :cache_read, :total_tokens,
    :cost_usd, :models, :burn_tokens_per_min, :burn_cost_per_hour,
    :projected_total_cost, :projected_remaining_min,
    :token_limit, :projected_percent_used, :limit_status, strftime('%s','now')
)
"""


def store_blocks(conn: sqlite3.Connection, blocks: list[dict]) -> int:
    rows = []
    for b in blocks:
        tc = b.get("tokenCounts") or {}
        br = b.get("burnRate") or {}
        pj = b.get("projection") or {}
        ls = b.get("tokenLimitStatus") or {}
        rows.append({
            "id":            b.get("id"),
            "start_ts":      _iso_to_epoch(b.get("startTime")),
            "end_ts":        _iso_to_epoch(b.get("endTime")),
            "actual_end_ts": _iso_to_epoch(b.get("actualEndTime")),
            "is_active":     1 if b.get("isActive") else 0,
            "is_gap":        1 if b.get("isGap") else 0,
            "entries":       b.get("entries"),
            "input_tokens":  tc.get("inputTokens"),
            "output_tokens": tc.get("outputTokens"),
            "cache_create":  tc.get("cacheCreationInputTokens"),
            "cache_read":    tc.get("cacheReadInputTokens"),
            "total_tokens":  b.get("totalTokens"),
            "cost_usd":      b.get("costUSD"),
            "models":        ",".join(b.get("models") or []),
            "burn_tokens_per_min":     br.get("tokensPerMinute"),
            "burn_cost_per_hour":      br.get("costPerHour"),
            "projected_total_cost":    pj.get("totalCost"),
            "projected_remaining_min": pj.get("remainingMinutes"),
            "token_limit":             ls.get("limit"),
            "projected_percent_used":  ls.get("percentUsed"),
            "limit_status":            ls.get("status"),
        })
    if not rows:
        return 0
    # Mark stale 'is_active' blocks as inactive — only keep the truly active one.
    conn.execute("UPDATE blocks SET is_active = 0 WHERE is_active = 1")
    conn.executemany(BLOCKS_INSERT_SQL, rows)
    conn.commit()
    return len(rows)


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
        blocks_n = store_blocks(conn, fetch_ccusage_blocks())
        elapsed = time.time() - start
        print(
            f"[ingest] {files_n} files processed, {rows_n} rows inserted, "
            f"{blocks_n} blocks synced ({elapsed:.1f}s)",
            flush=True,
        )
        if one_shot:
            return 0
        time.sleep(INTERVAL)


if __name__ == "__main__":
    sys.exit(main())
