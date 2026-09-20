"""
Unified Database Adapter for tg-download-upload-bot.

Supports:
- Remote LibSQL / Turso cloud database (via libsql:// with auth token)
- Local SQLite database (fallback or offline development)
- Multi-task persistence, media tracking, crash recovery, and logging.
"""

from __future__ import annotations

import env_loader  # Ensures .env is loaded

import logging
import os
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional, Union

try:
    import libsql
except ImportError:
    libsql = None

log = logging.getLogger("tg-db")


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class Database:
    def __init__(
        self,
        db_path: Optional[Union[str, Path]] = None,
        remote_url: Optional[str] = None,
        auth_token: Optional[str] = None,
    ):
        self.lock = threading.Lock()
        self.remote_url = (remote_url or os.environ.get("LIBSQL_URL", "")).strip()
        self.auth_token = (auth_token or os.environ.get("LIBSQL_AUTH_TOKEN", "")).strip()
        self.db_path = Path(db_path or os.environ.get("DB_PATH", "progress.db"))
        self.is_remote = False
        self.conn = None

        self._connect()
        self._init_schema()

    def _connect(self) -> None:
        if self.remote_url and self.remote_url.startswith(("libsql://", "https://", "http://")):
            if libsql is None:
                log.warning("libsql package not available. Falling back to local SQLite: %s", self.db_path)
            else:
                try:
                    log.info("Connecting to remote LibSQL/Turso: %s", self.remote_url)
                    self.conn = libsql.connect(self.remote_url, auth_token=self.auth_token or None)
                    self.is_remote = True
                    log.info("Connected to remote Turso/LibSQL database successfully.")
                    return
                except Exception as e:
                    log.error("Failed to connect to remote LibSQL (%s): %s. Falling back to local SQLite.", self.remote_url, e)

        # Local fallback
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        if libsql is not None:
            try:
                self.conn = libsql.connect(str(self.db_path))
                self.is_remote = False
                log.info("Using local database via libsql: %s", self.db_path)
                return
            except Exception:
                pass

        self.conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.is_remote = False
        log.info("Using local SQLite database: %s", self.db_path)

    def _init_schema(self) -> None:
        with self.lock:
            cur = self.conn.cursor()
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS tasks (
                    id              TEXT PRIMARY KEY,
                    name            TEXT NOT NULL,
                    source_peer     TEXT NOT NULL,
                    source_id       INTEGER,
                    source_title    TEXT,
                    dest_peer       TEXT,
                    dest_id         INTEGER,
                    dest_title      TEXT,
                    dest_payload    TEXT,
                    status          TEXT NOT NULL DEFAULT 'pending',
                    workers         INTEGER NOT NULL DEFAULT 3,
                    max_file_size   INTEGER DEFAULT 2097152000,
                    dl_parts        INTEGER DEFAULT 4,
                    ul_parts        INTEGER DEFAULT 4,
                    scan_complete   INTEGER DEFAULT 0,
                    scan_cursor     INTEGER DEFAULT 0,
                    last_seen       INTEGER DEFAULT 0,
                    catchup_cursor  INTEGER DEFAULT 0,
                    catchup_stop    INTEGER DEFAULT 0,
                    total_items     INTEGER DEFAULT 0,
                    done_items      INTEGER DEFAULT 0,
                    failed_items    INTEGER DEFAULT 0,
                    skipped_items   INTEGER DEFAULT 0,
                    pending_bytes   INTEGER DEFAULT 0,
                    done_bytes      INTEGER DEFAULT 0,
                    error_message   TEXT,
                    created_at      TEXT NOT NULL,
                    updated_at      TEXT NOT NULL
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS task_media (
                    task_id         TEXT NOT NULL,
                    chat_id         INTEGER NOT NULL,
                    message_id      INTEGER NOT NULL,
                    file_name       TEXT,
                    file_size       INTEGER,
                    date            TEXT,
                    status          TEXT NOT NULL DEFAULT 'pending',
                    dest_message_id INTEGER,
                    attempts        INTEGER NOT NULL DEFAULT 0,
                    error           TEXT,
                    updated_at      TEXT,
                    PRIMARY KEY (task_id, chat_id, message_id)
                );
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_tm_task_status ON task_media (task_id, status);
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS task_logs (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id     TEXT NOT NULL,
                    level       TEXT NOT NULL,
                    message     TEXT NOT NULL,
                    timestamp   TEXT NOT NULL
                );
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_task_logs ON task_logs (task_id, id DESC);
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS meta (
                    key         TEXT PRIMARY KEY,
                    value       TEXT
                );
                """
            )
            self._commit()

    def _commit(self) -> None:
        try:
            self.conn.commit()
        except Exception:
            pass

    def _row_to_dict(self, row: Any, cur: Any) -> dict:
        if isinstance(row, dict):
            return row
        if hasattr(row, "keys"):
            return dict(row)
        cols = [c[0] for c in cur.description] if cur and cur.description else []
        if cols and len(cols) == len(row):
            return dict(zip(cols, row))
        return {f"col_{i}": v for i, v in enumerate(row)}

    # -----------------------------------------------------------------------
    # Meta
    # -----------------------------------------------------------------------

    def get_meta(self, key: str) -> Optional[str]:
        with self.lock:
            cur = self.conn.cursor()
            cur.execute("SELECT value FROM meta WHERE key = ?", (key,))
            row = cur.fetchone()
            if not row:
                return None
            d = self._row_to_dict(row, cur)
            return d.get("value")

    def set_meta(self, key: str, value: str) -> None:
        with self.lock:
            cur = self.conn.cursor()
            cur.execute(
                "INSERT INTO meta (key, value) VALUES (?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (key, value),
            )
            self._commit()

    # -----------------------------------------------------------------------
    # Task Management
    # -----------------------------------------------------------------------

    def create_task(
        self,
        task_id: str,
        name: str,
        source_peer: str,
        dest_peer: Optional[str] = None,
        dest_title: Optional[str] = "Video Backup",
        workers: int = 3,
        max_file_size: int = 2097152000,
        dl_parts: int = 4,
        ul_parts: int = 4,
    ) -> dict:
        now = utcnow()
        with self.lock:
            cur = self.conn.cursor()
            cur.execute(
                """
                INSERT INTO tasks (
                    id, name, source_peer, dest_peer, dest_title,
                    workers, max_file_size, dl_parts, ul_parts,
                    status, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?)
                """,
                (
                    task_id,
                    name,
                    source_peer,
                    dest_peer,
                    dest_title,
                    workers,
                    max_file_size,
                    dl_parts,
                    ul_parts,
                    now,
                    now,
                ),
            )
            self._commit()
        return self.get_task(task_id)

    def get_task(self, task_id: str) -> Optional[dict]:
        with self.lock:
            cur = self.conn.cursor()
            cur.execute("SELECT * FROM tasks WHERE id = ?", (task_id,))
            row = cur.fetchone()
            if not row:
                return None
            return self._row_to_dict(row, cur)

    def list_tasks(self) -> list[dict]:
        with self.lock:
            cur = self.conn.cursor()
            cur.execute("SELECT * FROM tasks ORDER BY created_at DESC")
            rows = cur.fetchall()
            return [self._row_to_dict(r, cur) for r in rows]

    def update_task(self, task_id: str, **kwargs) -> None:
        if not kwargs:
            return
        kwargs["updated_at"] = utcnow()
        cols = [f"{k} = ?" for k in kwargs.keys()]
        vals = list(kwargs.values()) + [task_id]
        with self.lock:
            cur = self.conn.cursor()
            cur.execute(f"UPDATE tasks SET {', '.join(cols)} WHERE id = ?", vals)
            self._commit()

    def update_task_status(self, task_id: str, status: str, error_message: Optional[str] = None) -> None:
        self.update_task(task_id, status=status, error_message=error_message)

    def delete_task(self, task_id: str) -> bool:
        with self.lock:
            cur = self.conn.cursor()
            cur.execute("DELETE FROM task_media WHERE task_id = ?", (task_id,))
            cur.execute("DELETE FROM task_logs WHERE task_id = ?", (task_id,))
            cur.execute("DELETE FROM tasks WHERE id = ?", (task_id,))
            self._commit()
            return True

    # -----------------------------------------------------------------------
    # Media Progress Management
    # -----------------------------------------------------------------------

    def insert_media(self, task_id: str, rows: list) -> int:
        """
        rows: [(chat_id, message_id, file_name, file_size, date, status, error), ...]
        Existing items are ignored.
        """
        if not rows:
            return 0
        now = utcnow()
        payload = [(task_id,) + tuple(r) + (now,) for r in rows]
        with self.lock:
            cur = self.conn.cursor()
            cur.executemany(
                """
                INSERT OR IGNORE INTO task_media (
                    task_id, chat_id, message_id, file_name, file_size, date, status, error, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                payload,
            )
            count = cur.rowcount if hasattr(cur, "rowcount") and cur.rowcount > 0 else len(rows)
            self._commit()
        self.sync_task_metrics(task_id)
        return count

    def set_media_status(
        self,
        task_id: str,
        chat_id: int,
        message_id: int,
        status: str,
        *,
        dest_message_id: Optional[int] = None,
        error: Optional[str] = None,
    ) -> None:
        with self.lock:
            cur = self.conn.cursor()
            cur.execute(
                """
                UPDATE task_media
                SET status = ?,
                    dest_message_id = COALESCE(?, dest_message_id),
                    error = ?,
                    updated_at = ?
                WHERE task_id = ? AND chat_id = ? AND message_id = ?
                """,
                (status, dest_message_id, error, utcnow(), task_id, chat_id, message_id),
            )
            self._commit()

    def mark_media_failed(self, task_id: str, chat_id: int, message_id: int, error: str) -> None:
        with self.lock:
            cur = self.conn.cursor()
            cur.execute(
                """
                UPDATE task_media
                SET status = 'failed',
                    error = ?,
                    attempts = attempts + 1,
                    updated_at = ?
                WHERE task_id = ? AND chat_id = ? AND message_id = ?
                """,
                (error, utcnow(), task_id, chat_id, message_id),
            )
            self._commit()

    def get_pending_media(self, task_id: str, limit: Optional[int] = None) -> list[dict]:
        with self.lock:
            cur = self.conn.cursor()
            query = "SELECT * FROM task_media WHERE task_id = ? AND status = 'pending' ORDER BY message_id"
            if limit:
                query += f" LIMIT {int(limit)}"
            cur.execute(query, (task_id,))
            rows = cur.fetchall()
            return [self._row_to_dict(r, cur) for r in rows]

    def reset_incomplete_media(self, task_id: Optional[str] = None) -> int:
        """Reset downloading/uploading media back to pending (e.g. after abrupt crash)."""
        now = utcnow()
        with self.lock:
            cur = self.conn.cursor()
            if task_id:
                cur.execute(
                    """
                    UPDATE task_media
                    SET status = 'pending', error = NULL, updated_at = ?
                    WHERE task_id = ? AND status IN ('downloading', 'uploading')
                    """,
                    (now, task_id),
                )
            else:
                cur.execute(
                    """
                    UPDATE task_media
                    SET status = 'pending', error = NULL, updated_at = ?
                    WHERE status IN ('downloading', 'uploading')
                    """,
                    (now,),
                )
            self._commit()
            count = cur.rowcount if hasattr(cur, "rowcount") and cur.rowcount >= 0 else 0
        if task_id:
            self.sync_task_metrics(task_id)
        return count

    def retry_failed_media(self, task_id: str) -> int:
        now = utcnow()
        with self.lock:
            cur = self.conn.cursor()
            cur.execute(
                """
                UPDATE task_media
                SET status = 'pending', error = NULL, updated_at = ?
                WHERE task_id = ? AND status = 'failed'
                """,
                (now, task_id),
            )
            self._commit()
            count = cur.rowcount if hasattr(cur, "rowcount") and cur.rowcount >= 0 else 0
        self.sync_task_metrics(task_id)
        return count

    def get_task_counts(self, task_id: str) -> dict[str, int]:
        with self.lock:
            cur = self.conn.cursor()
            cur.execute(
                "SELECT status, COUNT(*) AS n FROM task_media WHERE task_id = ? GROUP BY status",
                (task_id,),
            )
            rows = cur.fetchall()
            counts = {d.get("status"): d.get("n", 0) for d in [self._row_to_dict(r, cur) for r in rows]}

            cur.execute(
                "SELECT COALESCE(SUM(file_size), 0) AS s FROM task_media WHERE task_id = ? AND status = 'pending'",
                (task_id,),
            )
            row = cur.fetchone()
            pending_bytes = int(self._row_to_dict(row, cur).get("s", 0)) if row else 0

            cur.execute(
                "SELECT COALESCE(SUM(file_size), 0) AS s FROM task_media WHERE task_id = ? AND status = 'done'",
                (task_id,),
            )
            row = cur.fetchone()
            done_bytes = int(self._row_to_dict(row, cur).get("s", 0)) if row else 0

            total = sum(v for k, v in counts.items() if k != "bytes")
            return {
                "total": total,
                "done": counts.get("done", 0),
                "pending": counts.get("pending", 0),
                "failed": counts.get("failed", 0),
                "skipped": counts.get("skipped", 0),
                "downloading": counts.get("downloading", 0),
                "uploading": counts.get("uploading", 0),
                "pending_bytes": pending_bytes,
                "done_bytes": done_bytes,
            }

    def sync_task_metrics(self, task_id: str) -> None:
        c = self.get_task_counts(task_id)
        self.update_task(
            task_id,
            total_items=c["total"],
            done_items=c["done"],
            failed_items=c["failed"],
            skipped_items=c["skipped"],
            pending_bytes=c["pending_bytes"],
            done_bytes=c["done_bytes"],
        )

    def list_media(
        self,
        task_id: str,
        status: Optional[str] = None,
        offset: int = 0,
        limit: int = 50,
    ) -> list[dict]:
        with self.lock:
            cur = self.conn.cursor()
            if status:
                cur.execute(
                    """
                    SELECT * FROM task_media
                    WHERE task_id = ? AND status = ?
                    ORDER BY message_id DESC
                    LIMIT ? OFFSET ?
                    """,
                    (task_id, status, limit, offset),
                )
            else:
                cur.execute(
                    """
                    SELECT * FROM task_media
                    WHERE task_id = ?
                    ORDER BY message_id DESC
                    LIMIT ? OFFSET ?
                    """,
                    (task_id, limit, offset),
                )
            rows = cur.fetchall()
            return [self._row_to_dict(r, cur) for r in rows]

    # -----------------------------------------------------------------------
    # Task Logs
    # -----------------------------------------------------------------------

    def add_log(self, task_id: str, level: str, message: str) -> None:
        with self.lock:
            cur = self.conn.cursor()
            cur.execute(
                "INSERT INTO task_logs (task_id, level, message, timestamp) VALUES (?, ?, ?, ?)",
                (task_id, level, message, utcnow()),
            )
            self._commit()

    def get_logs(self, task_id: str, limit: int = 100) -> list[dict]:
        with self.lock:
            cur = self.conn.cursor()
            cur.execute(
                "SELECT * FROM task_logs WHERE task_id = ? ORDER BY id DESC LIMIT ?",
                (task_id, limit),
            )
            rows = cur.fetchall()
            return [self._row_to_dict(r, cur) for r in rows][::-1]
