#!/usr/bin/env python3
"""
Telegram media transfer (videos & photos).

Downloads videos and photos from a Telegram group/channel the logged-in
account is a member of and re-uploads them to a private group owned by the
account.
Each downloaded file is deleted from disk right after its upload succeeds.
Progress is persisted in SQLite, so an interrupted run resumes where it
stopped.

Requires: pip install telethon python-dotenv
Config:   see .env.example / README.md
"""

from __future__ import annotations

import env_loader  # Ensures .env is loaded

import argparse
import asyncio
import json
import logging
import os
import random
import re
import shutil
import sqlite3
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

from telethon import TelegramClient, functions
from telethon.errors import FloodWaitError
from telethon.tl import types
from telethon.tl.types import InputMessagesFilterPhotos, InputMessagesFilterVideo

log = logging.getLogger("tg-transfer")

BASE_DIR = Path(__file__).resolve().parent

# Telegram blocks uploads above this for accounts without Premium (2000 MiB).
DEFAULT_MAX_SIZE = 2097152000

# Telegram moves at most 512 KiB per GetFile / SaveBigFilePart request, so
# parallel transfers split files into stripes of such chunks.
CHUNK_SIZE = 512 * 1024
# Files below this use the plain single-stream download path.
MIN_PARALLEL_DOWNLOAD = 4 * 1024 * 1024
# Telegram only accepts SaveBigFilePart ("big" files) above 10 MiB.
BIG_FILE_UPLOAD = 10 * 1024 * 1024

SCAN_BATCH = 500  # DB rows buffered before a flush while scanning history


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def human_size(n: Optional[int]) -> str:
    n = float(n or 0)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or unit == "TB":
            return f"{int(n)} B" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


def sanitize_filename(name: Optional[str]) -> Optional[str]:
    if not name:
        return None
    name = re.sub(r'[\\/:*?"<>|\x00-\x1f]', "_", name).strip().rstrip(". ")
    return name[:150] or None


def _pwrite(fd: int, data: bytes, offset: int) -> None:
    if hasattr(os, "pwrite"):
        os.pwrite(fd, data, offset)
    else:  # Windows: no pread/pwrite; nothing awaits between seek and write
        os.lseek(fd, offset, os.SEEK_SET)
        os.write(fd, data)


def _pread(fd: int, size: int, offset: int) -> bytes:
    if hasattr(os, "pread"):
        return os.pread(fd, size, offset)
    os.lseek(fd, offset, os.SEEK_SET)
    return os.read(fd, size)


# On Windows os.open() defaults to text mode, where every b"\n" grows into
# b"\r\n" and corrupts binary media; O_BINARY is 0 where it does not exist.
BINARY = getattr(os, "O_BINARY", 0)


def split_stripes(total: int, parts: int, chunk: int = CHUNK_SIZE) -> list:
    """Splits `total` bytes into at most `parts` contiguous (offset, length)
    stripes of whole chunks; totals under one chunk collapse to one stripe."""
    nchunks = (total + chunk - 1) // chunk
    parts = max(1, min(parts, nchunks))
    base, extra = divmod(nchunks, parts)
    out: list = []
    start = 0
    for i in range(parts):
        take = base + (1 if i < extra else 0)
        if not take:
            continue
        offset = start * chunk
        out.append((offset, min(total - offset, take * chunk)))
        start += take
    return out


# ---------------------------------------------------------------------------
# configuration
# ---------------------------------------------------------------------------

@dataclass
class Config:
    api_id: int
    api_hash: str
    phone: Optional[str]
    source: str
    dest: Optional[str]
    dest_title: str
    workers: int
    download_dir: Path
    db_path: Path
    session: str
    max_file_size: int
    max_attempts: int
    dl_parts: int = 4   # concurrent download stripes per file
    ul_parts: int = 4   # concurrent upload stripes per file


def load_env_file(path: Path) -> None:
    if load_dotenv is not None:
        load_dotenv(path)
        return
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def env_int(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        raise SystemExit(f"{name} in .env must be an integer, got: {raw!r}")


def env_path(name: str, default: Path) -> Path:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    p = Path(raw)
    return p if p.is_absolute() else BASE_DIR / p


def build_config(args: argparse.Namespace) -> Config:
    load_env_file(BASE_DIR / ".env")
    api_id = os.environ.get("API_ID", "").strip()
    api_hash = os.environ.get("API_HASH", "").strip()
    source = os.environ.get("SOURCE", "").strip()
    missing = [n for n, v in (("API_ID", api_id), ("API_HASH", api_hash), ("SOURCE", source)) if not v]
    if missing:
        raise SystemExit(
            "Missing required setting(s) in .env: " + ", ".join(missing)
            + "\nCopy .env.example to .env and fill them in (see README.md)."
        )

    session = os.environ.get("SESSION_NAME", "").strip() or "tg_transfer"
    if session.endswith(".session"):
        session = session[: -len(".session")]
    session_path = (BASE_DIR / session).with_suffix(".session")

    return Config(
        api_id=int(api_id),
        api_hash=api_hash,
        phone=os.environ.get("PHONE", "").strip() or None,
        source=source,
        dest=os.environ.get("DEST", "").strip() or None,
        dest_title=os.environ.get("DEST_TITLE", "").strip() or "Video Backup",
        workers=max(1, args.workers or env_int("WORKERS", 3)),
        download_dir=env_path("DOWNLOAD_DIR", BASE_DIR / "downloads"),
        db_path=env_path("DB_PATH", BASE_DIR / "progress.db"),
        session=str(session_path),
        max_file_size=env_int("MAX_FILE_SIZE", DEFAULT_MAX_SIZE),
        max_attempts=env_int("MAX_ATTEMPTS", 3),
        dl_parts=max(1, env_int("DL_PARTS", 4)),
        ul_parts=max(1, env_int("UL_PARTS", 4)),
    )


def parse_ref(raw: str) -> Any:
    """Accepts @name, name, t.me links, or a numeric id like -1001234567890."""
    s = raw.strip()
    m = re.search(r"t\.me/c/(\d+)", s)
    if m:
        return int("-100" + m.group(1))
    m = re.search(r"t\.me/([A-Za-z0-9_]+)", s)
    if m:
        return "@" + m.group(1)
    if re.fullmatch(r"-?\d+", s):
        return int(s)
    if re.fullmatch(r"@?[A-Za-z0-9_]+", s):
        return "@" + s.lstrip("@")
    raise SystemExit(
        f"Cannot parse Telegram peer: {raw!r} "
        "(use @name, a t.me link, or a numeric id like -1001234567890)"
    )


# ---------------------------------------------------------------------------
# progress database
# ---------------------------------------------------------------------------

class Database:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(path))
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        with self.conn:
            self.conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS media (
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
                    PRIMARY KEY (chat_id, message_id)
                );
                CREATE INDEX IF NOT EXISTS idx_media_status ON media (status);
                CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT);
                """
            )

    def get_meta(self, key: str) -> Optional[str]:
        row = self.conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
        return row["value"] if row else None

    def set_meta(self, key: str, value: str) -> None:
        with self.conn:
            self.conn.execute(
                "INSERT INTO meta (key, value) VALUES (?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (key, value),
            )

    def insert_media(self, rows: list) -> int:
        """rows: (chat_id, message_id, file_name, file_size, date, status, error).
        Existing rows are ignored so re-scans never clobber progress."""
        now = utcnow()
        payload = [tuple(r) + (now,) for r in rows]
        with self.conn:
            cur = self.conn.executemany(
                "INSERT OR IGNORE INTO media "
                "(chat_id, message_id, file_name, file_size, date, status, error, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                payload,
            )
        return cur.rowcount

    def reset_incomplete(self) -> int:
        with self.conn:
            cur = self.conn.execute(
                "UPDATE media SET status = 'pending', error = NULL, updated_at = ? "
                "WHERE status IN ('downloading', 'uploading')",
                (utcnow(),),
            )
        return cur.rowcount

    def retry_failed(self) -> int:
        with self.conn:
            cur = self.conn.execute(
                "UPDATE media SET status = 'pending', error = NULL, updated_at = ? "
                "WHERE status = 'failed'",
                (utcnow(),),
            )
        return cur.rowcount

    def set_status(
        self,
        chat_id: int,
        message_id: int,
        status: str,
        *,
        dest_message_id: Optional[int] = None,
        error: Optional[str] = None,
    ) -> None:
        with self.conn:
            self.conn.execute(
                "UPDATE media SET status = ?, "
                "dest_message_id = COALESCE(?, dest_message_id), "
                "error = ?, updated_at = ? WHERE chat_id = ? AND message_id = ?",
                (status, dest_message_id, error, utcnow(), chat_id, message_id),
            )

    def mark_failed(self, chat_id: int, message_id: int, error: str) -> None:
        with self.conn:
            self.conn.execute(
                "UPDATE media SET status = 'failed', error = ?, "
                "attempts = attempts + 1, updated_at = ? WHERE chat_id = ? AND message_id = ?",
                (error, utcnow(), chat_id, message_id),
            )

    def pending(self) -> list:
        return self.conn.execute(
            "SELECT * FROM media WHERE status = 'pending' ORDER BY message_id"
        ).fetchall()

    def counts(self) -> dict:
        return {
            r["status"]: r["n"]
            for r in self.conn.execute(
                "SELECT status, COUNT(*) AS n FROM media GROUP BY status"
            )
        }

    def pending_bytes(self) -> int:
        row = self.conn.execute(
            "SELECT COALESCE(SUM(file_size), 0) AS s FROM media WHERE status = 'pending'"
        ).fetchone()
        return int(row["s"])


# ---------------------------------------------------------------------------
# scanning the source history
# ---------------------------------------------------------------------------

def extract_media(msg: Any) -> Optional[tuple]:
    """Returns (size, file_name, kind) if the message carries a video or a
    photo, else None. "video" covers streamable videos and video documents
    (e.g. .mp4 sent as a file); self-destructing photos are ignored."""
    if getattr(msg, "photo", None) and not getattr(getattr(msg, "media", None), "ttl_seconds", None):
        size = msg.file.size if msg.file else 0
        return int(size or 0), None, "photo"
    doc = getattr(msg, "video", None)
    if doc is None:
        doc = getattr(msg, "document", None)
        if doc is None or not str(doc.mime_type or "").startswith("video/"):
            return None
    name = msg.file.name if msg.file else None
    return int(doc.size or 0), name, "video"


def media_filename(name: Optional[str], kind: str, message_id: int) -> str:
    """File name used on disk / in the DB, with a kind-aware fallback."""
    if kind == "photo":
        return sanitize_filename(name) or f"photo_{message_id}.jpg"
    return sanitize_filename(name) or f"video_{message_id}.mp4"


async def get_source_entity(client: TelegramClient, cfg: Config) -> Any:
    ref = parse_ref(cfg.source)
    try:
        return await client.get_entity(ref)
    except ValueError:
        # numeric ids only resolve if the entity is in the session cache;
        # loading dialogs once makes every joined chat resolvable
        log.info("Peer not in session cache, loading your dialogs once...")
        await client.get_dialogs(limit=None)
        return await client.get_entity(ref)


async def scan_source(client: TelegramClient, db: Database, cfg: Config, source: Any, chat_id: int) -> int:
    """Index videos and photos in the source chat. The first run walks the whole history;
    later runs only look at messages posted since the previous run. All scan
    state is namespaced per chat and persisted, so scans interrupted by
    crashes or flood waits resume where they stopped."""

    def key(k: str) -> str:
        return f"{chat_id}:{k}"

    state = {"last_seen": int(db.get_meta(key("last_seen")) or 0), "added": 0}
    rows: list = []

    def offer(msg: Any) -> None:
        if msg.id > state["last_seen"]:
            state["last_seen"] = msg.id
        info = extract_media(msg)
        if not info:
            return
        size, name, kind = info
        if size > cfg.max_file_size:
            status, err = "skipped", f"exceeds MAX_FILE_SIZE ({human_size(cfg.max_file_size)})"
        else:
            status, err = "pending", None
        rows.append((chat_id, msg.id, media_filename(name, kind, msg.id), size,
                     msg.date.isoformat() if msg.date else None, status, err))

    def flush() -> None:
        if rows:
            state["added"] += db.insert_media(rows)
            rows.clear()
        db.set_meta(key("last_seen"), str(state["last_seen"]))

    async def walk(start: int, stop: int, label: str, cursor_key: str) -> None:
        """Walk only photo/video messages (server-side filtered) with
        stop < id <= start, newest to oldest. start=0 means from the newest
        message. Media-only filtering returns pages dense with actual media
        instead of paging through every text message, and the two filtered
        walks run concurrently to roughly halve wall time."""

        async def walk_filter(filter_: Any) -> None:
            offset = start
            while True:
                try:
                    async for msg in client.iter_messages(source, offset_id=offset, filter=filter_):
                        if msg.id <= stop:
                            return
                        offer(msg)
                        offset = msg.id
                        if len(rows) >= SCAN_BATCH:
                            flush()
                            db.set_meta(key(cursor_key), str(offset))
                    db.set_meta(key(cursor_key), str(offset))
                    return
                except FloodWaitError as e:
                    flush()
                    db.set_meta(key(cursor_key), str(offset))
                    log.warning("Scan (%s) hit a flood wait of %ds; resuming from #%s afterwards.",
                                label, e.seconds, offset)
                    await asyncio.sleep(e.seconds + 1)

        await asyncio.gather(
            walk_filter(InputMessagesFilterPhotos()),
            walk_filter(InputMessagesFilterVideo()),
        )

    if db.get_meta(key("scan_complete")) != "1":
        cursor = int(db.get_meta(key("scan_cursor")) or 0)
        log.info("Indexing the full history of the source chat%s ...",
                 f" (continuing from #{cursor})" if cursor else "")
        await walk(cursor, 0, "full index", "scan_cursor")
        db.set_meta(key("scan_complete"), "1")
        flush()
        log.info("History fully indexed.")

    # catch-up pass for messages posted since the previous run
    catchup_stop = db.get_meta(key("catchup_stop"))
    if catchup_stop is None:
        catchup_stop = str(state["last_seen"])
        db.set_meta(key("catchup_stop"), catchup_stop)
    cursor = int(db.get_meta(key("catchup_cursor")) or 0)
    log.info("Checking for new videos/photos since the last run ...")
    await walk(cursor, int(catchup_stop), "catch-up", "catchup_cursor")
    db.set_meta(key("catchup_stop"), str(state["last_seen"]))
    db.set_meta(key("catchup_cursor"), "0")
    flush()

    return state["added"]


# ---------------------------------------------------------------------------
# destination group
# ---------------------------------------------------------------------------

def peer_from_json(data: str) -> Any:
    d = json.loads(data)
    if d.get("t") == "c":
        return types.InputPeerChannel(d["id"], d["h"])
    if d.get("t") == "g":
        return types.InputPeerChat(d["id"])
    return None


async def resolve_destination(client: TelegramClient, db: Database, cfg: Config) -> Any:
    stored = db.get_meta("dest_peer")
    if stored:
        peer = peer_from_json(stored)
        if peer:
            return peer

    if cfg.dest:
        log.info("Using destination from .env: %s", cfg.dest)
        ent = await client.get_entity(parse_ref(cfg.dest))
    else:
        log.info("No DEST set - creating private group %r ...", cfg.dest_title)
        result = await client(
            functions.channels.CreateChannelRequest(
                title=cfg.dest_title,
                about="Videos & photos mirrored by tg-transfer",
                megagroup=True,
            )
        )
        ent = result.chats[0]
        log.info("Created %r (id %s). It is reused on every following run.",
                 getattr(ent, "title", "?"), ent.id)

    if isinstance(ent, types.Channel):
        payload = json.dumps({"t": "c", "id": ent.id, "h": ent.access_hash})
    elif isinstance(ent, types.Chat):
        payload = json.dumps({"t": "g", "id": ent.id})
    else:
        raise SystemExit(
            f"DEST resolved to a {type(ent).__name__}; use a group or channel you own."
        )
    db.set_meta("dest_peer", payload)
    return peer_from_json(payload)


# ---------------------------------------------------------------------------
# transfer workers
# ---------------------------------------------------------------------------

@dataclass
class Ctx:
    client: TelegramClient
    db: Database
    cfg: Config
    source: Any
    dest: Any
    stats: dict
    progress: dict = field(default_factory=dict)  # worker_id -> live transfer state


async def parallel_download(client: TelegramClient, msg: Any, path: Path,
                            size: int, parts: int, on_bytes, *,
                            min_parallel: int = MIN_PARALLEL_DOWNLOAD) -> None:
    """Download msg's media into path, fetching `parts` byte stripes of the
    file concurrently (the requests pipeline over the DC connection, which
    multiplies throughput on fast links). Small files use one plain stream."""
    stripes = split_stripes(size, parts)
    if len(stripes) <= 1 or size < min_parallel:
        seen = {"n": 0}

        def cb(current: int, total: int) -> None:
            on_bytes(current - seen["n"])
            seen["n"] = current

        await client.download_media(msg, file=str(path), progress_callback=cb)
        return

    fd = os.open(str(path), os.O_CREAT | os.O_WRONLY | os.O_TRUNC | BINARY, 0o644)
    try:
        os.ftruncate(fd, size)

        async def stripe(offset: int, length: int) -> None:
            pos = offset
            stream = client.iter_download(
                msg.media, offset=offset,
                limit=(length + CHUNK_SIZE - 1) // CHUNK_SIZE,
                request_size=CHUNK_SIZE, file_size=size)
            async for chunk in stream:
                _pwrite(fd, chunk, pos)
                pos += len(chunk)
                on_bytes(len(chunk))
            if pos - offset < length:
                raise IOError(f"download stopped early at offset {offset}: "
                              f"{pos - offset} of {length} bytes")

        tasks = [asyncio.create_task(stripe(*s)) for s in stripes]
        try:
            await asyncio.gather(*tasks)
        except BaseException:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise
    finally:
        os.close(fd)


async def parallel_upload(client: TelegramClient, path: Path, parts: int,
                          on_bytes, *, big_threshold: int = BIG_FILE_UPLOAD):
    """Upload a big file with `parts` concurrent SaveBigFilePart stripes and
    return its InputFileBig handle (send_file accepts it directly). Returns
    None for small files, where send_file's own upload of the path is fine."""
    size = path.stat().st_size
    if parts <= 1 or size <= big_threshold:
        return None
    part_count = (size + CHUNK_SIZE - 1) // CHUNK_SIZE
    file_id = random.getrandbits(63)
    fd = os.open(str(path), os.O_RDONLY | BINARY)
    try:
        async def stripe(first: int) -> None:
            for index in range(first, part_count, parts):
                data = _pread(fd, CHUNK_SIZE, index * CHUNK_SIZE)
                ok = await client(functions.upload.SaveBigFilePartRequest(
                    file_id, index, part_count, data))
                if not ok:
                    raise RuntimeError(f"server rejected upload part {index}")
                on_bytes(len(data))

        tasks = [asyncio.create_task(stripe(k))
                 for k in range(min(parts, part_count))]
        try:
            await asyncio.gather(*tasks)
        except BaseException:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise
    finally:
        os.close(fd)
    return types.InputFileBig(file_id, part_count, path.name)


async def transfer_one(ctx: Ctx, worker_id: int, row: sqlite3.Row) -> None:
    tag = f"[w{worker_id}]"
    db, cfg = ctx.db, ctx.cfg
    chat_id, mid = row["chat_id"], row["message_id"]

    db.set_status(chat_id, mid, "downloading")
    msg = await ctx.client.get_messages(ctx.source, ids=mid)
    info = extract_media(msg) if msg else None
    if info is None:
        db.set_status(chat_id, mid, "skipped", error="message is gone or no longer a video/photo")
        log.warning("%s #%d: message is gone or no longer a video/photo, skipping", tag, mid)
        return

    size, name, kind = info
    fname = media_filename(name, kind, mid)
    errors = 0

    def begin(phase: str) -> None:
        ctx.progress[worker_id] = {"mid": mid, "phase": phase, "name": fname,
                                   "size": size, "bytes": 0,
                                   "t0": time.monotonic()}

    def on_bytes(stat_key: str):
        prog = ctx.progress[worker_id]

        def cb(n: int) -> None:
            prog["bytes"] += n
            ctx.stats[stat_key] += n

        return cb

    while True:
        job_dir = cfg.download_dir / f"job_{mid}"
        path = job_dir / fname
        try:
            job_dir.mkdir(parents=True, exist_ok=True)

            begin("down")
            t0 = time.monotonic()
            await parallel_download(ctx.client, msg, path, size,
                                    cfg.dl_parts, on_bytes("bytes_down"))
            log.info("%s #%d downloaded %s (%s) in %.1fs",
                     tag, mid, fname, human_size(size), time.monotonic() - t0)

            db.set_status(chat_id, mid, "uploading")
            begin("up")
            t0 = time.monotonic()
            handle = await parallel_upload(ctx.client, path, cfg.ul_parts,
                                           on_bytes("bytes_up"))
            sent = await ctx.client.send_file(
                ctx.dest, handle if handle is not None else str(path),
                supports_streaming=(kind == "video")
            )
            log.info("%s #%d uploaded as dest message #%d in %.1fs",
                     tag, mid, sent.id, time.monotonic() - t0)

            db.set_status(chat_id, mid, "done", dest_message_id=sent.id)
            ctx.progress.pop(worker_id, None)
            shutil.rmtree(job_dir, ignore_errors=True)  # free disk space
            ctx.stats["done"] += 1
            return
        except FloodWaitError as e:
            if e.seconds > 3600:
                db.mark_failed(chat_id, mid, f"flood wait too long ({e.seconds}s)")
                shutil.rmtree(job_dir, ignore_errors=True)
                ctx.stats["failed"] += 1
                return
            log.warning("%s #%d: Telegram asks to wait %ds ...", tag, mid, e.seconds)
            await asyncio.sleep(e.seconds + 1)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            errors += 1
            log.warning("%s #%d: attempt %d/%d failed: %s: %s",
                        tag, mid, errors, cfg.max_attempts, type(e).__name__, e)
            shutil.rmtree(job_dir, ignore_errors=True)
            if errors >= cfg.max_attempts:
                db.mark_failed(chat_id, mid, f"{type(e).__name__}: {e}")
                ctx.stats["failed"] += 1
                return
            await asyncio.sleep(min(2 ** errors, 60))


async def worker_loop(ctx: Ctx, worker_id: int, queue: asyncio.Queue) -> None:
    tag = f"[w{worker_id}]"
    while True:
        row = await queue.get()
        try:
            if row is None:
                return
            try:
                await transfer_one(ctx, worker_id, row)
            except Exception:
                log.exception("%s unexpected error on #%s, continuing", tag, row["message_id"])
        finally:
            queue.task_done()


async def heartbeat(ctx: Ctx) -> None:
    while True:
        await asyncio.sleep(60)
        c = ctx.db.counts()
        log.info("progress: %d done, %d pending, %d failed overall (this run: %d ok, %d failed)",
                 c.get("done", 0), c.get("pending", 0), c.get("failed", 0),
                 ctx.stats["done"], ctx.stats["failed"])
        elapsed = max(1.0, time.monotonic() - ctx.stats["t0"])
        log.info("run traffic: %s down, %s up (%.1f / %.1f MB/s)",
                 human_size(ctx.stats["bytes_down"]), human_size(ctx.stats["bytes_up"]),
                 ctx.stats["bytes_down"] / elapsed / 1e6,
                 ctx.stats["bytes_up"] / elapsed / 1e6)
        for wid in sorted(ctx.progress):
            p = ctx.progress[wid]
            secs = max(1e-9, time.monotonic() - p["t0"])
            log.info("  w%d %s #%d %s: %s / %s (%.1f MB/s)",
                     wid, "download" if p["phase"] == "down" else "upload",
                     p["mid"], p["name"], human_size(p["bytes"]),
                     human_size(p["size"]), p["bytes"] / secs / 1e6)


def sweep_dir(d: Path) -> int:
    """Delete leftover partial downloads from an interrupted run."""
    n = 0
    for child in d.iterdir():
        try:
            if child.is_dir():
                shutil.rmtree(child, ignore_errors=True)
                n += 1
            elif child.is_file():
                child.unlink()
                n += 1
        except OSError:
            pass
    return n


# ---------------------------------------------------------------------------
# entry point
# ---------------------------------------------------------------------------

async def main_async(cfg: Config, args: argparse.Namespace) -> int:
    db = Database(cfg.db_path)
    if args.retry_failed:
        n = db.retry_failed()
        if n:
            log.info("Re-queued %d failed item(s).", n)
    n = db.reset_incomplete()
    if n:
        log.info("Reset %d interrupted item(s) to pending.", n)

    cfg.download_dir.mkdir(parents=True, exist_ok=True)
    swept = sweep_dir(cfg.download_dir)
    if swept:
        log.info("Removed %d leftover partial download(s) from %s.", swept, cfg.download_dir)

    client = TelegramClient(cfg.session, cfg.api_id, cfg.api_hash)
    await client.start(phone=cfg.phone)
    me = await client.get_me()
    log.info("Logged in as %s (id %s).",
             "@" + me.username if me.username else (me.first_name or "me"), me.id)

    source = await get_source_entity(client, cfg)
    chat_id = source.id
    log.info("Source: %s (id %s).",
             getattr(source, "title", None) or getattr(source, "username", None) or cfg.source,
             chat_id)

    await scan_source(client, db, cfg, source, chat_id)

    counts = db.counts()
    pending_rows = db.pending()
    log.info("Media indexed: %d total | %d done | %d pending (%s) | %d skipped | %d failed",
             sum(counts.values()), counts.get("done", 0), len(pending_rows),
             human_size(db.pending_bytes()), counts.get("skipped", 0), counts.get("failed", 0))

    if args.dry_run:
        for r in pending_rows[:20]:
            log.info("  would transfer #%d %s (%s)",
                     r["message_id"], r["file_name"] or "?", human_size(r["file_size"]))
        if len(pending_rows) > 20:
            log.info("  ... and %d more", len(pending_rows) - 20)
        await client.disconnect()
        return 0

    if not pending_rows:
        log.info("Nothing to transfer. New media is picked up on the next run.")
        await client.disconnect()
        return 0

    dest = await resolve_destination(client, db, cfg)
    ctx = Ctx(client=client, db=db, cfg=cfg, source=source, dest=dest,
              stats={"done": 0, "failed": 0, "bytes_down": 0, "bytes_up": 0,
                     "t0": time.monotonic()})

    queue: asyncio.Queue = asyncio.Queue()
    for row in pending_rows:
        queue.put_nowait(row)

    hb = asyncio.create_task(heartbeat(ctx))
    workers = [asyncio.create_task(worker_loop(ctx, i + 1, queue))
               for i in range(cfg.workers)]
    await queue.join()
    for _ in workers:
        queue.put_nowait(None)
    await asyncio.gather(*workers)
    hb.cancel()

    counts = db.counts()
    log.info("Run finished: %d transferred, %d failed (this run).",
             ctx.stats["done"], ctx.stats["failed"])
    log.info("Overall: %d done, %d pending, %d skipped, %d failed.",
             counts.get("done", 0), counts.get("pending", 0),
             counts.get("skipped", 0), counts.get("failed", 0))
    await client.disconnect()
    return 0


def main() -> int:
    env_loader.load_env()
    parser = argparse.ArgumentParser(
        description="Telegram media transfer bot & dashboard server."
    )
    parser.add_argument("--server", action="store_true", default=False,
                        help="launch the web dashboard and Cloudflare tunnel server (default)")
    parser.add_argument("--cli", action="store_true", default=False,
                        help="run single transfer job directly in CLI mode")
    parser.add_argument("--workers", type=int, metavar="N",
                        help="number of parallel workers (overrides WORKERS in .env)")
    parser.add_argument("--dry-run", action="store_true",
                        help="scan only and show what would be transferred (CLI mode)")
    parser.add_argument("--retry-failed", action="store_true",
                        help="re-queue items previously marked failed")
    args = parser.parse_args()

    # If --cli or --dry-run is specified, run in CLI mode. Otherwise default to Dashboard Server.
    if args.cli or args.dry_run:
        logging.basicConfig(level=logging.INFO,
                            format="%(asctime)s %(levelname)-7s %(message)s",
                            datefmt="%H:%M:%S")
        logging.getLogger("telethon").setLevel(logging.WARNING)

        cfg = build_config(args)
        try:
            asyncio.run(main_async(cfg, args))
        except KeyboardInterrupt:
            print("\nInterrupted - progress is saved. Run the script again to resume.")
            return 130
        return 0

    # Default to web dashboard server
    import server
    server.run_server()
    return 0


if __name__ == "__main__":
    sys.exit(main())
