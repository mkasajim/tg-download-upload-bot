"""
TaskRunner: Individual Telegram media transfer task engine.

Handles:
- Scanning source chat history (with persistent cursors in DB)
- Resolving/creating destination peer
- Parallel striped downloading and uploading
- Live telemetry (worker status, transfer speed, bytes progress)
- Safe pausing, stopping, and resumption with abrupt-crash recovery
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import re
import shutil
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

from telethon import TelegramClient, functions
from telethon.errors import FloodWaitError
from telethon.tl import types
from telethon.tl.types import InputMessagesFilterPhotos, InputMessagesFilterVideo

from db import Database, utcnow

log = logging.getLogger("tg-task")

BASE_DIR = Path(__file__).resolve().parent
CHUNK_SIZE = 512 * 1024
MIN_PARALLEL_DOWNLOAD = 4 * 1024 * 1024
BIG_FILE_UPLOAD = 10 * 1024 * 1024
SCAN_BATCH = 200  # flush cadence for the dashboard media list; each flush is one batched commit
METRICS_SYNC_INTERVAL = 5.0  # seconds between task_media metric syncs (was per-file)

BINARY = getattr(os, "O_BINARY", 0)


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
    else:
        os.lseek(fd, offset, os.SEEK_SET)
        os.write(fd, data)


def _pread(fd: int, size: int, offset: int) -> bytes:
    if hasattr(os, "pread"):
        return os.pread(fd, size, offset)
    os.lseek(fd, offset, os.SEEK_SET)
    return os.read(fd, size)


def split_stripes(total: int, parts: int, chunk: int = CHUNK_SIZE) -> list:
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


def extract_media(msg: Any) -> Optional[tuple]:
    if getattr(msg, "photo", None) and not getattr(getattr(msg, "media", None), "ttl_seconds", None):
        size = msg.file.size if msg.file else 0
        return int(size or 0), None, "photo"
    doc = getattr(msg, "video", None)
    if doc is None:
        doc = getattr(msg, "document", None)
        if doc is None or not str(getattr(doc, "mime_type", "") or "").startswith("video/"):
            return None
    name = msg.file.name if msg.file else None
    return int(doc.size or 0), name, "video"


def media_filename(name: Optional[str], kind: str, message_id: int) -> str:
    if kind == "photo":
        return sanitize_filename(name) or f"photo_{message_id}.jpg"
    return sanitize_filename(name) or f"video_{message_id}.mp4"


def parse_ref(raw: str) -> Any:
    s = str(raw).strip()
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
    return s


def peer_from_json(data: str) -> Any:
    try:
        d = json.loads(data)
        if d.get("t") == "c":
            return types.InputPeerChannel(d["id"], d["h"])
        if d.get("t") == "g":
            return types.InputPeerChat(d["id"])
    except Exception:
        pass
    return None


async def parallel_download(
    client: TelegramClient,
    msg: Any,
    path: Path,
    size: int,
    parts: int,
    on_bytes: Callable[[int], None],
    *,
    min_parallel: int = MIN_PARALLEL_DOWNLOAD,
) -> None:
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
                msg.media,
                offset=offset,
                limit=(length + CHUNK_SIZE - 1) // CHUNK_SIZE,
                request_size=CHUNK_SIZE,
                file_size=size,
            )
            async for chunk in stream:
                _pwrite(fd, chunk, pos)
                pos += len(chunk)
                on_bytes(len(chunk))
            if pos - offset < length:
                raise IOError(f"download stopped early at offset {offset}: {pos - offset} of {length} bytes")

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


async def parallel_upload(
    client: TelegramClient,
    path: Path,
    parts: int,
    on_bytes: Callable[[int], None],
    *,
    big_threshold: int = BIG_FILE_UPLOAD,
):
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
                ok = await client(
                    functions.upload.SaveBigFilePartRequest(file_id, index, part_count, data)
                )
                if not ok:
                    raise RuntimeError(f"server rejected upload part {index}")
                on_bytes(len(data))

        tasks = [asyncio.create_task(stripe(k)) for k in range(min(parts, part_count))]
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


class TaskRunner:
    def __init__(self, task_id: str, db: Database, client: TelegramClient):
        self.task_id = task_id
        self.db = db
        self.client = client
        self.task_info: dict = self.db.get_task(task_id) or {}
        self.stop_requested = False
        self.pause_requested = False
        self.active_workers: list[asyncio.Task] = []
        self.worker_progress: dict[int, dict] = {}
        self.stats = {
            "bytes_down": 0,
            "bytes_up": 0,
            "done_this_run": 0,
            "failed_this_run": 0,
            "t0": time.monotonic(),
        }
        self.download_dir = (BASE_DIR / os.environ.get("DOWNLOAD_DIR", "downloads")) / f"task_{self.task_id}"
        self._last_metrics_sync = time.monotonic()

    def _log_sync(self, level: str, msg: str) -> None:
        log.log(getattr(logging, level.upper(), logging.INFO), "[Task %s] %s", self.task_id[:8], msg)
        self.db.add_log(self.task_id, level.upper(), msg)

    def log(self, level: str, msg: str) -> None:
        # DB write goes to a worker thread so it never blocks the event loop.
        asyncio.get_running_loop().run_in_executor(None, self._log_sync, level, msg)

    async def _sync_metrics(self, force: bool = False) -> None:
        """Aggregate task_media counters into the tasks row, throttled (and on a thread)."""
        now = time.monotonic()
        if force or (now - self._last_metrics_sync) >= METRICS_SYNC_INTERVAL:
            self._last_metrics_sync = now
            await asyncio.to_thread(self.db.sync_task_metrics, self.task_id)

    def get_status(self) -> dict:
        now = time.monotonic()
        elapsed = max(1.0, now - self.stats["t0"])
        down_speed = self.stats["bytes_down"] / elapsed
        up_speed = self.stats["bytes_up"] / elapsed

        workers_info = []
        for wid, p in sorted(self.worker_progress.items()):
            w_elapsed = max(0.001, now - p.get("t0", now))
            w_bytes = p.get("bytes", 0)
            w_size = max(1, p.get("size", 1))
            workers_info.append({
                "worker_id": wid,
                "message_id": p.get("mid"),
                "file_name": p.get("name"),
                "phase": p.get("phase"),  # 'down' or 'up'
                "transferred": w_bytes,
                "total": w_size,
                "percent": round((w_bytes / w_size) * 100, 1),
                "speed": round(w_bytes / w_elapsed, 1),
            })

        return {
            "task_id": self.task_id,
            "down_speed": round(down_speed, 1),
            "up_speed": round(up_speed, 1),
            "workers": workers_info,
            "bytes_down": self.stats["bytes_down"],
            "bytes_up": self.stats["bytes_up"],
            "done_this_run": self.stats["done_this_run"],
            "failed_this_run": self.stats["failed_this_run"],
        }

    async def resolve_source(self) -> Any:
        source_ref = parse_ref(self.task_info.get("source_peer", ""))
        try:
            entity = await self.client.get_entity(source_ref)
        except ValueError:
            self.log("INFO", "Peer not cached in session, loading dialogs once...")
            await self.client.get_dialogs(limit=None)
            entity = await self.client.get_entity(source_ref)

        source_title = getattr(entity, "title", None) or getattr(entity, "username", None) or str(source_ref)
        await asyncio.to_thread(self.db.update_task, self.task_id, source_id=entity.id, source_title=source_title)
        self.task_info["source_id"] = entity.id
        self.task_info["source_title"] = source_title
        return entity

    async def resolve_destination(self) -> Any:
        stored = self.task_info.get("dest_payload")
        if stored:
            peer = peer_from_json(stored)
            if peer:
                return peer

        dest_ref = self.task_info.get("dest_peer")
        if dest_ref:
            self.log("INFO", f"Resolving destination: {dest_ref}")
            ref = parse_ref(dest_ref)
            try:
                ent = await self.client.get_entity(ref)
            except ValueError:
                await self.client.get_dialogs(limit=None)
                ent = await self.client.get_entity(ref)
        else:
            title = self.task_info.get("dest_title") or "Video Backup"
            self.log("INFO", f"Creating private backup destination group: {title!r} ...")
            result = await self.client(
                functions.channels.CreateChannelRequest(
                    title=title,
                    about=f"Mirrored by tg-transfer for task {self.task_id}",
                    megagroup=True,
                )
            )
            ent = result.chats[0]
            self.log("INFO", f"Created destination {getattr(ent, 'title', '?')} (id: {ent.id})")

        if isinstance(ent, types.Channel):
            payload = json.dumps({"t": "c", "id": ent.id, "h": ent.access_hash})
        elif isinstance(ent, types.Chat):
            payload = json.dumps({"t": "g", "id": ent.id})
        else:
            raise ValueError(f"Destination resolved to unsupported peer type: {type(ent).__name__}")

        dest_title = getattr(ent, "title", None) or getattr(ent, "username", None) or str(ent.id)
        await asyncio.to_thread(
            self.db.update_task,
            self.task_id,
            dest_id=ent.id,
            dest_title=dest_title,
            dest_payload=payload,
        )
        self.task_info["dest_id"] = ent.id
        self.task_info["dest_title"] = dest_title
        self.task_info["dest_payload"] = payload
        return peer_from_json(payload)

    async def scan_source(self, source: Any, chat_id: int) -> int:
        await asyncio.to_thread(self.db.update_task_status, self.task_id, "scanning")
        self.log("INFO", f"Starting media scan on source {chat_id}...")

        task_data = self.task_info  # already loaded in __init__; avoids a DB round-trip
        max_size = int(task_data.get("max_file_size") or 2097152000)
        last_seen = int(task_data.get("last_seen") or 0)
        scan_cursor = int(task_data.get("scan_cursor") or 0)
        scan_complete = int(task_data.get("scan_complete") or 0)
        catchup_cursor = int(task_data.get("catchup_cursor") or 0)
        catchup_stop = int(task_data.get("catchup_stop") or 0)

        rows: list = []
        added = 0

        def offer(msg: Any) -> None:
            nonlocal last_seen
            if msg.id > last_seen:
                last_seen = msg.id
            info = extract_media(msg)
            if not info:
                return
            size, name, kind = info
            if size > max_size:
                status, err = "skipped", f"exceeds max_file_size ({human_size(max_size)})"
            else:
                status, err = "pending", None
            rows.append((
                chat_id,
                msg.id,
                media_filename(name, kind, msg.id),
                size,
                msg.date.isoformat() if msg.date else None,
                status,
                err,
            ))

        def _flush_sync(cursor_col: str, cursor_val: int, rows_snapshot: list) -> int:
            # Single transaction: batch insert + cursor update = 1 commit
            # instead of 2 (and 1 network round-trip on remote DBs).
            with self.db.batch():
                n = 0
                if rows_snapshot:
                    n = self.db.insert_media(self.task_id, rows_snapshot)
                self.db.update_task(
                    self.task_id,
                    last_seen=last_seen,
                    **{cursor_col: cursor_val},
                )
            return n

        async def flush(cursor_col: str, cursor_val: int) -> None:
            nonlocal added
            # Run the (network-bound, on remote DBs) flush on a worker thread
            # so the event loop stays responsive for dashboard requests.
            snapshot = list(rows)
            rows.clear()
            added += await asyncio.to_thread(_flush_sync, cursor_col, cursor_val, snapshot)

        async def walk_filter(start: int, stop: int, label: str, cursor_col: str, filter_: Any) -> None:
            """Walk only photo/video messages (server-side filtered).

            iter_messages with a media filter returns pages dense with actual
            media instead of paging through every text message, so scanning a
            chat full of text is orders of magnitude faster and needs far
            fewer API requests (less FloodWait risk).
            """
            offset = start
            while not self.stop_requested and not self.pause_requested:
                try:
                    async for msg in self.client.iter_messages(source, offset_id=offset, filter=filter_):
                        if self.stop_requested or self.pause_requested:
                            break
                        if msg.id <= stop:
                            return
                        offer(msg)
                        offset = msg.id
                        if len(rows) >= SCAN_BATCH:
                            await flush(cursor_col, offset)
                    await flush(cursor_col, offset)
                    return
                except FloodWaitError as e:
                    await flush(cursor_col, offset)
                    self.log("WARNING", f"FloodWait of {e.seconds}s during scan ({label}). Waiting...")
                    await asyncio.sleep(e.seconds + 1)

        async def walk(start: int, stop: int, label: str, cursor_col: str) -> None:
            # Photos and videos are filtered separately (the API has no
            # combined photo+video filter); walking both concurrently keeps a
            # request in flight at all times and roughly halves wall time.
            await asyncio.gather(
                walk_filter(start, stop, f"{label}/photos", cursor_col, InputMessagesFilterPhotos()),
                walk_filter(start, stop, f"{label}/videos", cursor_col, InputMessagesFilterVideo()),
            )

        # Full historical walk
        if not scan_complete:
            self.log("INFO", f"Scanning full history (offset #{scan_cursor})...")
            await walk(scan_cursor, 0, "full index", "scan_cursor")
            if not self.stop_requested and not self.pause_requested:
                await asyncio.to_thread(self.db.update_task, self.task_id, scan_complete=1)
                await flush("scan_cursor", 0)
                self.log("INFO", "Full history indexing finished.")

        # Catch-up walk for newly posted media
        if not self.stop_requested and not self.pause_requested:
            if catchup_stop == 0:
                catchup_stop = last_seen
                await asyncio.to_thread(self.db.update_task, self.task_id, catchup_stop=catchup_stop)
            self.log("INFO", f"Running catch-up scan for new media since #{catchup_stop}...")
            await walk(catchup_cursor, catchup_stop, "catch-up", "catchup_cursor")
            if not self.stop_requested and not self.pause_requested:
                await asyncio.to_thread(self.db.update_task, self.task_id, catchup_stop=last_seen, catchup_cursor=0)
                await flush("catchup_cursor", 0)

        await self._sync_metrics(force=True)
        return added

    async def transfer_one(self, worker_id: int, row: dict, source: Any, dest: Any) -> None:
        chat_id = row["chat_id"]
        mid = row["message_id"]
        fname = row["file_name"] or f"media_{mid}"
        size = int(row["file_size"] or 0)
        dl_parts = int(self.task_info.get("dl_parts") or 4)
        ul_parts = int(self.task_info.get("ul_parts") or 4)
        max_attempts = int(os.environ.get("MAX_ATTEMPTS", 3))

        await asyncio.to_thread(self.db.set_media_status, self.task_id, chat_id, mid, "downloading")
        msg = await self.client.get_messages(source, ids=mid)
        info = extract_media(msg) if msg else None
        if info is None:
            await asyncio.to_thread(self.db.set_media_status, self.task_id, chat_id, mid, "skipped", error="message deleted or not video/photo")
            self.log("WARNING", f"#{mid} no longer accessible, skipping.")
            return

        size, name, kind = info
        fname = media_filename(name, kind, mid)
        errors = 0

        def begin(phase: str) -> None:
            self.worker_progress[worker_id] = {
                "mid": mid,
                "phase": phase,
                "name": fname,
                "size": size,
                "bytes": 0,
                "t0": time.monotonic(),
            }

        def on_bytes(stat_key: str):
            prog = self.worker_progress[worker_id]

            def cb(n: int) -> None:
                prog["bytes"] += n
                self.stats[stat_key] += n

            return cb

        job_dir = self.download_dir / f"job_{mid}"
        path = job_dir / fname

        while not self.stop_requested and not self.pause_requested:
            try:
                job_dir.mkdir(parents=True, exist_ok=True)
                begin("down")
                t0 = time.monotonic()
                await parallel_download(self.client, msg, path, size, dl_parts, on_bytes("bytes_down"))

                if self.stop_requested or self.pause_requested:
                    shutil.rmtree(job_dir, ignore_errors=True)
                    self.worker_progress.pop(worker_id, None)
                    return

                await asyncio.to_thread(self.db.set_media_status, self.task_id, chat_id, mid, "uploading")
                begin("up")
                t0 = time.monotonic()
                handle = await parallel_upload(self.client, path, ul_parts, on_bytes("bytes_up"))

                sent = await self.client.send_file(
                    dest,
                    handle if handle is not None else str(path),
                    supports_streaming=(kind == "video"),
                )

                await asyncio.to_thread(self.db.set_media_status, self.task_id, chat_id, mid, "done", dest_message_id=sent.id)
                self.worker_progress.pop(worker_id, None)
                shutil.rmtree(job_dir, ignore_errors=True)
                self.stats["done_this_run"] += 1
                await self._sync_metrics()
                return

            except FloodWaitError as e:
                if e.seconds > 3600:
                    await asyncio.to_thread(self.db.mark_media_failed, self.task_id, chat_id, mid, f"flood wait too long ({e.seconds}s)")
                    shutil.rmtree(job_dir, ignore_errors=True)
                    self.stats["failed_this_run"] += 1
                    self.worker_progress.pop(worker_id, None)
                    await self._sync_metrics()
                    return
                self.log("WARNING", f"Worker #{worker_id} hit FloodWait of {e.seconds}s. Waiting...")
                await asyncio.sleep(e.seconds + 1)
            except asyncio.CancelledError:
                shutil.rmtree(job_dir, ignore_errors=True)
                self.worker_progress.pop(worker_id, None)
                raise
            except Exception as e:
                errors += 1
                self.log("WARNING", f"#{mid} attempt {errors}/{max_attempts} failed: {type(e).__name__}: {e}")
                shutil.rmtree(job_dir, ignore_errors=True)
                if errors >= max_attempts:
                    await asyncio.to_thread(self.db.mark_media_failed, self.task_id, chat_id, mid, f"{type(e).__name__}: {e}")
                    self.stats["failed_this_run"] += 1
                    self.worker_progress.pop(worker_id, None)
                    await self._sync_metrics()
                    return
                await asyncio.sleep(min(2 ** errors, 30))

    async def worker_loop(self, worker_id: int, queue: asyncio.Queue, source: Any, dest: Any) -> None:
        while not self.stop_requested and not self.pause_requested:
            try:
                row = await queue.get()
                if row is None:
                    queue.task_done()
                    break
                try:
                    await self.transfer_one(worker_id, row, source, dest)
                except asyncio.CancelledError:
                    queue.task_done()
                    break
                except Exception as e:
                    self.log("ERROR", f"Worker {worker_id} unhandled error on #{row.get('message_id')}: {e}")
                finally:
                    queue.task_done()
            except asyncio.CancelledError:
                break

    def sweep_dir(self) -> int:
        n = 0
        if not self.download_dir.exists():
            return 0
        for child in self.download_dir.iterdir():
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

    async def run(self) -> None:
        self.stop_requested = False
        self.pause_requested = False
        self.stats["t0"] = time.monotonic()

        # Step 1: Clean up any partial files from prior crash/run
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.sweep_dir()
        await asyncio.to_thread(self.db.reset_incomplete_media, self.task_id)

        try:
            # Step 2: Resolve Telegram peers
            source = await self.resolve_source()
            dest = await self.resolve_destination()

            # Step 3: Scan source history
            await self.scan_source(source, self.task_info["source_id"])
            if self.stop_requested or self.pause_requested:
                return

            # Step 4: Transfer queue
            await asyncio.to_thread(self.db.update_task_status, self.task_id, "running")
            pending_rows = await asyncio.to_thread(self.db.get_pending_media, self.task_id)
            self.log("INFO", f"Queuing {len(pending_rows)} pending media items for transfer...")

            if not pending_rows:
                self.log("INFO", "No pending items to transfer. Task marked completed.")
                await asyncio.to_thread(self.db.update_task_status, self.task_id, "completed")
                return

            queue: asyncio.Queue = asyncio.Queue()
            for r in pending_rows:
                queue.put_nowait(r)

            workers_count = int(self.task_info.get("workers") or 3)
            self.active_workers = [
                asyncio.create_task(self.worker_loop(i + 1, queue, source, dest))
                for i in range(workers_count)
            ]

            while not queue.empty() and not self.stop_requested and not self.pause_requested:
                await asyncio.sleep(1)

            if self.stop_requested or self.pause_requested:
                for w in self.active_workers:
                    w.cancel()
                await asyncio.gather(*self.active_workers, return_exceptions=True)
                self.sweep_dir()
                await asyncio.to_thread(self.db.reset_incomplete_media, self.task_id)
                return

            await queue.join()
            for _ in self.active_workers:
                queue.put_nowait(None)
            await asyncio.gather(*self.active_workers, return_exceptions=True)

            counts = await asyncio.to_thread(self.db.get_task_counts, self.task_id)
            if counts["pending"] == 0:
                await asyncio.to_thread(self.db.update_task_status, self.task_id, "completed")
                self.log("INFO", f"Task finished: {counts['done']} done, {counts['failed']} failed.")
            else:
                await asyncio.to_thread(self.db.update_task_status, self.task_id, "paused")

        except asyncio.CancelledError:
            self.log("INFO", "Task was cancelled/interrupted.")
            self.sweep_dir()
            await asyncio.to_thread(self.db.reset_incomplete_media, self.task_id)
            raise
        except Exception as e:
            self.log("ERROR", f"Task fatal error: {type(e).__name__}: {e}")
            await asyncio.to_thread(self.db.update_task_status, self.task_id, "failed", error_message=str(e))
            self.sweep_dir()
            await asyncio.to_thread(self.db.reset_incomplete_media, self.task_id)
        finally:
            self.worker_progress.clear()
            await self._sync_metrics(force=True)
            await asyncio.to_thread(self.db.flush)

    async def pause(self) -> None:
        self.pause_requested = True
        await asyncio.to_thread(self.db.update_task_status, self.task_id, "paused")
        await asyncio.to_thread(self.db.flush)
        self.log("INFO", "Task pause requested.")
        for w in self.active_workers:
            w.cancel()

    async def stop(self) -> None:
        self.stop_requested = True
        await asyncio.to_thread(self.db.update_task_status, self.task_id, "stopped")
        await asyncio.to_thread(self.db.flush)
        self.log("INFO", "Task stop requested.")
        for w in self.active_workers:
            w.cancel()
