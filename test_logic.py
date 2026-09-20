#!/usr/bin/env python3
"""Offline tests for tg-transfer logic that needs no network/Telegram account.

Run:  python test_logic.py
They use a fake client that mimics the small part of Telethon's interface the
script relies on (iter_messages / get_messages / download_media / send_file).
"""

from __future__ import annotations

import asyncio
import os
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace

import main as m
from telethon.errors import FloodWaitError


# ---------------------------------------------------------------------------
# fakes
# ---------------------------------------------------------------------------

@dataclass
class FakeMsg:
    id: int
    has_video: bool = False
    has_photo: bool = False
    size: int = 1000
    name: str = "v.mp4"
    raw_text: str = ""
    date: object = None

    def __post_init__(self):
        self.photo = None
        self.video = None
        self.media = None
        if self.has_video:
            self.video = SimpleNamespace(size=self.size)
            self.file = SimpleNamespace(name=self.name)
        elif self.has_photo:
            self.photo = SimpleNamespace()
            self.media = SimpleNamespace(ttl_seconds=None)
            self.file = SimpleNamespace(name=self.name, size=self.size)
        else:
            self.file = None


class FakeClient:
    """Messages live in self.msgs (a dict id -> FakeMsg)."""

    def __init__(self):
        self.msgs: dict[int, FakeMsg] = {}
        self.flood_after: dict[int, int] = {}   # nth yielded msg -> floodwait seconds
        self.downloaded: list[int] = []
        self.uploaded: list[tuple[int, str]] = []
        self.next_dest_id = 5000

    def add(self, *msgs: FakeMsg) -> None:
        for msg in msgs:
            self.msgs[msg.id] = msg

    # -- telethon interface --------------------------------------------------
    def iter_messages(self, entity, offset_id: int = 0, filter=None):
        from telethon.tl.types import InputMessagesFilterPhotos, InputMessagesFilterVideo
        client = self

        async def gen():
            n = 0
            for mid in sorted(client.msgs, reverse=True):
                if offset_id and mid >= offset_id:
                    continue
                msg = client.msgs[mid]
                if isinstance(filter, InputMessagesFilterPhotos) and not getattr(msg, "photo", None):
                    continue
                if isinstance(filter, InputMessagesFilterVideo) and not getattr(msg, "video", None):
                    continue
                n += 1
                if mid in client.flood_after and n > client.flood_after[mid]:
                    client.flood_after.pop(mid)
                    raise FloodWaitError(None, capture=1)
                yield msg

        return gen()

    async def get_messages(self, entity, ids: int):
        return self.msgs.get(ids)

    async def download_media(self, msg, file: str, progress_callback=None):
        self.downloaded.append(msg.id)
        Path(file).write_bytes(b"x" * 16)
        if progress_callback:
            progress_callback(16, 16)
        return file

    async def send_file(self, dest, file: str, caption=None, supports_streaming=True):
        mid = self.uploaded[-1][0] + 1 if self.uploaded else 4000
        self.uploaded.append((mid, Path(file).name))
        return SimpleNamespace(id=self.next_dest_id)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def make_ctx(client, tmp: Path, **cfg_overrides):
    cfg = m.Config(
        api_id=1, api_hash="x", phone=None, source="src", dest=None,
        dest_title="t", workers=1, download_dir=tmp / "dl",
        db_path=tmp / "t.db", session=str(tmp / "s"), max_file_size=10_000,
        max_attempts=2,
    )
    for k, v in cfg_overrides.items():
        setattr(cfg, k, v)
    db = m.Database(cfg.db_path)
    return m.Ctx(client=client, db=db, cfg=cfg, source=object(), dest=object(),
                 stats={"done": 0, "failed": 0, "bytes_down": 0, "bytes_up": 0,
                        "t0": time.monotonic()}), cfg


def msgs(count: int, video_every: int = 3) -> list[FakeMsg]:
    out = []
    for i in range(1, count + 1):
        if i % video_every == 0:
            out.append(FakeMsg(id=i, has_video=True, name=f"v{i}.mp4"))
        else:
            out.append(FakeMsg(id=i))
    return out


# ---------------------------------------------------------------------------
# tests
# ---------------------------------------------------------------------------

def test_full_scan_and_catchup():
    client = FakeClient()
    client.add(*msgs(20, video_every=3))  # ids 3,6,9,12,15,18 are videos
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as td:
        ctx, _ = make_ctx(client, Path(td))
        chat_id = 1

        async def run():
            added = await m.scan_source(ctx.client, ctx.db, ctx.cfg, ctx.source, chat_id)
            assert added == 6, added
            assert ctx.db.get_meta("1:scan_complete") == "1"
            # last_seen tracks the newest *media* message: the walk uses
            # server-side photo/video filters, so text messages (19, 20)
            # are never yielded. Newest video here is #18.
            assert ctx.db.get_meta("1:last_seen") == "18"
            assert len(ctx.db.pending()) == 6
            # rerun: nothing new
            assert await m.scan_source(ctx.client, ctx.db, ctx.cfg, ctx.source, chat_id) == 0
            # new messages arrive: 21 (text), 22 (video), 23 (video)
            client.add(FakeMsg(21), FakeMsg(22, has_video=True, name="new.mp4"),
                       FakeMsg(23, has_video=True))
            assert await m.scan_source(ctx.client, ctx.db, ctx.cfg, ctx.source, chat_id) == 2
            assert ctx.db.get_meta("1:last_seen") == "23"
            assert len(ctx.db.pending()) == 8

        asyncio.run(run())


def test_scan_resumes_after_floodwait():
    client = FakeClient()
    client.add(*msgs(30, video_every=4))
    # raise a flood wait after yielding a few messages from the newest page
    client.flood_after[30] = 3
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as td:
        ctx, _ = make_ctx(client, Path(td))

        async def run():
            added = await m.scan_source(ctx.client, ctx.db, ctx.cfg, ctx.source, 1)
            assert added == 7, added          # ids 4,8,12,16,20,24,28
            assert len(ctx.db.pending()) == 7

        asyncio.run(run())


def test_scan_resumes_from_cursor():
    """Simulate a crash mid-scan: only part of the history was indexed."""
    client = FakeClient()
    client.add(*msgs(30, video_every=4))
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as td:
        ctx, _ = make_ctx(client, Path(td))
        db = ctx.db

        async def run():
            # pretend a previous run walked ids 30..15 then died
            for mid in range(30, 14, -1):
                info = (1000, "v.mp4") if mid % 4 == 0 else None
                if info:
                    db.insert_media([(1, mid, info[1], info[0], None, "pending", None)])
            db.set_meta("1:last_seen", "30")
            db.set_meta("1:scan_cursor", "15")
            added = await m.scan_source(ctx.client, ctx.db, ctx.cfg, ctx.source, 1)
            assert added == 3, added          # 12,8,4 discovered below the cursor
            assert db.get_meta("1:scan_complete") == "1"
            assert len(db.pending()) == 7

        asyncio.run(run())


def test_oversize_videos_are_skipped():
    client = FakeClient()
    client.add(FakeMsg(1, has_video=True, size=999_999_999, name="huge.mkv"))
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as td:
        ctx, _ = make_ctx(client, Path(td))

        async def run():
            await m.scan_source(ctx.client, ctx.db, ctx.cfg, ctx.source, 1)
            c = ctx.db.counts()
            assert c.get("skipped") == 1 and "pending" not in c

        asyncio.run(run())


def test_transfer_one_flow():
    client = FakeClient()
    client.add(FakeMsg(7, has_video=True, size=50, name="clip.mp4", raw_text="hi"))
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as td:
        ctx, cfg = make_ctx(client, Path(td))
        ctx.db.insert_media([(1, 7, "clip.mp4", 50, None, "pending", None)])
        row = {"chat_id": 1, "message_id": 7}

        async def run():
            await m.transfer_one(ctx, 1, row)
            assert client.downloaded == [7]
            assert [u[1] for u in client.uploaded] == ["clip.mp4"]
            assert not (cfg.download_dir / "job_7").exists()   # file deleted
            r = ctx.db.pending()
            assert ctx.db.counts() == {"done": 1}
            assert ctx.db.conn.execute(
                "SELECT dest_message_id FROM media").fetchone()["dest_message_id"] == 5000

        asyncio.run(run())


def test_transfer_retries_then_fails():
    client = FakeClient()
    client.add(FakeMsg(9, has_video=True, name="x.mp4"))

    class FailingClient(FakeClient):
        async def send_file(self, *a, **k):
            raise RuntimeError("boom")

    failing = FailingClient()
    failing.msgs = client.msgs
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as td:
        ctx, cfg = make_ctx(failing, Path(td))
        ctx.db.insert_media([(1, 9, "x.mp4", 1000, None, "pending", None)])
        row = {"chat_id": 1, "message_id": 9}

        async def run():
            await m.transfer_one(ctx, 1, row)
            assert ctx.db.counts() == {"failed": 1}
            assert not (cfg.download_dir / "job_9").exists()   # cleaned up
            assert ctx.stats["failed"] == 1

        asyncio.run(run())


def test_transfer_floodwait_is_slept_off():
    client = FakeClient()
    client.add(FakeMsg(11, has_video=True, name="y.mp4"))
    calls = {"n": 0}

    async def slow_download(msg, file, progress_callback=None):
        calls["n"] += 1
        if calls["n"] == 1:
            raise FloodWaitError(None, capture=0)
        return await FakeClient.download_media(client, msg, file=file)

    client.download_media = slow_download
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as td:
        ctx, cfg = make_ctx(client, Path(td))

        ctx.db.insert_media([(1, 11, "y.mp4", 1000, None, "pending", None)])

        async def run():
            await m.transfer_one(ctx, 1, {"chat_id": 1, "message_id": 11})
            assert ctx.db.counts() == {"done": 1}

        asyncio.run(run())


def test_vanished_message_is_skipped():
    client = FakeClient()
    client.add(FakeMsg(3, has_video=True))
    client.msgs.pop(3)  # deleted from Telegram before transfer
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as td:
        ctx, _ = make_ctx(client, Path(td))

        ctx.db.insert_media([(1, 3, "gone.mp4", 1000, None, "pending", None)])

        async def run():
            await m.transfer_one(ctx, 1, {"chat_id": 1, "message_id": 3})
            assert ctx.db.counts() == {"skipped": 1}

        asyncio.run(run())


def test_photos_are_detected():
    client = FakeClient()
    client.add(FakeMsg(1), FakeMsg(2, has_video=True, name="clip.mp4"),
               FakeMsg(3, has_photo=True), FakeMsg(4, has_photo=True), FakeMsg(5))
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as td:
        ctx, _ = make_ctx(client, Path(td))

        async def run():
            await m.scan_source(ctx.client, ctx.db, ctx.cfg, ctx.source, 1)
            names = {r["message_id"]: r["file_name"] for r in ctx.db.pending()}
            assert set(names) == {2, 3, 4}, names
            assert names[2] == "clip.mp4"
            assert names[3] == "photo_3.jpg" and names[4] == "photo_4.jpg"

        asyncio.run(run())


def test_photo_transfer_uses_fallback_name():
    client = FakeClient()
    client.add(FakeMsg(9, has_photo=True, size=200))
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as td:
        ctx, cfg = make_ctx(client, Path(td))
        ctx.db.insert_media([(1, 9, "photo_9.jpg", 200, None, "pending", None)])

        async def run():
            await m.transfer_one(ctx, 1, {"chat_id": 1, "message_id": 9})
            assert [u[1] for u in client.uploaded] == ["photo_9.jpg"]
            assert ctx.db.counts() == {"done": 1}
            assert not (cfg.download_dir / "job_9").exists()

        asyncio.run(run())


def test_split_stripes():
    C = m.CHUNK_SIZE
    # exact multiple of the chunk: contiguous, chunk-aligned stripes
    assert m.split_stripes(4 * C, 4) == [(0, C), (C, C), (2 * C, C), (3 * C, C)]
    # odd size: full coverage without overlap, last stripe short
    total = 10 * C + 123
    stripes = m.split_stripes(total, 3)
    assert stripes == [(0, 4 * C), (4 * C, 4 * C), (8 * C, 2 * C + 123)]
    assert sum(length for _, length in stripes) == total
    # fewer chunks than parts collapses to one stripe; empty input to none
    assert m.split_stripes(1000, 4) == [(0, 1000)]
    assert m.split_stripes(0, 4) == []


def test_parallel_download_assembles_file():
    class BlobClient(FakeClient):
        def __init__(self, blob: bytes):
            super().__init__()
            self.blob = blob

        def iter_download(self, media, *, offset=0, limit=None,
                          request_size=None, file_size=None):
            async def gen():
                end = min(file_size, offset + limit * request_size)
                pos = offset
                while pos < end:
                    take = min(request_size, end - pos)
                    yield self.blob[pos:pos + take]
                    pos += take
            return gen()

    blob = os.urandom(2 * m.CHUNK_SIZE + 7)   # odd final chunk
    msg = FakeMsg(5, has_video=True, size=len(blob), name="v.mp4")
    msg.media = object()
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as td:
        path = Path(td) / "v.mp4"
        seen = {"n": 0}

        async def run():
            await m.parallel_download(
                BlobClient(blob), msg, path, len(blob), 3,
                lambda n: seen.__setitem__("n", seen["n"] + n),
                min_parallel=1)

        asyncio.run(run())
        assert path.read_bytes() == blob       # stripes reassembled in order
        assert seen["n"] == len(blob)


def test_parallel_upload_parts():
    class PartClient:
        def __init__(self):
            self.parts: dict[int, bytes] = {}
            self.file_ids: set = set()

        async def __call__(self, request):
            self.file_ids.add(request.file_id)
            self.parts[request.file_part] = request.bytes
            return True

    data = os.urandom(2 * m.CHUNK_SIZE + 5)   # last part is short
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as td:
        path = Path(td) / "v.mp4"
        path.write_bytes(data)
        client = PartClient()
        up = {"n": 0}

        async def run():
            return await m.parallel_upload(
                client, path, parts=2,
                on_bytes=lambda n: up.__setitem__("n", up["n"] + n),
                big_threshold=1024)

        handle = asyncio.run(run())
        assert isinstance(handle, m.types.InputFileBig)
        assert handle.name == "v.mp4"
        assert handle.parts == 3                      # ceil(size / CHUNK_SIZE)
        assert client.file_ids == {handle.id}         # one file id for all parts
        rebuilt = b"".join(client.parts[i] for i in range(handle.parts))
        assert rebuilt == data                        # no part lost or reordered
        assert up["n"] == len(data)

        # small files return None so send_file uploads the path itself
        small = Path(td) / "s.jpg"
        small.write_bytes(b"tiny")

        async def run_small():
            return await m.parallel_upload(client, small, parts=4,
                                           on_bytes=lambda n: None,
                                           big_threshold=1024)

        assert asyncio.run(run_small()) is None


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"PASS {name}")
    print("ALL TESTS PASSED")
