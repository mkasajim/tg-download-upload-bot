#!/usr/bin/env python3
"""
Read-only probe: is any media in SOURCE still recoverable?

Uses the same .env / session as main.py (API_ID, API_HASH, SOURCE,
SESSION_NAME, PHONE) and never downloads files to disk or writes the DB.

  python check_recoverable.py --limit 20
  python check_recoverable.py --ids 1196,1190-1200
  python check_recoverable.py --limit 50 --probe
  python check_recoverable.py --source @other_channel --limit 20

For each checked message it prints one of:
  RECOVERABLE  - photo/video main.py would copy (bytes still served)
  GONE         - get_messages returned None / MessageEmpty (deleted or
                 stripped server-side, e.g. TOS takedown -> not recoverable
                 via any client with this account)
  UNSUPPORTED  - has media but not video/photo (GIF, round video, voice,
                 sticker, poll, webpage, ttl-photo, ...)
  TEXT-ONLY    - no media at all

With --probe, RECOVERABLE candidates also fetch the first 512 KiB chunk
in memory to prove the bytes are actually served.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path

from telethon import TelegramClient
from telethon.errors import FloodWaitError
from telethon.tl import types

import main as m  # reuse load_env_file, parse_ref, extract_media, BASE_DIR

BASE_DIR = m.BASE_DIR


def ensure_utf8_console() -> None:
    for stream in (sys.stdin, sys.stdout, sys.stderr):
        try:
            reconfig = getattr(stream, "reconfigure", None)
            if reconfig is not None:
                reconfig(encoding="utf-8", errors="replace")
        except Exception:
            pass


def parse_ids(raw: str) -> list[int]:
    """'1196,1190-1200' -> sorted unique ids."""
    out: set[int] = set()
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part and not part.startswith("-"):
            a, _, b = part.partition("-")
            out.update(range(int(a.strip()), int(b.strip()) + 1))
        else:
            out.add(int(part))
    return sorted(out)


def describe_media(msg) -> str:
    """Human-readable media description for non-recoverable messages."""
    media = getattr(msg, "media", None)
    if media is None:
        return "no media attribute"
    t = type(media).__name__
    if isinstance(media, types.MessageMediaEmpty):
        return "MessageMediaEmpty (stripped by server)"
    if isinstance(media, types.MessageMediaPhoto):
        ttl = getattr(media, "ttl_seconds", None)
        return f"MessageMediaPhoto ttl={ttl}"
    if isinstance(media, types.MessageMediaDocument):
        doc = getattr(media, "document", None)
        if doc is None or isinstance(doc, types.DocumentEmpty):
            return "MessageMediaDocument with DocumentEmpty (stripped by server)"
        mime = getattr(doc, "mime_type", "?")
        size = getattr(doc, "size", "?")
        attrs = ",".join(type(a).__name__ for a in getattr(doc, "attributes", []))
        return f"Document mime={mime} size={size} attrs=[{attrs}]"
    if isinstance(media, types.MessageMediaWebPage):
        return "MessageMediaWebPage (link preview, not a file)"
    if isinstance(media, types.MessageMediaPoll):
        return "MessageMediaPoll"
    if isinstance(media, types.MessageMediaStory):
        return "MessageMediaStory"
    if isinstance(media, types.MessageMediaGiveaway):
        return "MessageMediaGiveaway"
    extra = ""
    if hasattr(media, "ttl_seconds"):
        extra = f" ttl={media.ttl_seconds}"
    return f"{t}{extra}"


def classify(msg) -> tuple[str, str]:
    """Returns (status, detail)."""
    if msg is None:
        return "GONE", "get_messages returned None (deleted / inaccessible)"
    if isinstance(msg, types.MessageEmpty):
        return "GONE", "MessageEmpty (deleted or stripped server-side)"
    info = m.extract_media(msg)
    if info is not None:
        size, name, kind = info
        return "RECOVERABLE", f"{kind} name={name or '(fallback)'} size={size}"
    media = getattr(msg, "media", None)
    if media is None:
        return "TEXT-ONLY", f"type={type(msg).__name__} (no media)"
    if isinstance(media, (types.MessageMediaEmpty,)):
        return "GONE", describe_media(msg)
    if isinstance(media, types.MessageMediaDocument):
        doc = getattr(media, "document", None)
        if doc is None or isinstance(doc, types.DocumentEmpty):
            return "GONE", describe_media(msg)
    return "UNSUPPORTED", describe_media(msg)


async def resolve_source(client: TelegramClient, ref_raw: str):
    ref = m.parse_ref(ref_raw)
    try:
        return await client.get_entity(ref)
    except ValueError:
        print("Peer not in cache, loading dialogs once...")
        await client.get_dialogs(limit=None)
        return await client.get_entity(ref)


def print_source_info(source) -> None:
    print(f"Source: {getattr(source, 'title', '?')} (id {getattr(source, 'id', '?')})")
    for attr in ("username", "broadcast", "megagroup", "scam", "fake",
                 "restricted", "noforwards"):
        if hasattr(source, attr):
            print(f"  {attr}: {getattr(source, attr)}")
    rr = getattr(source, "restriction_reason", None)
    print(f"  restriction_reason: {rr if rr else '-'}")
    if rr:
        for r in rr:
            print(f"    - platform={getattr(r, 'platform', '?')} "
                  f"reason={getattr(r, 'reason', '?')} "
                  f"text={getattr(r, 'text', '?')}")


async def check_one(client, source, mid: int, probe: bool) -> str:
    try:
        msg = await client.get_messages(source, ids=mid)
    except FloodWaitError as e:
        await asyncio.sleep(e.seconds + 1)
        msg = await client.get_messages(source, ids=mid)
    status, detail = classify(msg)
    date = getattr(msg, "date", None) if msg is not None else None
    line = f"#{mid} [{date}] {status}: {detail}"
    print(line)
    if status == "RECOVERABLE" and probe and msg is not None:
        try:
            n = 0
            async for chunk in client.iter_download(
                msg.media, request_size=512 * 1024, limit=1
            ):
                n += len(chunk)
                break
            print(f"  probe: got {n} bytes of first chunk -> "
                  f"{'BYTES SERVED' if n else 'EMPTY'}")
        except FloodWaitError as e:
            print(f"  probe: FloodWait {e.seconds}s, skipped")
        except Exception as e:  # noqa: BLE001 - diagnostic tool, show everything
            print(f"  probe: FAILED {type(e).__name__}: {e}")
    return status


async def main_async(args: argparse.Namespace) -> int:
    m.load_env_file(BASE_DIR / ".env")
    api_id = os.environ.get("API_ID", "").strip()
    api_hash = os.environ.get("API_HASH", "").strip()
    source_raw = (args.source or os.environ.get("SOURCE", "").strip())
    if not api_id or not api_hash or not source_raw:
        print("Missing API_ID / API_HASH / SOURCE (.env or --source).")
        return 1
    session = os.environ.get("SESSION_NAME", "").strip() or "tg_transfer"
    if session.endswith(".session"):
        session = session[: -len(".session")]
    session_path = str((BASE_DIR / session).with_suffix(".session"))
    phone = os.environ.get("PHONE", "").strip() or None

    client = TelegramClient(session_path, int(api_id), api_hash)
    await client.start(phone=phone)
    try:
        source = await resolve_source(client, source_raw)
        print_source_info(source)

        if args.ids:
            mids = parse_ids(args.ids)
        else:
            print(f"\nCollecting {args.limit} newest message ids...")
            mids = []
            async for msg in client.iter_messages(source, limit=args.limit):
                mids.append(msg.id)
            mids.sort()
        print(f"Checking {len(mids)} message(s)"
              f"{' with byte probe' if args.probe else ''} (read-only)...\n")

        counts: dict[str, int] = {}
        for mid in mids:
            try:
                status = await check_one(client, source, mid, args.probe)
            except FloodWaitError as e:
                print(f"#{mid} FloodWait {e.seconds}s, sleeping...")
                await asyncio.sleep(e.seconds + 1)
                status = await check_one(client, source, mid, args.probe)
            except Exception as e:  # noqa: BLE001
                status = "ERROR"
                print(f"#{mid} ERROR: {type(e).__name__}: {e}")
            counts[status] = counts.get(status, 0) + 1

        print("\nSummary:", ", ".join(f"{k}={v}" for k, v in sorted(counts.items())))
        rec = counts.get("RECOVERABLE", 0)
        if rec:
            print(f"YES - {rec} message(s) still serve bytes and main.py can copy them.")
        else:
            print("NO - nothing recoverable in this set via this account. "
                  "GONE means Telegram no longer serves the bytes (deletion / TOS strip).")
        return 0
    finally:
        await client.disconnect()


def main() -> int:
    ensure_utf8_console()
    p = argparse.ArgumentParser(description="Read-only check: is SOURCE media still downloadable?")
    p.add_argument("--source", default="", help="override SOURCE from .env")
    p.add_argument("--ids", default="", help='explicit ids, e.g. "1196" or "1190-1200,1210"')
    p.add_argument("--limit", type=int, default=20, help="newest N messages when --ids omitted (default 20)")
    p.add_argument("--probe", action="store_true",
                   help="also fetch first chunk in memory to prove bytes are served")
    args = p.parse_args()
    try:
        return asyncio.run(main_async(args))
    except KeyboardInterrupt:
        print("\nInterrupted.")
        return 130


if __name__ == "__main__":
    sys.exit(main())
