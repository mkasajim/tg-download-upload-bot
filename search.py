#!/usr/bin/env python3
"""
Interactive search for Telegram groups/channels.

  python search.py
  python search.py --query "sometext"

Prompts for a search string, scans all dialogs of the logged-in account
plus a global public search, and prints matching groups/channels with
their name and id (ready to paste into SOURCE/DEST in .env).

Works with any input language (Arabic, Persian, Russian, Chinese, ...):
  - console is forced to UTF-8 so non-English input/output never crashes
  - matching uses Unicode NFKC normalization + casefold, not ASCII lower()
  - multi-word queries match even if word order differs
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import unicodedata
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

from telethon import TelegramClient, functions
from telethon.tl import types

BASE_DIR = Path(__file__).resolve().parent


def ensure_utf8_console() -> None:
    """Windows consoles default to cp1252/cp936 and mangle non-English text."""
    for stream in (sys.stdin, sys.stdout, sys.stderr):
        try:
            reconfig = getattr(stream, "reconfigure", None)
            if reconfig is not None:
                reconfig(encoding="utf-8", errors="replace")
        except Exception:
            pass
    # Fallback for pipes without reconfigure support.
    try:
        if getattr(sys.stdout, "encoding", "").lower() != "utf-8":
            import io
            sys.stdout = io.TextIOWrapper(
                sys.stdout.buffer, encoding="utf-8", errors="replace"
            )
    except Exception:
        pass


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


def normalize(text: str | None) -> str:
    """Unicode-safe normalization for matching: NFKC + casefold.

    NFKC folds compatibility forms (fullwidth latin, composed/decomposed
    accents, etc.) so visually identical strings in any language compare
    equal. casefold() is stronger than lower() for e.g. German/Turkish.
    """
    if not text:
        return ""
    text = unicodedata.normalize("NFKC", text)
    text = text.casefold()
    # collapse all whitespace runs to single spaces
    text = " ".join(text.split())
    return text


def matches(title: str | None, username: str | None, query_norm: str) -> bool:
    """True if a dialog matches the normalized query."""
    if not query_norm:
        return True  # empty query lists everything
    title_norm = normalize(title)
    user_norm = normalize(username or "")
    hay = f"{title_norm} {user_norm}".strip()
    if query_norm in hay:
        return True
    tokens = query_norm.split()
    if len(tokens) > 1 and all(t in hay for t in tokens):
        # word order / extra spaces differ (common with RTL languages)
        return True
    return False


def describe_dialog(d) -> tuple[str, str, int, str]:
    """Returns (title, username_display, full_id, kind)."""
    ent = d.entity
    title = getattr(d, "title", None) or getattr(ent, "title", None) or "?"
    username = getattr(ent, "username", None)
    full_id = d.id  # already in -100... form for channels/supergroups
    if isinstance(ent, types.Channel):
        kind = "channel" if getattr(ent, "broadcast", False) else "supergroup"
    elif isinstance(ent, types.Chat):
        kind = "group"
    else:
        kind = type(ent).__name__
    return title, ("@" + username if username else "-"), full_id, kind


def print_results(rows: list[tuple[str, str, int, str]], source_hint: str = "") -> None:
    if not rows:
        print("  (no matches)")
        return
    for i, (title, username, full_id, kind) in enumerate(rows, 1):
        # SOURCE-ready value: prefer @username, else numeric id.
        usable = username if username != "-" else str(full_id)
        print(f"  {i}. {title}")
        print(f"     username : {username}")
        print(f"     id       : {full_id}")
        print(f"     type     : {kind}")
        print(f"     use as SOURCE/DEST: {usable}")
    if source_hint:
        print(source_hint)


async def local_search(client: TelegramClient, query_raw: str):
    query_norm = normalize(query_raw)
    dialogs = await client.get_dialogs(limit=None)
    out = []
    scanned = 0
    for d in dialogs:
        if d.is_user:
            continue  # only groups/channels, skip private chats/bots
        if not (d.is_group or d.is_channel):
            continue
        scanned += 1
        ent = d.entity
        title = getattr(d, "title", None) or getattr(ent, "title", None)
        username = getattr(ent, "username", None)
        if matches(title, username, query_norm):
            out.append(describe_dialog(d))
    return out, scanned, len(dialogs)


async def global_search(client: TelegramClient, query_raw: str, limit: int = 20):
    """Server-side public search (finds channels not yet joined)."""
    q = query_raw.strip()
    if not q:
        return []
    try:
        res = await client(functions.contacts.SearchRequest(q=q, limit=limit))
    except Exception as e:
        print(f"  (global search unavailable: {type(e).__name__}: {e})")
        return []
    out = []
    for chat in getattr(res, "chats", []):
        if isinstance(chat, (types.Channel, types.Chat)):
            title = getattr(chat, "title", None) or "?"
            username = getattr(chat, "username", None)
            if isinstance(chat, types.Channel):
                full_id = int("-100" + str(chat.id))
                kind = "channel" if getattr(chat, "broadcast", False) else "supergroup"
            else:
                full_id = -int(chat.id)
                kind = "group"
            out.append((title, ("@" + username if username else "-"), full_id, kind + " (public)"))
    return out


async def run_query(client: TelegramClient, query_raw: str, global_limit: int, no_global: bool) -> None:
    local, scanned_groups, total_dialogs = await local_search(client, query_raw)
    print(f"\nMy chats: {len(local)} match(es) "
          f"(scanned {scanned_groups} groups/channels out of {total_dialogs} dialogs)")
    print_results(local)

    if not no_global and query_raw.strip():
        glob = await global_search(client, query_raw, limit=global_limit)
        # drop ones already listed (same id or same @username)
        seen_ids = {r[2] for r in local}
        seen_users = {r[1] for r in local if r[1] != "-"}
        fresh = [r for r in glob if r[2] not in seen_ids and r[1] not in seen_users]
        print(f"\nPublic (not necessarily joined): {len(fresh)} match(es)")
        print_results(fresh)


async def main_async(args: argparse.Namespace) -> int:
    load_env_file(BASE_DIR / ".env")
    api_id = os.environ.get("API_ID", "").strip()
    api_hash = os.environ.get("API_HASH", "").strip()
    if not api_id or not api_hash:
        print("Missing API_ID / API_HASH in .env. Copy .env.example to .env first.")
        return 1
    session = os.environ.get("SESSION_NAME", "").strip() or "tg_transfer"
    if session.endswith(".session"):
        session = session[: -len(".session")]
    session_path = str((BASE_DIR / session).with_suffix(".session"))
    phone = os.environ.get("PHONE", "").strip() or None

    client = TelegramClient(session_path, int(api_id), api_hash)
    # Same session as main.py, so login is only asked once ever.
    await client.start(phone=phone)
    me = await client.get_me()
    who = "@" + me.username if getattr(me, "username", None) else (me.first_name or "me")
    print(f"Logged in as {who} (id {me.id}).")

    try:
        if args.query:
            await run_query(client, args.query, args.global_limit, args.no_global)
        else:
            while True:
                try:
                    query_raw = input(
                        "\nSearch groups/channels "
                        "(empty = list all mine, 'q' to quit): "
                    )
                except (EOFError, KeyboardInterrupt):
                    print()
                    break
                if query_raw.strip().lower() in ("q", "quit", "exit"):
                    break
                await run_query(client, query_raw, args.global_limit, args.no_global)
    finally:
        await client.disconnect()
    return 0


def main() -> int:
    ensure_utf8_console()
    p = argparse.ArgumentParser(description="Search your Telegram groups/channels by name (any language).")
    p.add_argument("--query", "-q", default="", help="search text (if omitted, ask interactively)")
    p.add_argument("--global-limit", type=int, default=20, help="max public results (default 20)")
    p.add_argument("--no-global", action="store_true", help="skip public search, only search my chats")
    args = p.parse_args()
    try:
        return asyncio.run(main_async(args))
    except KeyboardInterrupt:
        print("\nInterrupted.")
        return 130


if __name__ == "__main__":
    sys.exit(main())
