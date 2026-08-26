# tg-download-upload-bot

Copies **videos and photos** from a Telegram group/channel your account is a
member of (public or private — you just need to be joined) into a **private
group you own**, then deletes each local file to keep disk usage near zero.

- **Resumable** — progress is tracked in SQLite (`progress.db`). Kill it any
  time (Ctrl+C, crash, reboot); run it again and it continues where it stopped.
- **Parallel** — N workers each download → upload → clean up one video at a
  time, so downloads and uploads overlap.
- **Safe to re-run** — the source is scanned incrementally; already-copied
  media is never re-uploaded, and new videos/photos posted since the last run
  are picked up automatically.

## Setup

1. Python 3.9+ required.

   ```
   python -m venv .venv
   .venv\Scripts\activate        # Windows
   # source .venv/bin/activate   # Linux/macOS
   pip install -r requirements.txt
   ```

2. Get `API_ID` / `API_HASH` from <https://my.telegram.org> → *API development
   tools* (log in with the account that is a member of the source chat).

3. Fill in `.env` (see `.env.example` for documented defaults):

   ```
   API_ID=...
   API_HASH=...
   SOURCE=@some_channel        # or a t.me link, or -1001234567890
   ```

4. Run it:

   ```
   python main.py
   ```

   On the first run it asks for your phone number, the login code Telegram
   sends you (and your 2FA password if set), then saves the session so you
   never log in again.

## What it does on a run

1. Scans the source chat's history and records every video and photo in
   `progress.db` (the first run walks the whole history; later runs only check
   new messages).
2. Resolves the destination: if `DEST` is empty it **creates a new private
   group** named `DEST_TITLE` once and remembers it in the DB; otherwise it
   uses the chat you pointed at.
3. Workers pick up pending items: download to `downloads/job_<message_id>/`,
   upload with the original caption, mark `done`, delete the local file.
4. Files above `MAX_FILE_SIZE` are marked `skipped`; errors are retried up to
   `MAX_ATTEMPTS` times, then marked `failed` (see `--retry-failed`).

Interrupted runs: items stuck mid-transfer are reset to `pending` and leftover
partial files are swept automatically on the next start.

## Usage

```
python main.py                 # normal run (scan + transfer everything pending)
python main.py --dry-run       # scan only, show what would be transferred
python main.py --workers 5     # override WORKERS from .env
python main.py --retry-failed  # re-queue videos previously marked failed
```

## Speed / VPS tuning

Throughput depends on three things:

1. **`cryptg`** (already in `requirements.txt`) — without it Telethon falls
   back to a pure-Python AES implementation and downloads crawl at a few
   hundred KB/s. Verify with `python -c "import cryptg"`.
2. **Parallelism** — `WORKERS` files transfer at a time, each big file split
   into `DL_PARTS` / `UL_PARTS` concurrent 512 KiB request stripes (uploads
   over 10 MiB use them too). On a fast VPS start with `WORKERS=6`,
   `DL_PARTS=4`, `UL_PARTS=4` and adjust based on the MB/s figures logged
   every minute. Each in-flight worker holds one file on disk, so keep free
   space ≥ WORKERS × largest file.
3. **The account** — Telegram throttles per account server-side; a
   non-Premium account tops out well below 1 Gbps no matter how fast the VPS
   is. If MB/s stops rising as you raise WORKERS/DL_PARTS, you have hit that
   ceiling.

While transfers run, the heartbeat logs both run totals and per-worker
lines like `w1 download #812 clip.mp4: 41.2 MB / 62.1 MB (8.0 MB/s)`.

## Notes & limits

- **Account, not bot**: bots can't read chat history, so this logs in as your
  user account. The `*.session` file grants full account access — keep it
  private (it's gitignored).
- **Upload cap**: Telegram limits user accounts to 2000 MiB per file
  (4000 MiB with Premium). Larger source files are skipped; adjust
  `MAX_FILE_SIZE` if you have Premium.
- **Flood limits**: too many workers triggers `FloodWait` errors. The script
  sleeps them off automatically; keep `WORKERS` around 2–5 for long runs.
- **Protected content**: channels that forbid forwarding/saving
  (`chat_noforwards`) block forwarding-based copiers, but this script
  downloads each file and re-uploads it, which still works for members.
- **Speed**: see *Speed / VPS tuning* above — most of the perceived slowness
  is usually the missing `cryptg` package, not Telegram.
- **What gets copied**: streamable videos, video documents (e.g. `.mp4` sent
  as a file), and photos (each photo in an album is handled individually).
  Photos are re-sent as photos, so Telegram may recompress them; self-destructing
  photos, GIFs, and round video notes are not included.
- **Multiple sources**: point `SOURCE` at another chat and run again — the
  database tracks each chat separately and uploads go to the same destination.
