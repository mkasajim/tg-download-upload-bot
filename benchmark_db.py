"""Benchmark: remote Turso/LibSQL vs local SQLite read/write latency.

Measures the exact query patterns this bot actually performs:
  1. Single-row UPDATE (set_media_status - worker hot path)
  2. Batched INSERT OR IGNORE x200 (scan flush)
  3. Aggregated COUNT scan over task_media (dashboard list_tasks_with_counts)
  4. Paginated SELECT * LIMIT 100 (dashboard media modal)
  5. Commit behavior (network round-trip cost on remote)

Run: python benchmark_db.py
"""
from __future__ import annotations

import os
import sqlite3
import time

import env_loader  # noqa: F401  (loads .env)
import libsql

REMOTE_URL = os.environ.get("LIBSQL_URL", "")
AUTH_TOKEN = os.environ.get("LIBSQL_AUTH_TOKEN", "")

BENCH_TABLE = "bench_tmp"
MEDIA_ROWS = 69200  # match the real task size


def make_schema(conn):
    cur = conn.cursor()
    cur.execute(f"DROP TABLE IF EXISTS {BENCH_TABLE}")
    cur.execute(
        f"""
        CREATE TABLE {BENCH_TABLE} (
            task_id TEXT NOT NULL,
            chat_id INTEGER NOT NULL,
            message_id INTEGER NOT NULL,
            file_name TEXT,
            file_size INTEGER,
            date TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            dest_message_id INTEGER,
            attempts INTEGER NOT NULL DEFAULT 0,
            error TEXT,
            updated_at TEXT,
            PRIMARY KEY (task_id, chat_id, message_id)
        );
        """
    )
    cur.execute(f"CREATE INDEX idx_bench_status ON {BENCH_TABLE} (task_id, status)")
    conn.commit()


def seed_rows(conn, n=MEDIA_ROWS, batch=1000):
    """Simulate a 69k-row task_media table.

    NOTE: on remote Turso, executemany sends one HTTP round-trip PER ROW
    (measured 20-74s per 200 rows). So remote seeding uses chunked
    multi-row VALUES (1 round-trip per 200 rows). This difference is
    itself the main scan bottleneck - see benchmark results.
    """
    cur = conn.cursor()
    t0 = time.perf_counter()
    statuses = ["done", "done", "done", "pending", "skipped", "failed"]
    rows = [
        ("bench", -1001234567890, i, f"video_{i}.mp4", 50_000_000 + i,
         "2026-01-01T00:00:00", statuses[i % len(statuses)], None, 0, None,
         "2026-01-01T00:00:00")
        for i in range(n)
    ]
    try:
        is_remote = bool(getattr(conn, "sync", None)) or str(
            getattr(conn, "_remote_url", "") or "").startswith("libsql")
    except Exception:
        is_remote = False
    # libsql remote has no public is_remote flag; detect by probing once:
    # executemany of 10 rows > 300ms => remote-style per-row round trips.
    if not is_remote:
        probe = [(f"p{i}", 0, 10_000_000 + i, "x", 1, None, "pending", None, 0, None, None)
                 for i in range(10)]
        pt = time.perf_counter()
        cur.executemany(
            f"INSERT OR IGNORE INTO {BENCH_TABLE} VALUES (?,?,?,?,?,?,?,?,?,?,?)", probe)
        conn.commit()
        is_remote = (time.perf_counter() - pt) > 0.30
        cur.execute(f"DELETE FROM {BENCH_TABLE} WHERE task_id LIKE 'p%'")
        conn.commit()
    if is_remote:
        cols = "(task_id,chat_id,message_id,file_name,file_size,date,status,dest_message_id,attempts,error,updated_at)"
        for base in range(0, n, 200):
            chunk = rows[base:base + 200]
            ph = ",".join(["(?,?,?,?,?,?,?,?,?,?,?)"] * len(chunk))
            cur.execute(f"INSERT OR IGNORE INTO {BENCH_TABLE} {cols} VALUES {ph}",
                        [v for r in chunk for v in r])
            conn.commit()
    else:
        for base in range(0, n, batch):
            cur.executemany(
                f"INSERT OR IGNORE INTO {BENCH_TABLE} VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                rows[base:base + batch],
            )
            conn.commit()
    return time.perf_counter() - t0


def bench(label, fn, repeat=5):
    times = []
    for _ in range(repeat):
        t0 = time.perf_counter()
        fn()
        times.append(time.perf_counter() - t0)
    best = min(times)
    print(f"  {label:<48} {best*1000:>10.2f} ms  (median {sorted(times)[len(times)//2]*1000:.2f})")
    return best


def run_suite(name, conn, seed=True):
    print(f"\n=== {name} ===")
    make_schema(conn)
    if seed:
        t = seed_rows(conn)
        print(f"  seeded {MEDIA_ROWS} rows in {t:.2f}s")

    cur = conn.cursor()

    # 1. worker hot-path: single UPDATE + commit (one file done)
    def single_update():
        cur.execute(
            f"UPDATE {BENCH_TABLE} SET status='done', updated_at='x' WHERE task_id='bench' AND message_id=?",
            (hash(time.time()) % MEDIA_ROWS,),
        )
        conn.commit()
    bench("single UPDATE + commit (worker marks file done)", single_update)

    # 2. scan flush: batched insert of 200 new rows + commit
    def batch_insert():
        payload = [(
            "bench", -1001234567890, 10_000_000 + i,
            f"v.mp4", 1000, None, "pending", None, 0, None, None,
        ) for i in range(200)]
        cur.executemany(
            f"INSERT OR IGNORE INTO {BENCH_TABLE} VALUES (?,?,?,?,?,?,?,?,?,?,?)", payload)
        conn.commit()
    bench("batched INSERT x200 + commit (scan flush)", batch_insert, repeat=3)

    # 3. dashboard aggregate: full scan of 69k rows
    def aggregate():
        cur.execute(
            f"""
            SELECT task_id, COUNT(*),
                SUM(CASE WHEN status='done' THEN 1 ELSE 0 END),
                SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END),
                SUM(CASE WHEN status='skipped' THEN 1 ELSE 0 END),
                SUM(CASE WHEN status='pending' THEN COALESCE(file_size,0) ELSE 0 END),
                SUM(CASE WHEN status='done' THEN COALESCE(file_size,0) ELSE 0 END)
            FROM {BENCH_TABLE} GROUP BY task_id
            """
        )
        cur.fetchall()
    bench("dashboard aggregate (GROUP BY over 69k rows)", aggregate)

    # 4. media modal: paginated SELECT * page
    def page():
        cur.execute(
            f"SELECT * FROM {BENCH_TABLE} WHERE task_id='bench' ORDER BY message_id DESC LIMIT 100 OFFSET 100"
        )
        cur.fetchall()
    bench("media modal page (SELECT * LIMIT 100 OFFSET 100)", page)

    # 5. pending queue load (runner start)
    def pending():
        cur.execute(
            f"SELECT * FROM {BENCH_TABLE} WHERE task_id='bench' AND status='pending' ORDER BY message_id"
        )
        cur.fetchall()
    bench("runner queue (SELECT all pending rows)", pending)

    # 6. logs write
    def log_write():
        cur.execute(
            f"INSERT INTO {BENCH_TABLE} (task_id,chat_id,message_id,status) VALUES ('log',0,?,'x')",
            (int(time.time() * 1000) % 2_000_000_000,),
        )
        conn.commit()
    bench("single INSERT + commit (task_logs add_log)", log_write)

    cur.execute(f"DROP TABLE IF EXISTS {BENCH_TABLE}")
    conn.commit()


def main():
    print(f"libsql-experimental version: {getattr(libsql, '__version__', 'n/a')}")

    # --- local sqlite via libsql ---
    local = libsql.connect("benchmark_local.db")
    run_suite("LOCAL SQLite (file: benchmark_local.db)", local)

    # --- remote turso ---
    if REMOTE_URL:
        print(f"\nConnecting to remote: {REMOTE_URL}")
        try:
            remote = libsql.connect(REMOTE_URL, auth_token=AUTH_TOKEN or None)
            run_suite(f"REMOTE Turso ({REMOTE_URL})", remote, seed=True)
        except Exception as e:
            print(f"Remote benchmark failed: {e}")
    else:
        print("\nLIBSQL_URL not set; skipping remote benchmark.")


if __name__ == "__main__":
    main()
