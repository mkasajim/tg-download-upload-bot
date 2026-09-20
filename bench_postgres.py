"""Benchmark: Aiven PostgreSQL vs Turso remote — the bot's real workload patterns."""
import time, sys, io, os
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
import psycopg
from pathlib import Path
import env_loader  # noqa: F401  (loads .env)

_CA = str(Path(__file__).resolve().parent / "cert" / "ca.pem")

URL = os.environ.get("POSTGRE_URL", "")
N_ROWS = 69200

def timed(label, fn, repeat=5):
    times = []
    for _ in range(repeat):
        t0 = time.perf_counter(); fn(); times.append(time.perf_counter() - t0)
    print(f"  {label:<52} min {min(times)*1000:>8.1f} ms  median {sorted(times)[len(times)//2]*1000:>8.1f} ms")
    return min(times)

t0 = time.perf_counter()
conn = psycopg.connect(URL, autocommit=False, sslrootcert=_CA)
print(f"connect: {time.perf_counter()-t0:.3f}s  (server: {conn.execute('SELECT version()').fetchone()[0][:60]})")
cur = conn.cursor()

timed("SELECT 1 (round-trip latency)", lambda: cur.execute("SELECT 1").fetchall(), repeat=10)

# schema mirroring task_media
cur.execute("DROP TABLE IF EXISTS bench_media")
cur.execute("""
    CREATE TABLE bench_media (
        task_id TEXT NOT NULL,
        chat_id BIGINT NOT NULL,
        message_id BIGINT NOT NULL,
        file_name TEXT,
        file_size BIGINT,
        date TEXT,
        status TEXT NOT NULL DEFAULT 'pending',
        dest_message_id BIGINT,
        attempts INT NOT NULL DEFAULT 0,
        error TEXT,
        updated_at TEXT,
        PRIMARY KEY (task_id, chat_id, message_id)
    )""")
cur.execute("CREATE INDEX idx_bench_status ON bench_media (task_id, status)")
conn.commit()

# --- seed 69,200 rows (like scan) ---
t0 = time.perf_counter()
statuses = ["done", "done", "done", "pending", "skipped", "failed"]
batch = 1000
for base in range(0, N_ROWS, batch):
    rows = []
    for i in range(base, min(base + batch, N_ROWS)):
        rows.append(("bench", -1001234567890, i, f"video_{i}.mp4", 50_000_000 + i,
                     "2026-01-01", statuses[i % 6], None, 0, None, None))
    cur.executemany(
        "INSERT INTO bench_media VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", rows)
    conn.commit()
print(f"  seeded {N_ROWS} rows in {time.perf_counter()-t0:.2f}s ({N_ROWS/(time.perf_counter()-t0):.0f} rows/s)")

# --- 1. worker hot path: single UPDATE + commit ---
def one_update():
    cur.execute("UPDATE bench_media SET status='done', updated_at='x' WHERE task_id='bench' AND message_id=%s",
                (hash(time.time()) % N_ROWS,))
    conn.commit()
timed("single UPDATE + commit (worker marks file done)", one_update)

# --- 2. scan flush: batched insert 200 rows + commit ---
def batch_insert():
    rows = [("bench", -100, 10_000_000 + i, "v.mp4", 1000, None, "pending", None, 0, None, None)
            for i in range(200)]
    cur.executemany("INSERT INTO bench_media VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                    "ON CONFLICT DO NOTHING", rows)
    conn.commit()
timed("executemany INSERT x200 + commit (scan flush)", batch_insert, repeat=3)

# --- 3. dashboard aggregate over 69k rows ---
def aggregate():
    cur.execute("""
        SELECT task_id, COUNT(*),
            SUM(CASE WHEN status='done' THEN 1 ELSE 0 END),
            SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END),
            SUM(CASE WHEN status='skipped' THEN 1 ELSE 0 END),
            SUM(CASE WHEN status='pending' THEN COALESCE(file_size,0) ELSE 0 END),
            SUM(CASE WHEN status='done' THEN COALESCE(file_size,0) ELSE 0 END)
        FROM bench_media GROUP BY task_id
    """)
    cur.fetchall()
timed("dashboard aggregate (GROUP BY over 69k rows)", aggregate)

# --- 4. media modal page ---
def page():
    cur.execute("SELECT * FROM bench_media WHERE task_id='bench' ORDER BY message_id DESC LIMIT 100 OFFSET 100")
    cur.fetchall()
timed("media modal page (LIMIT 100 OFFSET 100)", page)

# --- 5. runner queue: select all pending ---
def pending():
    cur.execute("SELECT * FROM bench_media WHERE task_id='bench' AND status='pending' ORDER BY message_id")
    cur.fetchall()
timed("runner queue (SELECT all pending rows)", pending)

# --- 6. log write ---
def log_write():
    cur.execute("INSERT INTO bench_media (task_id,chat_id,message_id,status) VALUES ('log',0,%s,'x')",
                (int(time.time()*1000) % 2_000_000_000,))
    conn.commit()
timed("single INSERT + commit (task_logs add_log)", log_write)

# --- 7. count(*) ---
timed("COUNT(*) full table", lambda: cur.execute("SELECT COUNT(*) FROM bench_media").fetchone())

# --- 8. parallel-ish: 5 concurrent connections round-trip ---
import concurrent.futures
def worker_rtt(_):
    c = psycopg.connect(URL, sslrootcert=_CA)
    c.execute("SELECT 1").fetchall()
    c.close()
    return True
t0 = time.perf_counter()
with concurrent.futures.ThreadPoolExecutor(5) as ex:
    list(ex.map(worker_rtt, range(10)))
print(f"  10 parallel queries over 5 connections: {(time.perf_counter()-t0)*1000:.0f} ms total")

cur.execute("DROP TABLE IF EXISTS bench_media")
conn.commit()
conn.close()
print("done")
