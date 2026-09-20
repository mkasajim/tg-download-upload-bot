"""
Automated Test Suite for tg-download-upload-bot.

Tests:
1. Authentication & secure session tokens (auth.py)
2. Database operations & abrupt-stop recovery (db.py)
3. Cloudflare tunnel token parsing & binary detection (tunnel.py)
4. TaskManager lifecycle & crash recovery (task_manager.py)
5. FastAPI endpoints & session protection (server.py)
"""

import os
import shutil
import tempfile
import unittest
from pathlib import Path

# Ensure testing environment
os.environ["ADMIN_USERNAME"] = "testadmin"
os.environ["ADMIN_PASSWORD"] = "testpass123"
os.environ["ADMIN_SECRET_KEY"] = "test_secret_key_12345"
os.environ["LIBSQL_URL"] = ""
os.environ["LIBSQL_AUTH_TOKEN"] = ""

import auth
import db
import tunnel
from task_manager import TaskManager


class TestAuth(unittest.TestCase):
    def test_verify_credentials(self):
        self.assertTrue(auth.verify_credentials("testadmin", "testpass123"))
        self.assertFalse(auth.verify_credentials("wrong", "testpass123"))
        self.assertFalse(auth.verify_credentials("testadmin", "wrongpass"))

    def test_session_token_lifecycle(self):
        token = auth.create_session_token("testadmin")
        self.assertIsInstance(token, str)
        username = auth.decode_session_token(token)
        self.assertEqual(username, "testadmin")

        # Invalid token
        self.assertIsNone(auth.decode_session_token("tampered.token.here"))


class TestDatabase(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.db_path = Path(self.test_dir) / "test_progress.db"
        self.database = db.Database(db_path=self.db_path)

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_task_crud(self):
        t = self.database.create_task(
            task_id="t_001",
            name="Test Task 1",
            source_peer="@test_channel",
            dest_peer="@test_dest",
            workers=4,
        )
        self.assertEqual(t["id"], "t_001")
        self.assertEqual(t["name"], "Test Task 1")
        self.assertEqual(t["status"], "pending")

        # Update status
        self.database.update_task_status("t_001", "running")
        t_updated = self.database.get_task("t_001")
        self.assertEqual(t_updated["status"], "running")

        # List tasks
        all_tasks = self.database.list_tasks()
        self.assertEqual(len(all_tasks), 1)

    def test_media_and_crash_recovery(self):
        self.database.create_task(task_id="t_002", name="Recovery Task", source_peer="@src")

        # Insert 3 media items
        media_rows = [
            (1001, 10, "vid1.mp4", 1000000, "2026-01-01T00:00:00Z", "pending", None),
            (1001, 11, "vid2.mp4", 2000000, "2026-01-01T00:00:00Z", "pending", None),
            (1001, 12, "vid3.mp4", 3000000, "2026-01-01T00:00:00Z", "pending", None),
        ]
        inserted = self.database.insert_media("t_002", media_rows)
        self.assertEqual(inserted, 3)

        # Simulate ongoing transfer during crash: item 10 downloading, item 11 uploading
        self.database.set_media_status("t_002", 1001, 10, "downloading")
        self.database.set_media_status("t_002", 1001, 11, "uploading")

        # Verify before recovery
        counts_before = self.database.get_task_counts("t_002")
        self.assertEqual(counts_before["downloading"], 1)
        self.assertEqual(counts_before["uploading"], 1)
        self.assertEqual(counts_before["pending"], 1)

        # Execute crash recovery
        reset_count = self.database.reset_incomplete_media("t_002")
        self.assertEqual(reset_count, 2)

        # Verify after recovery: both are back to pending
        counts_after = self.database.get_task_counts("t_002")
        self.assertEqual(counts_after["downloading"], 0)
        self.assertEqual(counts_after["uploading"], 0)
        self.assertEqual(counts_after["pending"], 3)

    def test_retry_failed(self):
        self.database.create_task(task_id="t_003", name="Retry Task", source_peer="@src")
        self.database.insert_media("t_003", [(1001, 20, "f1.mp4", 500, None, "pending", None)])
        self.database.mark_media_failed("t_003", 1001, 20, "Network timeout")

        counts = self.database.get_task_counts("t_003")
        self.assertEqual(counts["failed"], 1)

        self.database.retry_failed_media("t_003")
        counts_retried = self.database.get_task_counts("t_003")
        self.assertEqual(counts_retried["failed"], 0)
        self.assertEqual(counts_retried["pending"], 1)

    def test_incremental_counters(self):
        # Chunked multi-row insert keeps exact counters (incl. skips)...
        self.database.create_task(task_id="t_004", name="Counter Task", source_peer="@src")
        rows = [
            (1001, i, f"f{i}.mp4", 1000 + i, None,
             "skipped" if i % 5 == 0 else "pending",
             "big" if i % 5 == 0 else None)
            for i in range(350)  # spans multiple INSERT_CHUNK batches
        ]
        self.assertEqual(self.database.insert_media("t_004", rows), 350)
        t = self.database.get_task("t_004")
        self.assertEqual(t["total_items"], 350)
        self.assertEqual(t["skipped_items"], 70)
        exp_pending = sum(1000 + i for i in range(350) if i % 5 != 0)
        self.assertEqual(t["pending_bytes"], exp_pending)

        # ...resume re-inserts are ignored and don't move counters...
        self.assertEqual(self.database.insert_media("t_004", rows), 0)
        self.assertEqual(self.database.get_task("t_004")["total_items"], 350)

        # ...status transitions bump terminal counters only...
        self.database.set_media_status("t_004", 1001, 1, "downloading")
        self.assertEqual(self.database.get_task("t_004")["pending_bytes"], exp_pending)
        self.database.set_media_status("t_004", 1001, 1, "done", dest_message_id=9, size=1001)
        self.database.set_media_status("t_004", 1001, 2, "skipped", error="gone", size=1002)
        self.database.mark_media_failed("t_004", 1001, 3, "boom", size=1003)
        t = self.database.get_task("t_004")
        self.assertEqual(t["done_items"], 1)
        self.assertEqual(t["done_bytes"], 1001)
        self.assertEqual(t["skipped_items"], 71)
        self.assertEqual(t["failed_items"], 1)
        self.assertEqual(t["pending_bytes"], exp_pending - 1001 - 1002 - 1003)

        # ...and the full aggregate agrees with the incremental counters.
        c = self.database.get_task_counts("t_004")
        self.assertEqual(c["total"], t["total_items"])
        self.assertEqual(c["done"], t["done_items"])
        self.assertEqual(c["failed"], t["failed_items"])
        self.assertEqual(c["skipped"], t["skipped_items"])
        self.assertEqual(c["pending_bytes"], t["pending_bytes"])
        self.assertEqual(c["done_bytes"], t["done_bytes"])

        # Dashboard read serves the precomputed counters (no aggregate).
        listed = self.database.list_tasks_with_counts()
        row = next(r for r in listed if r["id"] == "t_004")
        self.assertEqual(row["total_items"], 350)
        self.assertEqual(row["done_items"], 1)

    def test_pending_media_pages(self):
        self.database.create_task(task_id="t_005", name="Paged Task", source_peer="@src")
        self.database.insert_media(
            "t_005",
            [(2002, i, f"g{i}.mp4", 10, None, "pending", None) for i in range(2500)],
        )
        pages, last = [], -1
        while True:
            page = self.database.get_pending_media_page("t_005", last, 1000)
            if not page:
                break
            pages.append(page)
            last = page[-1]["message_id"]
            if len(page) < 1000:
                break
        ids = [r["message_id"] for p in pages for r in p]
        self.assertEqual(len(ids), 2500)
        self.assertEqual(ids, list(range(2500)))
        self.assertEqual(
            set(pages[0][0].keys()), {"chat_id", "message_id", "file_name", "file_size"}
        )


class TestCloudflareTunnel(unittest.TestCase):
    def test_find_cloudflared(self):
        exe = tunnel.find_cloudflared()
        self.assertIsNotNone(exe)
        self.assertTrue(os.path.exists(exe))

    def test_extract_tunnel_id_from_token(self):
        import base64
        import json
        sample = {"a": "account123", "t": "uuid-9999-8888", "s": "secret_abc"}
        b64 = base64.b64encode(json.dumps(sample).encode()).decode()
        tid = tunnel.extract_tunnel_id_from_token(b64)
        self.assertEqual(tid, "uuid-9999-8888")

        # Invalid token handling
        self.assertIsNone(tunnel.extract_tunnel_id_from_token("not-a-valid-token"))


class TestTaskManagerRecovery(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.db_path = Path(self.test_dir) / "test_tm.db"
        self.database = db.Database(db_path=self.db_path)
        # Mock telegram client
        self.mock_client = object()
        self.mgr = TaskManager(self.database, self.mock_client)

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_startup_recovery(self):
        # Create a task that was left 'running' when server abruptly shut down
        self.database.create_task(task_id="crash_task", name="Crash Task", source_peer="@src")
        self.database.update_task_status("crash_task", "running")
        self.database.insert_media("crash_task", [(100, 1, "vid.mp4", 1000, None, "downloading", None)])

        recovered = self.mgr.startup_recovery()
        self.assertEqual(recovered, 1)

        task = self.database.get_task("crash_task")
        self.assertEqual(task["status"], "paused")

        counts = self.database.get_task_counts("crash_task")
        self.assertEqual(counts["downloading"], 0)
        self.assertEqual(counts["pending"], 1)


class TestServerAPI(unittest.TestCase):
    def setUp(self):
        from starlette.testclient import TestClient
        import server
        server.db_instance = db.Database(db_path=":memory:")
        self.client = TestClient(server.app, raise_server_exceptions=False)

    def test_unauthenticated_dashboard_redirect(self):
        res = self.client.get("/", headers={"Accept": "text/html"}, follow_redirects=False)
        self.assertIn(res.status_code, (302, 303, 307))
        self.assertEqual(res.headers.get("location"), "/login")

    def test_login_flow(self):
        # Invalid login
        res_fail = self.client.post("/api/login", data={"username": "testadmin", "password": "wrongpassword"})
        self.assertEqual(res_fail.status_code, 401)

        # Valid login
        res_ok = self.client.post("/api/login", data={"username": "testadmin", "password": "testpass123"})
        self.assertEqual(res_ok.status_code, 200)
        self.assertIn("session_token", res_ok.cookies)

        # Access dashboard with session cookie
        res_dash = self.client.get("/", cookies={"session_token": res_ok.cookies["session_token"]})
        self.assertEqual(res_dash.status_code, 200)
        self.assertIn("Telegram Media Transfer", res_dash.text)

    def test_tasks_api_lifecycle(self):
        # Login and get cookie
        login_res = self.client.post("/api/login", data={"username": "testadmin", "password": "testpass123"})
        cookie = {"session_token": login_res.cookies["session_token"]}

        # List tasks (empty initially)
        list_res = self.client.get("/api/tasks", cookies=cookie)
        self.assertEqual(list_res.status_code, 200)
        self.assertEqual(list_res.json(), [])

        # Create new task
        create_res = self.client.post(
            "/api/tasks",
            json={
                "name": "Integration Test Task",
                "source": "@sample_channel",
                "dest": "@sample_dest",
                "workers": 2,
                "max_file_size": 104857600,
            },
            cookies=cookie,
        )
        self.assertEqual(create_res.status_code, 200)
        task = create_res.json()
        self.assertEqual(task["name"], "Integration Test Task")
        task_id = task["id"]

        # List tasks again
        list_res2 = self.client.get("/api/tasks", cookies=cookie)
        self.assertEqual(len(list_res2.json()), 1)

        # Delete task
        del_res = self.client.delete(f"/api/tasks/{task_id}", cookies=cookie)
        self.assertEqual(del_res.status_code, 200)
        self.assertEqual(del_res.json()["status"], "ok")


if __name__ == "__main__":
    unittest.main()
