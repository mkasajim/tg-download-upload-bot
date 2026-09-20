"""
Unit test for parallel multi-task execution and pause/resume logic.
"""

import asyncio
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace

from db import Database
from task_runner import TaskRunner


class MockMsg:
    def __init__(self, msg_id: int, size: int = 2048, name: str = "test.mp4"):
        self.id = msg_id
        self.file = SimpleNamespace(size=size, name=name)
        self.video = SimpleNamespace(size=size)
        self.photo = None
        self.media = SimpleNamespace()
        self.raw_text = "Test caption"
        self.date = None


class MockClient:
    def __init__(self):
        self.source_dialogs = []
        self.messages = {}
        self.uploaded_files = []

    async def get_dialogs(self, limit=None):
        return []

    async def get_entity(self, ref):
        return SimpleNamespace(id=123456, title="Mock Chat", username=None)

    async def iter_messages(self, source, offset_id=0, filter=None):
        from telethon.tl.types import InputMessagesFilterPhotos, InputMessagesFilterVideo
        for mid in sorted(self.messages.keys(), reverse=True):
            if offset_id == 0 or mid < offset_id:
                msg = self.messages[mid]
                if isinstance(filter, InputMessagesFilterPhotos) and not getattr(msg, "photo", None):
                    continue
                if isinstance(filter, InputMessagesFilterVideo) and not getattr(msg, "video", None):
                    continue
                yield msg

    async def get_messages(self, source, ids):
        return self.messages.get(ids)

    async def download_media(self, msg, file, progress_callback=None):
        path = Path(file)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"x" * msg.file.size)
        if progress_callback:
            progress_callback(msg.file.size, msg.file.size)

    async def send_file(self, dest, file_handle, caption=None, supports_streaming=False):
        self.uploaded_files.append((dest, caption))
        return SimpleNamespace(id=int(time.time() * 1000) % 100000)

    def is_connected(self):
        return True


class TestParallelTasks(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.db_path = Path(self.test_dir) / "parallel.db"
        self.db = Database(db_path=self.db_path)
        self.client = MockClient()

        # Seed mock messages
        for i in range(1, 6):
            self.client.messages[i] = MockMsg(i, size=1024, name=f"video_{i}.mp4")

    async def asyncTearDown(self):
        import shutil
        shutil.rmtree(self.test_dir, ignore_errors=True)

    async def test_parallel_tasks_and_pause_resume(self):
        # Create Task 1 and Task 2
        self.db.create_task("task_a", "Task A", "@source_a", workers=2)
        self.db.create_task("task_b", "Task B", "@source_b", workers=2)

        # Mock destination payload to skip channel creation
        self.db.update_task("task_a", dest_id=999, dest_payload='{"t": "g", "id": 999}')
        self.db.update_task("task_b", dest_id=999, dest_payload='{"t": "g", "id": 999}')

        runner_a = TaskRunner("task_a", self.db, self.client)
        runner_b = TaskRunner("task_b", self.db, self.client)

        # Run Task A and Task B in parallel
        t_a = asyncio.create_task(runner_a.run())
        t_b = asyncio.create_task(runner_b.run())

        # Let both run
        await asyncio.gather(t_a, t_b)

        # Verify results
        counts_a = self.db.get_task_counts("task_a")
        counts_b = self.db.get_task_counts("task_b")

        self.assertEqual(counts_a["done"], 5)
        self.assertEqual(counts_b["done"], 5)
        self.assertEqual(counts_a["pending"], 0)
        self.assertEqual(counts_b["pending"], 0)

        task_a_db = self.db.get_task("task_a")
        task_b_db = self.db.get_task("task_b")
        self.assertEqual(task_a_db["status"], "completed")
        self.assertEqual(task_b_db["status"], "completed")


if __name__ == "__main__":
    unittest.main()
