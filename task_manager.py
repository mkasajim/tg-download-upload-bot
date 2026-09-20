"""
TaskManager: Orchestrates parallel multi-task execution for tg-download-upload-bot.

Supports:
- Running multiple transfer tasks simultaneously
- Individual Pause, Resume, Stop, and Retry
- Telemetry aggregation across all active tasks
- Automated abrupt-stop recovery on system startup
"""

from __future__ import annotations

import asyncio
import logging
import shutil
from pathlib import Path
from typing import Optional

from telethon import TelegramClient

from db import Database
from task_runner import BASE_DIR, TaskRunner

log = logging.getLogger("tg-manager")


class TaskManager:
    def __init__(self, db: Database, client: TelegramClient):
        self.db = db
        self.client = client
        self.active_runners: dict[str, TaskRunner] = {}
        self.async_tasks: dict[str, asyncio.Task] = {}

    def startup_recovery(self) -> int:
        """Called when server starts. Recovers tasks interrupted by server shutdown/crash."""
        recovered = 0
        tasks = self.db.list_tasks()
        for t in tasks:
            tid = t["id"]
            if t.get("status") in ("running", "scanning"):
                log.info("Task %s was '%s' during last shutdown. Resetting to paused...", tid, t.get("status"))
                self.db.reset_incomplete_media(tid)
                self.db.update_task_status(tid, "paused")
                self.db.add_log(tid, "WARNING", "Recovered from abrupt server shutdown/restart. Ready to resume.")
                recovered += 1
            else:
                self.db.reset_incomplete_media(tid)
        return recovered

    async def start_task(self, task_id: str) -> dict:
        task = self.db.get_task(task_id)
        if not task:
            raise ValueError(f"Task {task_id} not found")

        if task_id in self.active_runners:
            return self.get_task_details(task_id)

        runner = TaskRunner(task_id, self.db, self.client)
        self.active_runners[task_id] = runner

        async def _run():
            try:
                await runner.run()
            finally:
                self.active_runners.pop(task_id, None)
                self.async_tasks.pop(task_id, None)

        self.async_tasks[task_id] = asyncio.create_task(_run())
        return self.get_task_details(task_id)

    async def pause_task(self, task_id: str) -> dict:
        runner = self.active_runners.get(task_id)
        if runner:
            await runner.pause()
        else:
            self.db.update_task_status(task_id, "paused")
        return self.get_task_details(task_id)

    async def resume_task(self, task_id: str) -> dict:
        return await self.start_task(task_id)

    async def stop_task(self, task_id: str) -> dict:
        runner = self.active_runners.get(task_id)
        if runner:
            await runner.stop()
        else:
            self.db.update_task_status(task_id, "stopped")
        return self.get_task_details(task_id)

    def retry_failed(self, task_id: str) -> dict:
        requeued = self.db.retry_failed_media(task_id)
        self.db.add_log(task_id, "INFO", f"Re-queued {requeued} failed media item(s).")
        return self.get_task_details(task_id)

    async def delete_task(self, task_id: str) -> bool:
        runner = self.active_runners.get(task_id)
        if runner:
            await runner.stop()
            self.active_runners.pop(task_id, None)
        task_coro = self.async_tasks.get(task_id)
        if task_coro and not task_coro.done():
            task_coro.cancel()
            self.async_tasks.pop(task_id, None)

        # Remove task download directory
        task_dir = (BASE_DIR / "downloads") / f"task_{task_id}"
        if task_dir.exists():
            shutil.rmtree(task_dir, ignore_errors=True)

        return self.db.delete_task(task_id)

    def get_task_details(self, task_id: str) -> Optional[dict]:
        task = self.db.get_task(task_id)
        if not task:
            return None

        # Merge live telemetry if running
        runner = self.active_runners.get(task_id)
        if runner:
            live = runner.get_status()
            task["live"] = live
            task["is_active"] = True
            task["down_speed"] = live["down_speed"]
            task["up_speed"] = live["up_speed"]
            task["workers_active"] = live["workers"]
        else:
            task["live"] = None
            task["is_active"] = False
            task["down_speed"] = 0
            task["up_speed"] = 0
            task["workers_active"] = []

        # Calculate progress percent
        total = task.get("total_items") or 0
        done = task.get("done_items") or 0
        task["percent"] = round((done / total) * 100, 1) if total > 0 else 0.0

        return task

    def list_all_tasks(self) -> list[dict]:
        tasks = self.db.list_tasks()
        res = []
        for t in tasks:
            details = self.get_task_details(t["id"])
            if details:
                res.append(details)
        return res

    def get_global_stats(self) -> dict:
        tasks = self.list_all_tasks()
        active_count = sum(1 for t in tasks if t.get("is_active"))
        total_down_speed = sum(t.get("down_speed", 0) for t in tasks)
        total_up_speed = sum(t.get("up_speed", 0) for t in tasks)
        total_done = sum(t.get("done_items", 0) for t in tasks)
        total_pending = sum(t.get("total_items", 0) - t.get("done_items", 0) for t in tasks)

        return {
            "total_tasks": len(tasks),
            "active_tasks": active_count,
            "total_down_speed": round(total_down_speed, 1),
            "total_up_speed": round(total_up_speed, 1),
            "total_done_items": total_done,
            "total_pending_items": max(0, total_pending),
        }
