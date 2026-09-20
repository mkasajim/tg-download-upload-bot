"""
FastAPI Server and Web Dashboard for tg-download-upload-bot.

Provides:
- Admin authentication with secure cookies
- Real-time bot management, Telegram channel search, and parallel tasks
- Resilient LibSQL/Turso cloud database integration
- Automated Cloudflare Tunnel lifecycle management
"""

from __future__ import annotations

import env_loader  # Must be first to load .env into os.environ

import logging
import os
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

from fastapi import Depends, FastAPI, Form, HTTPException, Request, Response, status
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from telethon import TelegramClient

import auth
import search
from db import Database
from task_manager import TaskManager
from tunnel import TunnelManager

log = logging.getLogger("tg-server")

BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

# Global singletons
db_instance: Optional[Database] = None
client_instance: Optional[TelegramClient] = None
manager_instance: Optional[TaskManager] = None
tunnel_instance: Optional[TunnelManager] = None
me_info: Optional[dict] = None


class CreateTaskRequest(BaseModel):
    name: str
    source: str
    dest: Optional[str] = None
    dest_title: Optional[str] = "Video Backup"
    workers: int = 3
    max_file_size: int = 2097152000


@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_instance, client_instance, manager_instance, tunnel_instance, me_info

    # 1. Initialize Database
    log.info("Initializing database...")
    db_instance = Database()

    # 2. Initialize Telegram Client
    api_id = os.environ.get("API_ID", "").strip()
    api_hash = os.environ.get("API_HASH", "").strip()
    session = os.environ.get("SESSION_NAME", "tg_transfer").strip()
    if session.endswith(".session"):
        session = session[:-8]
    session_path = str((BASE_DIR / session).with_suffix(".session"))
    phone = os.environ.get("PHONE", "").strip() or None

    if api_id and api_hash:
        try:
            log.info("Initializing Telegram client with session %s...", session_path)
            client_instance = TelegramClient(session_path, int(api_id), api_hash)
            await client_instance.start(phone=phone)
            me = await client_instance.get_me()
            me_info = {
                "id": me.id,
                "username": "@" + me.username if getattr(me, "username", None) else None,
                "first_name": me.first_name,
                "phone": getattr(me, "phone", None),
                "connected": True,
            }
            log.info("Telegram client connected as %s (id: %s)", me_info["username"] or me_info["first_name"], me.id)
        except Exception as e:
            log.error("Failed to initialize Telegram client: %s", e)
            me_info = {"connected": False, "error": str(e)}
    else:
        log.warning("API_ID or API_HASH missing in .env. Telegram client not started.")
        me_info = {"connected": False, "error": "API_ID or API_HASH missing"}

    # 3. Initialize TaskManager and run startup crash recovery
    if client_instance and db_instance:
        manager_instance = TaskManager(db_instance, client_instance)
        recovered = manager_instance.startup_recovery()
        if recovered:
            log.info("Crash recovery: Rescheduled %d interrupted task(s) for resumption.", recovered)

    # 4. Initialize Cloudflare Tunnel
    tunnel_instance = TunnelManager()
    tunnel_instance.start()

    yield

    # Shutdown
    log.info("Shutting down server...")
    if tunnel_instance:
        tunnel_instance.stop()
    if client_instance and client_instance.is_connected():
        await client_instance.disconnect()


app = FastAPI(title="Telegram Media Transfer Dashboard", lifespan=lifespan)


# ---------------------------------------------------------------------------
# HTML Pages
# ---------------------------------------------------------------------------

@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    # If already authenticated, redirect to dashboard
    token = request.cookies.get("session_token")
    if token and auth.decode_session_token(token):
        return RedirectResponse(url="/", status_code=status.HTTP_303_SEE_OTHER)
    return templates.TemplateResponse(request=request, name="login.html")


@app.get("/", response_class=HTMLResponse)
async def dashboard_page(request: Request, admin: str = Depends(auth.get_current_admin)):
    return templates.TemplateResponse(request=request, name="dashboard.html", context={"admin": admin})


# ---------------------------------------------------------------------------
# Auth API
# ---------------------------------------------------------------------------

@app.post("/api/login")
async def api_login(response: Response, username: str = Form(...), password: str = Form(...)):
    if not auth.verify_credentials(username, password):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid username or password")

    token = auth.create_session_token(username)
    response.set_cookie(
        key="session_token",
        value=token,
        httponly=True,
        max_age=auth.SESSION_MAX_AGE,
        samesite="lax",
        secure=False,  # Works seamlessly over HTTP localhost or Cloudflare HTTPS proxy
    )
    return {"status": "ok", "username": username}


@app.post("/api/logout")
async def api_logout(response: Response):
    response.delete_cookie("session_token")
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# System Status API
# ---------------------------------------------------------------------------

@app.get("/api/status")
async def api_status(admin: str = Depends(auth.get_current_admin)):
    stats = manager_instance.get_global_stats() if manager_instance else {}
    tunnel_status = tunnel_instance.get_status() if tunnel_instance else {"enabled": False}

    return {
        "status": "ok",
        "admin": admin,
        "telegram": me_info or {"connected": False},
        "database": {
            "is_remote": getattr(db_instance, "is_remote", False),
            "remote_url": getattr(db_instance, "remote_url", ""),
            "db_path": str(getattr(db_instance, "db_path", "")),
        },
        "tunnel": tunnel_status,
        "port": int(os.environ.get("PORT", 8000)),
        "stats": stats,
    }


# ---------------------------------------------------------------------------
# Telegram Search API
# ---------------------------------------------------------------------------

@app.get("/api/search/dialogs")
async def api_search_dialogs(q: str = "", admin: str = Depends(auth.get_current_admin)):
    if not client_instance or not client_instance.is_connected():
        raise HTTPException(status_code=503, detail="Telegram client not connected")
    results = await search.search_dialogs_structured(client_instance, q)
    return results


@app.get("/api/search/public")
async def api_search_public(q: str = "", limit: int = 20, admin: str = Depends(auth.get_current_admin)):
    if not client_instance or not client_instance.is_connected():
        raise HTTPException(status_code=503, detail="Telegram client not connected")
    results = await search.search_public_structured(client_instance, q, limit=limit)
    return results


# ---------------------------------------------------------------------------
# Task Management API
# ---------------------------------------------------------------------------

@app.get("/api/tasks")
async def api_list_tasks(admin: str = Depends(auth.get_current_admin)):
    if manager_instance:
        return manager_instance.list_all_tasks()
    if db_instance:
        return db_instance.list_tasks()
    return []


@app.post("/api/tasks")
async def api_create_task(req: CreateTaskRequest, admin: str = Depends(auth.get_current_admin)):
    if not db_instance:
        raise HTTPException(status_code=500, detail="Database not initialized")

    task_id = f"task_{uuid.uuid4().hex[:8]}"
    task = db_instance.create_task(
        task_id=task_id,
        name=req.name,
        source_peer=req.source,
        dest_peer=req.dest,
        dest_title=req.dest_title,
        workers=req.workers,
        max_file_size=req.max_file_size,
    )
    return task


@app.post("/api/tasks/{task_id}/start")
async def api_start_task(task_id: str, admin: str = Depends(auth.get_current_admin)):
    if not manager_instance:
        raise HTTPException(status_code=500, detail="TaskManager not ready")
    try:
        return await manager_instance.start_task(task_id)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/api/tasks/{task_id}/pause")
async def api_pause_task(task_id: str, admin: str = Depends(auth.get_current_admin)):
    if not manager_instance:
        raise HTTPException(status_code=500, detail="TaskManager not ready")
    return await manager_instance.pause_task(task_id)


@app.post("/api/tasks/{task_id}/resume")
async def api_resume_task(task_id: str, admin: str = Depends(auth.get_current_admin)):
    if not manager_instance:
        raise HTTPException(status_code=500, detail="TaskManager not ready")
    return await manager_instance.resume_task(task_id)


@app.post("/api/tasks/{task_id}/stop")
async def api_stop_task(task_id: str, admin: str = Depends(auth.get_current_admin)):
    if not manager_instance:
        raise HTTPException(status_code=500, detail="TaskManager not ready")
    return await manager_instance.stop_task(task_id)


@app.post("/api/tasks/{task_id}/retry-failed")
async def api_retry_failed(task_id: str, admin: str = Depends(auth.get_current_admin)):
    if manager_instance:
        return manager_instance.retry_failed(task_id)
    if db_instance:
        db_instance.retry_failed_media(task_id)
        return db_instance.get_task(task_id) or {}
    raise HTTPException(status_code=500, detail="Database not ready")


@app.delete("/api/tasks/{task_id}")
async def api_delete_task(task_id: str, admin: str = Depends(auth.get_current_admin)):
    if manager_instance:
        ok = await manager_instance.delete_task(task_id)
        return {"status": "ok" if ok else "not_found"}
    if db_instance:
        ok = db_instance.delete_task(task_id)
        return {"status": "ok" if ok else "not_found"}
    raise HTTPException(status_code=500, detail="Database not ready")


@app.get("/api/tasks/{task_id}/media")
async def api_task_media(
    task_id: str,
    status: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
    admin: str = Depends(auth.get_current_admin),
):
    if not db_instance:
        return []
    return db_instance.list_media(task_id, status=status, offset=offset, limit=limit)


@app.get("/api/tasks/{task_id}/logs")
async def api_task_logs(
    task_id: str,
    limit: int = 100,
    admin: str = Depends(auth.get_current_admin),
):
    if not db_instance:
        return []
    return db_instance.get_logs(task_id, limit=limit)


# ---------------------------------------------------------------------------
# Direct Runner
# ---------------------------------------------------------------------------

def run_server():
    import uvicorn
    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", 8000))
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
    )
    logging.getLogger("telethon").setLevel(logging.WARNING)
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)

    log.info("Starting Dashboard Server at http://%s:%d", host, port)
    uvicorn.run("server:app", host=host, port=port, reload=False)


if __name__ == "__main__":
    run_server()
