"""
Admin Authentication Module for tg-download-upload-bot dashboard.

Provides:
- Secure credential verification
- Signed session cookie creation and validation via itsdangerous
- FastAPI authentication dependency for protected API and HTML endpoints
"""

from __future__ import annotations

import env_loader  # Ensures .env is loaded

import os
import secrets
from typing import Optional

from fastapi import Depends, HTTPException, Request, status
from fastapi.responses import RedirectResponse
from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer

SESSION_MAX_AGE = 7 * 24 * 3600  # 7 days


def get_admin_username() -> str:
    return os.environ.get("ADMIN_USERNAME", "admin").strip()


def get_admin_password() -> str:
    return os.environ.get("ADMIN_PASSWORD", "admin").strip()


def get_serializer() -> URLSafeTimedSerializer:
    secret = os.environ.get("ADMIN_SECRET_KEY", "tg_bot_super_secret_key_change_me").strip()
    return URLSafeTimedSerializer(secret, salt="tg-admin-auth")


def verify_credentials(user: str, pwd: str) -> bool:
    is_user_ok = secrets.compare_digest(user.strip(), get_admin_username())
    is_pwd_ok = secrets.compare_digest(pwd.strip(), get_admin_password())
    return is_user_ok and is_pwd_ok


def create_session_token(username: str) -> str:
    return get_serializer().dumps({"sub": username})


def decode_session_token(token: str) -> Optional[str]:
    try:
        data = get_serializer().loads(token, max_age=SESSION_MAX_AGE)
        return data.get("sub")
    except (BadSignature, SignatureExpired):
        return None


def get_current_admin(request: Request) -> str:
    token = request.cookies.get("session_token")
    if not token:
        # Check Authorization header as alternative
        auth_hdr = request.headers.get("Authorization")
        if auth_hdr and auth_hdr.startswith("Bearer "):
            token = auth_hdr.split(" ", 1)[1]

    username = decode_session_token(token) if token else None
    if not username:
        if "text/html" in request.headers.get("Accept", ""):
            raise HTTPException(
                status_code=status.HTTP_307_TEMPORARY_REDIRECT,
                headers={"Location": "/login"},
            )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required",
        )
    return username
