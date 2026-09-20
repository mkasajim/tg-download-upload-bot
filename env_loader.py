"""
Environment loader for tg-download-upload-bot.

Ensures .env is loaded reliably regardless of entry point or working directory.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

log = logging.getLogger("tg-env")
BASE_DIR = Path(__file__).resolve().parent
ENV_PATH = BASE_DIR / ".env"


def load_env(path: Path | None = None, override: bool = False) -> bool:
    env_file = path or ENV_PATH
    if not env_file.exists():
        log.warning(".env file not found at %s", env_file)
        return False

    loaded = False
    if load_dotenv is not None:
        load_dotenv(dotenv_path=env_file, override=override)
        loaded = True

    # Manual parser fallback / guarantee
    try:
        content = env_file.read_text(encoding="utf-8")
        for line in content.splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, val = line.partition("=")
            key = key.strip()
            val = val.strip().strip('"').strip("'")
            if override or key not in os.environ:
                os.environ[key] = val
        loaded = True
    except Exception as e:
        log.error("Failed to manually read .env: %s", e)

    return loaded


# Automatically load on import
load_env()
