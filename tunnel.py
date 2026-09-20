"""
Cloudflare Tunnel Manager for tg-download-upload-bot.

Supports:
1. Cloudflare User API Tokens (cfut_...):
   - Automatically queries Cloudflare API
   - Discovers account and zone (e.g. tunnelme.eu.cc)
   - Checks for any previous stale DNS records or down tunnels using the subdomain and cleans them up
   - Dynamically provisions the tunnel, ingress configuration, and CNAME DNS record
   - Runs cloudflared with the provisioned TUNNEL_TOKEN
   - Cleans up tunnels and DNS records on shutdown
2. Cloudflare Zero Trust Connector Tokens (eyJh...):
   - Runs cloudflared tunnel run --token <token> directly
3. Quick Tunnels (fallback):
   - Spawns cloudflared tunnel --url http://localhost:{port} and captures trycloudflare.com URL
"""

from __future__ import annotations

import env_loader  # Ensures .env is loaded

import asyncio
import base64
import json
import logging
import os
import re
import secrets
import shutil
import subprocess
import threading
from pathlib import Path
from typing import Any, Optional

import httpx

log = logging.getLogger("tg-tunnel")

BASE_DIR = Path(__file__).resolve().parent
PID_FILE = BASE_DIR / "cloudflared.pid"
TRYCLOUDFLARE_RE = re.compile(r"https://[a-z0-9-]+\.trycloudflare\.com", re.IGNORECASE)


def find_cloudflared() -> Optional[str]:
    p = shutil.which("cloudflared")
    if p and os.path.exists(p):
        return p

    win_bin = Path("C:/Users/ASUS/bin/cloudflared.exe")
    if win_bin.exists():
        return str(win_bin)

    user_home = Path.home()
    candidates = [
        user_home / "bin" / "cloudflared.exe",
        user_home / "bin" / "cloudflared",
        Path("/usr/local/bin/cloudflared"),
        Path("/usr/bin/cloudflared"),
        BASE_DIR / "cloudflared.exe",
        BASE_DIR / "cloudflared",
    ]
    for c in candidates:
        if c.exists():
            return str(c)
    return None


def extract_tunnel_id_from_token(token: str) -> Optional[str]:
    """Extracts tunnel UUID from a base64 encoded Zero Trust token."""
    try:
        raw = token.strip()
        pad = len(raw) % 4
        if pad:
            raw += "=" * (4 - pad)
        decoded = base64.b64decode(raw).decode("utf-8")
        data = json.loads(decoded)
        return data.get("t")
    except Exception:
        return None


class TunnelManager:
    def __init__(self):
        self.exe = find_cloudflared()
        self.raw_token = (
            os.environ.get("CLOUDFLARE_TUNNEL_TOKEN", "")
            or os.environ.get("CLOUDFLARE_API_TOKEN", "")
        ).strip()
        self.domain_config = (
            os.environ.get("CLOUDFLARE_DOMAIN", "")
            or os.environ.get("CLOUDFLARE_TUNNEL_DOMAIN", "")
        ).strip()
        self.port = int(os.environ.get("PORT", 8000))

        self.process: Optional[subprocess.Popen] = None
        self.is_running = False
        self.is_connected = False
        self.public_url: str = ""
        self.error_message: Optional[str] = None
        self.logs: list[str] = []
        self._monitor_thread: Optional[threading.Thread] = None
        self._connected_event = threading.Event()

        # Cloudflare API tracking
        self.account_id: Optional[str] = None
        self.zone_id: Optional[str] = None
        self.hostname: Optional[str] = None
        self.tunnel_id: Optional[str] = None
        self.dns_record_id: Optional[str] = None
        self.is_api_managed: bool = False

    def _api_request(self, method: str, path: str, **kwargs) -> dict[str, Any]:
        url = f"https://api.cloudflare.com/client/v4{path}"
        headers = {
            "Authorization": f"Bearer {self.raw_token}",
            "Content-Type": "application/json",
        }
        with httpx.Client(timeout=30) as client:
            resp = client.request(method, url, headers=headers, **kwargs)
            try:
                data = resp.json()
            except Exception as e:
                raise RuntimeError(f"Cloudflare API returned non-JSON: HTTP {resp.status_code}") from e

            if not resp.is_success or not data.get("success", False):
                errs = data.get("errors", [])
                err_msg = "; ".join(str(e.get("message", e)) for e in errs) or resp.text
                raise RuntimeError(f"Cloudflare API error: {err_msg}")
            return data

    def _try_request(self, method: str, path: str, **kwargs) -> Optional[dict[str, Any]]:
        try:
            return self._api_request(method, path, **kwargs)
        except Exception:
            return None

    def cleanup_previous_processes(self) -> None:
        """Kills any lingering local cloudflared processes."""
        if PID_FILE.exists():
            try:
                old_pid = int(PID_FILE.read_text().strip())
                log.info("Checking previous cloudflared process PID: %d", old_pid)
                if os.name == "nt":
                    subprocess.run(["taskkill", "/F", "/PID", str(old_pid)], capture_output=True)
                else:
                    os.kill(old_pid, 9)
            except Exception:
                pass
            finally:
                PID_FILE.unlink(missing_ok=True)

    def _cleanup_stale_down_tunnels(self, account_id: str, keep_tunnel_id: Optional[str] = None) -> None:
        """Safely removes any previously stopped/down tgbot tunnels."""
        try:
            tunnels_res = self._api_request("GET", f"/accounts/{account_id}/cfd_tunnel?is_deleted=false")
            for t in tunnels_res.get("result", []):
                t_name = t.get("name", "")
                t_id = t.get("id")
                t_status = t.get("status", "")
                if t_name.startswith("tgbot-") and t_id != keep_tunnel_id:
                    if t_status in ("down", "inactive"):
                        log.info("Safely removing inactive previous tunnel: %s (%s)...", t_name, t_id)
                        self._try_request("DELETE", f"/accounts/{account_id}/cfd_tunnel/{t_id}")
        except Exception as e:
            log.warning("Notice while checking previous down tunnels: %s", e)

    def _setup_api_managed_tunnel(self) -> str:
        """Configures a tunnel via Cloudflare REST API using the user's API token."""
        log.info("Configuring Cloudflare Tunnel via API...")

        # 1. Discover available zones
        zones_res = self._api_request("GET", "/zones")
        zones = zones_res.get("result", [])
        if not zones:
            raise RuntimeError("No Cloudflare zones accessible with this API token.")

        # Match zone against configured domain
        selected_zone = None
        if self.domain_config:
            for z in zones:
                zname = z.get("name", "")
                if self.domain_config == zname or self.domain_config.endswith("." + zname):
                    selected_zone = z
                    break

        if not selected_zone:
            selected_zone = zones[0]
            log.info("Using primary Cloudflare zone: %s", selected_zone.get("name"))

        self.zone_id = selected_zone["id"]
        zone_name = selected_zone["name"]

        # 2. Discover account ID
        acct = selected_zone.get("account") or {}
        self.account_id = acct.get("id")
        if not self.account_id:
            accts_res = self._api_request("GET", "/accounts")
            accts = accts_res.get("result", [])
            if accts:
                self.account_id = accts[0]["id"]
            else:
                raise RuntimeError("Could not determine Cloudflare Account ID.")

        # 3. Determine Hostname & Subdomain
        subdomain = "bot"
        if self.domain_config:
            if self.domain_config.endswith("." + zone_name):
                self.hostname = self.domain_config
                prefix = self.domain_config[: -len("." + zone_name)].strip(".")
                if prefix:
                    subdomain = prefix
            elif self.domain_config == zone_name:
                self.hostname = f"{subdomain}.{zone_name}"
            else:
                first_part = self.domain_config.split(".")[0]
                subdomain = first_part
                self.hostname = f"{first_part}.{zone_name}"
        else:
            self.hostname = f"{subdomain}.{zone_name}"

        log.info("Resolved public tunnel hostname: %s", self.hostname)

        # 4. Check for existing persistent tunnel for this subdomain
        tunnel_name = f"tgbot-{subdomain}"
        tunnels_res = self._api_request("GET", f"/accounts/{self.account_id}/cfd_tunnel?is_deleted=false")
        active_tunnels = tunnels_res.get("result", [])
        existing_tun = next((t for t in active_tunnels if t.get("name") == tunnel_name), None)

        tunnel_token = None
        if existing_tun:
            self.tunnel_id = str(existing_tun["id"])
            log.info("Found existing Cloudflare tunnel '%s' (id: %s). Fetching token...", tunnel_name, self.tunnel_id)
            token_res = self._try_request("GET", f"/accounts/{self.account_id}/cfd_tunnel/{self.tunnel_id}/token")
            if token_res and token_res.get("result"):
                tunnel_token = str(token_res["result"])
            else:
                log.info("Could not fetch token for existing tunnel, recreating tunnel...")
                self._try_request("DELETE", f"/accounts/{self.account_id}/cfd_tunnel/{self.tunnel_id}")
                self.tunnel_id = None

        if not self.tunnel_id or not tunnel_token:
            log.info("Creating persistent Cloudflare tunnel '%s'...", tunnel_name)
            create_res = self._api_request(
                "POST",
                f"/accounts/{self.account_id}/cfd_tunnel",
                json={"name": tunnel_name, "config_src": "cloudflare"},
            )
            result = create_res["result"]
            self.tunnel_id = str(result["id"])
            tunnel_token = str(result.get("token") or "")
            if not tunnel_token:
                token_res = self._api_request("GET", f"/accounts/{self.account_id}/cfd_tunnel/{self.tunnel_id}/token")
                tunnel_token = str(token_res["result"])

        # Safely remove any OTHER stopped/down tunnels using tgbot prefix
        self._cleanup_stale_down_tunnels(self.account_id, keep_tunnel_id=self.tunnel_id)

        # 5. Configure Tunnel Ingress
        log.info("Configuring tunnel ingress for %s -> http://localhost:%d...", self.hostname, self.port)
        self._api_request(
            "PUT",
            f"/accounts/{self.account_id}/cfd_tunnel/{self.tunnel_id}/configurations",
            json={
                "config": {
                    "ingress": [
                        {
                            "hostname": self.hostname,
                            "service": f"http://localhost:{self.port}",
                            "originRequest": {},
                        },
                        {"service": "http_status:404"},
                    ]
                }
            },
        )

        # 6. Ensure CNAME DNS record in Cloudflare
        target_cname = f"{self.tunnel_id}.cfargotunnel.com"
        dns_res = self._api_request("GET", f"/zones/{self.zone_id}/dns_records?name={self.hostname}")
        records = dns_res.get("result", [])
        if records:
            r = records[0]
            self.dns_record_id = str(r["id"])
            if r.get("type") == "CNAME" and r.get("content") == target_cname and r.get("proxied") is True:
                log.info("DNS record %s is already up-to-date pointing to %s (proxied).", self.hostname, target_cname)
            else:
                log.info("Updating DNS record %s -> %s (proxied)...", self.hostname, target_cname)
                self._api_request(
                    "PUT",
                    f"/zones/{self.zone_id}/dns_records/{r['id']}",
                    json={
                        "type": "CNAME",
                        "proxied": True,
                        "name": self.hostname,
                        "content": target_cname,
                    },
                )
            for extra in records[1:]:
                self._try_request("DELETE", f"/zones/{self.zone_id}/dns_records/{extra['id']}")
        else:
            log.info("Creating DNS record %s -> %s (proxied)...", self.hostname, target_cname)
            dns_create = self._api_request(
                "POST",
                f"/zones/{self.zone_id}/dns_records",
                json={
                    "type": "CNAME",
                    "proxied": True,
                    "name": self.hostname,
                    "content": target_cname,
                },
            )
            self.dns_record_id = str(dns_create["result"]["id"])

        self.is_api_managed = True
        self.public_url = f"https://{self.hostname}"
        return str(tunnel_token)

    def start(self) -> bool:
        if not self.raw_token:
            log.info("No Cloudflare token configured. Running locally only.")
            return False

        if not self.exe:
            log.warning("cloudflared binary not found. Cannot launch Cloudflare Tunnel.")
            self.error_message = "cloudflared binary not found on system"
            return False

        self.cleanup_previous_processes()

        tunnel_run_token: Optional[str] = None
        cmd = [self.exe, "tunnel", "--no-autoupdate", "run"]

        # Detect token type
        if self.raw_token.startswith("cfut_"):
            # Cloudflare User API Token
            try:
                tunnel_run_token = self._setup_api_managed_tunnel()
            except Exception as e:
                log.error("Failed to setup Cloudflare Tunnel via API: %s", e)
                self.error_message = str(e)
                return False
        elif self.raw_token.startswith("eyJh"):
            # Direct Zero Trust Tunnel Connector Token
            tunnel_run_token = self.raw_token
            if self.domain_config:
                self.public_url = f"https://{self.domain_config}"
        else:
            log.warning("Unrecognized token format. Attempting direct tunnel execution...")
            tunnel_run_token = self.raw_token

        env = os.environ.copy()
        if tunnel_run_token:
            env["TUNNEL_TOKEN"] = tunnel_run_token

        log.info("Starting cloudflared edge connector...")
        self._connected_event.clear()
        try:
            self.process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                stdin=subprocess.DEVNULL,
                env=env,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1,
            )
            self.is_running = True
            PID_FILE.write_text(str(self.process.pid))

            self._monitor_thread = threading.Thread(target=self._read_logs, daemon=True)
            self._monitor_thread.start()

            # Wait briefly for edge registration before completing startup
            log.info("Waiting for Cloudflare edge registration...")
            ready = self._connected_event.wait(timeout=12)
            if ready:
                log.info("Cloudflare Tunnel connected and ready! URL: %s", self.public_url)
            else:
                log.info("Cloudflare Tunnel process active. Public URL: %s", self.public_url)

            return True
        except Exception as e:
            log.error("Failed to start cloudflared process: %s", e)
            self.error_message = str(e)
            return False

    def _read_logs(self) -> None:
        if not self.process or not self.process.stdout:
            return
        for line in iter(self.process.stdout.readline, ""):
            line = line.strip()
            if not line:
                continue
            self.logs.append(line)
            if len(self.logs) > 200:
                self.logs.pop(0)

            # Check for trycloudflare URL if running quick tunnel
            m = TRYCLOUDFLARE_RE.search(line)
            if m and not self.public_url:
                self.public_url = m.group(0)
                self.is_connected = True
                self._connected_event.set()
                log.info("[Cloudflare Tunnel] Public Quick URL: %s", self.public_url)

            # Detect edge registration
            if any(k in line.lower() for k in ("connection registered", "registered tunnel connection", "connection active", "connindex=")):
                self.is_connected = True
                self._connected_event.set()
                log.info("[Cloudflare Tunnel] %s", line)
            elif any(k in line.lower() for k in ("err", "fail", "not valid", "error", "warn")) and "context canceled" not in line.lower():
                log.warning("[Cloudflare Tunnel] %s", line)
            else:
                log.info("[Cloudflare Tunnel] %s", line)

        self.is_running = False
        self.is_connected = False
        log.info("Cloudflare tunnel process exited.")

    def stop(self) -> None:
        if self.process:
            log.info("Stopping Cloudflare Tunnel...")
            try:
                self.process.terminate()
                self.process.wait(timeout=5)
            except Exception:
                try:
                    self.process.kill()
                except Exception:
                    pass
            self.process = None

        self.is_running = False
        self.is_connected = False
        PID_FILE.unlink(missing_ok=True)
        # Note: Persistent tunnel and DNS record remain active in Cloudflare
        # to ensure subsequent starts are instantaneous without DNS propagation delays.
        log.info("Cloudflare Tunnel stopped.")

    def get_status(self) -> dict:
        return {
            "enabled": bool(self.raw_token),
            "running": self.is_running,
            "connected": self.is_connected,
            "tunnel_id": self.tunnel_id,
            "domain": self.hostname or self.domain_config,
            "public_url": self.public_url,
            "error": self.error_message,
            "recent_logs": self.logs[-10:],
        }
