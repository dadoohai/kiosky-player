#!/usr/bin/env python3
import argparse
import hashlib
import json
import logging
import os
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from logging.handlers import RotatingFileHandler
from typing import Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse

try:
    import requests
except Exception:  # pragma: no cover - handled at runtime
    requests = None


def default_ipc_path() -> str:
    if os.name == "nt":
        return r"\\.\pipe\mpv-kiosk"
    return os.path.join(tempfile.gettempdir(), "mpv-kiosk.sock")


DEFAULT_CONFIG = {
    "api_url": "https://us-central1-habitat-19883.cloudfunctions.net/api/search",
    "api_key": "",
    "environment_id": "",
    "only_standby": True,
    "search_in": "campaign",
    "include_descendants": True,
    "limit": 20,
    "poll_interval_sec": 1800,
    "request_timeout_sec": 15,
    "default_duration_ms": 10000,
    "cache_dir": "./media_cache",
    "mpv_path": "mpv",
    "ipc_path": default_ipc_path(),
    "rotation_deg": 0,
    "hotkeys_enabled": True,
    "hotkey_open_key": "Ctrl+s",
    "config_ui_enabled": True,
    "config_ui_bind": "127.0.0.1",
    "config_ui_port": 8765,
    "low_resource_mode": False,
    "telemetry_enabled": True,
    "telemetry_url": "https://api.dadooh.ai/api/v1/interact/telemetry",
    "telemetry_interval_sec": 300,
    "telemetry_timeout_sec": 10,
    "station_id": "",
    "preload_next": True,
    "mute": False,
    "lock_input": True,
    "hwdec": "auto",
    "log_file": "",
    "log_max_bytes": 5_000_000,
    "log_backup_count": 3,
    "watchdog_interval_sec": 10,
    "status_file": "",
    "status_interval_sec": 5,
    "cleanup_interval_sec": 1800,
}


@dataclass(frozen=True)
class MediaItem:
    url: str
    duration_ms: int
    path: str
    campaign_id: str
    campaign_name: str


class PlaylistState:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._items: List[MediaItem] = []
        self._version = 0
        self._fingerprint = ""

    def get(self) -> Tuple[List[MediaItem], int]:
        with self._lock:
            return list(self._items), self._version

    def update(self, items: List[MediaItem], fingerprint: str) -> bool:
        with self._lock:
            if fingerprint == self._fingerprint:
                return False
            self._items = list(items)
            self._version += 1
            self._fingerprint = fingerprint
            return True


def load_config(path: str) -> Dict:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Config not found: {path}")
    with open(path, "r", encoding="utf-8") as fh:
        data = json.load(fh)
    cfg = dict(DEFAULT_CONFIG)
    cfg.update(data)
    if not cfg.get("ipc_path"):
        cfg["ipc_path"] = default_ipc_path()
    return cfg


def setup_logging(cfg: Dict) -> None:
    level = logging.INFO
    handlers: List[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    log_file = cfg.get("log_file")
    if log_file:
        log_dir = os.path.dirname(log_file)
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)
        handlers.append(
            RotatingFileHandler(
                log_file,
                maxBytes=int(cfg.get("log_max_bytes") or 0),
                backupCount=int(cfg.get("log_backup_count") or 0),
            )
        )
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=handlers,
    )


def iso_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def config_snapshot(cfg: Dict, lock: threading.Lock) -> Dict:
    with lock:
        return dict(cfg)


def write_config(path: str, cfg: Dict) -> None:
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as fh:
        json.dump(cfg, fh, indent=2, ensure_ascii=False)
    os.replace(tmp_path, path)


def normalize_rotation(value: str) -> int:
    try:
        rotation = int(value)
    except Exception:
        return 0
    if rotation not in {0, 90, 180, 270}:
        return 0
    return rotation


def client_timestamp_ms() -> int:
    return int(time.time() * 1000)


def build_telemetry_payload(
    cfg_snapshot: Dict,
    status_snapshot: Dict[str, Optional[object]],
    heartbeat_type: str,
    status: str,
    error_code: Optional[str] = None,
    error_message: Optional[str] = None,
    notes: Optional[str] = None,
    uptime_seconds: Optional[int] = None,
) -> Dict:
    current = status_snapshot.get("current_item") or {}
    next_item = status_snapshot.get("next_item") or {}
    playlist_size = status_snapshot.get("playlist_size")
    preload_size = 1 if isinstance(next_item, dict) and next_item.get("path") else 0
    payload: Dict[str, object] = {
        "environmentId": cfg_snapshot.get("environment_id", ""),
        "status": status,
        "heartbeatType": heartbeat_type,
        "clientTimestamp": client_timestamp_ms(),
        "playlistSize": playlist_size,
        "activeCampaignName": current.get("campaign_name") if isinstance(current, dict) else None,
        "nextCampaignName": next_item.get("campaign_name") if isinstance(next_item, dict) else None,
        "rotation": cfg_snapshot.get("rotation_deg"),
        "metrics": {
            "uptimeSeconds": uptime_seconds,
            "preloadSize": preload_size,
            "pendingEntries": 0,
        },
        "notes": notes,
    }
    station_id = cfg_snapshot.get("station_id")
    if station_id:
        payload["stationId"] = station_id
    if error_code:
        payload["errorCode"] = error_code
    if error_message:
        payload["errorMessage"] = error_message
    if status_snapshot.get("consecutive_failures") is not None:
        payload["consecutiveFailures"] = int(status_snapshot.get("consecutive_failures") or 0)
    return payload


def send_telemetry(
    cfg_snapshot: Dict,
    status_snapshot: Dict[str, Optional[object]],
    heartbeat_type: str,
    status: str = "ok",
    error_code: Optional[str] = None,
    error_message: Optional[str] = None,
    notes: Optional[str] = None,
    uptime_seconds: Optional[int] = None,
) -> bool:
    if not cfg_snapshot.get("telemetry_enabled"):
        return False
    if requests is None:
        return False
    url = cfg_snapshot.get("telemetry_url")
    if not url:
        return False
    headers = {
        "x-interact-telemetry-token": "540fca561dcb494287e8f820381c0e0f",
    }
    payload = build_telemetry_payload(
        cfg_snapshot,
        status_snapshot,
        heartbeat_type,
        status,
        error_code=error_code,
        error_message=error_message,
        notes=notes,
        uptime_seconds=uptime_seconds,
    )
    try:
        requests.post(
            url,
            json=payload,
            headers=headers,
            timeout=int(cfg_snapshot.get("telemetry_timeout_sec") or 10),
        )
        return True
    except Exception as exc:
        logging.warning("Telemetry failed: %s", exc)
        return False


class StatusState:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._data: Dict[str, Optional[object]] = {
            "started_at": iso_now(),
            "last_poll_success": None,
            "last_poll_error": None,
            "playlist_size": None,
            "current_index": None,
            "current_item": None,
            "next_item": None,
            "mpv_running": None,
            "mpv_last_ok": None,
            "last_cleanup": None,
            "last_cleanup_removed": None,
            "consecutive_failures": 0,
            "last_telemetry_error": None,
        }
        self.start_time = time.time()

    def update(self, **kwargs: object) -> None:
        with self._lock:
            self._data.update(kwargs)

    def snapshot(self) -> Dict[str, Optional[object]]:
        with self._lock:
            return dict(self._data)


def sha1_hex(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8"), usedforsecurity=False).hexdigest()


def cache_path(cache_dir: str, url: str) -> str:
    parsed = urlparse(url)
    _, ext = os.path.splitext(parsed.path)
    if not ext:
        ext = ".bin"
    return os.path.join(cache_dir, f"{sha1_hex(url)}{ext}")


def fetch_media_list(cfg: Dict) -> List[Dict]:
    if requests is None:
        raise RuntimeError("requests is required. Install with: pip install -r requirements.txt")

    payload = {
        "environmentId": cfg["environment_id"],
        "onlyStandby": cfg["only_standby"],
        "searchIn": cfg["search_in"],
        "includeDescendants": cfg["include_descendants"],
        "limit": cfg["limit"],
    }
    headers = {"x-api-key": cfg["api_key"]}

    resp = requests.post(
        cfg["api_url"],
        headers=headers,
        json=payload,
        timeout=cfg["request_timeout_sec"],
    )
    resp.raise_for_status()
    data = resp.json()

    items: List[Dict] = []
    for unit in data.get("units", []):
        for campaign in unit.get("campaigns", []) or []:
            status = str(campaign.get("status", "")).lower()
            if status and status not in {"ativa", "active"}:
                continue
            duration_ms = int(campaign.get("exposure_time_ms") or cfg["default_duration_ms"])
            urls = list(campaign.get("media_urls") or [])
            if not urls and campaign.get("primary_media_url"):
                urls = [campaign["primary_media_url"]]
            for url in urls:
                if not url:
                    continue
                items.append(
                    {
                        "url": url,
                        "duration_ms": duration_ms,
                        "campaign_id": str(campaign.get("id", "")),
                        "campaign_name": str(campaign.get("name", "")),
                    }
                )
    return items


def download_media(cfg: Dict, raw_items: List[Dict]) -> List[MediaItem]:
    os.makedirs(cfg["cache_dir"], exist_ok=True)
    items: List[MediaItem] = []

    for item in raw_items:
        url = item["url"]
        dest = cache_path(cfg["cache_dir"], url)
        if not os.path.exists(dest):
            try:
                logging.info("Downloading %s", url)
                resp = requests.get(url, stream=True, timeout=cfg["request_timeout_sec"])
                resp.raise_for_status()
                tmp_path = f"{dest}.tmp"
                with open(tmp_path, "wb") as fh:
                    for chunk in resp.iter_content(chunk_size=1024 * 256):
                        if chunk:
                            fh.write(chunk)
                os.replace(tmp_path, dest)
            except Exception as exc:
                logging.warning("Failed to download %s: %s", url, exc)
                if os.path.exists(dest):
                    logging.info("Using cached file for %s", url)
                else:
                    continue

        items.append(
            MediaItem(
                url=url,
                duration_ms=int(item["duration_ms"]),
                path=dest,
                campaign_id=item.get("campaign_id", ""),
                campaign_name=item.get("campaign_name", ""),
            )
        )
    return items


def fingerprint_items(raw_items: List[Dict]) -> str:
    payload = [{"url": i["url"], "duration_ms": i["duration_ms"]} for i in raw_items]
    return sha1_hex(json.dumps(payload, sort_keys=True))


def build_open_command(cfg: Dict) -> List[str]:
    url = f"http://{cfg['config_ui_bind']}:{cfg['config_ui_port']}"
    if os.name == "nt":
        return ["cmd", "/c", "start", "", url]
    if sys.platform == "darwin":
        return ["open", url]
    return ["xdg-open", url]


def ensure_hotkey_conf(cfg: Dict) -> Optional[str]:
    if not cfg.get("hotkeys_enabled"):
        return None
    runtime_dir = os.path.join(".", "runtime")
    os.makedirs(runtime_dir, exist_ok=True)
    conf_path = os.path.join(runtime_dir, "hotkeys.conf")
    cmd = build_open_command(cfg)
    quoted = " ".join([f'"{arg}"' for arg in cmd])
    line = f"{cfg.get('hotkey_open_key', 'Ctrl+s')} run {quoted}\n"
    try:
        with open(conf_path, "w", encoding="utf-8") as fh:
            fh.write(line)
    except Exception as exc:
        logging.warning("Failed to write hotkey conf: %s", exc)
        return None
    return conf_path


def build_mpv_args(cfg: Dict) -> List[str]:
    args = [
        cfg["mpv_path"],
        "--fs",
        "--force-window=yes",
        "--idle=yes",
        "--keep-open=yes",
        "--no-terminal",
        "--loop-file=inf",
        "--image-display-duration=inf",
        "--no-osc",
        "--osd-level=0",
        f"--input-ipc-server={cfg['ipc_path']}",
    ]
    args.append("--no-input-default-bindings")
    if cfg.get("low_resource_mode"):
        args += [
            "--profile=low-latency",
            "--video-sync=audio",
            "--vd-lavc-threads=1",
            "--scale=bilinear",
            "--dscale=bilinear",
            "--cscale=bilinear",
            "--interpolation=no",
            "--correct-pts=no",
            "--framedrop=decoder+vo",
            "--hwdec-codecs=h264,mpeg4,mpeg2video",
        ]
    if cfg.get("rotation_deg") is not None:
        args.append(f"--video-rotate={int(cfg['rotation_deg'])}")
    hotkey_conf = ensure_hotkey_conf(cfg)
    if hotkey_conf:
        args.append(f"--input-conf={hotkey_conf}")
        args.append("--input-vo-keyboard=yes")
    elif cfg.get("lock_input", True):
        args.append("--input-vo-keyboard=no")
    if cfg.get("mute"):
        args.append("--mute=yes")
    if cfg.get("hwdec"):
        args.append(f"--hwdec={cfg['hwdec']}")
    return args


class MPVController:
    def __init__(self, cfg: Dict) -> None:
        self._cfg = cfg
        self._proc: Optional[subprocess.Popen] = None
        self._ipc = None
        self._ipc_socket = False
        self._lock = threading.Lock()

    def _cleanup_ipc_path(self) -> None:
        ipc_path = self._cfg["ipc_path"]
        if os.name == "nt":
            return
        if os.path.exists(ipc_path):
            try:
                os.remove(ipc_path)
            except OSError:
                pass

    def _open_ipc(self) -> bool:
        ipc_path = self._cfg["ipc_path"]
        start = time.time()
        timeout = 10
        while time.time() - start < timeout:
            try:
                if os.name == "nt" and ipc_path.startswith("\\\\.\\pipe\\"):
                    self._ipc = open(ipc_path, "r+b", buffering=0)
                    self._ipc_socket = False
                    return True
                if os.path.exists(ipc_path):
                    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                    sock.settimeout(2.0)
                    sock.connect(ipc_path)
                    self._ipc = sock
                    self._ipc_socket = True
                    return True
            except Exception:
                time.sleep(0.2)
        return False

    def _close_ipc(self) -> None:
        if self._ipc is None:
            return
        try:
            self._ipc.close()
        except Exception:
            pass
        finally:
            self._ipc = None
            self._ipc_socket = False

    def start(self) -> None:
        with self._lock:
            if self._proc and self._proc.poll() is None:
                return
            self._close_ipc()
            self._cleanup_ipc_path()
            args = build_mpv_args(self._cfg)
            popen_kwargs = {
                "stdout": subprocess.DEVNULL,
                "stderr": subprocess.DEVNULL,
            }
            if os.name == "nt":
                popen_kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
            else:
                popen_kwargs["start_new_session"] = True
            self._proc = subprocess.Popen(args, **popen_kwargs)
            if not self._open_ipc():
                logging.warning("MPV IPC not available, restarting...")
                self.restart()

    def restart(self) -> None:
        with self._lock:
            self.stop()
            time.sleep(1)
            self.start()

    def stop(self) -> None:
        self._close_ipc()
        if self._proc and self._proc.poll() is None:
            try:
                if os.name != "nt" and self._proc.pid:
                    os.killpg(self._proc.pid, signal.SIGTERM)
                else:
                    self._proc.terminate()
                self._proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                try:
                    if os.name != "nt" and self._proc.pid:
                        os.killpg(self._proc.pid, signal.SIGKILL)
                    else:
                        self._proc.kill()
                except Exception:
                    pass
        self._proc = None
        self._cleanup_ipc_path()

    def ensure_running(self) -> None:
        if self._proc is None or self._proc.poll() is not None:
            self.start()

    def is_running(self) -> bool:
        return self._proc is not None and self._proc.poll() is None

    def _send(self, payload: Dict) -> bool:
        data = (json.dumps(payload) + "\n").encode("utf-8")
        if self._ipc is None:
            return False
        try:
            if self._ipc_socket:
                self._ipc.sendall(data)
                self._ipc.settimeout(2.0)
                self._ipc.recv(4096)
            else:
                self._ipc.write(data)
                self._ipc.flush()
            return True
        except Exception:
            return False

    def load_file(self, path: str) -> bool:
        return self._send({"command": ["loadfile", path, "replace"]})

    def append_file(self, path: str) -> bool:
        return self._send({"command": ["loadfile", path, "append"]})

    def playlist_next(self) -> bool:
        return self._send({"command": ["playlist-next", "force"]})

    def playlist_remove(self, index: int) -> bool:
        return self._send({"command": ["playlist-remove", index]})

    def set_property(self, name: str, value: object) -> bool:
        return self._send({"command": ["set_property", name, value]})

    def ping(self) -> bool:
        return self._send({"command": ["get_property", "idle-active"]})


class ConfigServer:
    def __init__(
        self,
        cfg: Dict,
        cfg_lock: threading.Lock,
        config_path: str,
        mpv: MPVController,
        poll_now_event: threading.Event,
    ) -> None:
        self._cfg = cfg
        self._cfg_lock = cfg_lock
        self._config_path = config_path
        self._mpv = mpv
        self._poll_now_event = poll_now_event
        self._server: Optional[ThreadingHTTPServer] = None

    def start(self) -> None:
        snapshot = config_snapshot(self._cfg, self._cfg_lock)
        if not snapshot.get("config_ui_enabled"):
            return
        bind = snapshot.get("config_ui_bind", "127.0.0.1")
        port = int(snapshot.get("config_ui_port", 8765))

        server = ThreadingHTTPServer((bind, port), self._make_handler())
        self._server = server
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        logging.info("Config UI disponível em http://%s:%s", bind, port)

    def _make_handler(self):
        cfg = self._cfg
        cfg_lock = self._cfg_lock
        config_path = self._config_path
        mpv = self._mpv
        poll_now = self._poll_now_event

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, fmt, *args) -> None:
                logging.info("ConfigUI %s - %s", self.address_string(), fmt % args)

            def do_GET(self) -> None:  # noqa: N802
                if self.path not in {"/", ""}:
                    self.send_error(HTTPStatus.NOT_FOUND, "Not found")
                    return
                snapshot = config_snapshot(cfg, cfg_lock)
                env_id = snapshot.get("environment_id", "")
                rotation = int(snapshot.get("rotation_deg") or 0)
                html = f"""<!doctype html>
<html lang="pt-BR"><head>
<meta charset="utf-8">
<title>Kiosky Config</title>
<style>
body{{font-family:Arial,Helvetica,sans-serif;margin:24px;background:#111;color:#eee;}}
label{{display:block;margin:12px 0 6px;}}
input,select,button{{font-size:16px;padding:8px;border-radius:6px;border:1px solid #444;background:#1b1b1b;color:#eee;}}
button{{cursor:pointer;background:#2b7a78;border-color:#2b7a78;}}
.small{{font-size:12px;color:#aaa;}}
</style></head><body>
<h2>Configuração Kiosky</h2>
<form method="POST" action="/save">
<label>Environment ID</label>
<input name="environment_id" value="{env_id}" style="width:420px">
<label>Rotação</label>
<select name="rotation_deg">
  <option value="0" {"selected" if rotation==0 else ""}>0°</option>
  <option value="90" {"selected" if rotation==90 else ""}>90°</option>
  <option value="180" {"selected" if rotation==180 else ""}>180°</option>
  <option value="270" {"selected" if rotation==270 else ""}>270°</option>
</select>
<div style="margin-top:16px"><button type="submit">Salvar</button></div>
<p class="small">Após salvar, o player aplica a rotação e atualiza o ambiente.</p>
</form>
</body></html>
"""
                payload = html.encode("utf-8")
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def do_POST(self) -> None:  # noqa: N802
                if self.path != "/save":
                    self.send_error(HTTPStatus.NOT_FOUND, "Not found")
                    return
                length = int(self.headers.get("Content-Length", "0"))
                body = self.rfile.read(length).decode("utf-8")
                data = parse_qs(body)
                env_id = (data.get("environment_id") or [""])[0].strip()
                rotation = normalize_rotation((data.get("rotation_deg") or ["0"])[0])

                with cfg_lock:
                    if env_id:
                        cfg["environment_id"] = env_id
                    cfg["rotation_deg"] = rotation
                    write_config(config_path, cfg)

                mpv.set_property("video-rotate", rotation)
                poll_now.set()

                html = """<!doctype html><html><head><meta charset='utf-8'>
<title>Salvo</title></head><body>
<p>Configuração salva com sucesso.</p>
<script>setTimeout(() => window.close(), 800);</script>
</body></html>"""
                payload = html.encode("utf-8")
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

        return Handler


def poller(
    cfg: Dict,
    cfg_lock: threading.Lock,
    poll_now_event: threading.Event,
    state: PlaylistState,
    status: StatusState,
    stop_event: threading.Event,
) -> None:
    backoff = 2
    consecutive_failures = 0
    while not stop_event.is_set():
        cfg_snapshot = config_snapshot(cfg, cfg_lock)
        try:
            raw_items = fetch_media_list(cfg_snapshot)
            fingerprint = fingerprint_items(raw_items)
            items = download_media(cfg_snapshot, raw_items)
            updated = state.update(items, fingerprint)
            if updated:
                logging.info("Playlist updated: %d items", len(items))
                status_snapshot = status.snapshot()
                keep_paths = {item.path for item in items}
                current = status_snapshot.get("current_item") or {}
                next_item = status_snapshot.get("next_item") or {}
                if isinstance(current, dict) and current.get("path"):
                    keep_paths.add(current["path"])
                if isinstance(next_item, dict) and next_item.get("path"):
                    keep_paths.add(next_item["path"])
                removed = cleanup_cache_dir(cfg_snapshot["cache_dir"], keep_paths)
                status.update(last_cleanup=iso_now(), last_cleanup_removed=removed)
                status_snapshot = status.snapshot()
                send_telemetry(
                    cfg_snapshot,
                    status_snapshot,
                    heartbeat_type="playlist",
                    status="ok",
                    notes="playlist updated",
                    uptime_seconds=int(time.time() - status.start_time),
                )
            status.update(last_poll_success=iso_now(), last_poll_error=None, playlist_size=len(items))
            consecutive_failures = 0
            status.update(consecutive_failures=consecutive_failures)
            backoff = 2
        except Exception as exc:
            logging.warning("API polling failed: %s", exc)
            status.update(last_poll_error=f"{iso_now()} {exc}")
            consecutive_failures += 1
            status.update(consecutive_failures=consecutive_failures)
            status_snapshot = status.snapshot()
            send_telemetry(
                cfg_snapshot,
                status_snapshot,
                heartbeat_type="media_fetch",
                status="error",
                error_code="media_fetch_failed",
                error_message=str(exc),
                uptime_seconds=int(time.time() - status.start_time),
            )
            time.sleep(backoff)
            backoff = min(backoff * 2, 300)
        else:
            interval = int(cfg_snapshot.get("poll_interval_sec") or 0)
            for _ in range(int(interval * 5)):
                if stop_event.is_set():
                    break
                if poll_now_event.is_set():
                    poll_now_event.clear()
                    break
                time.sleep(0.2)


def watchdog(
    cfg: Dict,
    cfg_lock: threading.Lock,
    mpv: MPVController,
    status: StatusState,
    stop_event: threading.Event,
) -> None:
    while not stop_event.is_set():
        try:
            mpv.ensure_running()
            if not mpv.ping():
                logging.warning("MPV IPC unresponsive, restarting")
                mpv.restart()
            status.update(mpv_running=mpv.is_running(), mpv_last_ok=iso_now())
        except Exception as exc:
            logging.warning("Watchdog error: %s", exc)
        cfg_snapshot = config_snapshot(cfg, cfg_lock)
        interval = int(cfg_snapshot.get("watchdog_interval_sec") or 0)
        for _ in range(int(interval * 5)):
            if stop_event.is_set():
                break
            time.sleep(0.2)


def telemetry_worker(
    cfg: Dict,
    cfg_lock: threading.Lock,
    status: StatusState,
    stop_event: threading.Event,
) -> None:
    cfg_snapshot = config_snapshot(cfg, cfg_lock)
    if not cfg_snapshot.get("telemetry_enabled") or not cfg_snapshot.get("telemetry_url"):
        return
    interval = int(cfg_snapshot.get("telemetry_interval_sec") or 0)
    if interval <= 0:
        return

    status_snapshot = status.snapshot()
    ok = send_telemetry(
        cfg_snapshot,
        status_snapshot,
        heartbeat_type="startup",
        status="ok",
        notes="startup",
        uptime_seconds=int(time.time() - status.start_time),
    )
    if not ok:
        status.update(last_telemetry_error=iso_now())

    while not stop_event.is_set():
        cfg_snapshot = config_snapshot(cfg, cfg_lock)
        status_snapshot = status.snapshot()
        failures = int(status_snapshot.get("consecutive_failures") or 0)
        hb_status = "ok"
        error_message = None
        if failures >= 3:
            hb_status = "error"
            error_message = str(status_snapshot.get("last_poll_error") or "")
        elif failures > 0:
            hb_status = "warning"
            error_message = str(status_snapshot.get("last_poll_error") or "")

        ok = send_telemetry(
            cfg_snapshot,
            status_snapshot,
            heartbeat_type="healthcheck",
            status=hb_status,
            error_code="media_fetch_failed" if failures > 0 else None,
            error_message=error_message if failures > 0 else None,
            notes="healthcheck",
            uptime_seconds=int(time.time() - status.start_time),
        )
        if not ok:
            status.update(last_telemetry_error=iso_now())

        for _ in range(int(interval * 5)):
            if stop_event.is_set():
                break
            time.sleep(0.2)


def status_writer(cfg: Dict, cfg_lock: threading.Lock, status: StatusState, stop_event: threading.Event) -> None:
    cfg_snapshot = config_snapshot(cfg, cfg_lock)
    if not cfg_snapshot.get("status_file"):
        return
    status_path = cfg_snapshot["status_file"]
    interval = int(cfg_snapshot.get("status_interval_sec") or 0)
    if interval <= 0:
        return
    status_dir = os.path.dirname(status_path)
    if status_dir:
        os.makedirs(status_dir, exist_ok=True)
    while not stop_event.is_set():
        snapshot = status.snapshot()
        snapshot["uptime_sec"] = int(time.time() - status.start_time)
        tmp_path = f"{status_path}.tmp"
        try:
            with open(tmp_path, "w", encoding="utf-8") as fh:
                json.dump(snapshot, fh, ensure_ascii=True)
            os.replace(tmp_path, status_path)
        except Exception as exc:
            logging.warning("Status write failed: %s", exc)
        for _ in range(int(interval * 5)):
            if stop_event.is_set():
                break
            time.sleep(0.2)


def cleanup_cache_dir(cache_dir: str, keep_paths: set) -> int:
    removed = 0
    if not os.path.isdir(cache_dir):
        return removed
    for name in os.listdir(cache_dir):
        path = os.path.join(cache_dir, name)
        if not os.path.isfile(path):
            continue
        if path in keep_paths:
            continue
        try:
            os.remove(path)
            removed += 1
        except Exception as exc:
            logging.warning("Failed to delete %s: %s", path, exc)
    return removed


def cleanup_worker(
    cfg: Dict,
    cfg_lock: threading.Lock,
    state: PlaylistState,
    status: StatusState,
    stop_event: threading.Event,
) -> None:
    while not stop_event.is_set():
        cfg_snapshot = config_snapshot(cfg, cfg_lock)
        interval = int(cfg_snapshot.get("cleanup_interval_sec") or 0)
        if interval <= 0:
            time.sleep(1)
            continue
        items, _ = state.get()
        keep_paths = {item.path for item in items}
        snapshot = status.snapshot()
        current = snapshot.get("current_item") or {}
        next_item = snapshot.get("next_item") or {}
        if isinstance(current, dict) and current.get("path"):
            keep_paths.add(current["path"])
        if isinstance(next_item, dict) and next_item.get("path"):
            keep_paths.add(next_item["path"])

        removed = cleanup_cache_dir(cfg_snapshot["cache_dir"], keep_paths)
        status.update(last_cleanup=iso_now(), last_cleanup_removed=removed)

        for _ in range(int(interval * 5)):
            if stop_event.is_set():
                break
            time.sleep(0.2)


def playback_loop(
    cfg: Dict,
    cfg_lock: threading.Lock,
    state: PlaylistState,
    status: StatusState,
    mpv: MPVController,
    stop_event: threading.Event,
) -> None:
    idx = 0
    last_version = -1
    preloaded_path: Optional[str] = None
    while not stop_event.is_set():
        items, version = state.get()
        if version != last_version:
            idx = 0
            last_version = version
            preloaded_path = None

        if not items:
            time.sleep(1)
            continue

        cfg_snapshot = config_snapshot(cfg, cfg_lock)
        item = items[idx % len(items)]
        next_item = None
        if cfg_snapshot.get("preload_next") and len(items) > 1:
            next_item = items[(idx + 1) % len(items)]

        mpv.ensure_running()
        if preloaded_path != item.path:
            if not mpv.load_file(item.path):
                logging.warning("Failed to load media, restarting MPV")
                mpv.restart()
                if not mpv.load_file(item.path):
                    time.sleep(1)
                    continue
        preloaded_path = None

        if next_item is not None:
            mpv.append_file(next_item.path)

        status.update(
            current_index=idx % len(items),
            current_item={
                "url": item.url,
                "path": item.path,
                "duration_ms": item.duration_ms,
                "campaign_id": item.campaign_id,
                "campaign_name": item.campaign_name,
                "started_at": iso_now(),
            },
            next_item=(
                {
                    "url": next_item.url,
                    "path": next_item.path,
                    "duration_ms": next_item.duration_ms,
                    "campaign_id": next_item.campaign_id,
                    "campaign_name": next_item.campaign_name,
                }
                if next_item is not None
                else None
            ),
        )

        logging.info("Playing %s (%s ms)", item.url, item.duration_ms)
        end_time = time.time() + max(item.duration_ms, 1000) / 1000.0
        while time.time() < end_time and not stop_event.is_set():
            time.sleep(0.2)

        if next_item is not None and cfg_snapshot.get("preload_next"):
            if mpv.playlist_next():
                mpv.playlist_remove(0)
                preloaded_path = next_item.path
                idx += 1
                continue

        idx += 1


def main() -> int:
    parser = argparse.ArgumentParser(description="Kiosky MPV player")
    parser.add_argument("--config", default="config.json", help="Path to config.json")
    args = parser.parse_args()

    cfg = load_config(args.config)
    setup_logging(cfg)

    if not cfg.get("api_key") or not cfg.get("environment_id"):
        logging.error("api_key and environment_id must be set in config.json")
        return 2

    cfg_lock = threading.Lock()
    poll_now_event = threading.Event()

    state = PlaylistState()
    status = StatusState()
    mpv = MPVController(cfg)
    stop_event = threading.Event()
    force_exit = threading.Event()

    def _force_kill_after_delay() -> None:
        time.sleep(5)
        if not force_exit.is_set():
            return
        try:
            mpv.stop()
        finally:
            os._exit(1)

    def _handle(sig, _frame):
        logging.info("Signal %s received, stopping...", sig)
        if stop_event.is_set():
            force_exit.set()
            threading.Thread(target=_force_kill_after_delay, daemon=True).start()
            return
        stop_event.set()
        force_exit.set()
        threading.Thread(target=_force_kill_after_delay, daemon=True).start()

    signal.signal(signal.SIGINT, _handle)
    signal.signal(signal.SIGTERM, _handle)

    mpv.start()

    threads = [
        threading.Thread(
            target=poller,
            args=(cfg, cfg_lock, poll_now_event, state, status, stop_event),
            daemon=True,
        ),
        threading.Thread(
            target=watchdog,
            args=(cfg, cfg_lock, mpv, status, stop_event),
            daemon=True,
        ),
        threading.Thread(
            target=status_writer,
            args=(cfg, cfg_lock, status, stop_event),
            daemon=True,
        ),
        threading.Thread(
            target=cleanup_worker,
            args=(cfg, cfg_lock, state, status, stop_event),
            daemon=True,
        ),
        threading.Thread(
            target=telemetry_worker,
            args=(cfg, cfg_lock, status, stop_event),
            daemon=True,
        ),
    ]
    for thread in threads:
        thread.start()

    config_server = ConfigServer(cfg, cfg_lock, args.config, mpv, poll_now_event)
    config_server.start()

    try:
        playback_loop(cfg, cfg_lock, state, status, mpv, stop_event)
    finally:
        stop_event.set()
        for thread in threads:
            thread.join(timeout=5)
        mpv.stop()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
