#!/usr/bin/env python3
import argparse
import calendar
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


def default_sync_ntp_command() -> str:
    if sys.platform.startswith("linux"):
        return "chronyc -a makestep"
    return ""


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
    "state_dir": "",
    "offline_fallback": True,
    "offline_max_age_hours": 0,
    "offline_ignore_max_age_when_no_network": True,
    "require_full_download_before_switch": True,
    "allow_empty_playlist_from_api": False,
    "disable_cleanup_when_offline": True,
    "cache_max_files": 0,
    "cache_max_bytes": 0,
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
    "telemetry_interval_sec": 60,
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
    "playback_stall_sec": 25,
    "playback_mismatch_sec": 10,
    "media_load_retry_cooldown_sec": 60,
    "tmp_max_age_sec": 3600,
    "status_file": "",
    "status_interval_sec": 5,
    "cleanup_interval_sec": 1800,
    "sync_enabled": True,
    "sync_drift_threshold_ms": 300,
    "sync_hard_resync_ms": 1200,
    "sync_boot_hard_check_sec": 300,
    "sync_checkpoint_interval_sec": 3600,
    "sync_prep_mode": "play_then_resync",
    "sync_ntp_command": default_sync_ntp_command(),
}

SECONDS_PER_DAY = 24 * 3600
SYNC_DAILY_ANCHOR_SEC_UTC = 5 * 60
SYNC_PREP_WINDOW_START_SEC_UTC = 23 * 3600 + 58 * 60


@dataclass(frozen=True)
class MediaItem:
    url: str
    duration_ms: int
    path: str
    campaign_id: str
    campaign_name: str


@dataclass(frozen=True)
class CyclePosition:
    index: int
    offset_ms: int
    cycle_pos_ms: int
    cycle_total_ms: int
    anchor_ts: float


class PlaylistState:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._items: List[MediaItem] = []
        self._version = 0
        self._fingerprint = ""
        self._items_signature = ""

    def get(self) -> Tuple[List[MediaItem], int]:
        with self._lock:
            return list(self._items), self._version

    def update(self, items: List[MediaItem], fingerprint: str) -> bool:
        with self._lock:
            signature = items_signature(items)
            if fingerprint == self._fingerprint and signature == self._items_signature:
                return False
            self._items = list(items)
            self._version += 1
            self._fingerprint = fingerprint
            self._items_signature = signature
            return True


def load_config(path: str) -> Dict:
    abs_path = os.path.abspath(path)
    if not os.path.exists(abs_path):
        raise FileNotFoundError(f"Config not found: {path}")
    with open(abs_path, "r", encoding="utf-8") as fh:
        data = json.load(fh)
    cfg = dict(DEFAULT_CONFIG)
    cfg.update(data)
    if not cfg.get("ipc_path"):
        cfg["ipc_path"] = default_ipc_path()
    config_dir = os.path.dirname(abs_path)
    for key in ("cache_dir", "state_dir", "log_file", "status_file"):
        value = cfg.get(key)
        if isinstance(value, str) and value:
            cfg[key] = resolve_path_from_base(config_dir, value)
    ipc_path = cfg.get("ipc_path")
    if isinstance(ipc_path, str) and ipc_path and not is_windows_named_pipe(ipc_path):
        cfg["ipc_path"] = resolve_path_from_base(config_dir, ipc_path)
    return cfg


def is_windows_named_pipe(path: str) -> bool:
    return path.startswith("\\\\.\\pipe\\")


def resolve_path_from_base(base_dir: str, value: str) -> str:
    if not value:
        return value
    if os.path.isabs(value):
        return os.path.normpath(value)
    return os.path.normpath(os.path.join(base_dir, value))


def normalize_media_path(path: object, cache_dir: Optional[str] = None) -> Optional[str]:
    if not isinstance(path, str):
        return None
    raw = path.strip()
    if not raw:
        return None
    if "://" in raw:
        return raw

    candidates: List[str] = []
    if os.path.isabs(raw):
        candidates.append(raw)
    else:
        candidates.append(os.path.abspath(raw))
        if cache_dir:
            candidates.append(os.path.abspath(os.path.join(cache_dir, raw)))
            candidates.append(os.path.abspath(os.path.join(cache_dir, os.path.basename(raw))))

    seen: set = set()
    for candidate in candidates:
        normalized = os.path.normpath(candidate)
        if normalized in seen:
            continue
        seen.add(normalized)
        if os.path.exists(normalized):
            return normalized

    if candidates:
        return os.path.normpath(candidates[0])
    return None


def media_paths_match(expected_path: object, actual_path: object, cache_dir: Optional[str] = None) -> bool:
    expected = normalize_media_path(expected_path, cache_dir)
    actual = normalize_media_path(actual_path, cache_dir)
    if not expected or not actual:
        return False
    if "://" not in expected and "://" not in actual:
        if os.path.exists(expected):
            expected = os.path.realpath(expected)
        if os.path.exists(actual):
            actual = os.path.realpath(actual)
    return os.path.normcase(expected) == os.path.normcase(actual)


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


def iso_from_ts(timestamp: float) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(timestamp))


def parse_iso_utc(value: str) -> Optional[int]:
    try:
        return calendar.timegm(time.strptime(value, "%Y-%m-%dT%H:%M:%SZ"))
    except Exception:
        return None


def effective_duration_ms(duration_ms: int) -> int:
    try:
        parsed = int(duration_ms)
    except Exception:
        parsed = 0
    return max(parsed, 1000)


def cycle_timeline(items: List[MediaItem]) -> Tuple[List[int], List[int], int]:
    durations: List[int] = []
    cycle_start_ms: List[int] = []
    total = 0
    for item in items:
        duration = effective_duration_ms(item.duration_ms)
        durations.append(duration)
        cycle_start_ms.append(total)
        total += duration
    return durations, cycle_start_ms, total


def seconds_since_midnight_utc(now_ts: float) -> int:
    utc_now = time.gmtime(now_ts)
    return utc_now.tm_hour * 3600 + utc_now.tm_min * 60 + utc_now.tm_sec


def daily_anchor_utc_ts(now_ts: float) -> float:
    utc_now = time.gmtime(now_ts)
    anchor = calendar.timegm((utc_now.tm_year, utc_now.tm_mon, utc_now.tm_mday, 0, 5, 0, 0, 0, 0))
    if now_ts < anchor:
        anchor -= SECONDS_PER_DAY
    return float(anchor)


def next_daily_anchor_utc_ts(now_ts: float) -> float:
    utc_now = time.gmtime(now_ts)
    anchor = calendar.timegm((utc_now.tm_year, utc_now.tm_mon, utc_now.tm_mday, 0, 5, 0, 0, 0, 0))
    if now_ts < anchor:
        return float(anchor)
    return float(anchor + SECONDS_PER_DAY)


def is_prep_window_utc(now_ts: float) -> bool:
    sec = seconds_since_midnight_utc(now_ts)
    return sec >= SYNC_PREP_WINDOW_START_SEC_UTC or sec < SYNC_DAILY_ANCHOR_SEC_UTC


def compute_cycle_position_from_utc(now_ts: float, durations_ms: List[int]) -> CyclePosition:
    if not durations_ms:
        raise ValueError("durations_ms cannot be empty")
    cycle_total = max(sum(durations_ms), 1)
    anchor_ts = daily_anchor_utc_ts(now_ts)
    elapsed_ms = int((now_ts - anchor_ts) * 1000) % cycle_total
    cursor = 0
    for idx, duration in enumerate(durations_ms):
        next_cursor = cursor + duration
        if elapsed_ms < next_cursor:
            return CyclePosition(
                index=idx,
                offset_ms=elapsed_ms - cursor,
                cycle_pos_ms=elapsed_ms,
                cycle_total_ms=cycle_total,
                anchor_ts=anchor_ts,
            )
        cursor = next_cursor
    last_idx = len(durations_ms) - 1
    last_duration = durations_ms[last_idx]
    return CyclePosition(
        index=last_idx,
        offset_ms=max(last_duration - 1, 0),
        cycle_pos_ms=max(cycle_total - 1, 0),
        cycle_total_ms=cycle_total,
        anchor_ts=anchor_ts,
    )


def signed_cycle_delta_ms(target_ms: int, current_ms: int, cycle_total_ms: int) -> int:
    if cycle_total_ms <= 0:
        return 0
    half = cycle_total_ms / 2.0
    delta = ((target_ms - current_ms + half) % cycle_total_ms) - half
    return int(round(delta))


def classify_drift_action(
    drift_ms: int,
    drift_threshold_ms: int,
    hard_resync_ms: int,
) -> str:
    threshold = max(int(drift_threshold_ms), 0)
    hard = max(int(hard_resync_ms), threshold)
    abs_drift = abs(int(drift_ms))
    if abs_drift < threshold:
        return "none"
    if abs_drift >= hard:
        return "hard_resync"
    return "soft_resync"


def next_hour_checkpoint_utc_ts(now_ts: float, interval_sec: int = 3600) -> float:
    if interval_sec <= 0:
        interval_sec = 3600
    now_int = int(now_ts)
    return float(((now_int // interval_sec) + 1) * interval_sec)


def run_ntp_sync_command(cfg_snapshot: Dict) -> None:
    command = str(cfg_snapshot.get("sync_ntp_command") or "").strip()
    if not command:
        logging.info("Sync prep: sync_ntp_command vazio; assumindo NTP do sistema.")
        return
    try:
        result = subprocess.run(
            command,
            shell=True,
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=20,
        )
    except Exception as exc:
        logging.warning("Sync prep: falha ao executar sync_ntp_command: %s", exc)
        return
    if result.returncode == 0:
        logging.info("Sync prep: sync_ntp_command executado com sucesso.")
    else:
        logging.warning("Sync prep: sync_ntp_command retornou %s.", result.returncode)


def api_endpoint_reachable(cfg: Dict, timeout_sec: float = 2.0) -> bool:
    api_url = str(cfg.get("api_url") or "").strip()
    if not api_url:
        return False
    parsed = urlparse(api_url)
    host = parsed.hostname
    if not host:
        return False
    if parsed.port:
        port = int(parsed.port)
    elif parsed.scheme == "https":
        port = 443
    else:
        port = 80
    try:
        with socket.create_connection((host, port), timeout=timeout_sec):
            return True
    except Exception:
        return False


def state_dir(cfg: Dict) -> str:
    configured = cfg.get("state_dir")
    if configured:
        return configured
    cache_dir = cfg.get("cache_dir") or "."
    return os.path.join(cache_dir, ".state")


def state_path(cfg: Dict, filename: str) -> str:
    return os.path.join(state_dir(cfg), filename)


def load_json_file(path: str) -> Optional[Dict]:
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except FileNotFoundError:
        return None
    except Exception as exc:
        logging.warning("Failed to read state file %s: %s", path, exc)
        return None


def write_json_file(path: str, data: Dict, ensure_ascii: bool = True) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2, ensure_ascii=ensure_ascii)
    os.replace(tmp_path, path)


def playlist_state_path(cfg: Dict) -> str:
    return state_path(cfg, "playlist_last.json")


def cache_index_path(cfg: Dict) -> str:
    return state_path(cfg, "cache_index.json")


def last_success_path(cfg: Dict) -> str:
    return state_path(cfg, "last_success.json")


def save_last_success(cfg: Dict, timestamp: str) -> None:
    payload = {"last_success": timestamp}
    write_json_file(last_success_path(cfg), payload, ensure_ascii=True)


def load_last_success(cfg: Dict) -> Optional[str]:
    data = load_json_file(last_success_path(cfg))
    if not data:
        return None
    value = data.get("last_success")
    return value if isinstance(value, str) else None


def save_playlist_state(cfg: Dict, items: List["MediaItem"], fingerprint: str) -> None:
    payload = {
        "version": 1,
        "saved_at": iso_now(),
        "fingerprint": fingerprint,
        "playlist": [
            {
                "url": item.url,
                "duration_ms": item.duration_ms,
                "path": item.path,
                "campaign_id": item.campaign_id,
                "campaign_name": item.campaign_name,
            }
            for item in items
        ],
    }
    write_json_file(playlist_state_path(cfg), payload, ensure_ascii=False)


def load_playlist_state(cfg: Dict) -> Tuple[List[Dict], Optional[str], Optional[str]]:
    data = load_json_file(playlist_state_path(cfg))
    if not data:
        return [], None, None
    raw_items = data.get("playlist") or []
    fingerprint = data.get("fingerprint")
    saved_at = data.get("saved_at")
    if not isinstance(raw_items, list):
        return [], None, None
    return raw_items, fingerprint if isinstance(fingerprint, str) else None, saved_at if isinstance(saved_at, str) else None


def saved_playlist_paths(cfg: Dict) -> set:
    raw_items, _fingerprint, _saved_at = load_playlist_state(cfg)
    keep_paths: set = set()
    cache_dir = cfg.get("cache_dir") or "."
    for item in raw_items:
        if not isinstance(item, dict):
            continue
        path = normalize_media_path(item.get("path"), cache_dir)
        if path and os.path.exists(path):
            keep_paths.add(path)
            continue
        url = item.get("url")
        if not url:
            continue
        path = cache_path(cache_dir, url)
        if os.path.exists(path):
            keep_paths.add(path)
    return keep_paths


def media_items_from_saved(cfg: Dict, raw_items: List[Dict]) -> Tuple[List["MediaItem"], List[Dict]]:
    items: List[MediaItem] = []
    fingerprint_items_payload: List[Dict] = []
    cache_dir = cfg.get("cache_dir") or "."
    for item in raw_items:
        if not isinstance(item, dict):
            continue
        resolved_path = normalize_media_path(item.get("path"), cache_dir) or ""
        url = item.get("url")
        try:
            duration_ms = int(item.get("duration_ms") or cfg.get("default_duration_ms") or 0)
        except Exception:
            duration_ms = int(cfg.get("default_duration_ms") or 0)
        if duration_ms <= 0:
            duration_ms = int(cfg.get("default_duration_ms") or 10000)
        if not resolved_path and url:
            resolved_path = normalize_media_path(cache_path(cache_dir, str(url)), cache_dir) or ""
        if not resolved_path or not os.path.exists(resolved_path):
            continue
        if not is_supported_media_path(resolved_path, allow_bin=bool(url)):
            continue
        if (safe_getsize(resolved_path) or 0) <= 0:
            continue
        resolved_url = str(url) if url else f"cache://{os.path.basename(resolved_path)}"
        items.append(
            MediaItem(
                url=resolved_url,
                duration_ms=duration_ms,
                path=resolved_path,
                campaign_id=str(item.get("campaign_id", "")),
                campaign_name=str(item.get("campaign_name", "")),
            )
        )
        fingerprint_items_payload.append({"url": resolved_url, "duration_ms": duration_ms, "path": resolved_path})
    return items, fingerprint_items_payload


def media_items_from_cache(
    cfg: Dict,
    cache_index: Optional["CacheIndex"] = None,
) -> Tuple[List["MediaItem"], List[Dict]]:
    cache_dir = cfg.get("cache_dir") or "."
    if not os.path.isdir(cache_dir):
        return [], []

    index_snapshot: Dict[str, Dict[str, object]] = {}
    if cache_index is not None:
        try:
            index_snapshot = cache_index.snapshot()
        except Exception:
            index_snapshot = {}

    seen_paths: set = set()
    candidates: List[Tuple[str, Dict[str, object], float]] = []

    def _add_candidate(path: str, meta: Dict[str, object]) -> None:
        if path in seen_paths:
            return
        if not os.path.isfile(path):
            return
        if path.endswith(".tmp"):
            return
        if not is_supported_media_path(path, allow_bin=bool(meta.get("url"))):
            return
        if (safe_getsize(path) or 0) <= 0:
            return
        last_used_ts = None
        last_used = meta.get("last_used")
        if isinstance(last_used, str):
            last_used_ts = parse_iso_utc(last_used)
        if last_used_ts is None:
            try:
                last_used_ts = os.path.getmtime(path)
            except OSError:
                last_used_ts = 0.0
        candidates.append((path, dict(meta), float(last_used_ts)))
        seen_paths.add(path)

    for path, meta in index_snapshot.items():
        if isinstance(meta, dict):
            _add_candidate(path, meta)

    for name in os.listdir(cache_dir):
        path = os.path.join(cache_dir, name)
        _add_candidate(path, {})

    candidates.sort(key=lambda entry: (entry[2], entry[0]))
    default_duration_ms = int(cfg.get("default_duration_ms") or 10000)
    items: List[MediaItem] = []
    fingerprint_items_payload: List[Dict] = []

    for path, meta, _ in candidates:
        raw_duration = meta.get("duration_ms", default_duration_ms)
        try:
            duration_ms = int(raw_duration)
        except Exception:
            duration_ms = default_duration_ms
        if duration_ms <= 0:
            duration_ms = default_duration_ms
        url = str(meta.get("url") or f"cache://{os.path.basename(path)}")
        campaign_id = str(meta.get("campaign_id", ""))
        campaign_name = str(meta.get("campaign_name", ""))
        items.append(
            MediaItem(
                url=url,
                duration_ms=duration_ms,
                path=path,
                campaign_id=campaign_id,
                campaign_name=campaign_name,
            )
        )
        fingerprint_items_payload.append({"url": url, "duration_ms": duration_ms, "path": path})

    return items, fingerprint_items_payload


def offline_playlist_allowed(
    cfg: Dict,
    saved_at: Optional[str],
    network_available: Optional[bool] = None,
) -> bool:
    max_age_hours = float(cfg.get("offline_max_age_hours") or 0)
    if max_age_hours <= 0:
        return True
    if (
        cfg.get("offline_ignore_max_age_when_no_network", True)
        and network_available is False
    ):
        return True
    ref = load_last_success(cfg) or saved_at
    if not ref:
        return True
    ref_ts = parse_iso_utc(ref)
    if ref_ts is None:
        return True
    age_hours = (time.time() - ref_ts) / 3600.0
    return age_hours <= max_age_hours


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
            "sync_mode": "idle",
            "sync_anchor_utc": None,
            "sync_drift_ms": None,
            "sync_last_check_utc": None,
            "sync_last_action": None,
            "sync_next_checkpoint_utc": None,
            "sync_checkpoint_reason": None,
            "sync_cycle_ms": None,
            "playback_state": "starting",
            "black_screen_risk_reason": None,
            "blocked_media_count": 0,
            "last_render_ok": None,
            "last_render_error": None,
        }
        self.start_time = time.time()

    def update(self, **kwargs: object) -> None:
        with self._lock:
            self._data.update(kwargs)

    def snapshot(self) -> Dict[str, Optional[object]]:
        with self._lock:
            return dict(self._data)


def safe_getsize(path: str) -> Optional[int]:
    try:
        return os.path.getsize(path)
    except OSError:
        return None


class CacheIndex:
    def __init__(self, cfg: Dict) -> None:
        self._path = cache_index_path(cfg)
        self._lock = threading.Lock()
        self._items: Dict[str, Dict[str, object]] = {}
        self._last_save = 0.0
        self._save_interval = 5.0
        self._load()

    def _load(self) -> None:
        data = load_json_file(self._path) or {}
        items = data.get("items")
        if isinstance(items, dict):
            self._items = {k: v for k, v in items.items() if isinstance(v, dict)}

    def _save(self, force: bool = False) -> None:
        now = time.time()
        if not force and now - self._last_save < self._save_interval:
            return
        payload = {
            "version": 1,
            "updated_at": iso_now(),
            "items": self._items,
        }
        write_json_file(self._path, payload, ensure_ascii=False)
        self._last_save = now

    def record_download(self, item: MediaItem) -> None:
        with self._lock:
            meta = dict(self._items.get(item.path, {}))
            meta.update(
                {
                    "url": item.url,
                    "duration_ms": item.duration_ms,
                    "campaign_id": item.campaign_id,
                    "campaign_name": item.campaign_name,
                    "last_used": iso_now(),
                    "size": safe_getsize(item.path) or meta.get("size"),
                }
            )
            self._items[item.path] = meta
            self._save()

    def touch(self, item: MediaItem) -> None:
        with self._lock:
            meta = dict(self._items.get(item.path, {}))
            meta.update(
                {
                    "url": item.url,
                    "duration_ms": item.duration_ms,
                    "campaign_id": item.campaign_id,
                    "campaign_name": item.campaign_name,
                    "last_used": iso_now(),
                    "size": safe_getsize(item.path) or meta.get("size"),
                }
            )
            self._items[item.path] = meta
            self._save()

    def remove_missing(self) -> None:
        with self._lock:
            missing = [path for path in self._items if not os.path.exists(path)]
            if not missing:
                return
            for path in missing:
                self._items.pop(path, None)
            self._save(force=True)

    def snapshot(self) -> Dict[str, Dict[str, object]]:
        with self._lock:
            return dict(self._items)

def sha1_hex(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8"), usedforsecurity=False).hexdigest()


def cache_path(cache_dir: str, url: str) -> str:
    parsed = urlparse(url)
    _, ext = os.path.splitext(parsed.path)
    if not ext:
        ext = ".bin"
    return os.path.join(cache_dir, f"{sha1_hex(url)}{ext}")


IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp"}
VIDEO_EXTENSIONS = {".mp4", ".m4v", ".mov", ".mkv", ".webm", ".avi", ".mpeg", ".mpg"}


def is_image_path(path: str) -> bool:
    ext = os.path.splitext(path.lower())[1]
    return ext in IMAGE_EXTENSIONS


def is_supported_media_path(path: str, allow_bin: bool = False) -> bool:
    ext = os.path.splitext(path.lower())[1]
    if ext in IMAGE_EXTENSIONS or ext in VIDEO_EXTENSIONS:
        return True
    return allow_bin and ext == ".bin"


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


def download_media(cfg: Dict, raw_items: List[Dict], cache_index: Optional[CacheIndex]) -> List[MediaItem]:
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
                expected_size = None
                content_length = resp.headers.get("Content-Length")
                if content_length and content_length.isdigit():
                    expected_size = int(content_length)
                tmp_path = f"{dest}.tmp"
                bytes_written = 0
                with open(tmp_path, "wb") as fh:
                    for chunk in resp.iter_content(chunk_size=1024 * 256):
                        if chunk:
                            fh.write(chunk)
                            bytes_written += len(chunk)
                if expected_size is not None and bytes_written < expected_size:
                    raise IOError(f"Incomplete download ({bytes_written}/{expected_size} bytes)")
                os.replace(tmp_path, dest)
            except Exception as exc:
                logging.warning("Failed to download %s: %s", url, exc)
                try:
                    tmp_path = f"{dest}.tmp"
                    if os.path.exists(tmp_path):
                        os.remove(tmp_path)
                except Exception as cleanup_exc:
                    logging.warning("Failed to cleanup temp file for %s: %s", url, cleanup_exc)
                if os.path.exists(dest):
                    logging.info("Using cached file for %s", url)
                else:
                    continue

        media_item = MediaItem(
            url=url,
            duration_ms=int(item["duration_ms"]),
            path=dest,
            campaign_id=item.get("campaign_id", ""),
            campaign_name=item.get("campaign_name", ""),
        )
        items.append(media_item)
        if cache_index is not None:
            cache_index.record_download(media_item)
    return items


def fingerprint_items(raw_items: List[Dict]) -> str:
    payload = [{"url": i["url"], "duration_ms": i["duration_ms"]} for i in raw_items]
    return sha1_hex(json.dumps(payload, sort_keys=True))


def items_signature(items: List[MediaItem]) -> str:
    payload = [{"path": i.path, "duration_ms": i.duration_ms} for i in items]
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
        self._lock = threading.RLock()
        self._ipc_lock = threading.Lock()
        self._request_id = 0
        self._recv_buffer = ""
        self._generation = 0

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
            self._recv_buffer = ""

    def _stop_locked(self) -> None:
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

    def _start_locked(self) -> bool:
        if self._proc and self._proc.poll() is None and self._ipc is not None:
            return True
        if self._proc and self._proc.poll() is None and self._ipc is None:
            self._stop_locked()

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
        try:
            self._proc = subprocess.Popen(args, **popen_kwargs)
        except Exception as exc:
            self._proc = None
            logging.error("Failed to start MPV process: %s", exc)
            return False
        self._generation += 1
        if self._open_ipc():
            return True
        logging.warning("MPV IPC not available after launch; will retry.")
        self._stop_locked()
        return False

    def start(self) -> None:
        with self._lock:
            if self._start_locked():
                return
            time.sleep(1)
            self._start_locked()

    def restart(self) -> None:
        with self._lock:
            self._stop_locked()
            time.sleep(1)
            self._start_locked()

    def stop(self) -> None:
        with self._lock:
            self._stop_locked()

    def ensure_running(self) -> None:
        if self._proc is None or self._proc.poll() is not None:
            self.start()

    def is_running(self) -> bool:
        return self._proc is not None and self._proc.poll() is None

    def generation(self) -> int:
        return self._generation

    def _send(self, payload: Dict, expect_response: bool = False, timeout: float = 2.0) -> Optional[Dict]:
        data = (json.dumps(payload) + "\n").encode("utf-8")
        if self._ipc is None:
            return None if expect_response else False
        with self._ipc_lock:
            if expect_response:
                self._request_id += 1
                payload["request_id"] = self._request_id
                data = (json.dumps(payload) + "\n").encode("utf-8")
            try:
                if self._ipc_socket:
                    self._ipc.sendall(data)
                else:
                    self._ipc.write(data)
                    self._ipc.flush()
            except Exception:
                return None if expect_response else False
            if not expect_response:
                return True
            return self._recv_response(self._request_id, timeout)

    def _recv_response(self, request_id: int, timeout: float) -> Optional[Dict]:
        if not self._ipc_socket or self._ipc is None:
            return None
        deadline = time.time() + max(timeout, 0.1)
        buffer = self._recv_buffer
        while time.time() < deadline:
            if "\n" in buffer:
                line, buffer = buffer.split("\n", 1)
                if line:
                    try:
                        payload = json.loads(line)
                    except Exception:
                        payload = None
                    if isinstance(payload, dict) and payload.get("request_id") == request_id:
                        self._recv_buffer = buffer
                        return payload
            try:
                self._ipc.settimeout(max(deadline - time.time(), 0.1))
                chunk = self._ipc.recv(4096)
                if not chunk:
                    break
                buffer += chunk.decode("utf-8", errors="ignore")
            except socket.timeout:
                continue
            except Exception:
                break
        self._recv_buffer = buffer
        return None

    def load_file(self, path: str) -> bool:
        return bool(self._send({"command": ["loadfile", path, "replace"]}))

    def append_file(self, path: str) -> bool:
        return bool(self._send({"command": ["loadfile", path, "append"]}))

    def playlist_next(self) -> bool:
        return bool(self._send({"command": ["playlist-next", "force"]}))

    def playlist_remove(self, index: int) -> bool:
        return bool(self._send({"command": ["playlist-remove", index]}))

    def set_property(self, name: str, value: object) -> bool:
        return bool(self._send({"command": ["set_property", name, value]}))

    def seek_absolute(self, seconds: float) -> bool:
        return bool(self._send({"command": ["seek", float(seconds), "absolute+exact"]}))

    def ping(self) -> bool:
        if not self._ipc_socket:
            return bool(self._send({"command": ["get_property", "idle-active"]}))
        payload = self._send({"command": ["get_property", "idle-active"]}, expect_response=True, timeout=2.0)
        return isinstance(payload, dict) and payload.get("error") == "success"

    def get_property(self, name: str, timeout: float = 2.0) -> Optional[object]:
        payload = self._send({"command": ["get_property", name]}, expect_response=True, timeout=timeout)
        if isinstance(payload, dict) and payload.get("error") == "success":
            return payload.get("data")
        return None


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
        try:
            server = ThreadingHTTPServer((bind, port), self._make_handler())
        except Exception as exc:
            logging.warning("Config UI unavailable on %s:%s: %s", bind, port, exc)
            return
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
    cache_index: CacheIndex,
    stop_event: threading.Event,
) -> None:
    def wait_poll_interval(cfg_snapshot: Dict) -> None:
        interval = int(cfg_snapshot.get("poll_interval_sec") or 0)
        for _ in range(int(interval * 5)):
            if stop_event.is_set():
                break
            if poll_now_event.is_set():
                poll_now_event.clear()
                break
            time.sleep(0.2)

    backoff = 2
    consecutive_failures = 0
    while not stop_event.is_set():
        cfg_snapshot = config_snapshot(cfg, cfg_lock)
        try:
            raw_items = fetch_media_list(cfg_snapshot)
            if not raw_items and not cfg_snapshot.get("allow_empty_playlist_from_api", False):
                current_items, _ = state.get()
                if current_items:
                    logging.warning(
                        "API returned empty playlist; keeping current playlist (%d items).",
                        len(current_items),
                    )
                    status.update(playlist_size=len(current_items))
                    status.update(last_poll_success=iso_now(), last_poll_error=None)
                    save_last_success(cfg_snapshot, iso_now())
                    consecutive_failures = 0
                    status.update(consecutive_failures=consecutive_failures)
                    backoff = 2
                    wait_poll_interval(cfg_snapshot)
                    continue

                cache_items, cache_payload = media_items_from_cache(cfg_snapshot, cache_index)
                if cache_items:
                    cache_fp = fingerprint_items(cache_payload)
                    updated = state.update(cache_items, cache_fp)
                    if updated:
                        save_playlist_state(cfg_snapshot, cache_items, cache_fp)
                        logging.warning(
                            "API returned empty playlist; loaded %d items from local cache.",
                            len(cache_items),
                        )
                    status.update(playlist_size=len(cache_items))
                    status.update(last_poll_success=iso_now(), last_poll_error=None)
                    save_last_success(cfg_snapshot, iso_now())
                    consecutive_failures = 0
                    status.update(consecutive_failures=consecutive_failures)
                    backoff = 2
                    wait_poll_interval(cfg_snapshot)
                    continue

                raise RuntimeError("API returned empty playlist and no local media is available")

            fingerprint = fingerprint_items(raw_items)
            items = download_media(cfg_snapshot, raw_items, cache_index)
            switch_ok = True
            if cfg_snapshot.get("require_full_download_before_switch"):
                switch_ok = len(items) >= len(raw_items)
                if not switch_ok:
                    logging.warning(
                        "Playlist download incomplete (%d/%d). Keeping current playlist.",
                        len(items),
                        len(raw_items),
                    )
            if switch_ok:
                updated = state.update(items, fingerprint)
                if updated:
                    logging.info("Playlist updated: %d items", len(items))
                if items and (updated or not os.path.exists(playlist_state_path(cfg_snapshot))):
                    save_playlist_state(cfg_snapshot, items, fingerprint)
                if updated:
                    status_snapshot = status.snapshot()
                    keep_paths = {item.path for item in items}
                    current = status_snapshot.get("current_item") or {}
                    next_item = status_snapshot.get("next_item") or {}
                    if isinstance(current, dict) and current.get("path"):
                        keep_paths.add(current["path"])
                    if isinstance(next_item, dict) and next_item.get("path"):
                        keep_paths.add(next_item["path"])
                    removed = cleanup_cache_dir(
                        cfg_snapshot["cache_dir"],
                        keep_paths,
                        cache_index,
                        cfg_snapshot,
                    )
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
                status.update(playlist_size=len(items))
            else:
                current_items, _ = state.get()
                status.update(playlist_size=len(current_items))
            status.update(last_poll_success=iso_now(), last_poll_error=None)
            save_last_success(cfg_snapshot, iso_now())
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
            wait_poll_interval(cfg_snapshot)


def watchdog(
    cfg: Dict,
    cfg_lock: threading.Lock,
    mpv: MPVController,
    status: StatusState,
    stop_event: threading.Event,
) -> None:
    last_time_pos: Optional[float] = None
    last_time_pos_at = time.time()
    last_path_mismatch_at: Optional[float] = None
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
        stall_sec = int(cfg_snapshot.get("playback_stall_sec") or 0)
        mismatch_sec = int(cfg_snapshot.get("playback_mismatch_sec") or 0)
        cache_dir = cfg_snapshot.get("cache_dir")
        status_snapshot = status.snapshot()
        current_item = status_snapshot.get("current_item") or {}
        next_item = status_snapshot.get("next_item") or {}
        expected_path = current_item.get("path") if isinstance(current_item, dict) else None
        expected_paths: List[str] = []
        if isinstance(current_item, dict) and isinstance(current_item.get("path"), str):
            expected_paths.append(current_item["path"])
        if isinstance(next_item, dict) and isinstance(next_item.get("path"), str):
            expected_paths.append(next_item["path"])
        if mismatch_sec <= 0:
            last_path_mismatch_at = None
        elif expected_paths:
            actual_path = mpv.get_property("path")
            if isinstance(actual_path, str):
                has_path_match = any(
                    media_paths_match(candidate, actual_path, cache_dir)
                    for candidate in expected_paths
                )
                if not has_path_match:
                    if last_path_mismatch_at is None:
                        last_path_mismatch_at = time.time()
                    elif time.time() - last_path_mismatch_at > mismatch_sec:
                        logging.warning(
                            "MPV path mismatch (expected one of %s, got %s), restarting",
                            expected_paths,
                            actual_path,
                        )
                        mpv.restart()
                        last_path_mismatch_at = None
                        last_time_pos = None
                        last_time_pos_at = time.time()
                else:
                    last_path_mismatch_at = None
            else:
                last_path_mismatch_at = None

        if stall_sec > 0 and expected_path and not is_image_path(expected_path):
            time_pos = mpv.get_property("time-pos")
            if isinstance(time_pos, (int, float)):
                if last_time_pos is None or time_pos != last_time_pos:
                    last_time_pos = float(time_pos)
                    last_time_pos_at = time.time()
                elif time.time() - last_time_pos_at > stall_sec:
                    logging.warning("MPV playback stalled for %.1fs, restarting", time.time() - last_time_pos_at)
                    mpv.restart()
                    last_time_pos = None
                    last_time_pos_at = time.time()
            else:
                last_time_pos = None
                last_time_pos_at = time.time()
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


def cleanup_cache_dir(
    cache_dir: str,
    keep_paths: set,
    cache_index: CacheIndex,
    cfg_snapshot: Dict,
) -> int:
    removed = 0
    if not os.path.isdir(cache_dir):
        return removed

    max_files = int(cfg_snapshot.get("cache_max_files") or 0)
    max_bytes = int(cfg_snapshot.get("cache_max_bytes") or 0)
    index_snapshot = cache_index.snapshot()

    candidates: List[Tuple[str, int, float]] = []
    total_size = 0
    total_count = 0
    for name in os.listdir(cache_dir):
        path = os.path.join(cache_dir, name)
        if not os.path.isfile(path):
            continue
        size = safe_getsize(path) or 0
        total_size += size
        total_count += 1
        if path in keep_paths:
            continue
        meta = index_snapshot.get(path) or {}
        last_used = meta.get("last_used")
        last_used_ts = parse_iso_utc(last_used) if isinstance(last_used, str) else None
        if last_used_ts is None:
            try:
                last_used_ts = os.path.getmtime(path)
            except OSError:
                last_used_ts = 0.0
        candidates.append((path, size, float(last_used_ts)))

    to_remove: List[str] = []
    if max_files <= 0 and max_bytes <= 0:
        to_remove = [path for path, _size, _ts in candidates]
    else:
        candidates.sort(key=lambda entry: entry[2])
        while candidates and (
            (max_files > 0 and total_count > max_files)
            or (max_bytes > 0 and total_size > max_bytes)
        ):
            path, size, _ = candidates.pop(0)
            to_remove.append(path)
            total_count -= 1
            total_size -= size

    for path in to_remove:
        try:
            os.remove(path)
            removed += 1
        except Exception as exc:
            logging.warning("Failed to delete %s: %s", path, exc)

    if removed:
        cache_index.remove_missing()
    return removed


def cleanup_temp_files(cache_dir: str, max_age_sec: int) -> int:
    if max_age_sec <= 0 or not os.path.isdir(cache_dir):
        return 0
    now = time.time()
    removed = 0
    for name in os.listdir(cache_dir):
        if not name.endswith(".tmp"):
            continue
        path = os.path.join(cache_dir, name)
        if not os.path.isfile(path):
            continue
        try:
            age = now - os.path.getmtime(path)
        except OSError:
            age = max_age_sec + 1
        if age < max_age_sec:
            continue
        try:
            os.remove(path)
            removed += 1
        except Exception as exc:
            logging.warning("Failed to delete temp file %s: %s", path, exc)
    return removed


def cleanup_worker(
    cfg: Dict,
    cfg_lock: threading.Lock,
    state: PlaylistState,
    status: StatusState,
    cache_index: CacheIndex,
    stop_event: threading.Event,
) -> None:
    while not stop_event.is_set():
        cfg_snapshot = config_snapshot(cfg, cfg_lock)
        interval = int(cfg_snapshot.get("cleanup_interval_sec") or 0)
        if interval <= 0:
            time.sleep(1)
            continue
        temp_max_age = int(cfg_snapshot.get("tmp_max_age_sec") or 0)
        temp_removed = cleanup_temp_files(cfg_snapshot["cache_dir"], temp_max_age)
        status_snapshot = status.snapshot()
        if cfg_snapshot.get("disable_cleanup_when_offline"):
            failures = int(status_snapshot.get("consecutive_failures") or 0)
            if failures > 0 or not status_snapshot.get("last_poll_success"):
                for _ in range(int(interval * 5)):
                    if stop_event.is_set():
                        break
                    time.sleep(0.2)
                continue
        items, _ = state.get()
        keep_paths = {item.path for item in items}
        keep_paths.update(saved_playlist_paths(cfg_snapshot))
        snapshot = status.snapshot()
        current = snapshot.get("current_item") or {}
        next_item = snapshot.get("next_item") or {}
        if isinstance(current, dict) and current.get("path"):
            keep_paths.add(current["path"])
        if isinstance(next_item, dict) and next_item.get("path"):
            keep_paths.add(next_item["path"])

        removed = cleanup_cache_dir(cfg_snapshot["cache_dir"], keep_paths, cache_index, cfg_snapshot)
        status.update(last_cleanup=iso_now(), last_cleanup_removed=removed + temp_removed)

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
    cache_index: CacheIndex,
    stop_event: threading.Event,
) -> None:
    idx = 0
    offset_ms = 0
    last_version = -1
    preloaded_path: Optional[str] = None
    last_mpv_generation = -1
    blocked_media_until: Dict[str, float] = {}

    boot_wall_ts = time.time()
    boot_mono_ts = time.monotonic()
    pending_soft_resync = False
    pending_daily_zero_ts: Optional[float] = None

    cfg_snapshot = config_snapshot(cfg, cfg_lock)
    sync_enabled = bool(cfg_snapshot.get("sync_enabled", True))
    drift_threshold_ms = int(cfg_snapshot.get("sync_drift_threshold_ms") or 300)
    hard_resync_ms = int(cfg_snapshot.get("sync_hard_resync_ms") or 1200)
    checkpoint_interval_sec = int(cfg_snapshot.get("sync_checkpoint_interval_sec") or 3600)
    boot_hard_check_sec = int(cfg_snapshot.get("sync_boot_hard_check_sec") or 300)
    prep_mode = str(cfg_snapshot.get("sync_prep_mode") or "wait_until_anchor").strip().lower()
    prep_wait_mode = prep_mode in {"wait", "wait_until_anchor", "hold_until_anchor"}
    boot_hard_check_due_mono: Optional[float] = None
    next_checkpoint_ts: Optional[float] = None
    force_daily_zero_once = False

    if sync_enabled:
        if is_prep_window_utc(boot_wall_ts):
            prep_anchor_ts = next_daily_anchor_utc_ts(boot_wall_ts)
            status.update(
                sync_mode="prep",
                sync_anchor_utc=iso_from_ts(prep_anchor_ts),
                sync_last_action="prep_wait_anchor",
            )
            run_ntp_sync_command(cfg_snapshot)
            if prep_wait_mode:
                wait_sec = max(prep_anchor_ts - time.time(), 0.0)
                status.update(
                    playback_state="waiting_sync_anchor",
                    black_screen_risk_reason="sync_prep_wait",
                )
                logging.info(
                    "Sync PREP ativo no boot. Aguardando 00:05 UTC por %.2fs para forcar index=0/offset=0.",
                    wait_sec,
                )
                while not stop_event.is_set():
                    if time.time() >= prep_anchor_ts:
                        break
                    time.sleep(0.2)
                if stop_event.is_set():
                    return
                force_daily_zero_once = True
                status.update(
                    sync_mode="running",
                    sync_anchor_utc=iso_from_ts(prep_anchor_ts),
                    sync_last_action="daily_zero_ready",
                    playback_state="starting",
                    black_screen_risk_reason=None,
                )
                logging.info("Sync PREP concluido. Player iniciara em index=0/offset=0.")
            else:
                pending_daily_zero_ts = prep_anchor_ts
                status.update(
                    sync_mode="running",
                    sync_anchor_utc=iso_from_ts(prep_anchor_ts),
                    sync_last_action="prep_play_until_anchor",
                )
                logging.info("Sync PREP ativo no boot com modo play_then_resync; tocando ate 00:05 UTC.")
        else:
            status.update(sync_mode="running")

        if boot_hard_check_sec > 0:
            boot_hard_check_due_mono = boot_mono_ts + boot_hard_check_sec
        next_checkpoint_ts = next_hour_checkpoint_utc_ts(time.time(), checkpoint_interval_sec)
        status.update(sync_next_checkpoint_utc=iso_from_ts(next_checkpoint_ts))
    else:
        status.update(sync_mode="disabled")

    while not stop_event.is_set():
        items, version = state.get()
        if not items:
            status.update(
                playback_state="waiting_for_media",
                black_screen_risk_reason="playlist_empty",
                blocked_media_count=0,
            )
            time.sleep(1)
            continue

        durations_ms, cycle_start_ms, cycle_total_ms = cycle_timeline(items)
        if cycle_total_ms <= 0:
            status.update(
                playback_state="waiting_for_media",
                black_screen_risk_reason="invalid_playlist_timeline",
            )
            time.sleep(1)
            continue

        now_for_block = time.time()
        blocked_media_until = {p: ts for p, ts in blocked_media_until.items() if ts > now_for_block}
        blocked_count = sum(1 for media in items if blocked_media_until.get(media.path, 0.0) > now_for_block)
        if blocked_count >= len(items):
            status.update(
                playback_state="waiting_for_media",
                black_screen_risk_reason="all_media_temporarily_blocked",
                blocked_media_count=blocked_count,
            )
            time.sleep(1)
            continue

        cfg_snapshot = config_snapshot(cfg, cfg_lock)
        sync_enabled = bool(cfg_snapshot.get("sync_enabled", True))
        drift_threshold_ms = int(cfg_snapshot.get("sync_drift_threshold_ms") or 300)
        hard_resync_ms = int(cfg_snapshot.get("sync_hard_resync_ms") or 1200)
        checkpoint_interval_sec = int(cfg_snapshot.get("sync_checkpoint_interval_sec") or 3600)

        if sync_enabled and next_checkpoint_ts is None:
            next_checkpoint_ts = next_hour_checkpoint_utc_ts(time.time(), checkpoint_interval_sec)
            status.update(sync_next_checkpoint_utc=iso_from_ts(next_checkpoint_ts))
        if not sync_enabled:
            pending_soft_resync = False
            pending_daily_zero_ts = None
            boot_hard_check_due_mono = None
            next_checkpoint_ts = None
            status.update(sync_mode="disabled", sync_cycle_ms=cycle_total_ms)
        else:
            status.update(sync_mode="running", sync_cycle_ms=cycle_total_ms)

        if version != last_version:
            last_version = version
            preloaded_path = None
            pending_soft_resync = False
            if sync_enabled:
                if force_daily_zero_once:
                    idx = 0
                    offset_ms = 0
                    force_daily_zero_once = False
                    status.update(sync_last_action="daily_zero_applied")
                    logging.info("Sync daily zero aplicado em 00:05 UTC (index=0, offset=0).")
                else:
                    sync_pos = compute_cycle_position_from_utc(time.time(), durations_ms)
                    idx = sync_pos.index
                    offset_ms = sync_pos.offset_ms
                    status.update(
                        sync_anchor_utc=iso_from_ts(sync_pos.anchor_ts),
                        sync_last_action="playlist_realign",
                    )
                    logging.info(
                        "Playlist alterada. Recalculando posicao UTC: index=%d offset=%dms",
                        idx,
                        offset_ms,
                    )
            else:
                idx = 0
                offset_ms = 0

        idx = idx % len(items)
        item = items[idx]
        if blocked_media_until.get(item.path, 0.0) > time.time():
            idx += 1
            offset_ms = 0
            continue
        item_duration_ms = durations_ms[idx]
        next_item = None
        if len(items) > 1:
            next_item = items[(idx + 1) % len(items)]

        mpv.ensure_running()
        if mpv.generation() != last_mpv_generation:
            last_mpv_generation = mpv.generation()
            preloaded_path = None
        reuse_preloaded = preloaded_path == item.path and offset_ms <= 0
        if not reuse_preloaded:
            if not mpv.load_file(item.path):
                logging.warning("Failed to load media, restarting MPV")
                mpv.restart()
                if not mpv.load_file(item.path):
                    cooldown_sec = max(int(cfg_snapshot.get("media_load_retry_cooldown_sec") or 0), 5)
                    blocked_media_until[item.path] = time.time() + cooldown_sec
                    status.update(
                        playback_state="recovering",
                        black_screen_risk_reason="media_load_failed",
                        blocked_media_count=len(blocked_media_until),
                        last_render_error=f"{iso_now()} failed_to_load:{item.path}",
                    )
                    idx += 1
                    offset_ms = 0
                    time.sleep(0.2)
                    continue
            if offset_ms > 0 and not is_image_path(item.path):
                offset_seconds = offset_ms / 1000.0
                if not mpv.seek_absolute(offset_seconds):
                    mpv.set_property("time-pos", offset_seconds)
        preloaded_path = None
        blocked_media_until.pop(item.path, None)

        if next_item is not None and cfg_snapshot.get("preload_next"):
            mpv.append_file(next_item.path)

        status.update(
            playback_state="playing",
            black_screen_risk_reason=None,
            blocked_media_count=len(blocked_media_until),
            last_render_ok=iso_now(),
            last_render_error=None,
            current_index=idx % len(items),
            current_item={
                "url": item.url,
                "path": item.path,
                "duration_ms": item_duration_ms,
                "campaign_id": item.campaign_id,
                "campaign_name": item.campaign_name,
                "started_at": iso_now(),
                "offset_ms": offset_ms,
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
        cache_index.touch(item)

        logging.info("Playing %s (duration=%s ms, offset=%s ms)", item.url, item_duration_ms, offset_ms)
        item_started_mono = time.monotonic()
        remaining_ms = max(item_duration_ms - offset_ms, 1)
        current_cycle_start_ms = cycle_start_ms[idx]
        hard_resync_requested = False

        while not stop_event.is_set():
            now_mono = time.monotonic()
            elapsed_ms = int((now_mono - item_started_mono) * 1000)
            if elapsed_ms >= remaining_ms:
                break

            check_reason: Optional[str] = None
            now_ts = time.time()
            if sync_enabled:
                if pending_daily_zero_ts is not None and now_ts >= pending_daily_zero_ts:
                    check_reason = "daily_zero"
                    pending_daily_zero_ts = None
                elif boot_hard_check_due_mono is not None and now_mono >= boot_hard_check_due_mono:
                    check_reason = "boot_5min"
                    boot_hard_check_due_mono = None
                elif next_checkpoint_ts is not None and now_ts >= next_checkpoint_ts:
                    check_reason = "utc_checkpoint"
                    next_checkpoint_ts = next_hour_checkpoint_utc_ts(now_ts, checkpoint_interval_sec)
                    status.update(sync_next_checkpoint_utc=iso_from_ts(next_checkpoint_ts))

            if check_reason:
                if check_reason == "daily_zero":
                    idx = 0
                    offset_ms = 0
                    pending_soft_resync = False
                    hard_resync_requested = True
                    preloaded_path = None
                    status.update(
                        sync_anchor_utc=iso_from_ts(daily_anchor_utc_ts(now_ts)),
                        sync_last_check_utc=iso_from_ts(now_ts),
                        sync_checkpoint_reason=check_reason,
                        sync_last_action="daily_zero_applied",
                    )
                    logging.info("Sync daily zero aplicado em 00:05 UTC (index=0, offset=0).")
                    break

                sync_pos = compute_cycle_position_from_utc(now_ts, durations_ms)
                actual_offset_ms = min(offset_ms + elapsed_ms, item_duration_ms)
                actual_cycle_pos_ms = (current_cycle_start_ms + actual_offset_ms) % sync_pos.cycle_total_ms
                drift_ms = signed_cycle_delta_ms(
                    target_ms=sync_pos.cycle_pos_ms,
                    current_ms=actual_cycle_pos_ms,
                    cycle_total_ms=sync_pos.cycle_total_ms,
                )
                action = classify_drift_action(
                    drift_ms=drift_ms,
                    drift_threshold_ms=drift_threshold_ms,
                    hard_resync_ms=hard_resync_ms,
                )
                status.update(
                    sync_anchor_utc=iso_from_ts(sync_pos.anchor_ts),
                    sync_drift_ms=drift_ms,
                    sync_last_check_utc=iso_from_ts(now_ts),
                    sync_checkpoint_reason=check_reason,
                )

                if action == "hard_resync":
                    idx = sync_pos.index
                    offset_ms = sync_pos.offset_ms
                    pending_soft_resync = False
                    hard_resync_requested = True
                    preloaded_path = None
                    status.update(sync_last_action=f"hard_resync:{check_reason}")
                    logging.warning(
                        "Hard resync (%s): drift=%dms -> index=%d offset=%dms",
                        check_reason,
                        drift_ms,
                        idx,
                        offset_ms,
                    )
                    break
                if action == "soft_resync":
                    pending_soft_resync = True
                    status.update(sync_last_action=f"soft_resync_pending:{check_reason}")
                    logging.info("Soft resync agendado (%s): drift=%dms", check_reason, drift_ms)
                else:
                    status.update(sync_last_action=f"stable:{check_reason}")

            time.sleep(0.2)

        if hard_resync_requested:
            continue

        if sync_enabled and pending_soft_resync:
            sync_pos = compute_cycle_position_from_utc(time.time(), durations_ms)
            idx = sync_pos.index
            offset_ms = sync_pos.offset_ms
            pending_soft_resync = False
            preloaded_path = None
            status.update(
                sync_anchor_utc=iso_from_ts(sync_pos.anchor_ts),
                sync_last_action="soft_resync_applied",
            )
            logging.info("Soft resync aplicado na borda: index=%d offset=%dms", idx, offset_ms)
            continue

        if next_item is not None and cfg_snapshot.get("preload_next"):
            if mpv.playlist_next():
                mpv.playlist_remove(0)
                preloaded_path = next_item.path
                idx += 1
                offset_ms = 0
                continue

        idx += 1
        offset_ms = 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Kiosky MPV player")
    parser.add_argument("--config", default="config.json", help="Path to config.json")
    args = parser.parse_args()
    config_path = os.path.abspath(args.config)

    cfg = load_config(config_path)
    setup_logging(cfg)

    api_credentials_ready = bool(cfg.get("api_key") and cfg.get("environment_id"))
    if not api_credentials_ready:
        logging.warning("API credentials missing; startup will run in offline-only mode if local media is available.")
    if requests is None:
        logging.warning("requests dependency unavailable; API polling disabled.")
    api_polling_enabled = api_credentials_ready and requests is not None

    cfg_lock = threading.Lock()
    poll_now_event = threading.Event()

    state = PlaylistState()
    status = StatusState()
    mpv = MPVController(cfg)
    cache_index = CacheIndex(cfg)
    cache_index.remove_missing()
    stop_event = threading.Event()
    force_exit = threading.Event()

    if cfg.get("offline_fallback"):
        offline_network_available: Optional[bool] = None
        if (
            float(cfg.get("offline_max_age_hours") or 0) > 0
            and cfg.get("offline_ignore_max_age_when_no_network", True)
        ):
            offline_network_available = api_endpoint_reachable(cfg, timeout_sec=2.0)
            if offline_network_available is False:
                logging.warning("API endpoint unavailable at boot; ignoring offline age limit for startup fallback.")

        loaded_offline = False
        saved_items, _saved_fp, saved_at = load_playlist_state(cfg)
        if saved_items and offline_playlist_allowed(cfg, saved_at, offline_network_available):
            offline_items, fp_payload = media_items_from_saved(cfg, saved_items)
            if offline_items:
                offline_fp = fingerprint_items(fp_payload)
                state.update(offline_items, offline_fp)
                status.update(playlist_size=len(offline_items))
                logging.info("Loaded offline playlist: %d items", len(offline_items))
                loaded_offline = True
            else:
                logging.warning("Offline playlist found but no cached files are available.")
        elif saved_items:
            logging.info("Offline playlist skipped due to max age policy.")
        if not loaded_offline and offline_playlist_allowed(cfg, None, offline_network_available):
            cache_items, cache_payload = media_items_from_cache(cfg, cache_index)
            if cache_items:
                cache_fp = fingerprint_items(cache_payload)
                state.update(cache_items, cache_fp)
                save_playlist_state(cfg, cache_items, cache_fp)
                status.update(playlist_size=len(cache_items))
                logging.info("Loaded offline playlist from local cache: %d items", len(cache_items))

    current_items, _current_version = state.get()
    if not api_polling_enabled and not current_items:
        if not api_credentials_ready:
            logging.error("api_key/environment_id ausentes e nenhuma midia offline disponivel.")
        elif requests is None:
            logging.error("requests indisponivel e nenhuma midia offline disponivel.")
        return 2
    if not api_polling_enabled:
        status.update(last_poll_error=f"{iso_now()} polling_disabled")
        logging.warning("API polling disabled; player running with local media only.")

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

    threads: List[threading.Thread] = []
    if api_polling_enabled:
        threads.append(
            threading.Thread(
                target=poller,
                args=(cfg, cfg_lock, poll_now_event, state, status, cache_index, stop_event),
                daemon=True,
            )
        )
    threads.extend(
        [
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
                args=(cfg, cfg_lock, state, status, cache_index, stop_event),
                daemon=True,
            ),
            threading.Thread(
                target=telemetry_worker,
                args=(cfg, cfg_lock, status, stop_event),
                daemon=True,
            ),
        ]
    )
    for thread in threads:
        thread.start()

    config_server = ConfigServer(cfg, cfg_lock, config_path, mpv, poll_now_event)
    config_server.start()

    try:
        playback_loop(cfg, cfg_lock, state, status, mpv, cache_index, stop_event)
    finally:
        stop_event.set()
        for thread in threads:
            thread.join(timeout=5)
        mpv.stop()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
