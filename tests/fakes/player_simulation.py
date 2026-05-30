from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import kiosk


def sanitized_media_id(path: str, url: str = "") -> str:
    return kiosk.media_alias(path, url)


@dataclass
class SimEvent:
    name: str
    fields: Dict[str, object]


class FakeClock:
    def __init__(self, start_ts: float = 1_770_000_000.0) -> None:
        self._time = float(start_ts)
        self._monotonic = 0.0
        self.sleeps: List[float] = []

    def time(self) -> float:
        return self._time

    def monotonic(self) -> float:
        return self._monotonic

    def sleep(self, seconds: float) -> None:
        seconds = max(float(seconds), 0.0)
        self.sleeps.append(seconds)
        self._time += seconds
        self._monotonic += seconds

    def advance_ms(self, milliseconds: int) -> None:
        self.sleep(milliseconds / 1000.0)


class FakeAPIResponse:
    def __init__(self, payload: Dict[str, object], status_error: Optional[Exception] = None) -> None:
        self._payload = payload
        self._status_error = status_error

    def raise_for_status(self) -> None:
        if self._status_error is not None:
            raise self._status_error

    def json(self) -> Dict[str, object]:
        return self._payload


class FakeDownloadResponse:
    def __init__(self, body: bytes = b"media", headers: Optional[Dict[str, str]] = None) -> None:
        self._body = body
        self.headers = headers or {"Content-Length": str(len(body))}

    def raise_for_status(self) -> None:
        return None

    def iter_content(self, chunk_size: int):
        yield self._body


class FakeAPI:
    def __init__(
        self,
        playlist_payload: Optional[Dict[str, object]] = None,
        *,
        post_error: Optional[Exception] = None,
        get_error: Optional[Exception] = None,
    ) -> None:
        self.playlist_payload = playlist_payload or {"units": []}
        self.post_error = post_error
        self.get_error = get_error
        self.post_calls: List[Dict[str, object]] = []
        self.get_calls: List[Dict[str, object]] = []

    def post(self, url: str, headers: Dict[str, str], json: Dict[str, object], timeout: int) -> FakeAPIResponse:
        self.post_calls.append(
            {
                "url_present": bool(url),
                "api_key_header_present": bool(headers.get("x-api-key")),
                "environment_id_present": bool(json.get("environmentId")),
                "timeout": timeout,
            }
        )
        if self.post_error is not None:
            raise self.post_error
        return FakeAPIResponse(self.playlist_payload)

    def get(self, url: str, stream: bool, timeout: int) -> FakeDownloadResponse:
        self.get_calls.append(
            {
                "url_alias": sanitized_media_id("", url),
                "stream": stream,
                "timeout": timeout,
            }
        )
        if self.get_error is not None:
            raise self.get_error
        return FakeDownloadResponse()


class FakeMPV:
    def __init__(self, *, fail_load_paths: Optional[Iterable[str]] = None) -> None:
        self.commands: List[Dict[str, object]] = []
        self.events: List[SimEvent] = []
        self.fail_load_paths = set(fail_load_paths or [])
        self.restart_reasons: List[str] = []
        self._generation = 1
        self._running = True
        self._last_loadfile_monotonic: Optional[float] = None

    def ensure_running(self) -> None:
        self.commands.append({"command": "ensure_running"})
        self._running = True

    def is_running(self) -> bool:
        return self._running

    def generation(self) -> int:
        return self._generation

    def pid(self) -> int:
        return 4242

    def current_log_file(self) -> str:
        return "/tmp/kiosky-test/mpv-g001.log"

    def last_loadfile_monotonic(self) -> Optional[float]:
        return self._last_loadfile_monotonic

    def last_start_monotonic(self) -> Optional[float]:
        return 0.0

    def load_file(self, path: str, alias: str = "") -> bool:
        self.commands.append({"command": "loadfile", "alias": alias or sanitized_media_id(path), "mode": "replace"})
        self._last_loadfile_monotonic = 0.0
        if path in self.fail_load_paths:
            self.events.append(SimEvent("mpv_event_media_error", {"alias": alias or sanitized_media_id(path)}))
            return False
        self.events.append(SimEvent("mpv_event_file_loaded", {"alias": alias or sanitized_media_id(path)}))
        return True

    def append_file(self, path: str) -> bool:
        self.commands.append({"command": "loadfile", "alias": sanitized_media_id(path), "mode": "append"})
        return True

    def playlist_next(self) -> bool:
        self.commands.append({"command": "playlist-next"})
        return True

    def playlist_remove(self, index: int) -> bool:
        self.commands.append({"command": "playlist-remove", "index": index})
        return True

    def seek_absolute(self, seconds: float) -> bool:
        self.commands.append({"command": "seek", "seconds": seconds})
        return True

    def set_property(self, name: str, value: object) -> bool:
        self.commands.append({"command": "set_property", "name": name, "value": value})
        return True

    def restart(self, reason: str = "manual") -> None:
        self.restart_reasons.append(reason)
        self.commands.append({"command": "restart", "reason": reason})
        self._generation += 1
        self._running = True


def playlist_payload(campaigns: List[Dict[str, object]]) -> Dict[str, object]:
    return {"units": [{"campaigns": campaigns}]}


def campaign(
    *,
    media_urls: Optional[List[str]] = None,
    primary_media_url: str = "",
    exposure_time_ms: Optional[int] = None,
    exposureTimeMs: Optional[int] = None,
    exposureTimeSeconds: Optional[int] = None,
    duration: Optional[int] = None,
    status: str = "active",
    campaign_id: str = "campaign-sim",
    name: str = "Synthetic campaign",
) -> Dict[str, object]:
    data: Dict[str, object] = {
        "id": campaign_id,
        "name": name,
        "status": status,
    }
    if media_urls is not None:
        data["media_urls"] = media_urls
    if primary_media_url:
        data["primary_media_url"] = primary_media_url
    if exposure_time_ms is not None:
        data["exposure_time_ms"] = exposure_time_ms
    if exposureTimeMs is not None:
        data["exposureTimeMs"] = exposureTimeMs
    if exposureTimeSeconds is not None:
        data["exposureTimeSeconds"] = exposureTimeSeconds
    if duration is not None:
        data["duration"] = duration
    return data


def write_cached_media(cache_dir: Path, url: str, body: bytes = b"media") -> Path:
    cache_dir.mkdir(parents=True, exist_ok=True)
    path = Path(kiosk.cache_path(str(cache_dir), url))
    path.write_bytes(body)
    return path


class PlayerTimingSimulator:
    def __init__(
        self,
        cfg: Dict[str, object],
        items: List[kiosk.MediaItem],
        *,
        mpv: FakeMPV,
        clock: FakeClock,
        media_duration_ms: Optional[Dict[str, int]] = None,
    ) -> None:
        self.cfg = dict(cfg)
        self.items = list(items)
        self.mpv = mpv
        self.clock = clock
        self.media_duration_ms = dict(media_duration_ms or {})
        self.events: List[SimEvent] = []

    def _record(self, name: str, **fields: object) -> None:
        self.events.append(SimEvent(name, fields))

    def run_steps(self, steps: int, *, start_index: int = 0) -> List[SimEvent]:
        durations_ms, _cycle_starts, _cycle_total = kiosk.cycle_timeline(self.items)
        idx = start_index
        for _ in range(steps):
            item = self.items[idx % len(self.items)]
            duration_ms = durations_ms[idx % len(durations_ms)]
            alias = sanitized_media_id(item.path, item.url)
            self._record("item_selected", alias=alias, index=idx % len(self.items))
            self._record("duration_selected_ms", alias=alias, duration_ms=duration_ms)
            self._record("media_load_start", alias=alias)
            self.mpv.ensure_running()
            if not self.mpv.load_file(item.path, alias=alias):
                self.mpv.restart(reason=f"media_load_failed:{alias}")
                if not self.mpv.load_file(item.path, alias=alias):
                    self._record("media_load_done", alias=alias, ok=False)
                    self._record("playback_error", alias=alias, behavior="advance_after_retry_failure")
                    idx += 1
                    continue
            self._record("mpv_loadfile_sent", alias=alias)
            self._record("mpv_event_file_loaded", alias=alias)
            self._record("playback_start", alias=alias, monotonic_ms=int(self.clock.monotonic() * 1000))
            real_duration_ms = int(self.media_duration_ms.get(item.path, duration_ms))
            mpv_args = kiosk.build_mpv_args(self.cfg)
            loop_file_enabled = "--loop-file=inf" in mpv_args
            if not kiosk.is_image_path(item.path) and real_duration_ms < duration_ms and loop_file_enabled:
                self._record(
                    "loop_suspected",
                    alias=alias,
                    policy="repeat_to_fill_exposure",
                    reason="mpv_loop_file_repeats_short_video_within_exposure_window",
                    real_duration_ms=real_duration_ms,
                    exposure_window_ms=duration_ms,
                )
            expected_end_ms = int(self.clock.monotonic() * 1000) + duration_ms
            self._record("expected_end", alias=alias, monotonic_ms=expected_end_ms)
            self.clock.advance_ms(duration_ms)
            self._record("observed_end", alias=alias, monotonic_ms=int(self.clock.monotonic() * 1000))

            if len(self.items) > 1 and self.cfg.get("preload_next"):
                next_item = self.items[(idx + 1) % len(self.items)]
                self.mpv.append_file(next_item.path)
                self.mpv.playlist_next()
                self.mpv.playlist_remove(0)
            idx += 1
        return list(self.events)
