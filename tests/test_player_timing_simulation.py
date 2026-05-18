import tempfile
import threading
import unittest
import sys
from pathlib import Path
from unittest.mock import patch

import kiosk

sys.path.insert(0, str(Path(__file__).resolve().parent))

from fakes.player_simulation import (
    FakeAPI,
    FakeClock,
    FakeMPV,
    PlayerTimingSimulator,
    campaign,
    playlist_payload,
    write_cached_media,
)


def sim_cfg(cache_dir: Path) -> dict:
    return {
        "api_url": "https://api.invalid/search",
        "api_key": "synthetic-key",
        "environment_id": "00000000-0000-0000-0000-000000000000",
        "only_standby": True,
        "search_in": "campaign",
        "include_descendants": True,
        "limit": 20,
        "request_timeout_sec": 1,
        "default_duration_ms": 7000,
        "cache_dir": str(cache_dir),
        "state_dir": str(cache_dir / ".state"),
        "min_free_space_bytes": 0,
        "max_download_bytes": 1024 * 1024,
        "mpv_path": "mpv",
        "ipc_path": str(cache_dir / "mpv.sock"),
        "mpv_log_file": "",
        "mpv_msg_level": "",
        "mpv_vo": "",
        "mpv_gpu_context": "",
        "mpv_ao": "",
        "hotkeys_enabled": False,
        "lock_input": True,
        "low_resource_mode": False,
        "hwdec": "",
        "rotation_deg": 0,
        "preload_next": False,
        "media_load_retry_cooldown_sec": 5,
        "sync_enabled": False,
    }


def item_from_cached(cache_dir: Path, url: str, duration_ms: int) -> kiosk.MediaItem:
    path = write_cached_media(cache_dir, url)
    return kiosk.MediaItem(
        url=url,
        duration_ms=duration_ms,
        path=str(path),
        campaign_id="campaign-sim",
        campaign_name="Synthetic campaign",
    )


class PlayerTimingSimulationTests(unittest.TestCase):
    def test_image_respects_exposure_time_ms(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir)
            url = "https://media.invalid/image-a.png"
            fake_api = FakeAPI(playlist_payload([campaign(media_urls=[url], exposure_time_ms=10000)]))
            cfg = sim_cfg(cache_dir)

            with patch.object(kiosk, "requests", fake_api):
                raw_items = kiosk.fetch_media_list(cfg)

            self.assertEqual(raw_items[0]["duration_ms"], 10000)
            item = item_from_cached(cache_dir, url, int(raw_items[0]["duration_ms"]))
            simulator = PlayerTimingSimulator(cfg, [item], mpv=FakeMPV(), clock=FakeClock())
            events = simulator.run_steps(1)

            self.assertIn(("duration_selected_ms", 10000), [(event.name, event.fields.get("duration_ms")) for event in events])
            self.assertEqual(simulator.clock.monotonic(), 10.0)

    def test_video_shorter_than_exposure_policy_is_mpv_loop_until_window(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir)
            url = "https://media.invalid/short-video.mp4"
            cfg = sim_cfg(cache_dir)
            item = item_from_cached(cache_dir, url, 10000)
            simulator = PlayerTimingSimulator(
                cfg,
                [item],
                mpv=FakeMPV(),
                clock=FakeClock(),
                media_duration_ms={item.path: 3000},
            )

            events = simulator.run_steps(1)

            self.assertIn("--loop-file=inf", kiosk.build_mpv_args(cfg))
            loop_events = [event for event in events if event.name == "loop_suspected"]
            self.assertEqual(len(loop_events), 1)
            self.assertEqual(loop_events[0].fields["reason"], "mpv_loop_file_repeats_short_video_within_exposure_window")
            self.assertEqual(simulator.clock.monotonic(), 10.0)

    def test_missing_exposure_uses_default(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir)
            url = "https://media.invalid/default-duration.png"
            fake_api = FakeAPI(playlist_payload([campaign(media_urls=[url])]))
            cfg = sim_cfg(cache_dir)

            with patch.object(kiosk, "requests", fake_api):
                raw_items = kiosk.fetch_media_list(cfg)

            self.assertEqual(raw_items[0]["duration_ms"], cfg["default_duration_ms"])

    def test_camel_case_duration_fields_are_ignored_by_current_parser(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir)
            url = "https://media.invalid/camel-case-duration.png"
            fake_api = FakeAPI(
                playlist_payload(
                    [
                        campaign(
                            media_urls=[url],
                            exposureTimeMs=11000,
                            exposureTimeSeconds=12,
                            duration=13,
                        )
                    ]
                )
            )
            cfg = sim_cfg(cache_dir)

            with patch.object(kiosk, "requests", fake_api):
                raw_items = kiosk.fetch_media_list(cfg)

            self.assertEqual(raw_items[0]["duration_ms"], cfg["default_duration_ms"])

    def test_single_item_playlist_repeat_is_expected_cycle_behavior(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir)
            cfg = sim_cfg(cache_dir)
            item = item_from_cached(cache_dir, "https://media.invalid/only.png", 4000)
            mpv = FakeMPV()
            simulator = PlayerTimingSimulator(cfg, [item], mpv=mpv, clock=FakeClock())

            events = simulator.run_steps(2)

            selected = [event for event in events if event.name == "item_selected"]
            self.assertEqual(len(selected), 2)
            self.assertEqual(selected[0].fields["alias"], selected[1].fields["alias"])
            self.assertEqual([cmd["command"] for cmd in mpv.commands].count("loadfile"), 2)

    def test_multi_item_playlist_advances(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir)
            cfg = sim_cfg(cache_dir)
            cfg["preload_next"] = True
            first = item_from_cached(cache_dir, "https://media.invalid/first.png", 3000)
            second = item_from_cached(cache_dir, "https://media.invalid/second.png", 5000)
            mpv = FakeMPV()
            simulator = PlayerTimingSimulator(cfg, [first, second], mpv=mpv, clock=FakeClock())

            events = simulator.run_steps(2)

            selected_aliases = [event.fields["alias"] for event in events if event.name == "item_selected"]
            self.assertEqual(len(set(selected_aliases)), 2)
            self.assertIn("playlist-next", [cmd["command"] for cmd in mpv.commands])
            self.assertEqual(simulator.clock.monotonic(), 8.0)

    def test_api_empty_playlist_sets_waiting_for_media(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir)
            cfg = sim_cfg(cache_dir)
            state = kiosk.PlaylistState()
            status = kiosk.StatusState()
            stop_event = threading.Event()
            cache_index = kiosk.CacheIndex(cfg)

            def stop_after_sleep(_seconds: float) -> None:
                stop_event.set()

            with patch.object(kiosk.time, "sleep", side_effect=stop_after_sleep):
                kiosk.playback_loop(cfg, threading.Lock(), state, status, FakeMPV(), cache_index, stop_event)

            snapshot = status.snapshot()
            self.assertEqual(snapshot["playback_state"], "waiting_for_media")
            self.assertEqual(snapshot["content_state"], "waiting_for_playlist")

    def test_api_timeout_does_not_crash_and_sets_safe_status(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir)
            cfg = sim_cfg(cache_dir)
            fake_api = FakeAPI(post_error=TimeoutError("synthetic timeout"))
            state = kiosk.PlaylistState()
            status = kiosk.StatusState()
            stop_event = threading.Event()
            cache_index = kiosk.CacheIndex(cfg)

            def stop_after_sleep(_seconds: float) -> None:
                stop_event.set()

            with patch.object(kiosk, "requests", fake_api):
                with patch.object(kiosk.time, "sleep", side_effect=stop_after_sleep):
                    kiosk.poller(cfg, threading.Lock(), threading.Event(), state, status, cache_index, stop_event)

            snapshot = status.snapshot()
            self.assertEqual(snapshot["content_state"], "api_error_retrying")
            self.assertEqual(snapshot["consecutive_failures"], 1)
            self.assertNotIn("synthetic-key", str(snapshot))
            self.assertNotIn(str(cfg["environment_id"]), str(snapshot))

    def test_media_load_failure_advances_or_errors_cleanly(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir)
            cfg = sim_cfg(cache_dir)
            first = item_from_cached(cache_dir, "https://media.invalid/bad.mp4", 3000)
            second = item_from_cached(cache_dir, "https://media.invalid/good.png", 4000)
            mpv = FakeMPV(fail_load_paths=[first.path])
            simulator = PlayerTimingSimulator(cfg, [first, second], mpv=mpv, clock=FakeClock())

            events = simulator.run_steps(2)

            self.assertTrue(any(event.name == "playback_error" for event in events))
            selected_aliases = [event.fields["alias"] for event in events if event.name == "item_selected"]
            self.assertEqual(len(set(selected_aliases)), 2)
            self.assertTrue(mpv.restart_reasons)

    def test_mpv_loop_flags_do_not_force_playlist_loop(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            cfg = sim_cfg(Path(tmpdir))
            args = kiosk.build_mpv_args(cfg)

            self.assertIn("--loop-file=inf", args)
            self.assertIn("--image-display-duration=inf", args)
            self.assertNotIn("--loop-playlist=inf", args)

    def test_sync_resync_does_not_restart_same_item_when_drift_is_stable(self) -> None:
        now_ts = 1_770_000_000.0
        durations = [10000, 10000]
        sync_pos = kiosk.compute_cycle_position_from_utc(now_ts, durations)
        actual_cycle_pos_ms = sync_pos.cycle_pos_ms
        drift = kiosk.signed_cycle_delta_ms(sync_pos.cycle_pos_ms, actual_cycle_pos_ms, sync_pos.cycle_total_ms)
        action = kiosk.classify_drift_action(drift, drift_threshold_ms=300, hard_resync_ms=1200)

        self.assertEqual(drift, 0)
        self.assertEqual(action, "none")


if __name__ == "__main__":
    unittest.main()
