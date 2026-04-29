import json
import os
import tempfile
import unittest
from pathlib import Path

import kiosk


REPO_ROOT = Path(__file__).resolve().parents[1]


class ApplianceConfigTests(unittest.TestCase):
    def test_appliance_example_is_parseable(self) -> None:
        cfg = json.loads((REPO_ROOT / "config.appliance.example.json").read_text(encoding="utf-8"))

        self.assertEqual(cfg["cache_dir"], "/data/media/kiosky-player")
        self.assertEqual(cfg["state_dir"], "/data/state/kiosky-player")
        self.assertEqual(cfg["status_file"], "/tmp/kiosky-status.json")
        self.assertEqual(cfg["ipc_path"], "/tmp/kiosky/mpv.sock")
        self.assertFalse(cfg["hotkeys_enabled"])
        self.assertFalse(cfg["config_ui_enabled"])
        self.assertFalse(cfg["telemetry_enabled"])
        self.assertFalse(cfg["sync_enabled"])
        self.assertTrue(cfg["strict_paths_enabled"])
        self.assertEqual(cfg["sync_ntp_command"], "")
        self.assertEqual(cfg["hwdec"], "auto-safe")
        self.assertEqual(cfg["mpv_log_file"], "/tmp/kiosky/mpv.log")
        self.assertEqual(cfg["mpv_msg_level"], "all=v")
        self.assertEqual(cfg["mpv_ipc_timeout_sec"], 2.0)
        self.assertEqual(cfg["mpv_startup_timeout_sec"], 10.0)
        self.assertFalse(cfg["mpv_debug_events"])

    def test_appliance_paths_are_under_data_or_tmp(self) -> None:
        cfg = json.loads((REPO_ROOT / "config.appliance.example.json").read_text(encoding="utf-8"))

        self.assertTrue(cfg["cache_dir"].startswith("/data/"))
        self.assertTrue(cfg["state_dir"].startswith("/data/"))
        self.assertEqual(cfg["log_file"], "")
        self.assertTrue(cfg["mpv_log_file"].startswith("/tmp/"))
        self.assertTrue(cfg["status_file"].startswith("/tmp/"))
        self.assertTrue(cfg["ipc_path"].startswith("/tmp/"))
        self.assertTrue(cfg["runtime_dir"].startswith("/tmp/"))

    def test_hotkeys_disabled_does_not_create_app_runtime_dir(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            old_cwd = os.getcwd()
            try:
                os.chdir(tmpdir)
                conf_path = kiosk.ensure_hotkey_conf({"hotkeys_enabled": False})
            finally:
                os.chdir(old_cwd)

            self.assertIsNone(conf_path)
            self.assertFalse((Path(tmpdir) / "runtime").exists())

    def test_appliance_example_passes_strict_path_validation(self) -> None:
        cfg = kiosk.load_config(str(REPO_ROOT / "config.appliance.example.json"))

        self.assertTrue(cfg["strict_paths_enabled"])
        self.assertEqual(cfg["cache_dir"], "/data/media/kiosky-player")
        self.assertEqual(cfg["state_dir"], "/data/state/kiosky-player")
        self.assertEqual(cfg["mpv_log_file"], "/tmp/kiosky/mpv.log")

    def test_strict_paths_rejects_mutable_paths_outside_data_and_tmp(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            cfg_path = Path(tmpdir) / "config.json"
            cfg_path.write_text(
                json.dumps(
                    {
                        "strict_paths_enabled": True,
                        "cache_dir": "/opt/totem/cache",
                        "state_dir": "/data/state/kiosky-player",
                        "status_file": "/tmp/kiosky-status.json",
                        "ipc_path": "/tmp/kiosky/mpv.sock",
                        "runtime_dir": "/tmp/kiosky",
                        "log_file": "",
                    }
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "cache_dir must be under /data"):
                kiosk.load_config(str(cfg_path))

    def test_strict_paths_rejects_persistent_log_outside_data_logs(self) -> None:
        cfg = {
            "strict_paths_enabled": True,
            "cache_dir": "/data/media/kiosky-player",
            "state_dir": "/data/state/kiosky-player",
            "status_file": "/tmp/kiosky-status.json",
            "ipc_path": "/tmp/kiosky/mpv.sock",
            "runtime_dir": "/tmp/kiosky",
            "log_file": "/tmp/kiosky.log",
        }

        with self.assertRaisesRegex(ValueError, "log_file must be empty or under /data/logs"):
            kiosk.validate_strict_paths(cfg)

    def test_strict_paths_allow_mpv_log_file_under_tmp(self) -> None:
        cfg = {
            "strict_paths_enabled": True,
            "cache_dir": "/data/media/kiosky-player",
            "state_dir": "/data/state/kiosky-player",
            "status_file": "/tmp/kiosky-status.json",
            "ipc_path": "/tmp/kiosky/mpv.sock",
            "runtime_dir": "/tmp/kiosky",
            "log_file": "",
            "mpv_log_file": "/tmp/kiosky/mpv.log",
        }

        kiosk.validate_strict_paths(cfg)

    def test_strict_paths_rejects_mpv_log_file_outside_tmp_or_data_logs(self) -> None:
        cfg = {
            "strict_paths_enabled": True,
            "cache_dir": "/data/media/kiosky-player",
            "state_dir": "/data/state/kiosky-player",
            "status_file": "/tmp/kiosky-status.json",
            "ipc_path": "/tmp/kiosky/mpv.sock",
            "runtime_dir": "/tmp/kiosky",
            "log_file": "",
            "mpv_log_file": "/opt/totem/kiosky-player/mpv.log",
        }

        with self.assertRaisesRegex(ValueError, "mpv_log_file must be empty or under /tmp or /data/logs"):
            kiosk.validate_strict_paths(cfg)

    def test_build_mpv_args_includes_log_file_when_configured(self) -> None:
        cfg = {
            "mpv_path": "mpv",
            "ipc_path": "/tmp/kiosky/mpv.sock",
            "mpv_log_file": "/tmp/kiosky/mpv.log",
            "mpv_msg_level": "all=v",
            "hotkeys_enabled": False,
            "lock_input": True,
            "hwdec": "",
            "rotation_deg": 0,
        }

        args = kiosk.build_mpv_args(cfg)

        self.assertIn("--log-file=/tmp/kiosky/mpv.log", args)
        self.assertIn("--msg-level=all=v", args)

    def test_ensure_runtime_paths_creates_runtime_and_ipc_parent(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            cfg = {
                "runtime_dir": str(root / "runtime"),
                "ipc_path": str(root / "kiosky" / "mpv.sock"),
                "mpv_log_file": str(root / "kiosky" / "mpv.log"),
            }

            kiosk.ensure_runtime_paths(cfg)

            self.assertTrue((root / "runtime").is_dir())
            self.assertTrue((root / "kiosky").is_dir())
            self.assertFalse((root / "kiosky" / "mpv.sock").exists())

    def test_media_log_context_uses_alias_without_url(self) -> None:
        class FakeMPV:
            def generation(self) -> int:
                return 3

            def pid(self) -> int:
                return 1234

        url = "https://private.example.invalid/path/to/media.mp4?token=secret"
        item = kiosk.MediaItem(
            url=url,
            duration_ms=7000,
            path="/tmp/kiosky/media-cache/abc123.mp4",
            campaign_id="private-campaign",
            campaign_name="private-name",
        )

        context = kiosk.media_load_log_context(item, 2, item.duration_ms, FakeMPV())

        self.assertIn("alias=media-", context)
        self.assertIn("path=/tmp/kiosky/media-cache/abc123.mp4", context)
        self.assertIn("index=2", context)
        self.assertIn("duration_ms=7000", context)
        self.assertNotIn(url, context)
        self.assertNotIn("token=secret", context)


if __name__ == "__main__":
    unittest.main()
