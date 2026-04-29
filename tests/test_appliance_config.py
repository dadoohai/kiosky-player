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
        self.assertEqual(cfg["sync_ntp_command"], "")
        self.assertEqual(cfg["hwdec"], "auto-safe")

    def test_appliance_paths_are_under_data_or_tmp(self) -> None:
        cfg = json.loads((REPO_ROOT / "config.appliance.example.json").read_text(encoding="utf-8"))

        self.assertTrue(cfg["cache_dir"].startswith("/data/"))
        self.assertTrue(cfg["state_dir"].startswith("/data/"))
        self.assertEqual(cfg["log_file"], "")
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


if __name__ == "__main__":
    unittest.main()
