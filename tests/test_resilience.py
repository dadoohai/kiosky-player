import calendar
import json
import tempfile
import time
import unittest
from pathlib import Path

from kiosk import load_config, offline_playlist_allowed


def iso_utc(year: int, month: int, day: int, hour: int, minute: int, second: int) -> str:
    ts = calendar.timegm((year, month, day, hour, minute, second, 0, 0, 0))
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts))


class ResilienceTests(unittest.TestCase):
    def test_load_config_resolves_relative_paths_from_config_dir(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            cfg_path = root / "config.json"
            cfg_path.write_text(
                json.dumps(
                    {
                        "api_key": "k",
                        "environment_id": "e",
                        "cache_dir": "./cache",
                        "state_dir": "./state",
                        "log_file": "./logs/player.log",
                        "status_file": "./logs/status.json",
                        "ipc_path": "./runtime/mpv.sock",
                    }
                ),
                encoding="utf-8",
            )

            cfg = load_config(str(cfg_path))

            self.assertEqual(cfg["cache_dir"], str((root / "cache").absolute()))
            self.assertEqual(cfg["state_dir"], str((root / "state").absolute()))
            self.assertEqual(cfg["log_file"], str((root / "logs" / "player.log").absolute()))
            self.assertEqual(cfg["status_file"], str((root / "logs" / "status.json").absolute()))
            self.assertEqual(cfg["ipc_path"], str((root / "runtime" / "mpv.sock").absolute()))

    def test_offline_age_can_be_ignored_without_network(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            cfg = {
                "cache_dir": str(Path(tmpdir) / "cache"),
                "offline_max_age_hours": 1,
                "offline_ignore_max_age_when_no_network": True,
            }
            old_saved_at = iso_utc(2026, 1, 1, 0, 0, 0)

            self.assertTrue(offline_playlist_allowed(cfg, old_saved_at, network_available=False))
            self.assertFalse(offline_playlist_allowed(cfg, old_saved_at, network_available=True))


if __name__ == "__main__":
    unittest.main()
