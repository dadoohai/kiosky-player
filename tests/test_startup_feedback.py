import json
import tempfile
import unittest
from pathlib import Path

import kiosk


class StartupFeedbackTests(unittest.TestCase):
    def test_status_state_exposes_safe_startup_fields(self) -> None:
        status = kiosk.StatusState()
        snapshot = status.snapshot()

        self.assertEqual(snapshot["status_schema_version"], "kiosky-player-status.v2")
        self.assertEqual(snapshot["player_state"], "player_starting")
        self.assertEqual(snapshot["startup_phase"], "player_starting")
        self.assertFalse(snapshot["startup_feedback_visible"])
        self.assertFalse(snapshot["first_frame_ready"])

    def test_waiting_status_does_not_override_playing(self) -> None:
        status = kiosk.StatusState()
        status.update(playback_state="playing", first_frame_ready=True, startup_phase="playing")

        kiosk.update_waiting_status(
            status,
            startup_phase="waiting_for_api",
            content_state="waiting_for_api",
        )

        snapshot = status.snapshot()
        self.assertEqual(snapshot["playback_state"], "playing")
        self.assertEqual(snapshot["startup_phase"], "playing")
        self.assertTrue(snapshot["first_frame_ready"])

    def test_startup_feedback_svg_contains_only_public_text(self) -> None:
        content = kiosk.build_startup_feedback_svg("waiting_for_api")

        self.assertIn("Carregando conteudo", content)
        for forbidden in (
            "api_key",
            "environment_id",
            "ssid",
            "password",
            "/data/config",
        ):
            self.assertNotIn(forbidden, content.lower())

    def test_write_startup_feedback_svg_uses_runtime_dir(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            cfg = {"runtime_dir": tmpdir}
            path = Path(kiosk.write_startup_feedback_svg(cfg, "waiting_for_media_cache"))

            self.assertEqual(path.parent, Path(tmpdir))
            self.assertTrue(path.exists())
            self.assertIn("Carregando conteudo", path.read_text(encoding="utf-8"))

    def test_status_json_with_startup_fields_stays_serializable(self) -> None:
        status = kiosk.StatusState()
        kiosk.update_waiting_status(
            status,
            startup_phase="waiting_for_media_cache",
            content_state="downloading_or_validating_media",
        )

        payload = json.dumps(status.snapshot(), sort_keys=True)
        self.assertIn("waiting_for_media_cache", payload)
        for forbidden in ("api_key", "environment_id", "ssid", "password"):
            self.assertNotIn(forbidden, payload.lower())


if __name__ == "__main__":
    unittest.main()
