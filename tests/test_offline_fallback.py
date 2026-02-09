import tempfile
import unittest
from pathlib import Path

from kiosk import (
    MediaItem,
    load_playlist_state,
    media_items_from_cache,
    media_items_from_saved,
    save_playlist_state,
)


class OfflineFallbackTests(unittest.TestCase):
    def test_media_items_from_saved_uses_path_without_url(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir)
            media_path = cache_dir / "video.mp4"
            media_path.write_bytes(b"test")
            cfg = {"cache_dir": str(cache_dir), "default_duration_ms": 10000}

            items, payload = media_items_from_saved(
                cfg,
                [
                    {
                        "path": str(media_path),
                        "duration_ms": 5000,
                        "campaign_name": "Offline",
                    }
                ],
            )

            self.assertEqual(len(items), 1)
            self.assertEqual(items[0].path, str(media_path))
            self.assertEqual(items[0].duration_ms, 5000)
            self.assertTrue(items[0].url.startswith("cache://"))
            self.assertEqual(payload[0]["path"], str(media_path))

    def test_media_items_from_cache_without_index(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir)
            first = cache_dir / "a.mp4"
            second = cache_dir / "b.png"
            invalid_text = cache_dir / "note.txt"
            zero_size = cache_dir / "empty.mp4"
            temp_file = cache_dir / "partial.tmp"
            first.write_bytes(b"1")
            second.write_bytes(b"2")
            invalid_text.write_text("skip")
            zero_size.write_bytes(b"")
            temp_file.write_bytes(b"ignore")
            cfg = {"cache_dir": str(cache_dir), "default_duration_ms": 9000}

            items, payload = media_items_from_cache(cfg, None)
            item_paths = {item.path for item in items}
            payload_paths = {entry["path"] for entry in payload}

            self.assertEqual(item_paths, {str(first), str(second)})
            self.assertEqual(payload_paths, {str(first), str(second)})
            self.assertTrue(all(item.duration_ms == 9000 for item in items))

    def test_save_playlist_state_includes_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir)
            media_path = cache_dir / "saved.mp4"
            media_path.write_bytes(b"ok")
            cfg = {"cache_dir": str(cache_dir), "state_dir": "", "default_duration_ms": 10000}
            items = [
                MediaItem(
                    url="cache://saved.mp4",
                    duration_ms=1234,
                    path=str(media_path),
                    campaign_id="",
                    campaign_name="",
                )
            ]

            save_playlist_state(cfg, items, fingerprint="abc")
            playlist, _fingerprint, _saved_at = load_playlist_state(cfg)

            self.assertEqual(len(playlist), 1)
            self.assertEqual(playlist[0].get("path"), str(media_path))


if __name__ == "__main__":
    unittest.main()
