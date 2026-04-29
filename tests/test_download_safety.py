import tempfile
import unittest
from pathlib import Path
from typing import Dict, List, Optional
from unittest.mock import patch

import kiosk


class FakeStatvfs:
    def __init__(self, available_bytes: int) -> None:
        self.f_bavail = available_bytes
        self.f_frsize = 1


class FakeDownloadResponse:
    def __init__(
        self,
        headers: Optional[Dict[str, str]] = None,
        chunks: Optional[List[bytes]] = None,
        iter_error: Optional[Exception] = None,
    ) -> None:
        self.headers = headers or {}
        self._chunks = chunks or []
        self._iter_error = iter_error

    def raise_for_status(self) -> None:
        return None

    def iter_content(self, chunk_size: int):
        for chunk in self._chunks:
            yield chunk
        if self._iter_error is not None:
            raise self._iter_error


class FakeDownloadRequests:
    def __init__(self, response: FakeDownloadResponse) -> None:
        self._response = response
        self.calls = []

    def get(self, url: str, stream: bool, timeout: int) -> FakeDownloadResponse:
        self.calls.append({"url": url, "stream": stream, "timeout": timeout})
        return self._response


def download_cfg(cache_dir: Path, min_free: int = 10, max_download: int = 1000) -> Dict[str, object]:
    return {
        "cache_dir": str(cache_dir),
        "request_timeout_sec": 1,
        "min_free_space_bytes": min_free,
        "max_download_bytes": max_download,
    }


def raw_item(url: str) -> Dict[str, object]:
    return {"url": url, "duration_ms": 1000, "campaign_id": "", "campaign_name": ""}


class DownloadSafetyTests(unittest.TestCase):
    def test_content_length_larger_than_free_space_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir)
            url = "https://media.example.invalid/video.mp4"
            dest = Path(kiosk.cache_path(str(cache_dir), url))
            fake_requests = FakeDownloadRequests(FakeDownloadResponse(headers={"Content-Length": "100"}))

            with patch.object(kiosk, "requests", fake_requests):
                with patch.object(kiosk.os, "statvfs", return_value=FakeStatvfs(available_bytes=50)):
                    items = kiosk.download_media(download_cfg(cache_dir, min_free=10), [raw_item(url)], None)

            self.assertEqual(items, [])
            self.assertFalse(dest.exists())
            self.assertFalse(Path(f"{dest}.tmp").exists())

    def test_truncated_download_is_not_promoted_to_final_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir)
            url = "https://media.example.invalid/truncated.mp4"
            dest = Path(kiosk.cache_path(str(cache_dir), url))
            response = FakeDownloadResponse(headers={"Content-Length": "10"}, chunks=[b"abc"])
            fake_requests = FakeDownloadRequests(response)

            with patch.object(kiosk, "requests", fake_requests):
                with patch.object(kiosk.os, "statvfs", return_value=FakeStatvfs(available_bytes=1000)):
                    items = kiosk.download_media(download_cfg(cache_dir), [raw_item(url)], None)

            self.assertEqual(items, [])
            self.assertFalse(dest.exists())
            self.assertFalse(Path(f"{dest}.tmp").exists())

    def test_tmp_file_is_removed_when_stream_errors(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir)
            url = "https://media.example.invalid/error.mp4"
            dest = Path(kiosk.cache_path(str(cache_dir), url))
            response = FakeDownloadResponse(chunks=[b"partial"], iter_error=RuntimeError("stream stopped"))
            fake_requests = FakeDownloadRequests(response)

            with patch.object(kiosk, "requests", fake_requests):
                items = kiosk.download_media(download_cfg(cache_dir), [raw_item(url)], None)

            self.assertEqual(items, [])
            self.assertFalse(dest.exists())
            self.assertFalse(Path(f"{dest}.tmp").exists())

    def test_unknown_content_length_aborts_when_max_download_is_exceeded(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir)
            url = "https://media.example.invalid/unknown-size.mp4"
            dest = Path(kiosk.cache_path(str(cache_dir), url))
            response = FakeDownloadResponse(chunks=[b"abc", b"def"])
            fake_requests = FakeDownloadRequests(response)

            with patch.object(kiosk, "requests", fake_requests):
                items = kiosk.download_media(download_cfg(cache_dir, max_download=5), [raw_item(url)], None)

            self.assertEqual(items, [])
            self.assertFalse(dest.exists())
            self.assertFalse(Path(f"{dest}.tmp").exists())

    def test_existing_final_file_is_not_replaced(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir)
            url = "https://media.example.invalid/cached.mp4"
            dest = Path(kiosk.cache_path(str(cache_dir), url))
            dest.write_bytes(b"good")
            fake_requests = FakeDownloadRequests(FakeDownloadResponse(chunks=[b"bad"]))

            with patch.object(kiosk, "requests", fake_requests):
                items = kiosk.download_media(download_cfg(cache_dir), [raw_item(url)], None)

            self.assertEqual(dest.read_bytes(), b"good")
            self.assertEqual(len(items), 1)
            self.assertEqual(fake_requests.calls, [])


if __name__ == "__main__":
    unittest.main()
