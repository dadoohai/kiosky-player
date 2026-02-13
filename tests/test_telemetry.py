import unittest
from typing import Any, Dict, Optional
from unittest.mock import patch

import kiosk


class FakeResponse:
    def __init__(self, error: Optional[Exception] = None) -> None:
        self._error = error

    def raise_for_status(self) -> None:
        if self._error is not None:
            raise self._error


class FakeRequests:
    def __init__(self, response: Optional[FakeResponse] = None, post_error: Optional[Exception] = None) -> None:
        self._response = response or FakeResponse()
        self._post_error = post_error
        self.calls = []

    def post(self, url: str, json: Dict[str, Any], headers: Dict[str, str], timeout: int) -> FakeResponse:
        self.calls.append(
            {
                "url": url,
                "json": json,
                "headers": headers,
                "timeout": timeout,
            }
        )
        if self._post_error is not None:
            raise self._post_error
        return self._response


def sample_cfg() -> Dict[str, Any]:
    return {
        "telemetry_enabled": True,
        "telemetry_url": "http://127.0.0.1:9999/telemetry",
        "telemetry_timeout_sec": 7,
        "environment_id": "env-1",
        "station_id": "station-1",
        "rotation_deg": 90,
    }


def sample_status() -> Dict[str, Any]:
    return {
        "playlist_size": 2,
        "current_item": {"campaign_name": "Campaign A"},
        "next_item": {"campaign_name": "Campaign B", "path": "/tmp/next.mp4"},
        "consecutive_failures": 1,
    }


class TelemetryTests(unittest.TestCase):
    def test_send_telemetry_success_returns_true_and_sends_payload(self) -> None:
        fake_requests = FakeRequests(response=FakeResponse())
        with patch.object(kiosk, "requests", fake_requests):
            ok = kiosk.send_telemetry(sample_cfg(), sample_status(), heartbeat_type="healthcheck")

        self.assertTrue(ok)
        self.assertEqual(len(fake_requests.calls), 1)
        sent = fake_requests.calls[0]
        self.assertEqual(sent["timeout"], 7)
        self.assertEqual(sent["headers"]["x-interact-telemetry-token"], "540fca561dcb494287e8f820381c0e0f")
        self.assertEqual(sent["json"]["environmentId"], "env-1")
        self.assertEqual(sent["json"]["stationId"], "station-1")
        self.assertEqual(sent["json"]["heartbeatType"], "healthcheck")
        self.assertEqual(sent["json"]["status"], "ok")
        self.assertEqual(sent["json"]["activeCampaignName"], "Campaign A")
        self.assertEqual(sent["json"]["nextCampaignName"], "Campaign B")
        self.assertEqual(sent["json"]["consecutiveFailures"], 1)
        self.assertEqual(sent["json"]["metrics"]["preloadSize"], 1)

    def test_send_telemetry_http_error_returns_false(self) -> None:
        fake_requests = FakeRequests(response=FakeResponse(error=RuntimeError("500 Server Error")))
        with patch.object(kiosk, "requests", fake_requests):
            ok = kiosk.send_telemetry(sample_cfg(), sample_status(), heartbeat_type="healthcheck")

        self.assertFalse(ok)
        self.assertEqual(len(fake_requests.calls), 1)

    def test_send_telemetry_request_exception_returns_false(self) -> None:
        fake_requests = FakeRequests(post_error=RuntimeError("connection dropped"))
        with patch.object(kiosk, "requests", fake_requests):
            ok = kiosk.send_telemetry(sample_cfg(), sample_status(), heartbeat_type="healthcheck")

        self.assertFalse(ok)
        self.assertEqual(len(fake_requests.calls), 1)

    def test_send_telemetry_disabled_does_not_send(self) -> None:
        fake_requests = FakeRequests()
        cfg = sample_cfg()
        cfg["telemetry_enabled"] = False
        with patch.object(kiosk, "requests", fake_requests):
            ok = kiosk.send_telemetry(cfg, sample_status(), heartbeat_type="healthcheck")

        self.assertFalse(ok)
        self.assertEqual(fake_requests.calls, [])


if __name__ == "__main__":
    unittest.main()
