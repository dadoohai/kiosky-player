import threading
import unittest
from unittest import mock

import kiosk


class FakeMPV:
    def __init__(
        self,
        ping_results,
        stop_event,
        generations=None,
        last_load_at=None,
        last_start_at=None,
    ):
        self.ping_results = list(ping_results)
        self.generations = list(generations or [7])
        self.stop_event = stop_event
        self.restart_reasons = []
        self.ping_calls = 0
        self.last_load_at = last_load_at
        self.last_start_at = last_start_at

    def ensure_running(self) -> None:
        return None

    def ping(self) -> bool:
        if self.ping_calls >= len(self.ping_results):
            self.stop_event.set()
            return True
        result = self.ping_results[self.ping_calls]
        self.ping_calls += 1
        if self.ping_calls >= len(self.ping_results):
            self.stop_event.set()
        return result

    def restart(self, reason: str = "manual") -> None:
        self.restart_reasons.append(reason)

    def is_running(self) -> bool:
        return True

    def generation(self) -> int:
        generation_index = min(self.ping_calls, len(self.generations) - 1)
        return self.generations[generation_index]

    def pid(self) -> int:
        return 1234

    def current_log_file(self) -> str:
        return f"/tmp/kiosky/mpv-g{self.generation():03d}.log"

    def last_loadfile_monotonic(self):
        return self.last_load_at

    def last_start_monotonic(self):
        return self.last_start_at


class WatchdogTests(unittest.TestCase):
    def run_watchdog_once(
        self,
        ping_results,
        cfg=None,
        generations=None,
        last_load_at=None,
        last_start_at=None,
        monotonic_values=None,
    ):
        stop_event = threading.Event()
        mpv = FakeMPV(
            ping_results,
            stop_event,
            generations=generations,
            last_load_at=last_load_at,
            last_start_at=last_start_at,
        )
        watchdog_cfg = {
            "watchdog_interval_sec": 0,
            "mpv_ipc_timeout_sec": 2.0,
        }
        if cfg:
            watchdog_cfg.update(cfg)
        status = kiosk.StatusState()

        if monotonic_values is None:
            kiosk.watchdog(watchdog_cfg, threading.Lock(), mpv, status, stop_event)
        else:
            with mock.patch("kiosk.time.monotonic", side_effect=monotonic_values):
                kiosk.watchdog(watchdog_cfg, threading.Lock(), mpv, status, stop_event)

        return mpv, status

    def test_default_threshold_is_one_and_restarts_on_first_failure(self) -> None:
        self.assertEqual(kiosk.DEFAULT_CONFIG["mpv_watchdog_ping_failures_before_restart"], 1)
        self.assertEqual(kiosk.DEFAULT_CONFIG["mpv_watchdog_grace_after_load_sec"], 0)
        self.assertEqual(kiosk.DEFAULT_CONFIG["mpv_watchdog_grace_after_restart_sec"], 0)

        mpv, _status = self.run_watchdog_once([False])

        self.assertEqual(mpv.restart_reasons, ["ipc_unresponsive"])

    def test_threshold_two_does_not_restart_on_first_failure(self) -> None:
        mpv, _status = self.run_watchdog_once(
            [False],
            {"mpv_watchdog_ping_failures_before_restart": 2},
        )

        self.assertEqual(mpv.restart_reasons, [])

    def test_threshold_two_restarts_on_second_consecutive_failure(self) -> None:
        mpv, _status = self.run_watchdog_once(
            [False, False],
            {"mpv_watchdog_ping_failures_before_restart": 2},
        )

        self.assertEqual(mpv.restart_reasons, ["ipc_unresponsive"])

    def test_restart_resets_consecutive_failure_counter(self) -> None:
        mpv, _status = self.run_watchdog_once(
            [False, False, False],
            {"mpv_watchdog_ping_failures_before_restart": 2},
        )

        self.assertEqual(mpv.restart_reasons, ["ipc_unresponsive"])

    def test_successful_ping_resets_consecutive_failures(self) -> None:
        mpv, _status = self.run_watchdog_once(
            [False, True, False],
            {"mpv_watchdog_ping_failures_before_restart": 2},
        )

        self.assertEqual(mpv.restart_reasons, [])

    def test_grace_after_load_suppresses_restart_during_window(self) -> None:
        with self.assertLogs(level="WARNING") as logs:
            mpv, _status = self.run_watchdog_once(
                [False],
                {
                    "mpv_watchdog_ping_failures_before_restart": 1,
                    "mpv_watchdog_grace_after_load_sec": 10,
                },
                last_load_at=95.0,
                monotonic_values=[100.0],
            )

        self.assertEqual(mpv.restart_reasons, [])
        self.assertIn("within_grace_after_load", "\n".join(logs.output))

    def test_grace_after_restart_suppresses_restart_during_window(self) -> None:
        with self.assertLogs(level="WARNING") as logs:
            mpv, _status = self.run_watchdog_once(
                [False],
                {
                    "mpv_watchdog_ping_failures_before_restart": 1,
                    "mpv_watchdog_grace_after_restart_sec": 10,
                },
                last_start_at=195.0,
                monotonic_values=[200.0],
            )

        self.assertEqual(mpv.restart_reasons, [])
        self.assertIn("within_grace_after_restart", "\n".join(logs.output))

    def test_threshold_resumes_after_grace_window_expires(self) -> None:
        with self.assertLogs(level="INFO") as logs:
            mpv, _status = self.run_watchdog_once(
                [False, False, False],
                {
                    "mpv_watchdog_ping_failures_before_restart": 2,
                    "mpv_watchdog_grace_after_load_sec": 5,
                },
                last_load_at=100.0,
                monotonic_values=[101.0, 106.0, 107.0],
            )

        self.assertEqual(mpv.restart_reasons, ["ipc_unresponsive"])
        log_text = "\n".join(logs.output)
        self.assertIn("restart suppressed reason=within_grace_after_load", log_text)
        self.assertIn("grace window expired", log_text)

    def test_grace_suppressed_failure_does_not_count_toward_threshold(self) -> None:
        mpv, _status = self.run_watchdog_once(
            [False, False],
            {
                "mpv_watchdog_ping_failures_before_restart": 2,
                "mpv_watchdog_grace_after_load_sec": 5,
            },
            last_load_at=100.0,
            monotonic_values=[101.0, 106.0],
        )

        self.assertEqual(mpv.restart_reasons, [])

    def test_generation_change_between_failures_prevents_restart(self) -> None:
        mpv, _status = self.run_watchdog_once(
            [False, False],
            {"mpv_watchdog_ping_failures_before_restart": 2},
            generations=[7, 8],
        )

        self.assertEqual(mpv.restart_reasons, [])

    def test_generation_change_reset_is_logged(self) -> None:
        with self.assertLogs(level="INFO") as logs:
            self.run_watchdog_once(
                [False, False],
                {"mpv_watchdog_ping_failures_before_restart": 2},
                generations=[7, 8],
            )

        self.assertIn("counter reset after generation change", "\n".join(logs.output))

    def test_successful_ping_reset_is_logged(self) -> None:
        with self.assertLogs(level="INFO") as logs:
            self.run_watchdog_once(
                [False, True],
                {"mpv_watchdog_ping_failures_before_restart": 2},
            )

        self.assertIn("MPV IPC ping recovered", "\n".join(logs.output))

    def test_invalid_threshold_values_fall_back_to_one(self) -> None:
        for value in (0, -1, "invalid", None):
            with self.subTest(value=value):
                self.assertEqual(
                    kiosk.watchdog_ping_failure_threshold(
                        {"mpv_watchdog_ping_failures_before_restart": value}
                    ),
                    1,
                )


if __name__ == "__main__":
    unittest.main()
