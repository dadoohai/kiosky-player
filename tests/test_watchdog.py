import threading
import unittest

import kiosk


class FakeMPV:
    def __init__(self, ping_results, stop_event):
        self.ping_results = list(ping_results)
        self.stop_event = stop_event
        self.restart_reasons = []
        self.ping_calls = 0

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
        return 7

    def pid(self) -> int:
        return 1234

    def current_log_file(self) -> str:
        return "/tmp/kiosky/mpv-g007.log"


class WatchdogTests(unittest.TestCase):
    def run_watchdog_once(self, ping_results, cfg=None):
        stop_event = threading.Event()
        mpv = FakeMPV(ping_results, stop_event)
        watchdog_cfg = {
            "watchdog_interval_sec": 0,
            "mpv_ipc_timeout_sec": 2.0,
        }
        if cfg:
            watchdog_cfg.update(cfg)
        status = kiosk.StatusState()

        kiosk.watchdog(watchdog_cfg, threading.Lock(), mpv, status, stop_event)

        return mpv, status

    def test_default_threshold_is_one_and_restarts_on_first_failure(self) -> None:
        self.assertEqual(kiosk.DEFAULT_CONFIG["mpv_watchdog_ping_failures_before_restart"], 1)

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
