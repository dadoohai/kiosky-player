import calendar
import unittest

from kiosk import (
    classify_drift_action,
    compute_cycle_position_from_utc,
    daily_anchor_utc_ts,
    is_prep_window_utc,
    next_hour_checkpoint_utc_ts,
    signed_cycle_delta_ms,
)


def utc_ts(year: int, month: int, day: int, hour: int, minute: int, second: int) -> float:
    return float(calendar.timegm((year, month, day, hour, minute, second, 0, 0, 0)))


class SyncRulesTests(unittest.TestCase):
    def test_daily_anchor_before_0005_uses_previous_day(self) -> None:
        now_ts = utc_ts(2026, 2, 8, 0, 2, 0)
        self.assertEqual(daily_anchor_utc_ts(now_ts), utc_ts(2026, 2, 7, 0, 5, 0))

    def test_daily_anchor_after_0005_uses_current_day(self) -> None:
        now_ts = utc_ts(2026, 2, 8, 14, 10, 0)
        self.assertEqual(daily_anchor_utc_ts(now_ts), utc_ts(2026, 2, 8, 0, 5, 0))

    def test_prep_window_crosses_midnight(self) -> None:
        self.assertTrue(is_prep_window_utc(utc_ts(2026, 2, 7, 23, 58, 0)))
        self.assertTrue(is_prep_window_utc(utc_ts(2026, 2, 8, 0, 4, 59)))
        self.assertFalse(is_prep_window_utc(utc_ts(2026, 2, 8, 0, 5, 0)))

    def test_cycle_position_resolves_index_and_offset(self) -> None:
        anchor = utc_ts(2026, 2, 8, 0, 5, 0)
        now_ts = anchor + 25.0
        pos = compute_cycle_position_from_utc(now_ts, [10_000, 20_000, 30_000])
        self.assertEqual(pos.index, 1)
        self.assertEqual(pos.offset_ms, 15_000)
        self.assertEqual(pos.cycle_total_ms, 60_000)

    def test_signed_cycle_delta_wraparound(self) -> None:
        delta = signed_cycle_delta_ms(target_ms=100, current_ms=59_900, cycle_total_ms=60_000)
        self.assertEqual(delta, 200)

    def test_classify_drift_action(self) -> None:
        self.assertEqual(classify_drift_action(100, 300, 1200), "none")
        self.assertEqual(classify_drift_action(350, 300, 1200), "soft_resync")
        self.assertEqual(classify_drift_action(-1200, 300, 1200), "hard_resync")

    def test_next_hour_checkpoint_rounds_up(self) -> None:
        now_ts = utc_ts(2026, 2, 8, 10, 15, 1)
        self.assertEqual(next_hour_checkpoint_utc_ts(now_ts, 3600), utc_ts(2026, 2, 8, 11, 0, 0))


if __name__ == "__main__":
    unittest.main()
