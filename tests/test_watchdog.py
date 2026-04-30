import unittest
from urllib import error

import watchdog


class WatchdogSettingsTests(unittest.TestCase):
    def test_load_settings_uses_defaults(self):
        settings = watchdog.load_settings(
            {
                "WATCHDOG_HEALTH_URL": "http://127.0.0.1:8000/healthz",
                "WATCHDOG_RESTART_COMMAND": "systemctl restart open-workshop-storage",
            }
        )

        self.assertEqual(settings.health_url, "http://127.0.0.1:8000/healthz")
        self.assertEqual(settings.restart_command, "systemctl restart open-workshop-storage")
        self.assertEqual(settings.check_interval_seconds, 20.0)
        self.assertEqual(settings.restart_after_seconds, 300.0)
        self.assertEqual(settings.request_timeout_seconds, 5.0)

    def test_load_settings_rejects_missing_required_values(self):
        with self.assertRaises(ValueError):
            watchdog.load_settings({"WATCHDOG_RESTART_COMMAND": "restart svc"})

        with self.assertRaises(ValueError):
            watchdog.load_settings({"WATCHDOG_HEALTH_URL": "http://localhost/healthz"})

    def test_load_settings_rejects_non_positive_numbers(self):
        with self.assertRaises(ValueError):
            watchdog.load_settings(
                {
                    "WATCHDOG_HEALTH_URL": "http://localhost/healthz",
                    "WATCHDOG_RESTART_COMMAND": "restart svc",
                    "WATCHDOG_CHECK_INTERVAL_SECONDS": "0",
                }
            )


class FailureTrackerTests(unittest.TestCase):
    def test_failure_tracker_triggers_restart_after_threshold(self):
        tracker = watchdog.FailureTracker()

        self.assertEqual(tracker.observe(False, 0.0, 300.0), "first_failure")
        self.assertEqual(tracker.observe(False, 120.0, 300.0), "waiting")
        self.assertEqual(tracker.observe(False, 300.0, 300.0), "restart")
        self.assertIsNone(tracker.since)

    def test_failure_tracker_resets_after_recovery(self):
        tracker = watchdog.FailureTracker()

        self.assertEqual(tracker.observe(False, 0.0, 300.0), "first_failure")
        self.assertEqual(tracker.observe(True, 5.0, 300.0), "recovered")
        self.assertIsNone(tracker.since)


class ProbeHealthTests(unittest.TestCase):
    def test_probe_health_marks_http_errors_unhealthy(self):
        def failing_urlopen(*args, **kwargs):
            raise error.HTTPError(
                url="http://127.0.0.1:8000/healthz",
                code=503,
                msg="Service Unavailable",
                hdrs=None,
                fp=None,
            )

        original_urlopen = watchdog.request.urlopen
        watchdog.request.urlopen = failing_urlopen
        try:
            result = watchdog.probe_health("http://127.0.0.1:8000/healthz", 1.0)
        finally:
            watchdog.request.urlopen = original_urlopen

        self.assertFalse(result.healthy)
        self.assertEqual(result.detail, "http 503")


if __name__ == "__main__":
    unittest.main()
