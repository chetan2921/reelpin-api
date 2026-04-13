from datetime import datetime, timedelta, timezone
import unittest

from app.services.health_checks import evaluate_worker_health


class HealthCheckTests(unittest.TestCase):
    def test_worker_health_is_healthy_when_recent(self):
        now = datetime(2026, 4, 13, 12, 0, tzinfo=timezone.utc)
        result = evaluate_worker_health(
            record={
                "status": "ok",
                "last_heartbeat_at": now.isoformat(),
                "details": {"state": "idle"},
            },
            checked_at=now.isoformat(),
            stale_after_seconds=90,
            now=now,
        )

        self.assertTrue(result.healthy)
        self.assertEqual(result.status, "ok")

    def test_worker_health_is_degraded_when_stale(self):
        now = datetime(2026, 4, 13, 12, 0, tzinfo=timezone.utc)
        result = evaluate_worker_health(
            record={
                "status": "ok",
                "last_heartbeat_at": (now - timedelta(minutes=5)).isoformat(),
                "details": {},
            },
            checked_at=now.isoformat(),
            stale_after_seconds=90,
            now=now,
        )

        self.assertFalse(result.healthy)
        self.assertEqual(result.status, "degraded")
        self.assertIn("stale", result.message or "")

    def test_worker_health_is_degraded_when_missing(self):
        now = datetime(2026, 4, 13, 12, 0, tzinfo=timezone.utc)
        result = evaluate_worker_health(
            record=None,
            checked_at=now.isoformat(),
            stale_after_seconds=90,
            now=now,
        )

        self.assertFalse(result.healthy)
        self.assertEqual(result.status, "degraded")


if __name__ == "__main__":
    unittest.main()
