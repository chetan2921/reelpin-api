from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from app.services.health_checks import (
    _check_push_notifications,
    evaluate_worker_fleet_health,
    evaluate_worker_health,
)


class HealthCheckTests(unittest.TestCase):
    def test_push_notification_health_is_healthy_when_firebase_is_configured(self):
        get_firebase_app = Mock()
        with patch.dict(
            "sys.modules",
            {
                "app.services.notifications": SimpleNamespace(
                    _get_firebase_app=get_firebase_app
                )
            },
        ):
            result = _check_push_notifications("2026-04-17T00:00:00+00:00")

        self.assertTrue(result.healthy)
        self.assertEqual(result.status, "ok")
        self.assertIn("configured", result.message or "")
        get_firebase_app.assert_called_once()

    def test_push_notification_health_is_degraded_when_firebase_is_missing(self):
        get_firebase_app = Mock(
            side_effect=RuntimeError("missing firebase credentials")
        )
        with patch.dict(
            "sys.modules",
            {
                "app.services.notifications": SimpleNamespace(
                    _get_firebase_app=get_firebase_app
                )
            },
        ):
            result = _check_push_notifications("2026-04-17T00:00:00+00:00")

        self.assertFalse(result.healthy)
        self.assertEqual(result.status, "degraded")
        self.assertIn("missing firebase credentials", result.message or "")
        get_firebase_app.assert_called_once()

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

    def test_worker_fleet_health_counts_healthy_stale_and_error_replicas(self):
        now = datetime(2026, 4, 13, 12, 0, tzinfo=timezone.utc)
        result = evaluate_worker_fleet_health(
            records=[
                {
                    "service_name": "worker:replica-a",
                    "status": "ok",
                    "last_heartbeat_at": now.isoformat(),
                    "details": {
                        "worker_id": "replica-a:111",
                        "worker_instance_id": "replica-a",
                        "active_job_count": 2,
                        "max_concurrency": 4,
                        "platform_limits": {"instagram": 2},
                        "state": "processing",
                    },
                },
                {
                    "service_name": "worker:replica-b",
                    "status": "error",
                    "last_heartbeat_at": now.isoformat(),
                    "details": {
                        "worker_id": "replica-b:222",
                        "worker_instance_id": "replica-b",
                        "active_job_count": 1,
                        "max_concurrency": 4,
                        "platform_limits": {"instagram": 2},
                        "state": "error",
                    },
                },
                {
                    "service_name": "worker:replica-c",
                    "status": "ok",
                    "last_heartbeat_at": (now - timedelta(minutes=5)).isoformat(),
                    "details": {
                        "worker_id": "replica-c:333",
                        "worker_instance_id": "replica-c",
                        "active_job_count": 0,
                        "max_concurrency": 4,
                        "platform_limits": {"instagram": 2},
                        "state": "idle",
                    },
                },
            ],
            checked_at=now.isoformat(),
            stale_after_seconds=90,
            now=now,
        )

        self.assertTrue(result.healthy)
        self.assertEqual(result.status, "ok")
        self.assertEqual(result.details["worker_replica_count"], 3)
        self.assertEqual(result.details["healthy_worker_replica_count"], 1)
        self.assertEqual(result.details["stale_worker_replica_count"], 1)
        self.assertEqual(result.details["error_worker_replica_count"], 1)
        self.assertEqual(result.details["fleet_active_job_count"], 2)
        self.assertEqual(result.details["fleet_max_concurrency"], 4)


if __name__ == "__main__":
    unittest.main()
