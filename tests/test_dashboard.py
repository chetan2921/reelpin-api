import unittest
from unittest.mock import patch

from app.services.dashboard import build_dashboard_overview


class _HealthStub:
    def model_dump(self):
        return {"status": "ok", "ready": True}


class DashboardTests(unittest.TestCase):
    @patch("app.services.dashboard._instagram_cookie_health")
    @patch("app.services.dashboard._build_readiness_health_response")
    @patch("app.services.dashboard.build_processing_metrics")
    @patch("app.services.dashboard._list_processing_jobs_for_metrics")
    @patch("app.services.dashboard._get_processing_job_counts_by_status")
    @patch("app.services.dashboard._unique_count")
    @patch("app.services.dashboard._safe_count_since")
    @patch("app.services.dashboard._safe_count")
    def test_build_dashboard_overview(
        self,
        safe_count,
        safe_count_since,
        unique_count,
        job_counts,
        list_jobs,
        build_metrics,
        build_health,
        cookie_health,
    ):
        safe_count.side_effect = lambda table: {
            "profiles": 12,
            "device_push_tokens": 20,
            "reels": 44,
            "processing_jobs": 80,
        }[table]
        safe_count_since.return_value = 5
        unique_count.return_value = 2
        job_counts.return_value = {
            "queued": 2,
            "processing": 1,
            "completed": 70,
            "failed": 4,
            "dead_lettered": 3,
        }
        list_jobs.return_value = []
        build_metrics.return_value = {"sample_size": 0}
        build_health.return_value = _HealthStub()
        cookie_health.return_value = [{"slot": "active", "healthy": True, "configured": True}]

        overview = build_dashboard_overview()

        self.assertEqual(overview.registered_profile_count, 12)
        self.assertEqual(overview.registered_device_count, 20)
        self.assertEqual(overview.registered_device_user_count, 2)
        self.assertEqual(overview.saved_reel_count, 44)
        self.assertEqual(overview.processing_job_count, 80)
        self.assertEqual(overview.active_job_count, 3)
        self.assertEqual(overview.completed_job_count, 70)
        self.assertEqual(overview.failed_job_count, 4)
        self.assertEqual(overview.dead_lettered_job_count, 3)
        self.assertEqual(overview.health["status"], "ok")


if __name__ == "__main__":
    unittest.main()
