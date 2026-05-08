import unittest

from app.services.observability import build_processing_metrics


class ObservabilityMetricsTests(unittest.TestCase):
    def test_build_processing_metrics_aggregates_rates_and_timings(self):
        metrics = build_processing_metrics(
            jobs=[
                {
                    "status": "completed",
                    "source_platform": "instagram",
                    "attempt_count": 1,
                    "created_at": "2026-04-17T10:00:00+00:00",
                    "started_at": "2026-04-17T10:00:05+00:00",
                    "completed_at": "2026-04-17T10:00:20+00:00",
                    "step_durations": {
                        "download_seconds": 2.0,
                        "transcribe_seconds": 4.0,
                        "total_seconds": 10.0,
                    },
                },
                {
                    "status": "completed",
                    "source_platform": "instagram",
                    "attempt_count": 3,
                    "created_at": "2026-04-17T10:03:00+00:00",
                    "started_at": "2026-04-17T10:03:10+00:00",
                    "completed_at": "2026-04-17T10:03:18+00:00",
                    "step_durations": {
                        "download_seconds": 0.0,
                        "transcribe_seconds": 0.0,
                        "total_seconds": 3.0,
                    },
                },
                {
                    "status": "dead_lettered",
                    "source_platform": "instagram",
                    "attempt_count": 2,
                    "created_at": "2026-04-17T10:01:00+00:00",
                    "started_at": "2026-04-17T10:01:20+00:00",
                    "completed_at": "2026-04-17T10:02:00+00:00",
                    "step_durations": {},
                },
                {
                    "status": "queued",
                    "source_platform": "instagram",
                    "attempt_count": 1,
                    "step_durations": {},
                },
            ],
            queue_depth={
                "queued": 4,
                "processing": 1,
                "dead_lettered": 2,
            },
        )

        self.assertEqual(metrics["sample_size"], 3)
        self.assertEqual(metrics["queue_depth"]["queued"], 4)
        self.assertEqual(metrics["total_retries"], 3)
        self.assertAlmostEqual(metrics["success_rate_by_platform"]["instagram"], 2 / 3, places=3)
        self.assertAlmostEqual(metrics["failure_rate_by_platform"]["instagram"], 1 / 3, places=3)
        self.assertEqual(metrics["retry_count_by_platform"]["instagram"], 3)
        self.assertEqual(
            metrics["average_processing_seconds_by_platform"]["instagram"],
            metrics["average_processing_seconds"],
        )


if __name__ == "__main__":
    unittest.main()
