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
                    "step_durations": {
                        "download_seconds": 2.0,
                        "transcribe_seconds": 4.0,
                        "total_seconds": 10.0,
                    },
                },
                {
                    "status": "dead_lettered",
                    "source_platform": "instagram",
                    "attempt_count": 2,
                    "step_durations": {},
                },
                {
                    "status": "completed",
                    "source_platform": "youtube",
                    "attempt_count": 3,
                    "step_durations": {
                        "download_seconds": 0.0,
                        "transcribe_seconds": 0.0,
                        "total_seconds": 3.0,
                    },
                },
                {
                    "status": "queued",
                    "source_platform": "tiktok",
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
        self.assertEqual(metrics["success_rate_by_platform"]["instagram"], 0.5)
        self.assertEqual(metrics["failure_rate_by_platform"]["instagram"], 0.5)
        self.assertEqual(metrics["success_rate_by_platform"]["youtube"], 1.0)
        self.assertEqual(metrics["retry_count_by_platform"]["youtube"], 2)
        self.assertEqual(metrics["average_processing_seconds"], 6.5)
        self.assertEqual(metrics["average_processing_seconds_by_platform"]["instagram"], 10.0)
        self.assertEqual(metrics["average_step_seconds"]["download_seconds"], 1.0)
        self.assertEqual(metrics["average_step_seconds"]["transcribe_seconds"], 2.0)


if __name__ == "__main__":
    unittest.main()
