import unittest
from types import SimpleNamespace
from unittest.mock import patch

from app.main import enqueue_reel_processing
from app.models import EnqueueReelJobInput, ProcessingJobStatus
from app.tasks import process_reel_job


class ProcessingJobNotificationTests(unittest.IsolatedAsyncioTestCase):
    async def test_enqueue_reused_completed_job_sends_ready_notification(self):
        job = {
            "id": "job-1",
            "user_id": "user-1",
            "url": "https://www.instagram.com/reel/abc123/",
            "normalized_url": "https://www.instagram.com/reel/abc123/",
            "status": "completed",
            "current_step": "completed",
            "progress_percent": 100,
            "attempt_count": 0,
            "max_attempts": 3,
            "result_reel_id": "reel-1",
            "step_durations": {},
        }
        reel = {
            "id": "reel-1",
            "user_id": "user-1",
            "url": "https://www.instagram.com/reel/abc123/",
            "title": "Street food spots",
            "summary": "",
            "transcript": "",
            "category": "Food",
        }

        with (
            patch("app.main.find_processing_job_by_user_and_url", return_value=job),
            patch("app.main.get_reel", return_value=reel),
            patch("app.main.log_processing_event"),
            patch("app.main._notify_reel_ready") as notify_reel_ready,
        ):
            response = await enqueue_reel_processing(
                EnqueueReelJobInput(
                    user_id="user-1",
                    url="https://www.instagram.com/reel/abc123/",
                )
            )

        self.assertEqual(response.id, "job-1")
        self.assertEqual(response.status, ProcessingJobStatus.completed)
        notify_reel_ready.assert_called_once_with(
            user_id="user-1",
            reel_id="reel-1",
            job_id="job-1",
            reel_title="Street food spots",
        )

    async def test_enqueue_active_job_does_not_send_ready_notification(self):
        job = {
            "id": "job-2",
            "user_id": "user-1",
            "url": "https://www.instagram.com/reel/active123/",
            "normalized_url": "https://www.instagram.com/reel/active123/",
            "status": "queued",
            "current_step": "queued",
            "progress_percent": 0,
            "attempt_count": 0,
            "max_attempts": 3,
            "result_reel_id": None,
            "step_durations": {},
        }

        with (
            patch("app.main.find_processing_job_by_user_and_url", return_value=job),
            patch("app.main.log_processing_event"),
            patch("app.main._notify_reel_ready") as notify_reel_ready,
        ):
            response = await enqueue_reel_processing(
                EnqueueReelJobInput(
                    user_id="user-1",
                    url="https://www.instagram.com/reel/active123/",
                )
            )

        self.assertEqual(response.id, "job-2")
        self.assertEqual(response.status, ProcessingJobStatus.queued)
        notify_reel_ready.assert_not_called()

    async def test_enqueue_cached_reel_sends_ready_notification(self):
        reel = {
            "id": "reel-3",
            "user_id": "user-1",
            "url": "https://www.instagram.com/reel/cache123/",
            "title": "Monsoon cafe list",
            "summary": "",
            "transcript": "",
            "category": "Travel",
            "transcript_source": "groq",
        }
        job = {
            "id": "job-3",
            "user_id": "user-1",
            "url": "https://www.instagram.com/reel/cache123/",
            "normalized_url": "https://www.instagram.com/reel/cache123/",
            "status": "completed",
            "current_step": "completed",
            "progress_percent": 100,
            "attempt_count": 0,
            "max_attempts": 3,
            "result_reel_id": "reel-3",
            "step_durations": {},
        }

        with (
            patch("app.main.find_processing_job_by_user_and_url", return_value=None),
            patch("app.main.find_processing_job_by_user_and_source_identity", return_value=None),
            patch("app.main.find_reel_by_user_and_url", return_value=reel),
            patch("app.main.create_completed_processing_job", return_value=job),
            patch("app.main.get_reel", return_value=reel),
            patch("app.main.log_processing_event"),
            patch("app.main._notify_reel_ready") as notify_reel_ready,
        ):
            response = await enqueue_reel_processing(
                EnqueueReelJobInput(
                    user_id="user-1",
                    url="https://www.instagram.com/reel/cache123/",
                )
            )

        self.assertEqual(response.id, "job-3")
        self.assertEqual(response.status, ProcessingJobStatus.completed)
        notify_reel_ready.assert_called_once_with(
            user_id="user-1",
            reel_id="reel-3",
            job_id="job-3",
            reel_title="Monsoon cafe list",
        )

    def test_worker_reused_existing_reel_sends_ready_notification(self):
        job = {
            "id": "job-4",
            "user_id": "user-1",
            "url": "https://www.instagram.com/reel/worker123/",
            "attempt_count": 0,
            "max_attempts": 3,
        }
        existing_reel = {
            "id": "reel-4",
            "title": "Late night dessert places",
            "transcript_source": "groq",
        }
        source = SimpleNamespace(
            normalized_url="https://www.instagram.com/reel/worker123/",
            source_platform="instagram",
            source_content_id="worker123",
        )

        with (
            patch("app.tasks.resolve_source_identity", return_value=source),
            patch("app.tasks.find_reel_by_user_and_url", return_value=existing_reel),
            patch("app.tasks.update_processing_job_if_claimed", return_value={"id": "job-4"}),
            patch("app.tasks.log_processing_event"),
            patch("app.tasks._notify_reel_ready") as notify_reel_ready,
        ):
            process_reel_job(job, worker_id="worker-1")

        notify_reel_ready.assert_called_once_with(
            user_id="user-1",
            reel_id="reel-4",
            job_id="job-4",
            reel_title="Late night dessert places",
        )


if __name__ == "__main__":
    unittest.main()
