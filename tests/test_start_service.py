import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

import start_service


class StartServiceTests(unittest.TestCase):
    def test_worker_health_response_is_ok_when_worker_is_alive(self):
        status_code, payload = start_service._worker_health_response(True)

        self.assertEqual(status_code, 200)
        self.assertTrue(payload["ready"])
        self.assertEqual(payload["mode"], "worker")

    def test_worker_health_response_is_degraded_when_worker_is_down(self):
        status_code, payload = start_service._worker_health_response(False)

        self.assertEqual(status_code, 503)
        self.assertFalse(payload["ready"])
        self.assertEqual(payload["status"], "degraded")

    @patch("start_service._run_worker_service")
    def test_main_runs_worker_service_with_health_server_when_port_is_present(self, run_worker_service):
        with patch.dict(
            "os.environ",
            {"SERVICE_MODE": "worker", "PORT": "8080"},
            clear=False,
        ):
            start_service.main()

        run_worker_service.assert_called_once_with("8080")

    def test_main_runs_plain_worker_when_port_is_missing(self):
        run_worker = Mock()
        with patch.dict(
            "sys.modules",
            {"app.tasks": SimpleNamespace(run_worker=run_worker)},
        ):
            with patch.dict(
                "os.environ",
                {"SERVICE_MODE": "worker"},
                clear=True,
            ):
                start_service.main()

        run_worker.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
