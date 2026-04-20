import json
import os
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

VALID_SERVICE_MODES = {"api", "worker"}
WORKER_HEALTHCHECK_PATHS = {
    "/api/v1/health",
    "/api/v1/health/live",
    "/api/v1/health/ready",
}


class _WorkerHealthHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        path = self.path.split("?", 1)[0]
        if path not in WORKER_HEALTHCHECK_PATHS:
            self.send_error(404)
            return

        worker_thread = getattr(self.server, "worker_thread", None)
        worker_alive = bool(worker_thread and worker_thread.is_alive())
        status_code, payload = _worker_health_response(worker_alive)
        body = json.dumps(payload).encode("utf-8")

        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args) -> None:
        return


def _worker_health_response(worker_alive: bool) -> tuple[int, dict]:
    return (
        200 if worker_alive else 503,
        {
            "status": "ok" if worker_alive else "degraded",
            "ready": worker_alive,
            "service": "ReelMind Worker",
            "mode": "worker",
        },
    )


def _run_worker_service(port: str) -> None:
    from app.tasks import run_worker

    worker_thread = threading.Thread(
        target=run_worker,
        name="worker-loop",
        daemon=True,
    )
    worker_thread.start()

    server = ThreadingHTTPServer(("0.0.0.0", int(port)), _WorkerHealthHandler)
    server.timeout = 1
    server.worker_thread = worker_thread

    try:
        while worker_thread.is_alive():
            server.handle_request()
    finally:
        server.server_close()

    raise RuntimeError("Worker loop exited unexpectedly.")


def main() -> None:
    service_mode = os.getenv("SERVICE_MODE", "api").strip().lower()
    if service_mode not in VALID_SERVICE_MODES:
        allowed = ", ".join(sorted(VALID_SERVICE_MODES))
        raise RuntimeError(
            f"Unsupported SERVICE_MODE '{service_mode}'. Expected one of: {allowed}."
        )

    if service_mode == "worker":
        port = os.getenv("PORT", "").strip()
        if port:
            _run_worker_service(port)
            return

        from app.tasks import run_worker

        run_worker()
        return

    port = os.getenv("PORT", "8000")
    os.execvp(
        "uvicorn",
        [
            "uvicorn",
            "app.main:app",
            "--host",
            "0.0.0.0",
            "--port",
            port,
        ],
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Service bootstrap failed: {exc}", file=sys.stderr)
        raise
