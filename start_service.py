import os
import sys


def main() -> None:
    service_mode = os.getenv("SERVICE_MODE", "api").strip().lower()

    if service_mode == "worker":
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
