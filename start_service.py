import os
import sys


def main() -> None:
    service_mode = os.getenv("SERVICE_MODE", "api").strip().lower()

    if service_mode == "worker":
        processes = os.getenv("DRAMATIQ_PROCESSES", "4")
        threads = os.getenv("DRAMATIQ_THREADS", "2")
        os.execvp(
            "dramatiq",
            [
                "dramatiq",
                "app.tasks",
                "--processes",
                processes,
                "--threads",
                threads,
            ],
        )

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
