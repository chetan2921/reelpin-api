import logging
import json
from pathlib import Path

import firebase_admin
from firebase_admin import credentials, messaging

from app.config import get_settings

logger = logging.getLogger(__name__)

_firebase_app: firebase_admin.App | None = None


def _get_firebase_app() -> firebase_admin.App:
    global _firebase_app

    if _firebase_app is not None:
        return _firebase_app

    settings = get_settings()
    service_account_json = settings.FIREBASE_SERVICE_ACCOUNT_JSON
    service_account_path = settings.FIREBASE_SERVICE_ACCOUNT_PATH

    if service_account_json:
        credential = credentials.Certificate(json.loads(service_account_json))
    else:
        if not service_account_path:
            raise RuntimeError(
                "Either FIREBASE_SERVICE_ACCOUNT_JSON or FIREBASE_SERVICE_ACCOUNT_PATH must be configured"
            )

        path = Path(service_account_path)
        if not path.exists():
            raise RuntimeError("Configured Firebase service account file was not found.")

        credential = credentials.Certificate(str(path))

    _firebase_app = firebase_admin.initialize_app(credential)
    logger.info("Firebase Admin initialized successfully")
    return _firebase_app


def send_push_notification(
    *,
    tokens: list[str],
    title: str,
    body: str,
    data: dict[str, str] | None = None,
) -> int:
    if not tokens:
        return 0

    _get_firebase_app()

    message = messaging.MulticastMessage(
        notification=messaging.Notification(title=title, body=body),
        data=data or {},
        tokens=tokens,
    )
    response = messaging.send_each_for_multicast(message)
    logger.info(
        "FCM multicast complete: %s success / %s failure",
        response.success_count,
        response.failure_count,
    )
    return response.success_count
