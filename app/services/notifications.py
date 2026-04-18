import logging
import json
from pathlib import Path

import firebase_admin
from firebase_admin import exceptions as firebase_exceptions
from firebase_admin import credentials, messaging

from app.config import get_settings

logger = logging.getLogger(__name__)

_firebase_app: firebase_admin.App | None = None
_ANDROID_NOTIFICATION_CHANNEL_ID = "reelpin_updates"


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
    reel_id = (data or {}).get("reel_id")
    collapse_key = f"reel_ready_{reel_id}" if reel_id else None
    apns_headers = {"apns-priority": "10"}
    if collapse_key:
        apns_headers["apns-collapse-id"] = collapse_key

    message = messaging.MulticastMessage(
        notification=messaging.Notification(title=title, body=body),
        data=data or {},
        tokens=tokens,
        android=messaging.AndroidConfig(
            priority="high",
            collapse_key=collapse_key,
            notification=messaging.AndroidNotification(
                channel_id=_ANDROID_NOTIFICATION_CHANNEL_ID,
                priority="high",
                default_sound=True,
                tag=collapse_key,
            ),
        ),
        apns=messaging.APNSConfig(
            headers=apns_headers,
            payload=messaging.APNSPayload(
                aps=messaging.Aps(sound="default"),
            ),
        ),
    )
    response = messaging.send_each_for_multicast(message)
    logger.info(
        "FCM multicast complete: %s success / %s failure",
        response.success_count,
        response.failure_count,
    )
    _handle_send_failures(tokens=tokens, response=response)
    return response.success_count


def _handle_send_failures(
    *,
    tokens: list[str],
    response,
) -> None:
    invalid_tokens: list[str] = []

    for token, send_response in zip(tokens, response.responses):
        if send_response.success:
            continue

        error = send_response.exception
        error_code = _error_code(error)
        logger.warning(
            "FCM send failed for token %s: %s (%s)",
            _mask_token(token),
            error_code,
            error,
        )
        if isinstance(error, (messaging.UnregisteredError, messaging.SenderIdMismatchError)):
            invalid_tokens.append(token)

    if invalid_tokens:
        deleted = _delete_invalid_tokens(invalid_tokens)
        logger.info(
            "Removed %s invalid device push token(s) after FCM rejection.",
            deleted,
        )


def _delete_invalid_tokens(tokens: list[str]) -> int:
    from app.services.database import delete_device_push_tokens

    return delete_device_push_tokens(tokens)


def _error_code(error: Exception | None) -> str:
    if error is None:
        return "unknown"
    if isinstance(error, firebase_exceptions.FirebaseError):
        return error.code
    return error.__class__.__name__


def _mask_token(token: str) -> str:
    cleaned = token.strip()
    if len(cleaned) <= 8:
        return cleaned
    return f"...{cleaned[-8:]}"
