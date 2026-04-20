import logging
import json
from pathlib import Path
import time

import firebase_admin
from firebase_admin import exceptions as firebase_exceptions
from firebase_admin import credentials, messaging

from app.config import get_settings

logger = logging.getLogger(__name__)

_firebase_app: firebase_admin.App | None = None
_ANDROID_NOTIFICATION_CHANNEL_ID = "reelpin_updates"
_ANDROID_CLICK_ACTION = "FLUTTER_NOTIFICATION_CLICK"
_FCM_RETRYABLE_ERROR_CODES = {"internal", "unavailable", "deadline-exceeded", "unknown"}
_FCM_MAX_SEND_ATTEMPTS = 3


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
    normalized_tokens = _normalize_tokens(tokens)
    if not normalized_tokens:
        return 0

    _get_firebase_app()
    message = _build_multicast_message(
        tokens=normalized_tokens,
        title=title,
        body=body,
        data=data or {},
    )
    response = _send_multicast_with_retry(message)
    logger.info(
        "FCM multicast complete: %s success / %s failure",
        response.success_count,
        response.failure_count,
    )
    _handle_send_failures(tokens=normalized_tokens, response=response)
    return response.success_count


def _build_multicast_message(
    *,
    tokens: list[str],
    title: str,
    body: str,
    data: dict[str, str],
) -> messaging.MulticastMessage:
    reel_id = data.get("reel_id")
    collapse_key = f"reel_ready_{reel_id}" if reel_id else None
    apns_headers = {
        "apns-priority": "10",
        "apns-push-type": "alert",
    }
    if collapse_key:
        apns_headers["apns-collapse-id"] = collapse_key

    return messaging.MulticastMessage(
        notification=messaging.Notification(title=title, body=body),
        data=data,
        tokens=tokens,
        android=messaging.AndroidConfig(
            priority="high",
            collapse_key=collapse_key,
            notification=messaging.AndroidNotification(
                channel_id=_ANDROID_NOTIFICATION_CHANNEL_ID,
                priority="high",
                default_sound=True,
                sound="default",
                tag=collapse_key,
                click_action=_ANDROID_CLICK_ACTION,
            ),
        ),
        apns=messaging.APNSConfig(
            headers=apns_headers,
            payload=messaging.APNSPayload(
                aps=messaging.Aps(
                    sound="default",
                    content_available=True,
                    mutable_content=True,
                    category="REEL_READY",
                ),
            ),
        ),
    )


def _send_multicast_with_retry(message: messaging.MulticastMessage):
    for attempt in range(1, _FCM_MAX_SEND_ATTEMPTS + 1):
        try:
            return messaging.send_each_for_multicast(message)
        except Exception as e:
            if attempt >= _FCM_MAX_SEND_ATTEMPTS or not _is_retryable_send_error(e):
                raise

            delay_seconds = float(attempt)
            logger.warning(
                "FCM multicast attempt %s failed with %s; retrying in %.1fs",
                attempt,
                _error_code(e),
                delay_seconds,
            )
            time.sleep(delay_seconds)

    raise RuntimeError("FCM multicast exhausted all retry attempts.")


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


def _normalize_tokens(tokens: list[str]) -> list[str]:
    normalized_tokens: list[str] = []
    seen_tokens: set[str] = set()
    for token in tokens:
        cleaned = str(token or "").strip()
        if not cleaned or cleaned in seen_tokens:
            continue
        seen_tokens.add(cleaned)
        normalized_tokens.append(cleaned)
    return normalized_tokens


def _is_retryable_send_error(error: Exception) -> bool:
    if isinstance(error, (ConnectionError, OSError, TimeoutError)):
        return True

    code = _error_code(error)
    return code in _FCM_RETRYABLE_ERROR_CODES


def _mask_token(token: str) -> str:
    cleaned = token.strip()
    if len(cleaned) <= 8:
        return cleaned
    return f"...{cleaned[-8:]}"
