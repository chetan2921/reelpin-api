from datetime import datetime, timedelta, timezone
import hashlib
import json
import logging

from app.services.cookie_health import inspect_instagram_cookie_slots

logger = logging.getLogger(__name__)


def maybe_send_instagram_cookie_alert(settings) -> None:
    slots = inspect_instagram_cookie_slots(settings)
    unhealthy = [slot for slot in slots if slot["configured"] and not slot["healthy"]]
    signature = _alert_signature(unhealthy)

    previous = _safe_get_alert_state()
    previous_status = str(previous.get("status") or "").strip().lower() if previous else ""
    previous_signature = str(((previous.get("details") or {}) if previous else {}).get("signature") or "")
    previous_sent_at = _parse_datetime(previous.get("last_heartbeat_at")) if previous else None

    if not unhealthy:
        _upsert_service_health(
            service_name="instagram_cookie_alert",
            status="ok",
            details={
                "signature": "",
                "slot_count": len(slots),
                "unhealthy_slots": [],
            },
        )
        return

    if not settings.ADMIN_ALERT_USER_ID:
        _upsert_service_health(
            service_name="instagram_cookie_alert",
            status="degraded",
            details={
                "signature": signature,
                "slot_count": len(slots),
                "unhealthy_slots": unhealthy,
                "warning": "ADMIN_ALERT_USER_ID is not configured.",
            },
        )
        return

    if (
        previous_status == "alerted"
        and previous_signature == signature
        and previous_sent_at is not None
        and previous_sent_at >= datetime.now(timezone.utc) - timedelta(minutes=settings.ADMIN_ALERT_COOLDOWN_MINUTES)
    ):
        return

    tokens = _get_device_tokens(settings.ADMIN_ALERT_USER_ID)
    if not tokens:
        _upsert_service_health(
            service_name="instagram_cookie_alert",
            status="degraded",
            details={
                "signature": signature,
                "slot_count": len(slots),
                "unhealthy_slots": unhealthy,
                "warning": "No device tokens registered for ADMIN_ALERT_USER_ID.",
            },
        )
        return

    delivered = _send_push_notification(
        tokens=tokens,
        title="Instagram cookies need attention",
        body=_alert_body(unhealthy),
        data={
            "type": "ops_alert",
            "alert_kind": "instagram_cookie_health",
        },
    )
    logger.info("Instagram cookie alert delivered to %s device(s)", delivered)
    _upsert_service_health(
        service_name="instagram_cookie_alert",
        status="alerted",
        details={
            "signature": signature,
            "slot_count": len(slots),
            "unhealthy_slots": unhealthy,
            "delivered_device_count": delivered,
        },
    )


def _alert_body(unhealthy_slots: list[dict]) -> str:
    labels = ", ".join(
        f"{slot['slot']}: {slot['warning']}"
        for slot in unhealthy_slots
        if slot.get("warning")
    )
    if labels:
        return f"Instagram cookie issue detected. {labels}"
    return "Instagram cookie issue detected. Check the configured cookie slots."


def _alert_signature(unhealthy_slots: list[dict]) -> str:
    normalized = json.dumps(unhealthy_slots, sort_keys=True)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _parse_datetime(value) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def _safe_get_alert_state() -> dict | None:
    try:
        return _get_service_health("instagram_cookie_alert")
    except Exception as e:
        logger.warning("Could not read previous instagram cookie alert state: %s", e)
        return None


def _get_service_health(service_name: str) -> dict | None:
    from app.services.database import get_service_health

    return get_service_health(service_name)


def _upsert_service_health(*, service_name: str, status: str, details: dict) -> dict:
    from app.services.database import upsert_service_health

    return upsert_service_health(service_name=service_name, status=status, details=details)


def _get_device_tokens(user_id: str) -> list[str]:
    from app.services.database import get_device_push_tokens

    return get_device_push_tokens(user_id)


def _send_push_notification(
    *,
    tokens: list[str],
    title: str,
    body: str,
    data: dict[str, str],
) -> int:
    from app.services.notifications import send_push_notification

    return send_push_notification(
        tokens=tokens,
        title=title,
        body=body,
        data=data,
    )
