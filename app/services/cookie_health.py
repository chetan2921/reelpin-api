import base64
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace


def inspect_instagram_cookie_slots(settings) -> list[dict]:
    slots = []
    for label in ["active", "backup", "tertiary"]:
        slot = inspect_cookie_slot(settings, "instagram", label)
        if slot["configured"]:
            slots.append(slot)
    if not slots:
        legacy = inspect_legacy_instagram_cookie(settings)
        if legacy["configured"]:
            slots.append(legacy)
    return slots


def inspect_cookie_slot(settings, platform: str, label: str) -> dict:
    namespace = _as_namespace(settings)
    prefix = f"{platform.upper()}_{label.upper()}"
    file_path = getattr(namespace, f"{prefix}_COOKIES_FILE", None)
    plain = getattr(namespace, f"{prefix}_COOKIE_DATA", None)
    encoded = getattr(namespace, f"{prefix}_COOKIE_DATA_BASE64", None)
    return _inspect_cookie_material(
        slot_label=label,
        file_path=file_path,
        plain=plain,
        encoded=encoded,
        encoded_label=f"{prefix}_COOKIE_DATA_BASE64",
    )


def inspect_legacy_instagram_cookie(settings) -> dict:
    namespace = _as_namespace(settings)
    return _inspect_cookie_material(
        slot_label="legacy",
        file_path=getattr(namespace, "INSTAGRAM_COOKIES_FILE", None),
        plain=getattr(namespace, "INSTAGRAM_COOKIE_DATA", None),
        encoded=getattr(namespace, "INSTAGRAM_COOKIE_DATA_BASE64", None),
        encoded_label="INSTAGRAM_COOKIE_DATA_BASE64",
    )


def any_healthy_instagram_cookie(settings) -> bool:
    return any(slot["healthy"] for slot in inspect_instagram_cookie_slots(settings))


def _inspect_cookie_material(
    *,
    slot_label: str,
    file_path: str | None,
    plain: str | None,
    encoded: str | None,
    encoded_label: str,
) -> dict:
    raw = None
    source = "missing"
    warning = None

    if file_path:
        source = "file"
        path = Path(file_path)
        if not path.exists():
            return _result(
                slot_label=slot_label,
                configured=True,
                source=source,
                healthy=False,
                warning="configured file is missing",
            )
        raw = path.read_text(encoding="utf-8", errors="ignore")
    elif encoded:
        source = "base64"
        try:
            raw = base64.b64decode(encoded.encode("utf-8")).decode("utf-8")
        except Exception:
            return _result(
                slot_label=slot_label,
                configured=True,
                source=source,
                healthy=False,
                warning=f"{encoded_label} could not be decoded",
            )
    elif plain:
        source = "plain"
        raw = plain.replace("\\n", "\n")

    if raw is None:
        return _result(
            slot_label=slot_label,
            configured=False,
            source=source,
            healthy=False,
        )

    parsed = _parse_cookie_lines(raw)
    session_cookie = parsed.get("sessionid")
    csrf_cookie = parsed.get("csrftoken")

    if session_cookie is None:
        warning = "sessionid cookie is missing"
    elif _is_expired(session_cookie["expires_at"]):
        warning = "sessionid cookie is expired"

    if not warning and csrf_cookie is not None and _is_expired(csrf_cookie["expires_at"]):
        warning = "csrftoken cookie is expired"

    return _result(
        slot_label=slot_label,
        configured=True,
        source=source,
        healthy=warning is None,
        warning=warning,
        session_expires_at=_isoformat(session_cookie["expires_at"]) if session_cookie else None,
    )


def _parse_cookie_lines(raw: str) -> dict[str, dict]:
    cookies: dict[str, dict] = {}
    for line in raw.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        parts = stripped.split("\t")
        if len(parts) < 7:
            continue
        expires_at = parts[4]
        name = parts[5]
        value = parts[6]
        if not name or not value:
            continue
        cookies[name] = {
            "expires_at": int(expires_at) if expires_at.isdigit() else None,
        }
    return cookies


def _is_expired(expires_at: int | None) -> bool:
    if expires_at is None or expires_at == 0:
        return False
    return expires_at <= int(datetime.now(timezone.utc).timestamp())


def _isoformat(expires_at: int | None) -> str | None:
    if expires_at is None or expires_at == 0:
        return None
    return datetime.fromtimestamp(expires_at, tz=timezone.utc).isoformat()


def _result(
    *,
    slot_label: str,
    configured: bool,
    source: str,
    healthy: bool,
    warning: str | None = None,
    session_expires_at: str | None = None,
) -> dict:
    return {
        "slot": slot_label,
        "configured": configured,
        "source": source,
        "healthy": healthy,
        "warning": warning,
        "session_expires_at": session_expires_at,
    }


def _as_namespace(settings):
    if isinstance(settings, dict):
        return SimpleNamespace(**settings)
    return settings
