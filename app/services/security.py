import logging
from pathlib import Path
import re
from types import SimpleNamespace


_SECRET_PATTERNS = [
    (
        re.compile(
            r"(?i)\b(authorization|cookie|cookies|token|fcm_token|api[_ -]?key|service[_ -]?role[_ -]?key)\b\s*[:=]\s*([^\s,;]+(?:\s+[^\s,;]+)?)"
        ),
        r"\1=[REDACTED]",
    ),
    (
        re.compile(r"(?i)\bbearer\s+[A-Za-z0-9._\-]+"),
        "Bearer [REDACTED]",
    ),
    (
        re.compile(
            r"(?i)\b(sessionid|csrftoken|ds_user_id|mid|rur|datr|x-ig-www-claim)\s*=\s*[^;\s]+"
        ),
        lambda match: f"{match.group(1)}=[REDACTED]",
    ),
]


class SecretRedactionFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.msg = _redact_value(record.msg)
        if isinstance(record.args, tuple):
            record.args = tuple(_redact_value(arg) for arg in record.args)
        elif isinstance(record.args, dict):
            record.args = {key: _redact_value(value) for key, value in record.args.items()}
        return True


def configure_secure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    root_logger = logging.getLogger()
    if not any(isinstance(item, SecretRedactionFilter) for item in root_logger.filters):
        root_logger.addFilter(SecretRedactionFilter())


def redact_sensitive_text(value: str) -> str:
    redacted = value
    for pattern, replacement in _SECRET_PATTERNS:
        redacted = pattern.sub(replacement, redacted)
    return redacted


def build_secret_configuration_summary(settings) -> dict:
    return {
        "supabase_key_source": _supabase_key_source(settings),
        "firebase_credential_source": _firebase_credential_source(settings),
        "cookie_slots": {
            platform: {
                "active": has_cookie_slot(settings, platform, "active"),
                "backup": has_cookie_slot(settings, platform, "backup"),
            }
            for platform in ["instagram", "youtube", "tiktok", "ytdlp"]
        },
        "deprecated_envs_in_use": deprecated_secret_envs(settings),
    }


def secret_configuration_warnings(settings) -> list[str]:
    namespace = _as_namespace(settings)
    warnings: list[str] = []
    if getattr(namespace, "SUPABASE_SERVICE_ROLE_KEY", None) and getattr(namespace, "SUPABASE_KEY", None):
        warnings.append(
            "Both SUPABASE_SERVICE_ROLE_KEY and legacy SUPABASE_KEY are set. The service role key takes precedence."
        )
    if getattr(namespace, "FIREBASE_SERVICE_ACCOUNT_JSON", None) and getattr(namespace, "FIREBASE_SERVICE_ACCOUNT_PATH", None):
        warnings.append(
            "Both FIREBASE_SERVICE_ACCOUNT_JSON and FIREBASE_SERVICE_ACCOUNT_PATH are set. The JSON value takes precedence."
        )
    if deprecated_secret_envs(namespace):
        warnings.append(
            "Legacy secret env names are in use. Prefer the ACTIVE and BACKUP slot env names."
        )
    for path_env in [
        "FIREBASE_SERVICE_ACCOUNT_PATH",
        "INSTAGRAM_ACTIVE_COOKIES_FILE",
        "INSTAGRAM_BACKUP_COOKIES_FILE",
        "INSTAGRAM_COOKIES_FILE",
    ]:
        path_value = getattr(namespace, path_env, None)
        if path_value and not Path(path_value).exists():
            warnings.append(f"{path_env} is configured but the file does not exist.")
    return warnings


def has_cookie_slot(settings, platform: str, slot_name: str) -> bool:
    namespace = _as_namespace(settings)
    prefix = platform.upper()
    slot = slot_name.upper()
    values = [
        getattr(namespace, f"{prefix}_{slot}_COOKIES_FILE", None),
        getattr(namespace, f"{prefix}_{slot}_COOKIE_DATA", None),
        getattr(namespace, f"{prefix}_{slot}_COOKIE_DATA_BASE64", None),
    ]

    if any(bool(value) for value in values):
        return True

    if slot_name == "active":
        legacy_values = [
            getattr(namespace, f"{prefix}_COOKIE_DATA", None),
            getattr(namespace, f"{prefix}_COOKIE_DATA_BASE64", None),
            getattr(namespace, f"{prefix}_COOKIES_FILE", None),
        ]
        if platform == "ytdlp":
            legacy_values.extend(
                [
                    getattr(namespace, "YTDLP_COOKIE_DATA", None),
                    getattr(namespace, "YTDLP_COOKIE_DATA_BASE64", None),
                ]
            )
        if any(bool(value) for value in legacy_values):
            return True

    return False


def deprecated_secret_envs(settings) -> list[str]:
    namespace = _as_namespace(settings)
    deprecated = []
    for env_name in [
        "SUPABASE_KEY",
        "INSTAGRAM_COOKIES_FILE",
        "INSTAGRAM_COOKIE_DATA",
        "INSTAGRAM_COOKIE_DATA_BASE64",
        "YOUTUBE_COOKIE_DATA",
        "YOUTUBE_COOKIE_DATA_BASE64",
        "TIKTOK_COOKIE_DATA",
        "TIKTOK_COOKIE_DATA_BASE64",
        "YTDLP_COOKIE_DATA",
        "YTDLP_COOKIE_DATA_BASE64",
    ]:
        if getattr(namespace, env_name, None):
            deprecated.append(env_name)
    return deprecated


def _redact_value(value):
    if isinstance(value, str):
        return redact_sensitive_text(value)
    if isinstance(value, dict):
        return {key: _redact_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_redact_value(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_redact_value(item) for item in value)
    return value


def _supabase_key_source(settings) -> str:
    namespace = _as_namespace(settings)
    if getattr(namespace, "SUPABASE_SERVICE_ROLE_KEY", None):
        return "SUPABASE_SERVICE_ROLE_KEY"
    if getattr(namespace, "SUPABASE_KEY", None):
        return "SUPABASE_KEY"
    return "missing"


def _firebase_credential_source(settings) -> str:
    namespace = _as_namespace(settings)
    if getattr(namespace, "FIREBASE_SERVICE_ACCOUNT_JSON", None):
        return "FIREBASE_SERVICE_ACCOUNT_JSON"
    if getattr(namespace, "FIREBASE_SERVICE_ACCOUNT_PATH", None):
        return "FIREBASE_SERVICE_ACCOUNT_PATH"
    return "missing"


def _as_namespace(settings):
    if isinstance(settings, dict):
        return SimpleNamespace(**settings)
    return settings
