def build_reel_ready_notification_payload(
    *,
    reel_id: str,
    job_id: str,
    reel_title: str | None = None,
) -> dict:
    normalized_title = (reel_title or "").strip()
    if normalized_title:
        body = f"{normalized_title} is ready in ReelPin."
    else:
        body = "Reel saved and is ready in ReelPin."

    return {
        "title": "Reel pinned in ReelPin",
        "body": body,
        "data": {
            "type": "reel_ready",
            "reel_id": reel_id,
            "job_id": job_id,
        },
    }


def send_reel_ready_notification(
    *,
    user_id: str,
    reel_id: str,
    job_id: str,
    reel_title: str | None = None,
) -> int:
    payload = build_reel_ready_notification_payload(
        reel_id=reel_id,
        job_id=job_id,
        reel_title=reel_title,
    )
    tokens = _get_device_tokens(user_id)
    if not tokens:
        return 0

    return _send_push(
        tokens=tokens,
        title=payload["title"],
        body=payload["body"],
        data=payload["data"],
    )


def _get_device_tokens(user_id: str) -> list[str]:
    from app.services.database import get_device_push_tokens

    return get_device_push_tokens(user_id)


def _send_push(
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
