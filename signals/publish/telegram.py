"""Telegram publisher for signal posts.

Использует sendPhoto (с JPEG-сжатием) — у нас нет alpha в чарте,
а inline-preview важен для канала. Caption limit 1024 chars, наши ~150.
"""
from __future__ import annotations
import os
from pathlib import Path
from typing import Optional, Tuple

import requests

from signals import config

# TELEGRAM_API_ROOT — релей (Cloudflare Worker) для обхода РКН без зависимости от IPv6.
# Дефолт = прямой Telegram (поведение не меняется, пока env не задан на проде).
_API_ROOT = os.environ.get("TELEGRAM_API_ROOT", "https://api.telegram.org")


def send_signal_post(
    *,
    image_path: Path,
    caption: str,
) -> Tuple[bool, Optional[int], Optional[str]]:
    """Отправить (фото + caption) в SIGNALS_CHANNEL_ID.

    Returns:
        (ok, message_id, error_text)
        - ok=True  → message_id заполнен, error_text=None
        - ok=False → message_id=None, error_text содержит описание
    """
    url = f"{_API_ROOT}/bot{config.BOT_TOKEN}/sendPhoto"
    try:
        with image_path.open("rb") as f:
            resp = requests.post(
                url,
                data={
                    "chat_id": str(config.SIGNALS_CHANNEL_ID),
                    "caption": caption,
                },
                files={"photo": (image_path.name, f, "image/png")},
                timeout=60,
            )
    except requests.exceptions.RequestException as e:
        # Sanitize: requests печатает full URL (с токеном) в repr ошибки.
        safe = str(e).replace(config.BOT_TOKEN, "<REDACTED>") if config.BOT_TOKEN else str(e)
        return False, None, safe

    try:
        data = resp.json()
    except ValueError:
        return False, None, f"non-JSON response (HTTP {resp.status_code})"
    if data.get("ok"):
        return True, data["result"]["message_id"], None
    return False, None, data.get("description") or str(data)
