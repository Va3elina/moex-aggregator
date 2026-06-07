"""
Отправка персонального Telegram-сообщения (для alert eval-loop и alert-бота).
sendMessage с динамическим chat_id. Санитизация ALERT_BOT_TOKEN в логах.
ENV: ALERT_BOT_TOKEN.
"""
import os

import requests

_TOKEN = os.environ["ALERT_BOT_TOKEN"]
_API = f"https://api.telegram.org/bot{_TOKEN}"


def _redact(s) -> str:
    return str(s).replace(_TOKEN, "<REDACTED>")


def send_message(chat_id, text: str) -> bool:
    """True если ушло. Не бросает (логирует ошибку с redact)."""
    try:
        resp = requests.post(
            f"{_API}/sendMessage",
            data={
                "chat_id": chat_id,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
            timeout=15,
        )
        if not resp.json().get("ok"):
            print(f"[alert_notify] telegram not ok: {_redact(resp.text)}")
            return False
        return True
    except requests.RequestException as e:
        print(f"[alert_notify] send error: {_redact(e)}")
        return False
