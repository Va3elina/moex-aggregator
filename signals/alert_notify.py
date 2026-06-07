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


def send_message(chat_id, text: str) -> str:
    """'ok' — ушло; 'blocked' — юзер забанил бота / чат мёртв (→ авто-отвязка
    в eval-loop); 'error' — временная ошибка (повтор позже). Не бросает."""
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
        data = resp.json()
        if data.get("ok"):
            return "ok"
        desc = str(data.get("description", "")).lower()
        code = data.get("error_code")
        # Терминальные: бот заблокирован / чат не найден / юзер деактивирован →
        # доставить уже невозможно, eval-loop авто-отвяжет (chat_id=NULL).
        if code == 403 or "blocked" in desc or "deactivated" in desc \
                or "chat not found" in desc or "user is deactivated" in desc:
            return "blocked"
        print(f"[alert_notify] telegram not ok: {_redact(resp.text)}")
        return "error"
    except requests.RequestException as e:
        print(f"[alert_notify] send error: {_redact(e)}")
        return "error"
