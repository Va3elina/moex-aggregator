"""Отправка служебных уведомлений в @framesignalbot (личка админов): токен ALERT_BOT_TOKEN, получатели — ADMIN_CHAT_ID
через запятую, релей — TELEGRAM_API_ROOT. Общая для signals/record_notify.py и signals/season_notify.py."""
import os

import requests
from dotenv import load_dotenv

SITE = "https://framedata.ru"

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(_ROOT, ".env"))


def send(text: str, tag: str = "framesignal") -> bool:
    """HTML-сообщение всем получателям; True — дошло хотя бы до одного."""
    tok = os.environ.get("ALERT_BOT_TOKEN")
    chats = [c.strip() for c in os.environ.get("ADMIN_CHAT_ID", "").split(",") if c.strip()]
    if not tok or not chats:
        print(f"[{tag}] нет ALERT_BOT_TOKEN или ADMIN_CHAT_ID — не отправлено")
        return False
    root = os.environ.get("TELEGRAM_API_ROOT", "https://api.telegram.org").rstrip("/")
    ok = False
    for chat in chats:
        try:
            r = requests.post(f"{root}/bot{tok}/sendMessage", timeout=20, data={
                "chat_id": chat, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True})
            if r.json().get("ok"):
                ok = True
            else:
                print(f"[{tag}] чат {chat}: не принято: {r.text[:200].replace(tok, '<REDACTED>')}")
        except requests.RequestException as e:
            print(f"[{tag}] чат {chat}: {str(e).replace(tok, '<REDACTED>')}")
    return ok
