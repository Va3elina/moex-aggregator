"""Служебные уведомления в личку админов (получатели — ADMIN_CHAT_ID через запятую, релей — TELEGRAM_API_ROOT).

Боты разнесены по назначению (Вадим 03.11.2026): bot="signal" — @framesignalbot (ALERT_BOT_TOKEN),
"editor" — FRAME · Редакция @frame_post_bot (поводы для постов), "duty" — FRAME · Дежурный @frameadminbot (BOT_TOKEN).
Если у бота нет токена или получатель не нажал «Старт» — сообщение уходит через следующий токен в списке,
чтобы ничего не терялось на переезде."""
import os

import requests
from dotenv import load_dotenv

SITE = "https://framedata.ru"

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(_ROOT, ".env"))

BOTS = {
    "signal": ("ALERT_BOT_TOKEN",),
    "editor": ("EDITOR_BOT_TOKEN", "ALERT_BOT_TOKEN"),
    "duty": ("BOT_TOKEN", "ALERT_BOT_TOKEN"),
}


def send(text: str, tag: str = "framesignal", bot: str = "signal", silent: bool = False) -> bool:
    """HTML-сообщение всем получателям; True — дошло хотя бы до одного. silent — без звука уведомления."""
    toks = [t for t in (os.environ.get(k, "").strip() for k in BOTS[bot]) if t]
    chats = [c.strip() for c in os.environ.get("ADMIN_CHAT_ID", "").split(",") if c.strip()]
    if not toks or not chats:
        print(f"[{tag}] нет токена бота {bot} или ADMIN_CHAT_ID — не отправлено")
        return False
    root = os.environ.get("TELEGRAM_API_ROOT", "https://api.telegram.org").rstrip("/")
    ok = False
    for chat in chats:
        for tok in toks:
            try:
                r = requests.post(f"{root}/bot{tok}/sendMessage", timeout=20, data={
                    "chat_id": chat, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True,
                    "disable_notification": silent})
                if r.json().get("ok"):
                    ok = True
                    break
                print(f"[{tag}] чат {chat}: не принято: {r.text[:200].replace(tok, '<REDACTED>')}")
            except requests.RequestException as e:
                print(f"[{tag}] чат {chat}: {str(e).replace(tok, '<REDACTED>')}")
    return ok


def send_photo(png: bytes, caption: str, tag: str = "framesignal", bot: str = "signal", silent: bool = False) -> bool:
    """Картинка с HTML-подписью всем получателям (подпись Telegram — до 1024 знаков); True — дошло хотя бы до одного."""
    toks = [t for t in (os.environ.get(k, "").strip() for k in BOTS[bot]) if t]
    chats = [c.strip() for c in os.environ.get("ADMIN_CHAT_ID", "").split(",") if c.strip()]
    if not toks or not chats:
        print(f"[{tag}] нет токена бота {bot} или ADMIN_CHAT_ID — не отправлено")
        return False
    root = os.environ.get("TELEGRAM_API_ROOT", "https://api.telegram.org").rstrip("/")
    ok = False
    for chat in chats:
        for tok in toks:
            try:
                r = requests.post(f"{root}/bot{tok}/sendPhoto", timeout=60,
                                  data={"chat_id": chat, "caption": caption, "parse_mode": "HTML",
                                        "disable_notification": silent},
                                  files={"photo": ("signal.png", png, "image/png")})
                if r.json().get("ok"):
                    ok = True
                    break
                print(f"[{tag}] чат {chat}: фото не принято: {r.text[:200].replace(tok, '<REDACTED>')}")
            except requests.RequestException as e:
                print(f"[{tag}] чат {chat}: {str(e).replace(tok, '<REDACTED>')}")
    return ok
