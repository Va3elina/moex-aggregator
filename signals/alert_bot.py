"""
Telegram Alert-Bot (Frame) — host-side long-poll поллер. Phase 1: привязка аккаунта.

ЗАПУСК НА ХОСТЕ (не в контейнере!): РКН блокирует IPv4 Telegram → нужен IPv6 хоста.
  /opt/frame/signals/.venv/bin/python -m signals.alert_bot
Под systemd (frame-alert-bot.service, auto-restart). ENV (.env корня): ALERT_BOT_TOKEN.

DB: используем общий api.database.SessionLocal. На хосте DB_URL указывает на @db:5432
(docker-network) — переопределяем на 127.0.0.1 ДО импорта api.database (как делает
signals/run.sh через sed, только in-process т.к. процесс долгоживущий).

Команды: /start <token> — привязка; /alerts, /stop, /help — заглушки (Phase 2/3).
"""
import os
import time
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv

# ── .env + DB_URL override ДО импорта api.database ──────────────────────────
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # /opt/frame
load_dotenv(os.path.join(_ROOT, ".env"))
_db = os.environ.get("DB_URL", "")
if "@db:" in _db:  # host-side: docker-hostname → localhost
    os.environ["DB_URL"] = _db.replace("@db:", "@127.0.0.1:")

from sqlalchemy import text  # noqa: E402
from api.database import SessionLocal  # noqa: E402  (импорт ПОСЛЕ DB_URL override)

BOT_TOKEN = os.environ["ALERT_BOT_TOKEN"]
API_BASE = f"https://api.telegram.org/bot{BOT_TOKEN}"

HELP_TEXT = (
    "🔔 <b>Frame Signal</b> — алерты по рынку MOEX.\n\n"
    "Параметры алертов задаются на сайте таймфрейм.рф (Личный кабинет → Алерты),\n"
    "а уведомления приходят сюда.\n\n"
    "Команды: /alerts — список · /stop — пауза · /help — помощь."
)


def _redact(s) -> str:
    """Не дать токену утечь в логи (RequestException печатает full URL)."""
    return str(s).replace(BOT_TOKEN, "<REDACTED>")


def send(chat_id, text_msg: str) -> None:
    try:
        requests.post(
            f"{API_BASE}/sendMessage",
            data={
                "chat_id": chat_id,
                "text": text_msg,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
            timeout=15,
        )
    except requests.RequestException as e:
        print(f"[alert_bot] send error: {_redact(e)}")


def link_account(token: str, chat_id: int, username) -> bool:
    """Привязать chat_id к юзеру по одноразовому link-токену. True если успешно."""
    db = SessionLocal()
    try:
        row = db.execute(
            text("SELECT user_id FROM telegram_link_tokens "
                 "WHERE token = :t AND expires_at > now()"),
            {"t": token},
        ).fetchone()
        if not row:
            return False
        user_id = row[0]
        # один Telegram-чат = один аккаунт: снять чат с других юзеров
        db.execute(
            text("UPDATE users SET telegram_chat_id = NULL "
                 "WHERE telegram_chat_id = :c AND id <> :u"),
            {"c": chat_id, "u": user_id},
        )
        db.execute(
            text("UPDATE users SET telegram_chat_id = :c, telegram_username = :un, "
                 "telegram_linked_at = now() WHERE id = :u"),
            {"c": chat_id, "un": username, "u": user_id},
        )
        # все link-токены юзера отработали — чистим
        db.execute(text("DELETE FROM telegram_link_tokens WHERE user_id = :u"), {"u": user_id})
        db.commit()
        return True
    except Exception as e:
        db.rollback()
        print(f"[alert_bot] link error: {e}")
        return False
    finally:
        db.close()


def process_update(update: dict) -> None:
    msg = update.get("message")
    if not msg:
        return
    chat_id = msg["chat"]["id"]
    username = msg["chat"].get("username")
    txt = (msg.get("text") or "").strip()

    if txt.startswith("/start"):
        parts = txt.split(maxsplit=1)
        if len(parts) == 2 and parts[1].strip():
            ok = link_account(parts[1].strip(), chat_id, username)
            send(chat_id,
                 "✅ Аккаунт Фрейм подключён. Алерты будут приходить сюда.\n"
                 "Настроить — на сайте таймфрейм.рф (ЛК → Алерты)." if ok else
                 "⚠️ Ссылка недействительна или истекла.\n"
                 "Сгенерируйте новую на сайте: ЛК → Подключить Telegram.")
        else:
            send(chat_id,
                 "Чтобы подключить алерты — откройте таймфрейм.рф → Личный кабинет → "
                 "«Подключить Telegram» и перейдите по кнопке.")
    elif txt == "/help":
        send(chat_id, HELP_TEXT)
    elif txt == "/alerts":
        send(chat_id, "Список и управление алертами — на сайте (ЛК → Алерты). Скоро — и здесь.")
    elif txt == "/stop":
        send(chat_id, "Поставить алерты на паузу можно на сайте (ЛК → Алерты). Скоро — и здесь.")


def main() -> None:
    print(f"[{datetime.now(timezone.utc)}] alert_bot started")
    offset = None
    while True:
        try:
            params = {"timeout": 30, "allowed_updates": ["message"]}
            if offset:
                params["offset"] = offset
            resp = requests.get(f"{API_BASE}/getUpdates", params=params, timeout=35)
            data = resp.json()
            if not data.get("ok"):
                time.sleep(5)
                continue
            for update in data["result"]:
                offset = update["update_id"] + 1
                try:
                    process_update(update)
                except Exception as e:
                    print(f"[alert_bot] update error: {_redact(e)}")
        except requests.RequestException as e:
            print(f"[alert_bot] polling error: {_redact(e)}")
            time.sleep(5)
        except Exception as e:
            print(f"[alert_bot] loop error: {_redact(e)}")
            time.sleep(5)


if __name__ == "__main__":
    main()
