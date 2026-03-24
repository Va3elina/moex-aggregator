"""
Telegram admin bot for Frame (таймфрейм.рф)
Commands: /start, /users, /stats, /backup
"""
import os
import time
import json
import subprocess
import requests
from sqlalchemy import create_engine, text
from datetime import datetime

BOT_TOKEN = os.environ["BOT_TOKEN"]
ADMIN_CHAT_ID = int(os.environ["ADMIN_CHAT_ID"])
# pg8000 driver (already in requirements)
_raw_url = os.environ["DB_URL_SYNC"]  # postgresql://user:pass@host/db
DB_URL = _raw_url.replace("postgresql://", "postgresql+pg8000://")
API_BASE = f"https://api.telegram.org/bot{BOT_TOKEN}"

engine = create_engine(DB_URL)


# ─── Telegram helpers ──────────────────────────────────────────────────────

def send(chat_id, text, reply_markup=None):
    data = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
    if reply_markup:
        data["reply_markup"] = json.dumps(reply_markup)
    requests.post(f"{API_BASE}/sendMessage", data=data, timeout=10)


def answer_callback(callback_id):
    requests.post(f"{API_BASE}/answerCallbackQuery", data={"callback_query_id": callback_id}, timeout=5)


def main_keyboard():
    return {
        "inline_keyboard": [
            [
                {"text": "👥 Пользователи", "callback_data": "users"},
                {"text": "📊 Статистика БД", "callback_data": "stats"},
            ],
            [
                {"text": "💾 Бэкап сейчас", "callback_data": "backup"},
            ],
        ]
    }


# ─── DB helpers ────────────────────────────────────────────────────────────

def cmd_users():
    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT id, email, created_at, is_active FROM users ORDER BY created_at"
        )).fetchall()

    lines = ["<b>👥 Пользователи</b>\n"]
    for row in rows:
        uid, email, created_at, is_active = row
        status = "✅" if is_active else "❌"
        dt = created_at.strftime("%d.%m.%Y %H:%M") if created_at else "—"
        lines.append(f"{status} <b>#{uid}</b> <code>{email}</code>\n   📅 {dt}")

    lines.append(f"\n<b>Всего: {len(rows)}</b>")
    return "\n".join(lines)


def cmd_stats():
    with engine.connect() as conn:
        users = conn.execute(text("SELECT COUNT(*) FROM users")).scalar()
        last_oi = conn.execute(text(
            "SELECT MAX(tradedate) FROM open_interests WHERE interval=5"
        )).scalar()
        last_candle = conn.execute(text(
            "SELECT MAX(end_time) FROM candles WHERE interval=5 AND sectype='futures'"
        )).scalar()
        db_size = conn.execute(text(
            "SELECT pg_size_pretty(pg_database_size('moex_db'))"
        )).scalar()

    last_oi_str = last_oi.strftime("%d.%m.%Y %H:%M") if last_oi else "—"
    last_candle_str = last_candle.strftime("%d.%m.%Y %H:%M") if last_candle else "—"

    return (
        f"<b>📊 Статистика</b>\n\n"
        f"👥 Пользователей: <b>{users}</b>\n"
        f"🗄 Размер БД: <b>{db_size}</b>\n"
        f"📈 Последний OI: <b>{last_oi_str}</b>\n"
        f"🕯 Последняя свеча: <b>{last_candle_str}</b>"
    )


def cmd_backup():
    send(ADMIN_CHAT_ID, "⏳ Запускаю бэкап, подожди пару минут...")
    try:
        result = subprocess.run(
            ["bash", "/app/backup_db.sh"],
            capture_output=True, text=True, timeout=600
        )
        if result.returncode == 0:
            return "✅ Бэкап выполнен, файлы отправлены выше"
        else:
            return f"❌ Ошибка бэкапа:\n<code>{result.stderr[-500:]}</code>"
    except subprocess.TimeoutExpired:
        return "❌ Бэкап завис (timeout 10 мин)"


# ─── Main loop ─────────────────────────────────────────────────────────────

def process_update(update):
    # Сообщение
    if "message" in update:
        msg = update["message"]
        chat_id = msg["chat"]["id"]
        if chat_id != ADMIN_CHAT_ID:
            send(chat_id, "⛔ Нет доступа")
            return
        text = msg.get("text", "")
        if text in ("/start", "/menu"):
            send(chat_id, "👋 <b>Frame Admin Bot</b>\n\nВыбери действие:", main_keyboard())
        elif text == "/users":
            send(chat_id, cmd_users())
        elif text == "/stats":
            send(chat_id, cmd_stats())
        elif text == "/backup":
            send(chat_id, cmd_backup())

    # Кнопка
    elif "callback_query" in update:
        cb = update["callback_query"]
        chat_id = cb["message"]["chat"]["id"]
        if chat_id != ADMIN_CHAT_ID:
            answer_callback(cb["id"])
            return
        data = cb.get("data", "")
        answer_callback(cb["id"])

        if data == "users":
            send(chat_id, cmd_users(), main_keyboard())
        elif data == "stats":
            send(chat_id, cmd_stats(), main_keyboard())
        elif data == "backup":
            send(chat_id, cmd_backup(), main_keyboard())


def main():
    print(f"[{datetime.now()}] Bot started, admin={ADMIN_CHAT_ID}")
    offset = None
    while True:
        try:
            params = {"timeout": 30, "allowed_updates": ["message", "callback_query"]}
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
                    print(f"Error processing update: {e}")
        except Exception as e:
            print(f"Polling error: {e}")
            time.sleep(5)


if __name__ == "__main__":
    main()
