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
from datetime import datetime, timedelta, timezone

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
                {"text": "🔗 Инвайт-ссылки", "callback_data": "joins"},
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


# ─── Инвайт-ссылки каналов ─────────────────────────────────────────────────

_MEMBER_STATUSES = {"member", "administrator", "creator"}


def _is_member(cm):
    st = cm.get("status")
    return st in _MEMBER_STATUSES or (st == "restricted" and cm.get("is_member"))


def ensure_joins_table():
    """Копия db/migrations/105_tg_channel_joins.sql: бот стартует и без ручной миграции."""
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS tg_channel_joins (
                id BIGSERIAL PRIMARY KEY,
                chat_id BIGINT NOT NULL,
                chat_title TEXT,
                user_id BIGINT NOT NULL,
                event TEXT NOT NULL CHECK (event IN ('join', 'leave')),
                invite_link TEXT,
                invite_name TEXT,
                via_folder BOOLEAN NOT NULL DEFAULT FALSE,
                event_at TIMESTAMPTZ NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now())"""))
        conn.execute(text("CREATE INDEX IF NOT EXISTS tg_channel_joins_chat_time_idx"
                          " ON tg_channel_joins (chat_id, event_at)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS tg_channel_joins_user_idx"
                          " ON tg_channel_joins (chat_id, user_id, event_at)"))
        # Копия db/migrations/106_tg_channel_members_alerts.sql.
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS tg_channel_members (
                chat_id BIGINT NOT NULL,
                day DATE NOT NULL,
                members INTEGER NOT NULL,
                taken_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                PRIMARY KEY (chat_id, day))"""))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS tg_join_alerts (
                chat_id BIGINT NOT NULL,
                day DATE NOT NULL,
                kind TEXT NOT NULL CHECK (kind IN ('join', 'leave')),
                count INTEGER NOT NULL,
                sent_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                PRIMARY KEY (chat_id, day, kind))"""))


def record_chat_member(upd):
    """Апдейт chat_member → строка join/leave. Прочие переходы (смена прав) пропускаем."""
    was, now = _is_member(upd["old_chat_member"]), _is_member(upd["new_chat_member"])
    if was == now:
        return
    link = upd.get("invite_link") or {}
    with engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO tg_channel_joins (chat_id, chat_title, user_id, event, invite_link,"
            " invite_name, via_folder, event_at)"
            " VALUES (:chat_id, :title, :uid, :event, :link, :name, :folder, to_timestamp(:ts))"
        ), {
            "chat_id": upd["chat"]["id"],
            "title": upd["chat"].get("title"),
            "uid": upd["new_chat_member"]["user"]["id"],
            "event": "join" if now else "leave",
            "link": link.get("invite_link") if now else None,
            "name": link.get("name") if now else None,
            "folder": bool(upd.get("via_chat_folder_invite_link")) if now else False,
            "ts": upd["date"],
        })


def cmd_joins(days=30):
    """Вступления по ссылкам за N дней и сколько из них ещё в канале."""
    with engine.connect() as conn:
        rows = conn.execute(text("""
            WITH j AS (
                SELECT chat_id, chat_title, user_id, event_at,
                       COALESCE(invite_name, invite_link,
                                CASE WHEN via_folder THEN 'папка' ELSE 'без ссылки' END) AS src
                FROM tg_channel_joins
                WHERE event = 'join' AND event_at >= now() - make_interval(days => :days)
            )
            SELECT j.chat_title, j.src,
                   COUNT(*) AS joined,
                   COUNT(*) FILTER (WHERE NOT EXISTS (
                       SELECT 1 FROM tg_channel_joins l
                       WHERE l.chat_id = j.chat_id AND l.user_id = j.user_id
                         AND l.event = 'leave' AND l.event_at > j.event_at)) AS stayed,
                   COUNT(*) FILTER (WHERE j.event_at >= now() - interval '7 days') AS joined_7d
            FROM j
            GROUP BY j.chat_title, j.src
            ORDER BY j.chat_title, joined DESC
        """), {"days": days}).fetchall()
    if not rows:
        return f"<b>🔗 Инвайт-ссылки</b>\n\nЗа {days} дн. вступлений не записано."
    lines = [f"<b>🔗 Инвайт-ссылки за {days} дн.</b>\nвсего / за 7 дн. / остались"]
    title = None
    for chat_title, src, joined, stayed, j7 in rows:
        if chat_title != title:
            title = chat_title
            lines.append(f"\n<b>{chat_title}</b>")
        lines.append(f"<code>{src}</code>: {joined} / {j7} / {stayed}")
    return "\n".join(lines)


# ─── Часовой тик: снимок подписчиков и аномалии ─────────────────────────────

# Москва без перехода на летнее время: фиксированный сдвиг надёжнее tzdata в образе.
MSK = timezone(timedelta(hours=3))
TICK_SEC = 3600
# Всплеск: сегодня >= среднее + 3σ по последним ALERT_BASELINE_DAYS полным дням
# и не меньше ALERT_MIN_COUNT (иначе на пустой базе сработает первый же человек).
# Пока истории меньше ALERT_MIN_HISTORY_DAYS, сравнивать не с чем — молчим.
ALERT_BASELINE_DAYS = 30
ALERT_MIN_HISTORY_DAYS = 7
ALERT_MIN_COUNT = 3
_SRC_SQL = "COALESCE(invite_name, invite_link, CASE WHEN via_folder THEN 'папка' ELSE 'без ссылки' END)"


def _tracked_chats(conn):
    return [r[0] for r in conn.execute(text(
        "SELECT chat_id FROM tg_channel_joins UNION SELECT chat_id FROM tg_channel_members"))]


def _chat_title(conn, chat_id):
    row = conn.execute(text(
        "SELECT chat_title FROM tg_channel_joins WHERE chat_id = :c AND chat_title IS NOT NULL"
        " ORDER BY event_at DESC LIMIT 1"), {"c": chat_id}).fetchone()
    if row:
        return row[0]
    r = requests.get(f"{API_BASE}/getChat", params={"chat_id": chat_id}, timeout=10).json()
    return (r.get("result") or {}).get("title") or str(chat_id)


def snapshot_members(conn, chat_id, today):
    r = requests.get(f"{API_BASE}/getChatMemberCount", params={"chat_id": chat_id}, timeout=10).json()
    if not r.get("ok"):
        return
    conn.execute(text(
        "INSERT INTO tg_channel_members (chat_id, day, members, taken_at)"
        " VALUES (:c, :d, :m, now())"
        " ON CONFLICT (chat_id, day) DO UPDATE SET members = EXCLUDED.members, taken_at = now()"
    ), {"c": chat_id, "d": today, "m": int(r["result"])})


def check_anomaly(conn, chat_id, today, kind):
    """Сравниваем сегодняшний счётчик join/leave с базой полных дней; повтор за день
    только если счётчик удвоился с прошлого уведомления."""
    first = conn.execute(text(
        "SELECT MIN((event_at AT TIME ZONE 'Europe/Moscow')::date) FROM tg_channel_joins WHERE chat_id = :c"
    ), {"c": chat_id}).scalar()
    if not first or (today - first).days < ALERT_MIN_HISTORY_DAYS:
        return
    base_from = max(first, today - timedelta(days=ALERT_BASELINE_DAYS))
    rows = conn.execute(text(
        "SELECT (event_at AT TIME ZONE 'Europe/Moscow')::date AS day, COUNT(*)"
        " FROM tg_channel_joins WHERE chat_id = :c AND event = :k"
        "   AND (event_at AT TIME ZONE 'Europe/Moscow')::date >= :f"
        " GROUP BY 1"), {"c": chat_id, "k": kind, "f": base_from}).fetchall()
    by_day = {d: n for d, n in rows}
    today_n = by_day.pop(today, 0)
    ndays = (today - base_from).days
    base = [by_day.get(base_from + timedelta(days=i), 0) for i in range(ndays)]
    mean = sum(base) / ndays
    std = (sum((v - mean) ** 2 for v in base) / ndays) ** 0.5
    threshold = max(mean + 3 * std, ALERT_MIN_COUNT)
    if today_n < threshold:
        return
    prev = conn.execute(text(
        "SELECT count FROM tg_join_alerts WHERE chat_id = :c AND day = :d AND kind = :k"
    ), {"c": chat_id, "d": today, "k": kind}).scalar()
    if prev is not None and today_n < 2 * prev:
        return
    conn.execute(text(
        "INSERT INTO tg_join_alerts (chat_id, day, kind, count) VALUES (:c, :d, :k, :n)"
        " ON CONFLICT (chat_id, day, kind) DO UPDATE SET count = EXCLUDED.count, sent_at = now()"
    ), {"c": chat_id, "d": today, "k": kind, "n": today_n})

    title = _chat_title(conn, chat_id)
    if kind == "join":
        head = f"🚀 <b>Всплеск подписок в {title}</b>"
        verb = "вступило"
    else:
        head = f"📉 <b>Волна отписок в {title}</b>"
        verb = "отписалось"
    lines = [head, f"Сегодня {verb} <b>{today_n}</b>, обычно {mean:.1f} в день, максимум за {ndays} дн. {max(base)}."]
    if kind == "join":
        srcs = conn.execute(text(
            f"SELECT {_SRC_SQL} AS src, COUNT(*) FROM tg_channel_joins"
            " WHERE chat_id = :c AND event = 'join'"
            "   AND (event_at AT TIME ZONE 'Europe/Moscow')::date = :d"
            " GROUP BY 1 ORDER BY 2 DESC"), {"c": chat_id, "d": today}).fetchall()
        lines.append("")
        lines += [f"• <code>{src}</code>: {n}" for src, n in srcs]
    send(ADMIN_CHAT_ID, "\n".join(lines))


def hourly_tick():
    today = datetime.now(MSK).date()
    with engine.begin() as conn:
        for chat_id in _tracked_chats(conn):
            snapshot_members(conn, chat_id, today)
            for kind in ("join", "leave"):
                check_anomaly(conn, chat_id, today, kind)


# ─── Main loop ─────────────────────────────────────────────────────────────

def process_update(update):
    if "chat_member" in update:
        record_chat_member(update["chat_member"])
        return

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
        elif text.startswith("/joins"):
            arg = text.split()[1] if len(text.split()) > 1 else ""
            send(chat_id, cmd_joins(int(arg) if arg.isdigit() else 30))

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
        elif data == "joins":
            send(chat_id, cmd_joins(), main_keyboard())


def main():
    print(f"[{datetime.now()}] Bot started, admin={ADMIN_CHAT_ID}")
    ensure_joins_table()
    offset = None
    last_tick = 0.0
    while True:
        if time.time() - last_tick >= TICK_SEC:
            last_tick = time.time()
            try:
                hourly_tick()
            except Exception as e:
                print(f"Hourly tick error: {e}")
        try:
            # allowed_updates — JSON-массив; chat_member Телеграм не шлёт, пока его не попросить явно.
            params = {"timeout": 30,
                      "allowed_updates": json.dumps(["message", "callback_query", "chat_member"])}
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
