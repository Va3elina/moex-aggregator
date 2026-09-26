"""
Подписчики Telegram-каналов по инвайт-ссылкам: запись join/leave из апдейтов
chat_member, часовой снимок числа подписчиков и уведомление админу о всплеске.

Живёт в signals/content_review_bot.py (host-side, через TELEGRAM_API_ROOT):
это единственный рабочий getUpdates-поллер @frameadminbot. Контейнер tg-bot
(tg_bot.py) до api.telegram.org с прода не достаёт — IPv6-only адрес без
маршрута из docker-сети, — поэтому логика здесь, а не там.

Таблицы: tg_channel_joins (миграция 105), tg_channel_members и tg_join_alerts
(106). Отчёт на сайте — GET /api/analytics/tg-joins, тот же _SRC_SQL.

Всплеск: сегодня >= среднее + 3σ по последним ALERT_BASELINE_DAYS полным дням
и не меньше ALERT_MIN_COUNT (иначе на пустой базе сработает первый же человек).
Пока истории меньше ALERT_MIN_HISTORY_DAYS, сравнивать не с чем — молчим.
Повтор за день — только если счётчик удвоился с прошлого уведомления.
"""
from datetime import datetime, timedelta, timezone

import requests
from sqlalchemy import text

# Москва без перехода на летнее время: фиксированный сдвиг надёжнее tzdata.
MSK = timezone(timedelta(hours=3))
TICK_SEC = 3600
ALERT_BASELINE_DAYS = 30
ALERT_MIN_HISTORY_DAYS = 7
ALERT_MIN_COUNT = 3
_MEMBER_STATUSES = {"member", "administrator", "creator"}
# Ссылка чужого админа приходит обрезанной (https://t.me/+AbCd...), ключ отчёта —
# название, иначе сам обрезок. Зеркало _TG_SRC_SQL в api/routers/analytics.py.
_SRC_SQL = "COALESCE(invite_name, invite_link, CASE WHEN via_folder THEN 'папка' ELSE 'без ссылки' END)"
_DAY_SQL = "(event_at AT TIME ZONE 'Europe/Moscow')::date"


def ensure_tables(conn) -> None:
    """Копия db/migrations/105 и 106: бот стартует и без ручной миграции."""
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


def _is_member(cm: dict) -> bool:
    st = cm.get("status")
    return st in _MEMBER_STATUSES or (st == "restricted" and bool(cm.get("is_member")))


def record_chat_member(conn, upd: dict) -> None:
    """Апдейт chat_member → строка join/leave. Прочие переходы (смена прав) пропускаем."""
    was, now = _is_member(upd["old_chat_member"]), _is_member(upd["new_chat_member"])
    if was == now:
        return
    link = upd.get("invite_link") or {}
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


def joins_report(conn, days: int = 30) -> str:
    """HTML для бота: вступления по ссылкам за N дней и сколько из них ещё в канале."""
    rows = conn.execute(text(f"""
        WITH j AS (
            SELECT chat_id, chat_title, user_id, event_at, {_SRC_SQL} AS src
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


def _tracked_chats(conn) -> list:
    return [r[0] for r in conn.execute(text(
        "SELECT chat_id FROM tg_channel_joins UNION SELECT chat_id FROM tg_channel_members"))]


def _chat_title(conn, api_base: str, chat_id: int) -> str:
    row = conn.execute(text(
        "SELECT chat_title FROM tg_channel_joins WHERE chat_id = :c AND chat_title IS NOT NULL"
        " ORDER BY event_at DESC LIMIT 1"), {"c": chat_id}).fetchone()
    if row:
        return row[0]
    r = requests.get(f"{api_base}/getChat", params={"chat_id": chat_id}, timeout=10).json()
    return (r.get("result") or {}).get("title") or str(chat_id)


def snapshot_members(conn, api_base: str, chat_id: int, today) -> None:
    r = requests.get(f"{api_base}/getChatMemberCount", params={"chat_id": chat_id}, timeout=10).json()
    if not r.get("ok"):
        return
    conn.execute(text(
        "INSERT INTO tg_channel_members (chat_id, day, members, taken_at)"
        " VALUES (:c, :d, :m, now())"
        " ON CONFLICT (chat_id, day) DO UPDATE SET members = EXCLUDED.members, taken_at = now()"
    ), {"c": chat_id, "d": today, "m": int(r["result"])})


def check_anomaly(conn, api_base: str, admin_chat_id: int, chat_id: int, today, kind: str) -> None:
    first = conn.execute(text(
        f"SELECT MIN({_DAY_SQL}) FROM tg_channel_joins WHERE chat_id = :c"), {"c": chat_id}).scalar()
    if not first or (today - first).days < ALERT_MIN_HISTORY_DAYS:
        return
    base_from = max(first, today - timedelta(days=ALERT_BASELINE_DAYS))
    rows = conn.execute(text(
        f"SELECT {_DAY_SQL} AS day, COUNT(*) FROM tg_channel_joins"
        f" WHERE chat_id = :c AND event = :k AND {_DAY_SQL} >= :f GROUP BY 1"
    ), {"c": chat_id, "k": kind, "f": base_from}).fetchall()
    by_day = {d: n for d, n in rows}
    today_n = by_day.pop(today, 0)
    ndays = (today - base_from).days
    base = [by_day.get(base_from + timedelta(days=i), 0) for i in range(ndays)]
    mean = sum(base) / ndays
    std = (sum((v - mean) ** 2 for v in base) / ndays) ** 0.5
    if today_n < max(mean + 3 * std, ALERT_MIN_COUNT):
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

    title = _chat_title(conn, api_base, chat_id)
    if kind == "join":
        head, verb = f"🚀 <b>Всплеск подписок в {title}</b>", "вступило"
    else:
        head, verb = f"📉 <b>Волна отписок в {title}</b>", "отписалось"
    lines = [head, f"Сегодня {verb} <b>{today_n}</b>, обычно {mean:.1f} в день, максимум за {ndays} дн. {max(base)}."]
    if kind == "join":
        srcs = conn.execute(text(
            f"SELECT {_SRC_SQL} AS src, COUNT(*) FROM tg_channel_joins"
            f" WHERE chat_id = :c AND event = 'join' AND {_DAY_SQL} = :d"
            " GROUP BY 1 ORDER BY 2 DESC"), {"c": chat_id, "d": today}).fetchall()
        lines.append("")
        lines += [f"• <code>{src}</code>: {n}" for src, n in srcs]
    send_html(api_base, admin_chat_id, "\n".join(lines))


def send_html(api_base: str, chat_id: int, html: str) -> None:
    requests.post(f"{api_base}/sendMessage",
                  data={"chat_id": chat_id, "text": html, "parse_mode": "HTML",
                        "disable_web_page_preview": True},
                  timeout=15)


def hourly_tick(conn, api_base: str, admin_chat_id: int) -> None:
    """Снимок подписчиков всех известных каналов и проверка всплесков за сегодня."""
    today = datetime.now(MSK).date()
    for chat_id in _tracked_chats(conn):
        snapshot_members(conn, api_base, chat_id, today)
        for kind in ("join", "leave"):
            check_anomaly(conn, api_base, admin_chat_id, chat_id, today, kind)
