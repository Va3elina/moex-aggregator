"""Настройки сигналов @framesignalbot (Вадим 18.11.2026): что присылать, кого не присылать, тихие часы.

Где живут: мини-приложение /tg → экран «Сигналы» пишет их в localStorage под ключом PREFS_KEY, а штатная синхронизация
настроек сайта (services/settingsSync.ts: всё под «frame:» уезжает на сервер) кладёт их в user_settings.data. Бот читает
их по получателю: users.telegram_chat_id = чат → user_settings.data[PREFS_KEY] (строка JSON). Нет настроек — шлём всё.

Формат (всё необязательное, по умолчанию «включено»):
    {"on": true,
     "types": {"oi_record": {"on": true, "min": "3m", "markets": ["Акции", ...], "modes": ["positions", "participants"]},
               "funds_flow": {"on": true, "min": "3m"}, "funds_streak": {"on": true}, "season": {"on": true},
               "strength": {"on": true}, "buffett": {"on": true}, "fund_trades": {"on": true}, "cbr_flows": {"on": true}},
     "hidden": ["oi:SR", "se:SBER"],
     "quiet": {"on": false, "from": "23:00", "to": "08:00"}}
Тихие часы не глушат сигнал, а присылают его без звука (по Москве).
"""
from __future__ import annotations

import datetime as dt
import json
import os
import urllib.parse

PREFS_KEY = "frame:signals:prefs"
# виды сигналов — те же ключи у мини-приложения (frontend/src/pages/tg/TgSignalsApp.tsx, SIGNAL_TYPES)
TYPES = ("oi_record", "funds_flow", "funds_streak", "season", "strength", "buffett", "fund_trades", "cbr_flows")
# порог окна рекорда: «рекорд не короче» → ранг окна (как signals.record_notify.WINDOWS)
MIN_WINDOW = {"3m": "за 3 месяца", "6m": "за полгода", "1y": "за год", "all": "за всё время"}
WINDOW_DAYS = {"за 3 месяца": 91, "за полгода": 182, "за год": 365, "за 2 года": 730, "за 3 года": 1095,
               "за 5 лет": 1825, "за всё время": 10 ** 6}
MSK = dt.timezone(dt.timedelta(hours=3))


def load(chats: list) -> dict:
    """{chat_id (строка): настройки} для получателей, у кого они есть; сбой базы — пусто (шлём всё)."""
    if not chats:
        return {}
    try:
        from sqlalchemy import text
        url = os.environ.get("DB_URL", "")
        if "@db:" in url:
            os.environ["DB_URL"] = url.replace("@db:", "@127.0.0.1:")
        from api.database import SessionLocal
        db = SessionLocal()
        try:
            rows = db.execute(text("""
                SELECT u.telegram_chat_id, s.data ->> :k FROM users u JOIN user_settings s ON s.user_id = u.id
                WHERE u.telegram_chat_id = ANY(:c)"""), {"k": PREFS_KEY, "c": [int(c) for c in chats]}).all()
        finally:
            db.close()
    except Exception as e:  # noqa: BLE001 — без настроек лучше, чем без сигнала
        print(f"[signal_prefs] настройки не прочитаны: {type(e).__name__}: {e}")
        return {}
    out = {}
    for chat, raw in rows:
        try:
            out[str(chat)] = json.loads(raw) if raw else {}
        except (TypeError, ValueError):
            out[str(chat)] = {}
    return out


def _quiet(q: dict | None, now: dt.datetime) -> bool:
    if not q or not q.get("on"):
        return False
    try:
        f = dt.time.fromisoformat(q.get("from", "23:00"))
        t = dt.time.fromisoformat(q.get("to", "08:00"))
    except ValueError:
        return False
    cur = now.astimezone(MSK).time()
    return (f <= cur or cur < t) if f > t else (f <= cur < t)


def decide(p: dict | None, meta: dict | None, now: dt.datetime | None = None) -> str | None:
    """Что делать с сигналом для получателя: None — не слать, "silent" — без звука, "sound" — как есть.
    meta: {"type": вид, "asset": "oi:SR" | "se:SBER" | None, "window": «за год» | None, "market": «Акции» | None,
           "mode": "positions" | "participants" | None}."""
    if not meta or not p:
        return "sound"
    if p.get("on") is False:
        return None
    t = (p.get("types") or {}).get(meta.get("type") or "", {})
    if t.get("on") is False:
        return None
    if meta.get("asset") and meta["asset"] in set(p.get("hidden") or []):
        return None
    w, mn = meta.get("window"), t.get("min")
    if w and mn in MIN_WINDOW and WINDOW_DAYS.get(w, 0) < WINDOW_DAYS[MIN_WINDOW[mn]]:
        return None
    if meta.get("market") and t.get("markets") is not None and meta["market"] not in t["markets"]:
        return None
    if meta.get("mode") and t.get("modes") is not None and meta["mode"] not in t["modes"]:
        return None
    return "silent" if _quiet(p.get("quiet"), now or dt.datetime.now(MSK)) else "sound"


def settings_url(site: str, meta: dict | None) -> str:
    """Ссылка мини-приложения на экран сигналов; «откуда пришли» — чтобы сверху предложить скрыть этот актив."""
    q = {"s": "signals"}
    if meta and meta.get("type"):
        q["from"] = meta["type"]
        if meta.get("asset"):
            q["asset"] = meta["asset"]
            if meta.get("name"):
                q["name"] = meta["name"]
    return f"{site}/tg?" + urllib.parse.urlencode(q)
