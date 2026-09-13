"""Связки → кандидаты завода постов: source='combo', status='draft_ready' сразу.

Сюжет-связку собирает signals/insights/combos.py (новость + находки детекторов + отчёт ЦБ,
≥2 разных источника). Дальше путь тот же, что у находок: писатель по находке
(content_ai.py, TRIGGER_ID_STEP_C_INSIGHT) — у него есть раздел про жанр «связка», — проверка
чисел кодом при приёмке (api/services/insight_check.py), бот ревью с графиком.

Два режима:
    python3 -m signals.combo_scan --mode data   # утром: сюжеты вечера последнего торгового дня
    python3 -m signals.combo_scan --mode news   # днём, каждые 20 минут: сюжет в момент новости
    ... --dry-run                               # что создал бы — без записи
    ... --mode news --at 2026-06-19T15:00Z      # как выглядел бы момент в прошлом (только с --dry-run)

Лимиты — чтобы связки не завалили ревью: утром до двух, днём до трёх новостных за день; одна
тема — не чаще раза в три дня; тема, о которой канал писал за двое суток, — пропуск.
"""
import os

# Хост ходит в Postgres через localhost (тот же приём, что в content_ai.py) — до импорта api.database.
_url = os.environ.get("DB_URL", "")
if "@db:" in _url:
    os.environ["DB_URL"] = _url.replace("@db:", "@127.0.0.1:")

import argparse  # noqa: E402
import json  # noqa: E402
from datetime import datetime, timezone  # noqa: E402

import pandas as pd  # noqa: E402
from sqlalchemy import text  # noqa: E402

from api.database import SessionLocal  # noqa: E402
from signals.insight_scan import detect_window  # noqa: E402
from signals.insights import cards, combos, data  # noqa: E402

MAX_DATA = 2            # утром — не больше двух связок
MAX_NEWS_DAY = 3        # днём — не больше трёх новостных связок за сутки
REPEAT_DAYS = 3         # тема у завода — не чаще раза в три дня
CHANNEL_DAYS = 2        # канал писал о том же за двое суток — пропуск
MEDIA_DIR = os.environ.get("CONTENT_MEDIA_DIR", "/opt/frame/data/content_media")
CACHE_DIR = os.environ.get("COMBO_CACHE_DIR", "/opt/frame/data/combo_cache")

_RECENT = text("""
    SELECT thread_key, reasoning, created_at FROM content_candidates
    WHERE source = 'combo' AND created_at > now() - interval '3 days'
""")
_INSERT = text("""
    INSERT INTO content_candidates
        (status, source, headline, raw_text, tickers, futures_ticker, event_type,
         importance_1_5, reasoning, media_filename, thread_key)
    VALUES ('draft_ready', 'combo', :headline, :raw_text, CAST(:tickers AS text[]), NULL,
            :event_type, 3, :reasoning, :media_filename, :thread_key)
    RETURNING id
""")


def detections(until: pd.Timestamp) -> list:
    """Находки детекторов за 30 дней — раз в торговый день: дневной режим зовётся каждые 20
    минут, а данные по вчерашний день за это время не меняются."""
    os.makedirs(CACHE_DIR, exist_ok=True)
    p = os.path.join(CACHE_DIR, f"detections_{until:%Y%m%d}.json")
    if os.path.exists(p):
        return json.load(open(p, encoding="utf-8"))
    items = detect_window(until)
    json.dump(items, open(p, "w", encoding="utf-8"), ensure_ascii=False,
              default=lambda o: o.item() if hasattr(o, "item") else str(o))
    return items


def channel_wrote(story: dict, now: pd.Timestamp) -> str | None:
    for d, txt in combos.channel_posts(now, days=CHANNEL_DAYS):
        if combos.buckets(txt) & set(story["buckets"]):
            return f"{d:%d.%m} «{txt.strip().splitlines()[0][:50]}»"
    return None


def run_once(mode: str, dry_run: bool = False, at: str | None = None) -> dict:
    now = pd.Timestamp(at) if at else pd.Timestamp(datetime.now(timezone.utc))
    now = now.tz_localize("UTC") if now.tzinfo is None else now
    oi = data.read("oi_daily", parse_dates=["tradedate"])
    until = oi.tradedate.max()
    eng = combos.Engine(detections(until))
    stories = eng.evening() if mode == "data" else eng.at(now)
    summary = {"mode": mode, "data_until": str(until.date()), "stories": len(stories), "created": 0}
    db = SessionLocal()
    try:
        recent = db.execute(_RECENT).fetchall()
        themes = {(r[0] or "").split(":")[1] for r in recent if (r[0] or "").startswith("combo:")}
        today = now.tz_convert("Europe/Moscow").date()
        news_today = sum(1 for r in recent if (r[1] or "").startswith("новость")
                         and pd.Timestamp(r[2]).date() >= today)
        limit = MAX_DATA if mode == "data" else max(0, MAX_NEWS_DAY - news_today)
        leads = set()
        for s in stories:
            if summary["created"] >= limit:
                break
            lead = s["legs"][0]
            # одна находка — один сюжет: шорт по доллару держал и «рубль», и «мировые активы»
            if lead["title"] in leads:
                print(f"[combo_scan] пропуск, та же главная нога уже в сюжете: {lead['title'][:70]}")
                continue
            if s["theme"] in themes:
                print(f"[combo_scan] пропуск, тема «{s['theme']}» у завода была за {REPEAT_DAYS} дня: {lead['title'][:70]}")
                continue
            wrote = channel_wrote(s, now.tz_convert("Europe/Moscow").tz_localize(None))
            if wrote:
                print(f"[combo_scan] пропуск, канал писал {wrote}: {lead['title'][:70]}")
                continue
            brief, card = combos.brief(s)
            head = f"{combos.THEME_RU.get(s['theme'], s['theme'])}: {combos.legible(lead['title'])}"[:300]
            themes.add(s["theme"])
            leads.add(lead["title"])
            summary["created"] += 1
            print(f"[combo_scan] {s['theme']} ({s['score']}): {head}")
            if dry_run:
                print(brief[:2500] + "\n")
                continue
            media = None
            if card:
                media = f"combo_{s['data_day'].replace('-', '')}_{s['theme']}.png"
                os.makedirs(MEDIA_DIR, exist_ok=True)
                cards.draw_chart(card, os.path.join(MEDIA_DIR, media))
            row = db.execute(_INSERT, {
                "headline": head, "raw_text": brief, "tickers": [lead["instrument"] or s["theme"]],
                "event_type": f"combo_{s['theme']}",
                "reasoning": ("новость" if s["is_news"] else "данные") + f": связка {'+'.join(s['families'])} "
                             f"(балл {s['score']})",
                "media_filename": media, "thread_key": f"combo:{s['theme']}:{s['day']}",
            }).first()
            db.commit()
            print(f"[combo_scan] кандидат {row[0]} создан")
    finally:
        db.close()
    return summary


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["data", "news"], required=True)
    ap.add_argument("--dry-run", action="store_true", help="показать сюжеты и карточки, ничего не писать")
    ap.add_argument("--at", help="момент для режима news в прошлом (ISO, UTC); только с --dry-run")
    a = ap.parse_args()
    if a.at and not a.dry_run:
        ap.error("--at только вместе с --dry-run")
    print(f"[combo_scan] {run_once(a.mode, dry_run=a.dry_run, at=a.at)}")
