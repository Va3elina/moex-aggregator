"""
MOEX-календарь корпоративных событий (закрытие реестра под дивиденды) →
content_candidates (см. db/migrations/022_content_candidates.sql). Полностью
структурированный источник (дата+тикер официальные, из MOEX) — пропускает Шаг А
content-пайплайна (AI-оценка релевантности не нужна, событие детерминированно
классифицируется кодом как значимое), сразу уходит в статус 'pending' и ждёт
сверки Шага Б (signals/content_match.py) с таблицей anomalies.

Источник: web.moex.com CSV-экспорт закрытий реестра ВСЕХ эмитентов вперёд
(кодировка Windows-1251!, проверено вручную 2026-07-13 — реальные будущие даты
на 2026 год). Тикер извлекается прямо из поля "Эмитент" (формат
'ПАО "НК "Роснефть" - 1-02-00122-A, ROSN [Акция обыкновенная]').

Матчится ТОЛЬКО против ticker_futures_map (db/migrations/023) — если тикера там
нет, событие пропускается (нет фьючерса → Шаг Б всё равно не сможет сверить с
anomalies). Дедуп — на уровне приложения (нет БД-констрейнта): проверка по
(source, futures_ticker, headline) перед INSERT, т.к. календарь при каждом
прогоне отдаёт ВСЕ предстоящие события заново, не только новые.

Запуск раз в сутки (данные не меняются внутри дня):
  /opt/frame/signals/moex_calendar_scan.sh   (cron, напр. 0 6 * * *)
"""
import csv
import io
import os
import re
from datetime import datetime, timedelta, timezone

import requests
from dotenv import load_dotenv
from sqlalchemy import text

# ── .env + DB_URL override ДО импорта api.database (host-side, как anomaly_scan) ──
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(_ROOT, ".env"))
_db = os.environ.get("DB_URL", "")
if "@db:" in _db:
    os.environ["DB_URL"] = _db.replace("@db:", "@127.0.0.1:")

import pipeline_heartbeat              # noqa: E402
from api.database import SessionLocal  # noqa: E402
from signals import config             # noqa: E402

CSV_URL = ("https://web.moex.com/moex-web-icdb-api/api/v1/export/"
           "register-closing-dates/csv?separator=2&language=1")
_UA = "Mozilla/5.0 (compatible; FrameBot/1.0; +https://framedata.ru)"
_TICKER_RE = re.compile(r",\s*([A-Z]{2,10})\s*\[")
HTTP_TIMEOUT = 20

_SELECT_MAP = text("SELECT stock_ticker, futures_sectype, display_name FROM ticker_futures_map")
_EXISTS = text("""
    SELECT 1 FROM content_candidates
    WHERE source = 'moex_calendar' AND futures_ticker = :ft AND headline = :headline
    LIMIT 1
""")
_INSERT = text("""
    INSERT INTO content_candidates
        (status, source, headline, raw_text, tickers, futures_ticker, event_type,
         importance_1_5, reasoning, thread_key, pending_expires_at, created_at, updated_at)
    VALUES
        ('pending', 'moex_calendar', :headline, :raw_text, ARRAY[:ticker]::text[],
         :futures_ticker, 'register_closing', 3, :reasoning, :thread_key,
         :pending_expires_at, now(), now())
""")


def _fetch_events() -> list[dict]:
    """Парсит CSV закрытий реестра. Кодировка Windows-1251 — НЕ UTF-8."""
    r = requests.get(CSV_URL, headers={"User-Agent": _UA}, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    raw = r.content.decode("windows-1251")
    reader = csv.reader(io.StringIO(raw), delimiter=";")
    next(reader, None)  # header: Эмитент;Дата события;Адрес сайта;Тип события
    events = []
    for row in reader:
        if len(row) < 4:
            continue
        name, date_str, _url, event_type = row[0], row[1], row[2], row[3]
        if "закрытие реестра" not in event_type.lower():
            continue
        m = _TICKER_RE.search(name)
        if not m:
            continue
        try:
            dt = datetime.strptime(date_str.strip(), "%m/%d/%Y %H:%M:%S")
        except ValueError:
            continue
        events.append({"ticker": m.group(1), "date": dt, "name": name})
    return events


def run_once(lookahead_days: int | None = None) -> dict:
    lookahead_days = lookahead_days or getattr(config, "CALENDAR_LOOKAHEAD_DAYS", 14)
    summary = {"fetched": 0, "matched": 0, "inserted": 0, "skipped_dup": 0, "errors": 0}

    db = SessionLocal()
    try:
        universe = {r[0]: (r[1], r[2]) for r in db.execute(_SELECT_MAP).fetchall()}
    finally:
        db.close()

    if not universe:
        summary["errors"] += 1
        print("[moex_calendar_scan] ticker_futures_map пуст — нечего матчить")
        return summary

    try:
        events = _fetch_events()
    except Exception as e:
        summary["errors"] += 1
        print(f"[moex_calendar_scan] fetch failed: {type(e).__name__}: {e}")
        return summary
    summary["fetched"] = len(events)

    now = datetime.now(timezone.utc)
    horizon = now + timedelta(days=lookahead_days)

    db = SessionLocal()
    try:
        for ev in events:
            if ev["ticker"] not in universe:
                continue
            ev_date = ev["date"].replace(tzinfo=timezone.utc)
            if not (now <= ev_date <= horizon):
                continue
            summary["matched"] += 1
            futures_sectype, display_name = universe[ev["ticker"]]
            date_label = ev["date"].strftime("%d.%m.%Y")
            headline = f"{display_name or ev['ticker']}: закрытие реестра {date_label}"
            reasoning = ("Календарное корпоративное событие (закрытие реестра под "
                         "дивиденды), дата и эмитент официальные (MOEX). Не требует "
                         "оценки ИИ — релевантно по построению.")
            try:
                if db.execute(_EXISTS, {"ft": futures_sectype, "headline": headline}).fetchone():
                    summary["skipped_dup"] += 1
                    continue
                db.execute(_INSERT, {
                    "headline": headline,
                    "raw_text": ev["name"],
                    "ticker": ev["ticker"],
                    "futures_ticker": futures_sectype,
                    "reasoning": reasoning,
                    "thread_key": f"calendar:{ev['ticker']}:{date_label}",
                    "pending_expires_at": ev_date + timedelta(days=5),
                })
                summary["inserted"] += 1
            except Exception as e:
                summary["errors"] += 1
                print(f"[moex_calendar_scan] insert failed for {ev['ticker']}: "
                      f"{type(e).__name__}: {e}")
        db.commit()
    except Exception as e:
        db.rollback()
        summary["errors"] += 1
        print(f"[moex_calendar_scan] fatal: {e}")
    finally:
        db.close()
    return summary


def main():
    t0 = datetime.now(timezone.utc)
    s = run_once()
    dur = (datetime.now(timezone.utc) - t0).total_seconds()
    print(f"[{datetime.now(timezone.utc)}] moex_calendar_scan: {s}")
    pipeline_heartbeat.record_pipeline_run(
        "content_moex_calendar", s["errors"] == 0, str(s), dur
    )


if __name__ == "__main__":
    main()
