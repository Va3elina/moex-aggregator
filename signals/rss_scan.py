"""
RSS-сборщик + кросс-издательская конвергенция → content_candidates (status
'candidate'). БЕЗ ИИ — чистая детерминированная логика, ровно та же, что живьём
проверена вручную 2026-07-13 на 613 реальных заголовках (18 реальных совпадений
за неделю, ложные срабатывания отсеяны окном/порогом ниже).

Конвергенция = ≥CONTENT_WORD_OVERLAP_MIN значимых слов пересечения между статьями
РАЗНЫХ издателей (family = домен, не отдельный RSS-feed — Ведомости дублирует одну
статью в finance+economics с идентичным pubDate, это НЕ конвергенция) в окне
≤CONTENT_WINDOW_HOURS часов. Каждый прогон делает полный свежий фетч всех фидов
(они и так покрывают часы/дни истории) — отдельная таблица "сырых" RSS-айтемов не
нужна, конвергенция считается заново на актуальном срезе.

thread_key НЕ проставляется здесь (RSS ещё не знает тикер — это результат Шага А),
остаётся NULL до обработки content_ai.py (Этап 8). Дедуп повторных прогонов —
по точному совпадению headline (как moex_calendar_scan.py).

Запуск каждые 5-15 минут:
  /opt/frame/signals/rss_scan.sh   (cron, напр. */10 * * * *)
"""
import json
import os
import re
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from itertools import combinations
from urllib.parse import urlparse
from xml.etree import ElementTree as ET

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
from signals.content_ai import (       # noqa: E402
    _fire, _step_a_payload, _known_tickers_line, _known_categories_line, TRIGGER_ID_STEP_A,
)

_UA = "Mozilla/5.0 (compatible; FrameBot/1.0; +https://xn--80aklbnczmv.xn--p1ai)"
HTTP_TIMEOUT = 15

# Проверено вживую 2026-07-13. family — домен издателя (для дедупа самодублей
# одного СМИ по нескольким рубрикам, см. докстрока выше).
RSS_FEEDS = [
    ("kommersant", "https://www.kommersant.ru/RSS/section-business.xml"),
    ("kommersant", "https://www.kommersant.ru/RSS/section-economics.xml"),
    ("vedomosti", "https://www.vedomosti.ru/rss/rubric/finance.xml"),
    ("vedomosti", "https://www.vedomosti.ru/rss/rubric/economics.xml"),
    ("vedomosti", "https://www.vedomosti.ru/rss/rubric/business/energy.xml"),
    ("finam", "https://www.finam.ru/analysis/conews/rsspoint/"),
    ("investing", "https://ru.investing.com/rss/stock.rss"),
    ("investing", "https://ru.investing.com/rss/news_12.rss"),
    ("frankmedia", "https://frankmedia.ru/feed"),
    ("smartlab", "https://smart-lab.ru/news/rss/"),
    ("interfax", "https://www.interfax.ru/rss.asp"),
    ("rbc", "https://rssexport.rbc.ru/rbcnews/news/30/full.rss"),
    ("tass", "https://tass.ru/rss/v2.xml"),
    ("forbes", "https://forbes.ru/newrss.xml"),
    ("finmarket", "https://www.finmarket.ru/rss/mainnews.asp"),
    ("ria", "https://ria.ru/export/rss2/index.xml"),
    ("prime", "https://1prime.ru/export/rss2/index.xml"),
]

_STOP = set(
    "в на из за со к по от до для не что как это его её их а и или но при об о у "
    "же ли ещё уже быть был была было были меж между при про без через над под "
    "который которая которые также этот эта эти всё все"
    .split()
)
_WORD_RE = re.compile(r"[А-ЯЁа-яё]{4,}")


def _sig_words(title: str) -> set:
    return {w.lower() for w in _WORD_RE.findall(title) if w.lower() not in _STOP}


def _fetch_feed(family: str, url: str) -> list[dict]:
    items = []
    try:
        r = requests.get(url, headers={"User-Agent": _UA}, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        root = ET.fromstring(r.content)
        for it in root.findall(".//item"):
            title = (it.findtext("title") or "").strip()
            pub = (it.findtext("pubDate") or "").strip()
            link = (it.findtext("link") or "").strip()
            desc = (it.findtext("description") or "").strip()
            if not title or not pub:
                continue
            try:
                dt = parsedate_to_datetime(pub)
            except (TypeError, ValueError):
                continue
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            items.append({
                "family": family, "title": title, "dt": dt, "link": link,
                "desc": desc[:400], "sig": _sig_words(title),
            })
    except Exception as e:
        print(f"[rss_scan] {family} ({urlparse(url).netloc}) failed: {type(e).__name__}: {e}")
    return items


_EXISTS = text("SELECT 1 FROM content_candidates WHERE source = 'rss' AND headline = :headline LIMIT 1")
_INSERT = text("""
    INSERT INTO content_candidates
        (status, source, headline, raw_text, tickers, sources, created_at, updated_at)
    VALUES
        ('candidate', 'rss', :headline, :raw_text, ARRAY[]::text[],
         CAST(:sources AS JSONB), now(), now())
    RETURNING id
""")
_MARK_DISPATCHED = text("UPDATE content_candidates SET last_checked_at = now() WHERE id = :id")


def _find_convergent_groups(items: list[dict]) -> list[list[dict]]:
    """Пары items из РАЗНЫХ family с пересечением >=WORD_OVERLAP_MIN слов в окне
    <=WINDOW_HOURS -> объединяем транзитивно (union-find) в группы конвергенции."""
    parent = list(range(len(items)))

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[ra] = rb

    window = config.CONTENT_WINDOW_HOURS
    min_overlap = config.CONTENT_WORD_OVERLAP_MIN
    for i, j in combinations(range(len(items)), 2):
        a, b = items[i], items[j]
        if a["family"] == b["family"]:
            continue
        delta_h = abs((a["dt"] - b["dt"]).total_seconds()) / 3600
        if delta_h > window:
            continue
        if len(a["sig"] & b["sig"]) >= min_overlap:
            union(i, j)

    groups: dict[int, list[dict]] = {}
    for idx, item in enumerate(items):
        groups.setdefault(find(idx), []).append(item)
    return [g for g in groups.values() if len({it["family"] for it in g}) >= 2]


def run_once() -> dict:
    summary = {"feeds_ok": 0, "feeds_failed": 0, "items": 0,
               "convergent_groups": 0, "inserted": 0, "skipped_dup": 0, "errors": 0,
               "step_a_fired": 0, "step_a_fire_errors": 0}

    all_items: list[dict] = []
    for family, url in RSS_FEEDS:
        items = _fetch_feed(family, url)
        if items:
            summary["feeds_ok"] += 1
        else:
            summary["feeds_failed"] += 1
        all_items.extend(items)
    summary["items"] = len(all_items)

    groups = _find_convergent_groups(all_items)
    summary["convergent_groups"] = len(groups)

    # Событийный вызов Шага А: сразу после вставки НОВОГО кандидата, а не
    # ожидая отдельный крон-поллер (content_ai.py остаётся редкой подстраховкой
    # на случай, если этот вызов не долетел — см. его докстроку). Токены/список
    # тикеров читаем ОДИН раз на весь прогон, не на каждую вставку.
    internal_token = os.environ.get("CONTENT_AI_INTERNAL_TOKEN", "")
    token_a = os.environ.get("CLAUDE_ROUTINE_FIRE_TOKEN_STEP_A", "")
    can_fire = bool(internal_token and token_a)
    if not can_fire:
        print("[rss_scan] CONTENT_AI_INTERNAL_TOKEN / CLAUDE_ROUTINE_FIRE_TOKEN_STEP_A "
              "не заданы — новые кандидаты вставятся, но Шаг А не будет вызван сразу "
              "(дождутся content_ai.py по расписанию)")

    db = SessionLocal()
    try:
        known_tickers = _known_tickers_line(db) if can_fire else ""
        known_categories = _known_categories_line(db) if can_fire else ""
        for group in groups:
            group.sort(key=lambda it: it["dt"])
            earliest = group[0]
            headline = earliest["title"]
            try:
                if db.execute(_EXISTS, {"headline": headline}).fetchone():
                    summary["skipped_dup"] += 1
                    continue
                sources = [
                    {"publisher": it["family"], "url": it["link"],
                     "published_at": it["dt"].isoformat(), "title": it["title"]}
                    for it in group
                ]
                raw_text = earliest["desc"] or headline
                new_id = db.execute(_INSERT, {
                    "headline": headline,
                    "raw_text": raw_text,
                    "sources": json.dumps(sources, ensure_ascii=False),
                }).scalar()
                db.commit()  # ДО fire — Routine PATCH'ит через отдельное соединение (API), строка должна уже существовать
                summary["inserted"] += 1

                if can_fire:
                    try:
                        payload = _step_a_payload(
                            {"id": new_id, "source": "rss", "headline": headline, "raw_text": raw_text},
                            internal_token, known_tickers, known_categories,
                        )
                        _fire(TRIGGER_ID_STEP_A, token_a, payload)
                        db.execute(_MARK_DISPATCHED, {"id": new_id})
                        db.commit()
                        summary["step_a_fired"] += 1
                    except Exception as e:
                        summary["step_a_fire_errors"] += 1
                        print(f"[rss_scan] step-a fire failed for candidate {new_id}: "
                              f"{type(e).__name__}: {e}")
            except Exception as e:
                db.rollback()
                summary["errors"] += 1
                print(f"[rss_scan] insert failed for {headline!r}: {type(e).__name__}: {e}")
    except Exception as e:
        db.rollback()
        summary["errors"] += 1
        print(f"[rss_scan] fatal: {e}")
    finally:
        db.close()
    return summary


def main():
    t0 = datetime.now(timezone.utc)
    s = run_once()
    dur = (datetime.now(timezone.utc) - t0).total_seconds()
    print(f"[{datetime.now(timezone.utc)}] rss_scan: {s}")
    pipeline_heartbeat.record_pipeline_run(
        "content_rss_scan", s["errors"] == 0, str(s), dur
    )


if __name__ == "__main__":
    main()
