"""
Шаг Б′ content-пайплайна: «аномалия сначала», обратное направление к обычному
потоку (новость → Шаг А → Шаг Б подтверждает данными). Найдено 2026-07-14
(Вадим): аномалии, для которых новость никогда не приходит через RSS/
MarketTwits (или приходит, но конвергенция её не поймала), сейчас НИКОГДА не
становятся постом — хотя данные реальные. Живой пример из архива — Газпром
06-07.07: сигнал ОИ обнаружен РАНЬШЕ новости на ~9 часов, но пост состоялся
только потому, что новость всё же пришла и её поймал RSS. Если бы не поймал —
сигнал бы просто потерялся.

БЕЗ ИИ, детерминированно — та же строгость, что и у прямого Шага Б, и та же
кросс-издательская логика, что уже проверена в rss_scan.py (не просто «ИИ
погуглил и поверил»): ищем упоминание компании минимум в
ANOMALY_FIRST_SOURCES_MIN независимых RSS-изданиях в окне вокруг даты сигнала.
Порог значимости аномалии — ВЫШЕ пассивного (мы сами идём искать повод, а не
подтверждаем уже пришедшую новость).

⚠️ v1: только RSS-подтверждение. WebSearch-фолбэк (если RSS ничего не нашёл)
— осознанно НЕ реализован в этом проходе, чтобы не размывать строгость
(единственный AI-поиск без структурной кросс-проверки — более слабое
доказательство, чем детерминированная RSS-конвергенция). Кандидат-расширение
на будущее, см. память content-pipeline-design.

Найдена подтверждённая аномалия → сразу draft_ready (у нас уже есть И данные,
И новость, Шаг А/Б не нужны) → сразу стреляет Шаг В (event-driven, как
content_match.py). Не нашлась — аномалия остаётся без объяснения, пост не
пишем (честный отказ, не выдумываем повод).

Запуск раз в час (в паре с content_match.py):
  /opt/frame/signals/anomaly_context_scan.sh   (cron, напр. 20 * * * *)
"""
import json
import os
import re
import time
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv
from sqlalchemy import text

# ── .env + DB_URL override ДО импорта api.database (host-side, как rss_scan) ──
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(_ROOT, ".env"))
_db = os.environ.get("DB_URL", "")
if "@db:" in _db:
    os.environ["DB_URL"] = _db.replace("@db:", "@127.0.0.1:")

import pipeline_heartbeat              # noqa: E402
from api.database import SessionLocal  # noqa: E402
from signals import config             # noqa: E402
from signals.rss_scan import RSS_FEEDS, _fetch_feed  # noqa: E402 — переиспользуем те же 12 источников
from signals.content_ai import (       # noqa: E402
    _fire, _step_c_payload, TRIGGER_ID_STEP_C,
)

# Найдено 2026-07-14 (session 3, см. rss_scan.py) — пауза между _fire() подряд
# в одном прогоне, иначе несколько облачных контейнеров запрашиваются
# одновременно и конкурируют за мощность аккаунта.
FIRE_STAGGER_SEC = 8

_SELECT_ORPHAN_ANOMALIES = text("""
    SELECT a.id, a.asset_id, a.asset_name, a.type, a.direction, a.severity_value,
           a.signal_date, a.headline, m.stock_ticker, m.display_name
    FROM anomalies a
    JOIN ticker_futures_map m ON m.futures_sectype = a.asset_id
    WHERE a.severity_value >= :ratio_min
      AND a.signal_date <= :max_signal_date
      AND a.signal_date >= :min_signal_date
      AND NOT EXISTS (
          SELECT 1 FROM content_candidates c WHERE c.matched_anomaly_id = a.id
      )
    ORDER BY a.severity_value DESC
""")

_EXISTS_CANDIDATE = text(
    "SELECT 1 FROM content_candidates WHERE source = 'anomaly_first' AND matched_anomaly_id = :aid LIMIT 1"
)
_INSERT_CANDIDATE = text("""
    INSERT INTO content_candidates
        (status, source, headline, raw_text, tickers, futures_ticker, match_type, event_type,
         importance_1_5, reasoning, matched_anomaly_id, thread_key, sources,
         created_at, updated_at)
    VALUES
        ('draft_ready', 'anomaly_first', :headline, :raw_text, :tickers, :futures_ticker, 'ticker',
         'other', 4, :reasoning, :anomaly_id, :thread_key, CAST(:sources AS JSONB),
         now(), now())
    RETURNING id
""")
_MARK_DISPATCHED = text("UPDATE content_candidates SET last_checked_at = now() WHERE id = :id")

_WORD_RE = re.compile(r"[А-ЯЁа-яё]{3,}")


def _stem(display_name: str) -> str:
    """Грубый стем под русские склонения — первые 5 символов названия
    компании (тот же приём, что 'Лукойл*'-wildcard из поста AlgoPack,
    зафиксированного в памяти content-pipeline-design)."""
    word = display_name.strip().split()[0] if display_name.strip() else ""
    return word[:5].lower()


def _mentions_company(title: str, stem: str) -> bool:
    if not stem:
        return False
    for w in _WORD_RE.findall(title):
        if w.lower().startswith(stem):
            return True
    return False


def _find_confirming_articles(stem: str, window_start: datetime, window_end: datetime) -> list[dict]:
    """Живой фетч тех же 12 RSS-источников, ищем упоминание компании (по стему)
    в заголовке, в окне вокруг даты сигнала. Возвращает СЫРЫЕ совпадения (не
    сгруппированные) — вызывающий код сам считает distinct-издателей."""
    matches = []
    for family, url in RSS_FEEDS:
        for item in _fetch_feed(family, url):
            if not (window_start <= item["dt"] <= window_end):
                continue
            if _mentions_company(item["title"], stem):
                matches.append(item)
    return matches


def run_once() -> dict:
    summary = {"orphans_checked": 0, "confirmed": 0, "unconfirmed": 0,
               "promoted": 0, "step_c_fired": 0, "step_c_fire_errors": 0, "errors": 0}

    internal_token = os.environ.get("CONTENT_AI_INTERNAL_TOKEN", "")
    token_c = os.environ.get("CLAUDE_ROUTINE_FIRE_TOKEN_STEP_C", "")
    can_fire = bool(internal_token and token_c)

    now = datetime.now(timezone.utc)
    max_signal_date = (now - timedelta(hours=config.ANOMALY_FIRST_MIN_AGE_HOURS)).date()
    min_signal_date = (now - timedelta(days=config.ANOMALY_FIRST_MAX_AGE_DAYS)).date()

    db = SessionLocal()
    try:
        orphans = db.execute(_SELECT_ORPHAN_ANOMALIES, {
            "ratio_min": config.ANOMALY_FIRST_RATIO_MIN,
            "max_signal_date": max_signal_date, "min_signal_date": min_signal_date,
        }).mappings().all()

        for a in orphans:
            summary["orphans_checked"] += 1
            try:
                if db.execute(_EXISTS_CANDIDATE, {"aid": a["id"]}).fetchone():
                    continue  # уже обработана в прошлом прогоне (страховка от гонки)

                stem = _stem(a["display_name"] or a["stock_ticker"])
                signal_dt = datetime.combine(a["signal_date"], datetime.min.time(), tzinfo=timezone.utc)
                window_start = signal_dt - timedelta(days=config.ANOMALY_FIRST_NEWS_WINDOW_DAYS)
                window_end = signal_dt + timedelta(days=config.ANOMALY_FIRST_NEWS_WINDOW_DAYS)

                raw_matches = _find_confirming_articles(stem, window_start, window_end)
                families = {m["family"] for m in raw_matches}
                if len(families) < config.ANOMALY_FIRST_SOURCES_MIN:
                    summary["unconfirmed"] += 1
                    continue

                summary["confirmed"] += 1
                raw_matches.sort(key=lambda m: m["dt"])
                earliest = raw_matches[0]
                sources = [
                    {"publisher": m["family"], "url": m["link"],
                     "published_at": m["dt"].isoformat(), "title": m["title"]}
                    for m in raw_matches
                ]
                reasoning = (
                    f"Аномалия ОИ обнаружена без предшествующей новости (anomaly-first): "
                    f"{a['asset_name']} (×{a['severity_value']}, {a['signal_date']}). "
                    f"Подтверждено {len(families)} независимыми изданиями: "
                    + ", ".join(sorted(families))
                )
                thread_key = f"anomaly_first:{a['stock_ticker']}:{a['id']}"

                new_id = db.execute(_INSERT_CANDIDATE, {
                    "headline": earliest["title"], "raw_text": earliest["desc"] or earliest["title"],
                    "tickers": [a["stock_ticker"]], "futures_ticker": a["asset_id"],
                    "reasoning": reasoning, "anomaly_id": a["id"], "thread_key": thread_key,
                    "sources": json.dumps(sources, ensure_ascii=False),
                }).scalar()
                db.commit()
                summary["promoted"] += 1

                if can_fire:
                    try:
                        payload_row = {
                            "id": new_id, "headline": earliest["title"], "tickers": [a["stock_ticker"]],
                            "event_type": "other", "reasoning": reasoning,
                            "asset_id": a["asset_id"], "asset_name": a["asset_name"],
                            "anomaly_type": a["type"], "direction": a["direction"],
                            "severity_value": a["severity_value"], "signal_date": a["signal_date"],
                            "anomaly_headline": a["headline"],
                        }
                        _fire(TRIGGER_ID_STEP_C, token_c, _step_c_payload(payload_row, internal_token))
                        db.execute(_MARK_DISPATCHED, {"id": new_id})
                        db.commit()
                        summary["step_c_fired"] += 1
                        time.sleep(FIRE_STAGGER_SEC)  # см. FIRE_STAGGER_SEC выше
                    except Exception as e:
                        summary["step_c_fire_errors"] += 1
                        print(f"[anomaly_context_scan] step-c fire failed for candidate "
                              f"{new_id}: {type(e).__name__}: {e}")
            except Exception as e:
                db.rollback()
                summary["errors"] += 1
                print(f"[anomaly_context_scan] anomaly {a['id']} failed: {type(e).__name__}: {e}")
    except Exception as e:
        db.rollback()
        summary["errors"] += 1
        print(f"[anomaly_context_scan] fatal: {e}")
    finally:
        db.close()
    return summary


def main():
    t0 = datetime.now(timezone.utc)
    s = run_once()
    dur = (datetime.now(timezone.utc) - t0).total_seconds()
    print(f"[{datetime.now(timezone.utc)}] anomaly_context_scan: {s}")
    pipeline_heartbeat.record_pipeline_run(
        "content_anomaly_context", s["errors"] == 0, str(s), dur
    )


if __name__ == "__main__":
    main()
