"""
Шаг Б content-пайплайна: детерминированная сверка кандидатов (status='pending')
с таблицей anomalies. БЕЗ ИИ — чистый SQL-джойн. См. db/migrations/022 для
state machine и signals/moex_calendar_scan.py / content_ai.py для соседних шагов.

Совпадение (asset_id=futures_ticker, signal_date в окне [created_at-1 …
created_at+CONTENT_PENDING_DAYS дней], аномалия ЕЩЁ НЕ использована этим же
thread_key) → status='draft_ready', matched_anomaly_id заполняется. Истёк
pending_expires_at без совпадения → status='no_data'.

Granularity-осознанность (измерено вживую 2026-07-13: Полюс/PX — только дневной
снэпшот ОИ, лаг обнаружения сигнала ~21ч от новости; блю-чипы вроде GAZPF/SBERF —
интрадей каждые 5-60 мин): для активов БЕЗ интрадей-данных (has_intraday_oi=False)
повторная проверка чаще раза в сутки бессмысленна — new данных всё равно не
появится до следующего EOD-снэпшота. last_checked_at не даёт лишний раз дёргать
такие кандидаты в рамках одного дня.

Запуск раз в час (в такт anomaly_scan, чуть позже — интрадей-активы это отрабатывают
полностью; дневные-only сами себя троттлят через last_checked_at):
  /opt/frame/signals/content_match.sh   (cron, напр. 15 * * * *)
"""
import os
import time
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv
from sqlalchemy import text

# ── .env + DB_URL override ДО импорта api.database (host-side, как anomaly_scan) ──
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(_ROOT, ".env"))
_db = os.environ.get("DB_URL", "")
if "@db:" in _db:
    os.environ["DB_URL"] = _db.replace("@db:", "@127.0.0.1:")

import pipeline_heartbeat                  # noqa: E402
from api.database import SessionLocal      # noqa: E402
from signals import config                 # noqa: E402
from signals.db import has_intraday_oi     # noqa: E402
from signals.content_ai import (           # noqa: E402
    _fire, _step_c_payload, TRIGGER_ID_STEP_C,
)

# Найдено 2026-07-14 (session 3, см. rss_scan.py) — пауза между _fire() подряд
# в одном прогоне, иначе несколько облачных контейнеров запрашиваются
# одновременно и конкурируют за мощность аккаунта.
FIRE_STAGGER_SEC = 8

_SELECT_PENDING = text("""
    SELECT id, futures_ticker, category, thread_key, created_at, pending_expires_at,
           last_checked_at, headline, tickers, event_type, reasoning
    FROM content_candidates
    WHERE status = 'pending' AND (futures_ticker IS NOT NULL OR category IS NOT NULL)
""")

_ALREADY_USED = text("""
    SELECT matched_anomaly_id FROM content_candidates
    WHERE thread_key = :thread_key AND matched_anomaly_id IS NOT NULL
""")

# ASC, не DESC: берём САМОЕ РАННЕЕ ещё не использованное совпадение — это первое
# реальное подтверждение новости данными. Более поздние аномалии на том же
# тикере (повторный сигнал, как второй всплеск Полюса ×9.22 через 2 дня после
# первого ×24.43) НЕ должны "перепрыгивать" через более раннее подтверждение —
# они достаются watch-thread'у как follow-up к уже готовому черновику, не как
# замена ему.
_FIND_MATCH = text("""
    SELECT id, signal_date, severity_value, direction, headline, asset_id, asset_name, type
    FROM anomalies
    WHERE asset_id = :futures_ticker
      AND signal_date BETWEEN :date_from AND :date_to
      AND id != ALL(:exclude_ids)
    ORDER BY signal_date ASC, id ASC
    LIMIT 1
""")

# Категорийный fallback (найдено 2026-07-14, session 3, db/migrations/027) — когда
# новость не называет конкретную компанию, но Шаг А определил тему (category).
# Тут, В ОТЛИЧИЕ от _FIND_MATCH, разные строки — это РАЗНЫЕ активы (не повторные
# сигналы одного и того же), поэтому порядок другой: берём САМУЮ СИЛЬНУЮ аномалию
# в категории (severity_value DESC), а не самую раннюю — это самый заметный повод
# для поста, а не "первое подтверждение" (нечего подтверждать — конкретный актив
# новостью не назван).
_FIND_MATCH_BY_CATEGORY = text("""
    SELECT a.id, a.signal_date, a.severity_value, a.direction, a.headline,
           a.asset_id, a.asset_name, a.type
    FROM anomalies a
    JOIN asset_category_map m ON m.sectype = a.asset_id
    WHERE m.category = :category
      AND a.signal_date BETWEEN :date_from AND :date_to
      AND a.id != ALL(:exclude_ids)
    ORDER BY a.severity_value DESC, a.signal_date ASC, a.id ASC
    LIMIT 1
""")

_MARK_DRAFT_READY = text("""
    UPDATE content_candidates
    SET status = 'draft_ready', matched_anomaly_id = :anomaly_id, match_type = :match_type,
        last_checked_at = now(), updated_at = now()
    WHERE id = :id
""")

_MARK_CHECKED = text("""
    UPDATE content_candidates SET last_checked_at = now() WHERE id = :id
""")

_MARK_NO_DATA = text("""
    UPDATE content_candidates
    SET status = 'no_data', last_checked_at = now(), updated_at = now()
    WHERE id = :id
""")


def run_once() -> dict:
    summary = {"checked": 0, "skipped_daily_already_checked": 0,
               "matched": 0, "expired": 0, "still_pending": 0, "errors": 0,
               "step_c_fired": 0, "step_c_fire_errors": 0}

    internal_token = os.environ.get("CONTENT_AI_INTERNAL_TOKEN", "")
    token_c = os.environ.get("CLAUDE_ROUTINE_FIRE_TOKEN_STEP_C", "")
    can_fire = bool(internal_token and token_c)
    if not can_fire:
        print("[content_match] CONTENT_AI_INTERNAL_TOKEN / CLAUDE_ROUTINE_FIRE_TOKEN_STEP_C "
              "не заданы — draft_ready проставится, но Шаг В не будет вызван сразу "
              "(дождётся content_ai.py по расписанию)")

    db = SessionLocal()
    try:
        pending = db.execute(_SELECT_PENDING).mappings().all()
        now = datetime.now(timezone.utc)

        for row in pending:
            try:
                ft = row["futures_ticker"]
                category = row["category"]

                # Дневные-only активы: не проверять повторно чаще раза в сутки —
                # новых данных всё равно не будет до следующего EOD-снэпшота.
                # Категорийные кандидаты (ft отсутствует) в эту логику не попадают —
                # категория обычно покрывает несколько активов сразу, среди которых
                # почти всегда есть интрадей, троттлить нечем.
                if ft and not has_intraday_oi(ft):
                    lc = row["last_checked_at"]
                    if lc and lc.astimezone(timezone.utc).date() == now.date():
                        summary["skipped_daily_already_checked"] += 1
                        continue

                summary["checked"] += 1

                used = [r[0] for r in db.execute(
                    _ALREADY_USED, {"thread_key": row["thread_key"]}
                ).fetchall()] if row["thread_key"] else []
                if not used:
                    used = [0]  # ALL(:exclude_ids) на пустом массиве не работает как ожидается

                created = row["created_at"]
                date_from = created.date() - timedelta(days=1)
                date_to = created.date() + timedelta(days=config.CONTENT_PENDING_DAYS)

                if ft:
                    match = db.execute(_FIND_MATCH, {
                        "futures_ticker": ft, "date_from": date_from, "date_to": date_to,
                        "exclude_ids": used,
                    }).mappings().first()
                    match_type = "ticker"
                else:
                    match = db.execute(_FIND_MATCH_BY_CATEGORY, {
                        "category": category, "date_from": date_from, "date_to": date_to,
                        "exclude_ids": used,
                    }).mappings().first()
                    match_type = "category"

                if match:
                    db.execute(_MARK_DRAFT_READY, {
                        "id": row["id"], "anomaly_id": match["id"], "match_type": match_type,
                    })
                    db.commit()  # ДО fire — Routine PATCH'ит через отдельное соединение (API)
                    summary["matched"] += 1

                    if can_fire:
                        try:
                            payload_row = {
                                "id": row["id"], "headline": row["headline"],
                                "tickers": row["tickers"], "event_type": row["event_type"],
                                "reasoning": row["reasoning"],
                                "category": category, "match_type": match_type,
                                "asset_id": match["asset_id"], "asset_name": match["asset_name"],
                                "anomaly_type": match["type"], "direction": match["direction"],
                                "severity_value": match["severity_value"],
                                "signal_date": match["signal_date"],
                                "anomaly_headline": match["headline"],
                            }
                            _fire(TRIGGER_ID_STEP_C, token_c,
                                  _step_c_payload(payload_row, internal_token))
                            summary["step_c_fired"] += 1
                            time.sleep(FIRE_STAGGER_SEC)  # см. FIRE_STAGGER_SEC выше
                        except Exception as e:
                            summary["step_c_fire_errors"] += 1
                            print(f"[content_match] step-c fire failed for candidate "
                                  f"{row['id']}: {type(e).__name__}: {e}")
                    continue

                expires = row["pending_expires_at"]
                if expires and expires <= now:
                    db.execute(_MARK_NO_DATA, {"id": row["id"]})
                    summary["expired"] += 1
                else:
                    db.execute(_MARK_CHECKED, {"id": row["id"]})
                    summary["still_pending"] += 1
            except Exception as e:
                summary["errors"] += 1
                print(f"[content_match] candidate {row['id']} failed: {type(e).__name__}: {e}")

        db.commit()
    except Exception as e:
        db.rollback()
        summary["errors"] += 1
        print(f"[content_match] fatal: {e}")
    finally:
        db.close()
    return summary


def main():
    t0 = datetime.now(timezone.utc)
    s = run_once()
    dur = (datetime.now(timezone.utc) - t0).total_seconds()
    print(f"[{datetime.now(timezone.utc)}] content_match: {s}")
    pipeline_heartbeat.record_pipeline_run(
        "content_match", s["errors"] == 0, str(s), dur
    )


if __name__ == "__main__":
    main()
