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
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv
from sqlalchemy import text

# ── .env + DB_URL override ДО импорта api.database (host-side, как anomaly_scan) ──
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(_ROOT, ".env"))
_db = os.environ.get("DB_URL", "")
if "@db:" in _db:
    os.environ["DB_URL"] = _db.replace("@db:", "@127.0.0.1:")

from api.database import SessionLocal      # noqa: E402
from signals import config                 # noqa: E402
from signals.db import has_intraday_oi     # noqa: E402

_SELECT_PENDING = text("""
    SELECT id, futures_ticker, thread_key, created_at, pending_expires_at, last_checked_at
    FROM content_candidates
    WHERE status = 'pending' AND futures_ticker IS NOT NULL
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
    SELECT id, signal_date, severity_value, direction, headline
    FROM anomalies
    WHERE asset_id = :futures_ticker
      AND signal_date BETWEEN :date_from AND :date_to
      AND id != ALL(:exclude_ids)
    ORDER BY signal_date ASC, id ASC
    LIMIT 1
""")

_MARK_DRAFT_READY = text("""
    UPDATE content_candidates
    SET status = 'draft_ready', matched_anomaly_id = :anomaly_id,
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
               "matched": 0, "expired": 0, "still_pending": 0, "errors": 0}

    db = SessionLocal()
    try:
        pending = db.execute(_SELECT_PENDING).mappings().all()
        now = datetime.now(timezone.utc)

        for row in pending:
            try:
                ft = row["futures_ticker"]

                # Дневные-only активы: не проверять повторно чаще раза в сутки —
                # новых данных всё равно не будет до следующего EOD-снэпшота.
                if not has_intraday_oi(ft):
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

                match = db.execute(_FIND_MATCH, {
                    "futures_ticker": ft, "date_from": date_from, "date_to": date_to,
                    "exclude_ids": used,
                }).mappings().first()

                if match:
                    db.execute(_MARK_DRAFT_READY, {"id": row["id"], "anomaly_id": match["id"]})
                    summary["matched"] += 1
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
    s = run_once()
    print(f"[{datetime.now(timezone.utc)}] content_match: {s}")


if __name__ == "__main__":
    main()
