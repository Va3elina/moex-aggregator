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
появится до следующего EOD-снэпшота. **step_b_checked_at** (СВОЯ колонка, не
last_checked_at, миграция 037) не даёт лишний раз дёргать такие кандидаты в
рамках одного дня.

⚠️ **Найдено 2026-07-16: last_checked_at раньше была одной колонкой на три
несвязанных смысла** (tg_hype_scan.py — "продиспатчил Шаг А", content_match —
"уже проверял сегодня", content_review_bot.py — "ещё не отправлял ревьюеру") —
взаимно затирали друг друга: content_match ставил last_checked_at=now() ровно
в той же команде, что переводила статус в draft_ready, а content_review_bot
ждал last_checked_at IS NULL как признак "не отправлено" — условие никогда не
выполнялось, ревью-бот НИ РАЗУ не сработал (0 published/rejected через него).
Плюс дневные-only тикеры ложно считались "уже проверенными сегодня" из-за
марки tg_hype_scan сразу при создании кандидата, ещё ДО первой реальной
попытки сверки. **Теперь у content_match — своя step_b_checked_at, у
content_review_bot — своя reviewer_notified_at, last_checked_at остаётся
только за tg_hype_scan/content_ai.py (диспатч-троттлинг Шага А/В).**

Запуск раз в 5 минут (Вадим 2026-07-16: раз в час — слишком редко, кандидат
может ждать до 55 минут совпадения, которое УЖЕ лежит в anomalies, просто
anomaly_scan его написал раньше, чем content_match успел пройти по расписанию;
раз в 15 мин — тоже мало, ужали дальше до раза в 5 мин, в такт tg_hype_scan.sh).
anomaly_scan сам пишет НОВЫЕ аномалии всё ещё раз в час — учащение
content_match не ускоряет появление новых сигналов, только устраняет
задержку ПОДБОРА уже существующих. Интрадей-активы это отрабатывают
полностью каждый прогон; дневные-only сами себя троттлят через
last_checked_at (не чаще раза в сутки, см. выше):
  /opt/frame/signals/content_match.sh   (cron: */5 * * * *)
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

# Найдено 2026-07-14 (session 3) — пауза между _fire() подряд
# в одном прогоне, иначе несколько облачных контейнеров запрашиваются
# одновременно и конкурируют за мощность аккаунта.
FIRE_STAGGER_SEC = 8

_SELECT_PENDING = text("""
    SELECT id, futures_ticker, thread_key, created_at, pending_expires_at,
           step_b_checked_at, headline, raw_text, tickers, event_type, reasoning,
           forwards_count
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
    SELECT id, signal_date, severity_value, direction, headline, asset_id, asset_name,
           type, clgroup
    FROM anomalies
    WHERE asset_id = :futures_ticker
      AND signal_date BETWEEN :date_from AND :date_to
      AND id != ALL(:exclude_ids)
    ORDER BY signal_date ASC, id ASC
    LIMIT 1
""")

# Найдено 2026-07-16 (VK/Google Play): для активов БЕЗ интрадей-данных
# (has_intraday_oi=False) новая аномалия физически не появится до следующего
# EOD-снэпшота — если единственная дневная аномалия уже использована ПЕРВЫМ
# постом треда, все последующие кандидаты (развитие той же новости) никогда
# не найдут "новую" и тихо протухнут в no_data через CONTENT_PENDING_DAYS,
# даже когда новость реально продолжается (см. VK: −8.5% на фоне удаления
# из Google Play, тред ticker:VKCO). Разрешаем ОДИН follow-up на УЖЕ
# использованном сигнале (len(used_real) == 1 — ровно один прежний пост,
# не больше) — Шаг В получит prior_post и явную оговорку "сигнал не новый"
# (см. content_ai.py::_step_c_payload), напишет продолжение, а не повтор.
_FIND_REUSE_ANOMALY = text("""
    SELECT id, signal_date, severity_value, direction, headline, asset_id, asset_name,
           type, clgroup
    FROM anomalies
    WHERE id = ANY(:used_ids)
    ORDER BY signal_date DESC, id DESC
    LIMIT 1
""")

_MARK_DRAFT_READY = text("""
    UPDATE content_candidates
    SET status = 'draft_ready', matched_anomaly_id = :anomaly_id,
        step_b_checked_at = now(), updated_at = now()
    WHERE id = :id
""")

_MARK_CHECKED = text("""
    UPDATE content_candidates SET step_b_checked_at = now() WHERE id = :id
""")

_MARK_NO_DATA = text("""
    UPDATE content_candidates
    SET status = 'no_data', step_b_checked_at = now(), updated_at = now()
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

                # Дневные-only активы: не проверять повторно чаще раза в сутки —
                # новых данных всё равно не будет до следующего EOD-снэпшота.
                if not has_intraday_oi(ft):
                    lc = row["step_b_checked_at"]
                    if lc and lc.astimezone(timezone.utc).date() == now.date():
                        summary["skipped_daily_already_checked"] += 1
                        continue

                summary["checked"] += 1

                used_real = [r[0] for r in db.execute(
                    _ALREADY_USED, {"thread_key": row["thread_key"]}
                ).fetchall()] if row["thread_key"] else []
                used = used_real or [0]  # ALL(:exclude_ids) на пустом массиве не работает как ожидается

                created = row["created_at"]
                date_from = created.date() - timedelta(days=1)
                date_to = created.date() + timedelta(days=config.CONTENT_PENDING_DAYS)

                match = db.execute(_FIND_MATCH, {
                    "futures_ticker": ft, "date_from": date_from, "date_to": date_to,
                    "exclude_ids": used,
                }).mappings().first()

                if not match and not has_intraday_oi(ft) and len(used_real) == 1:
                    # Дневной актив, новой аномалии сегодня не будет, сигнал уже
                    # использован РОВНО одним прежним постом — один follow-up
                    # на переиспользованном сигнале, не больше (см. комментарий
                    # у _FIND_REUSE_ANOMALY).
                    match = db.execute(
                        _FIND_REUSE_ANOMALY, {"used_ids": used_real}
                    ).mappings().first()

                if match:
                    db.execute(_MARK_DRAFT_READY, {
                        "id": row["id"], "anomaly_id": match["id"],
                    })
                    db.commit()  # ДО fire — Routine PATCH'ит через отдельное соединение (API)
                    summary["matched"] += 1

                    if can_fire:
                        try:
                            payload_row = {
                                "id": row["id"], "headline": row["headline"],
                                "raw_text": row["raw_text"],
                                "tickers": row["tickers"], "event_type": row["event_type"],
                                "reasoning": row["reasoning"],
                                "forwards_count": row["forwards_count"],
                                "thread_key": row["thread_key"], "created_at": row["created_at"],
                                "anomaly_id": match["id"],
                                "asset_id": match["asset_id"], "asset_name": match["asset_name"],
                                "anomaly_type": match["type"], "direction": match["direction"],
                                "anomaly_clgroup": match["clgroup"],
                                "severity_value": match["severity_value"],
                                "signal_date": match["signal_date"],
                                "anomaly_headline": match["headline"],
                            }
                            _fire(TRIGGER_ID_STEP_C, token_c,
                                  _step_c_payload(db, payload_row, internal_token))
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
    # Найдено 2026-07-17: step_c_fire_errors ловится ВНУТРЕННИМ try/except
    # (см. run_once) и не пробрасывается в errors — раньше сбой fire-вызова
    # (напр. 401 на Anthropic) вообще не отражался на статусе, ok молча
    # оставался ok. Складываем оба счётчика для решения по статусу.
    total_errors = s["errors"] + s["step_c_fire_errors"]
    ok = total_errors == 0
    fired_any = s["checked"] > 0 or s["step_c_fired"] > 0
    pipeline_heartbeat.record_pipeline_run(
        "content_match", ok, str(s), dur, degraded=(not ok and fired_any)
    )


if __name__ == "__main__":
    main()
