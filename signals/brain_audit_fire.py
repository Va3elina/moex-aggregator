"""Ночной запуск аудита разметки второго мозга — Routine frame-brain-audit.

Агент читает новости, которые карта связала с компанией по имени, и отвечает, про ту
ли компанию новость (research/brain/prompt_name_audit_routine.md, ручки
/api/internal/brain/audit/*). Первый проход — по всей базе, дальше раз в неделю.

⚠️ Почему стреляет сервер, а не расписание самой Routine. Квота Routines — дневная на
весь аккаунт и общая с писателем и судьёй (при исчерпании — 429, и черновик не
допишется). Сервер знает то, чего не знает расписание: занят ли сейчас конвейер
постов, вернулась ли прошлая партия и закончился ли первый проход. Routine по
расписанию жгла бы сессию каждую ночь даже тогда, когда проверять нечего.

  20 22,23,0,1 * * * /bin/bash /opt/frame/signals/brain_audit_fire.sh >> /opt/frame/logs/brain_audit.log 2>&1

(01:20–04:20 МСК: четыре окна за ночь; первый проход ≈ 12 партий по 400 ≈ три ночи.)
"""
import os
import sys
from datetime import datetime, timedelta, timezone

MSK = timezone(timedelta(hours=3))
PARTY = 400                  # связей за один запуск Routine (агент берёт их кругами по 40)
WEEKLY_RESAMPLE = 20         # по воскресеньям — ещё столько старых «верно» на перепроверку
NIGHT_HOURS = range(1, 6)    # МСК: новостей почти нет, писатель и судья простаивают
FIRST_PASS_MIN = 50          # непроверенных больше — первый проход не закончен, стреляем каждую ночь
WEEKLY_DAY = 6               # воскресенье
OVERLAP = timedelta(minutes=90)


def решение(now_msk: datetime, backlog: int, busy: int, last_fire, last_review):
    """(почему не стреляем | None, сколько старых перепроверить). Чистая — под тесты."""
    if now_msk.hour not in NIGHT_HOURS:
        return "не ночь", 0
    if busy:
        return f"конвейер постов занят ({busy} черновиков ждут писателя или судью)", 0
    if last_fire and now_msk - last_fire < OVERLAP and (not last_review or last_review < last_fire):
        return "прошлая партия ещё не вернулась", 0
    if backlog > FIRST_PASS_MIN:
        return None, 0
    if now_msk.weekday() == WEEKLY_DAY and (not last_fire or now_msk - last_fire > timedelta(days=6)):
        return None, WEEKLY_RESAMPLE
    return "первый проход закончен, до воскресенья проверять нечего", 0


def main() -> int:
    from sqlalchemy import text
    from signals.content_ai import INTERNAL_API_HOST, _fire
    from signals.db import SessionLocal

    trigger = os.environ.get("TRIGGER_ID_BRAIN_AUDIT", "")
    token = os.environ.get("CLAUDE_ROUTINE_FIRE_TOKEN_BRAIN_AUDIT", "")
    internal = os.environ.get("CONTENT_AI_INTERNAL_TOKEN", "")
    now = datetime.now(MSK)
    if not (trigger and token and internal):
        print(f"[{now:%Y-%m-%d %H:%M}] аудит не настроен: нет TRIGGER_ID/токена в .env")
        return 0
    db = SessionLocal()
    try:
        backlog = db.execute(text("""
            SELECT COUNT(*) FROM brain_edges e
             WHERE e.kind = 'упоминает' AND e.method = 'имя'
               AND NOT EXISTS (SELECT 1 FROM brain_edge_reviews r
                                WHERE r.src = e.src AND r.dst = e.dst AND r.kind = e.kind)
        """)).scalar() or 0
        busy = db.execute(text("""
            SELECT COUNT(*) FROM content_candidates
             WHERE status = 'draft_ready' AND updated_at > NOW() - INTERVAL '6 hours'
               AND (draft_text IS NULL OR (judge_verdict IS NULL AND judge_gave_up_at IS NULL))
        """)).scalar() or 0
        last_fire = db.execute(text("SELECT watermark FROM brain_sync_state WHERE source = 'audit_fire'")).scalar()
        last_review = db.execute(text("SELECT MAX(reviewed_at) FROM brain_edge_reviews")).scalar()
        why, resample = решение(now, int(backlog), int(busy), last_fire, last_review)
        if why:
            print(f"[{now:%Y-%m-%d %H:%M}] аудит пропущен: {why}; непроверенных {backlog}")
            return 0
        payload = (f"Аудит разметки второго мозга.\n"
                   f"лимит: {PARTY}\nперепроверка: {resample}\n"
                   f"internal_token: {internal}\napi_host: {INTERNAL_API_HOST}")
        try:
            _fire(trigger, token, payload)
        except Exception as e:  # noqa: BLE001 — 429 квоты и сетевые сбои: до следующего окна
            code = getattr(getattr(e, "response", None), "status_code", None)
            print(f"[{now:%Y-%m-%d %H:%M}] аудит не запущен ({code or type(e).__name__}) — попробую в следующее окно")
            return 0
        db.execute(text("""
            INSERT INTO brain_sync_state (source, watermark, rows_last, updated_at)
            VALUES ('audit_fire', NOW(), :n, NOW())
            ON CONFLICT (source) DO UPDATE SET watermark = NOW(), rows_last = EXCLUDED.rows_last, updated_at = NOW()
        """), {"n": int(backlog)})
        db.commit()
        print(f"[{now:%Y-%m-%d %H:%M}] аудит запущен: партия {PARTY}, перепроверка {resample}; непроверенных {backlog}")
        return 0
    finally:
        db.close()


if __name__ == "__main__":
    sys.exit(main())
