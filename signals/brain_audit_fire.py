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
SECOND_MIN = 20              # столько «неверно» ждут второго мнения — стреляем им в любую ночь


def решение(now_msk: datetime, backlog: int, busy: int, last_main, last_review,
            pending_second: int = 0, last_second=None):
    """(почему не стреляем | None, сколько старых перепроверить, режим main|second).
    Чистая — под тесты.

    Второе мнение идёт ПЕРВЫМ: пока его нет, «неверно» ничего не удаляет, а спор ждёт
    человека. Основная партия и второе мнение помнят свой последний запуск раздельно,
    иначе ночное второе мнение сбивало бы воскресную основную партию."""
    if now_msk.hour not in NIGHT_HOURS:
        return "не ночь", 0, None
    if busy:
        return f"конвейер постов занят ({busy} черновиков ждут писателя или судью)", 0, None
    last_any = max([t for t in (last_main, last_second) if t], default=None)
    if last_any and now_msk - last_any < OVERLAP and (not last_review or last_review < last_any):
        return "прошлая партия ещё не вернулась", 0, None
    воскресенье = now_msk.weekday() == WEEKLY_DAY
    # Копим до SECOND_MIN: запуск ради одной-двух связей — это отдельная сессия из общей
    # квоты, и первый проход растянулся бы вдвое. Остаток добирается в воскресенье.
    if pending_second >= SECOND_MIN or (pending_second and воскресенье and backlog <= FIRST_PASS_MIN):
        return None, 0, "second"
    if backlog > FIRST_PASS_MIN:
        return None, 0, "main"
    if воскресенье and (not last_main or now_msk - last_main > timedelta(days=6)):
        return None, WEEKLY_RESAMPLE, "main"
    return "первый проход закончен, до воскресенья проверять нечего", 0, None


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
        состояние = dict(db.execute(text(
            "SELECT source, watermark FROM brain_sync_state WHERE source IN ('audit_fire', 'audit_fire_second')")).all())
        last_review = db.execute(text(
            "SELECT GREATEST(MAX(reviewed_at), MAX(second_at)) FROM brain_edge_reviews")).scalar()
        pending_second = db.execute(text("""
            SELECT COUNT(*) FROM brain_edge_reviews r
              JOIN brain_edges e ON e.src = r.src AND e.dst = r.dst AND e.kind = r.kind
             WHERE r.verdict = 'неверно' AND r.second_verdict IS NULL AND r.human_decision IS NULL
        """)).scalar() or 0
        why, resample, mode = решение(now, int(backlog), int(busy), состояние.get("audit_fire"), last_review,
                                      int(pending_second), состояние.get("audit_fire_second"))
        if why:
            print(f"[{now:%Y-%m-%d %H:%M}] аудит пропущен: {why}; непроверенных {backlog}, "
                  f"ждут второго мнения {pending_second}")
            return 0
        # Второе мнение: подозрительные плюс столько же «верно» вслепую — отсюда ×2.
        лимит = min(PARTY, 2 * int(pending_second)) if mode == "second" else PARTY
        payload = (f"Аудит разметки второго мозга.\n"
                   f"режим: {mode}\nлимит: {лимит}\nперепроверка: {resample}\n"
                   f"internal_token: {internal}\napi_host: {INTERNAL_API_HOST}")
        try:
            _fire(trigger, token, payload)
        except Exception as e:  # noqa: BLE001 — 429 квоты и сетевые сбои: до следующего окна
            code = getattr(getattr(e, "response", None), "status_code", None)
            print(f"[{now:%Y-%m-%d %H:%M}] аудит не запущен ({code or type(e).__name__}) — попробую в следующее окно")
            return 0
        db.execute(text("""
            INSERT INTO brain_sync_state (source, watermark, rows_last, updated_at)
            VALUES (:src, NOW(), :n, NOW())
            ON CONFLICT (source) DO UPDATE SET watermark = NOW(), rows_last = EXCLUDED.rows_last, updated_at = NOW()
        """), {"src": "audit_fire_second" if mode == "second" else "audit_fire",
               "n": int(pending_second if mode == "second" else backlog)})
        db.commit()
        print(f"[{now:%Y-%m-%d %H:%M}] аудит запущен: режим {mode}, лимит {лимит}, перепроверка {resample}; "
              f"непроверенных {backlog}, ждут второго мнения {pending_second}")
        return 0
    finally:
        db.close()


if __name__ == "__main__":
    sys.exit(main())
