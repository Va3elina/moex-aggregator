"""Диспетчер общей очереди Claude Code Routines — ЕДИНСТВЕННЫЙ код, который вызывает /fire.

Cron раз в минуту (signals/routine_dispatch.sh). За прогон:
  1. задания без колбэка дольше timeout_sec → повтор с паузой или 'failed';
  2. если очередь на паузе (кончилась квота) — выходим;
  3. запускаем следующие по приоритету, пока одновременно идёт меньше ROUTINE_MAX_RUNNING сессий,
     не чаще раза в ROUTINE_FIRE_GAP_SEC секунд.

Почему так: несколько /fire разом роняют часть запусков ещё на подъёме облачного контейнера
(14.07.2026, см. FIRE_STAGGER_SEC в content_ai.py), а исчерпанная квота подписки роняла
все рутины разом (16.07). Один диспетчер на все заводы снимает и то и другое.

Рутина → env: ключ SCHA_CHECK → TRIGGER_ID_SCHA_CHECK + CLAUDE_ROUTINE_FIRE_TOKEN_SCHA_CHECK.
Релей — CLAUDE_ROUTINE_API_ROOT (api.anthropic.com отдаёт 403 российским IP).
"""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path

import requests
from sqlalchemy import text

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# .env + DB_URL на хост — ДО импорта api.database (как в content_ai.py)
from dotenv import load_dotenv  # noqa: E402
load_dotenv(ROOT / ".env")
os.environ["DB_URL"] = os.environ.get("DB_URL", "").replace("@db:", "@127.0.0.1:")
from api.database import get_engine  # noqa: E402

MAX_RUNNING = int(os.environ.get("ROUTINE_MAX_RUNNING", "2"))
FIRE_GAP_SEC = int(os.environ.get("ROUTINE_FIRE_GAP_SEC", "10"))
QUOTA_PAUSE_MIN = int(os.environ.get("ROUTINE_QUOTA_PAUSE_MIN", "60"))
RUN_BUDGET_SEC = 50          # cron раз в минуту — укладываемся, чтобы прогоны не наслаивались
FIRING_STUCK_MIN = 5         # 'firing' дольше — прогон упал посреди запуска
LOCK_KEY = 0x524F5554        # pg advisory lock «ROUT»: второй прогон выходит сразу

API_ROOT = os.environ.get("CLAUDE_ROUTINE_API_ROOT", "https://api.anthropic.com").rstrip("/")
HEADERS = {   # те же, что у signals/content_ai.py
    "anthropic-version": "2023-06-01",
    "anthropic-beta": "experimental-cc-routine-2026-04-01",
    "Content-Type": "application/json",
}
_QUOTA_WORDS = ("rate limit", "rate_limit", "quota", "usage limit", "overloaded", "too many")


def log(msg: str) -> None:
    print(time.strftime("%Y-%m-%d %H:%M:%S"), msg, flush=True)


def _notify_once(conn, key: str, text_: str) -> None:
    """Сбой — в @framesignalbot, не чаще раза в сутки на ключ."""
    fresh = conn.execute(text("""
        INSERT INTO scha_notify_log (key, sent_at) VALUES (:k, now())
        ON CONFLICT (key) DO UPDATE SET sent_at = now()
            WHERE scha_notify_log.sent_at < now() - interval '1 day'
        RETURNING key
    """), {"k": key}).first()
    if fresh:
        sys.path.insert(0, str(ROOT / "research" / "robots_tg"))
        import robots_tg
        robots_tg.send(text_)


def expire(conn) -> None:
    """Колбэк не пришёл — повторить с паузой (5 мин × попытка) или сдаться."""
    rows = conn.execute(text("""
        UPDATE routine_queue SET
            status = CASE WHEN status = 'running' AND done_on_timeout THEN 'done'
                          WHEN attempts < max_attempts THEN 'queued' ELSE 'failed' END,
            not_before = now() + make_interval(mins => 5 * attempts),
            finished_at = CASE WHEN (status = 'running' AND done_on_timeout) OR attempts >= max_attempts
                               THEN now() ELSE NULL END,
            last_error = CASE WHEN status = 'running' AND done_on_timeout THEN NULL
                              WHEN status = 'running' THEN 'нет колбэка за timeout_sec'
                              ELSE 'прогон диспетчера упал посреди запуска' END
        WHERE (status = 'running' AND fired_at < now() - make_interval(secs => timeout_sec))
           OR (status = 'firing' AND fired_at < now() - make_interval(mins => %d))
        RETURNING id, routine, status
    """ % FIRING_STUCK_MIN)).all()
    for r in rows:
        log(f"#{r.id} {r.routine}: без колбэка → {r.status}")


def _fire(routine: str, payload: str) -> str:
    trig = os.environ.get(f"TRIGGER_ID_{routine}", "")
    tok = os.environ.get(f"CLAUDE_ROUTINE_FIRE_TOKEN_{routine}", "")
    if not trig or not tok:
        raise PermissionError(f"нет TRIGGER_ID_{routine} / CLAUDE_ROUTINE_FIRE_TOKEN_{routine} в .env")
    r = requests.post(f"{API_ROOT}/v1/claude_code/routines/{trig}/fire",
                      headers={**HEADERS, "Authorization": f"Bearer {tok}"},
                      json={"text": payload}, timeout=20)
    if r.status_code >= 400:
        raise requests.HTTPError(f"{r.status_code} {r.text[:300]}", response=r)
    try:
        body = r.json()
    except ValueError:
        body = {}
    return str(body.get("session_id") or body.get("id") or "")[:80]


def _retry_after_sec(err: str) -> int | None:
    """«Try again in 39m58s» / «in 1h2m» / «in 45s» → секунды."""
    import re
    m = re.search(r"try again in\s+((?:\d+\s*[hms]\s*)+)", err, re.I)
    if not m:
        return None
    total = 0
    for n, unit in re.findall(r"(\d+)\s*([hms])", m.group(1).lower()):
        total += int(n) * {"h": 3600, "m": 60, "s": 1}[unit]
    return total or None


def _is_routine_rate_limit(err: str) -> bool:
    """Предел частоты запусков ОДНОЙ рутины (16.10.2026: ~30 /fire в час на рутину) —
    в отличие от исчерпанной подписки, другие рутины он не трогает."""
    low = err.lower()
    return "routine's fire rate limit" in low or "fire rate limit" in low


def _is_quota(e: Exception) -> bool:
    resp = getattr(e, "response", None)
    if resp is not None and resp.status_code == 429:
        return True
    return any(w in str(e).lower() for w in _QUOTA_WORDS)


def dispatch_one(eng) -> bool:
    """Запустить одно задание, если можно. True — запустили (или попытались)."""
    with eng.begin() as conn:
        st = conn.execute(text("""
            SELECT paused_until > now() AS paused, last_fire_at,
                   extract(epoch FROM now() - last_fire_at) AS since
            FROM routine_queue_state WHERE id = 1 FOR UPDATE
        """)).first()
        if st.paused:
            return False
        if st.since is not None and st.since < FIRE_GAP_SEC:
            time.sleep(FIRE_GAP_SEC - float(st.since))
        busy = conn.execute(text(
            "SELECT count(*) FROM routine_queue WHERE status IN ('firing', 'running')")).scalar()
        if busy >= MAX_RUNNING:
            return False
        job = conn.execute(text("""
            SELECT id, routine, payload FROM routine_queue
            WHERE status = 'queued' AND not_before <= now()
            ORDER BY priority, id LIMIT 1 FOR UPDATE SKIP LOCKED
        """)).first()
        if not job:
            return False
        conn.execute(text("""
            UPDATE routine_queue SET status = 'firing', attempts = attempts + 1, fired_at = now()
            WHERE id = :id
        """), {"id": job.id})
        conn.execute(text("UPDATE routine_queue_state SET last_fire_at = now() WHERE id = 1"))
    # /fire — вне транзакции: если прогон упадёт здесь, задание останется 'firing'
    # и через FIRING_STUCK_MIN expire() вернёт его в очередь.
    try:
        sid = _fire(job.routine, job.payload)
        with eng.begin() as conn:
            conn.execute(text("""
                UPDATE routine_queue SET status = 'running', session_id = :s, last_error = NULL
                WHERE id = :id AND status = 'firing'
            """), {"id": job.id, "s": sid})
        log(f"#{job.id} {job.routine}: запущена {sid}")
    except Exception as e:  # noqa: BLE001 — любая ошибка запуска разбирается ниже
        err = f"{type(e).__name__}: {e}"[:500]
        with eng.begin() as conn:
            if _is_quota(e) and _is_routine_rate_limit(err):
                # предел запусков ЭТОЙ рутины: откладываем только её задания, остальные заводы
                # идут дальше (16.10: пауза всей очереди из-за справок держала посты до часа).
                wait = _retry_after_sec(err) or QUOTA_PAUSE_MIN * 60
                conn.execute(text("""
                    UPDATE routine_queue SET status = 'queued', attempts = attempts - 1, last_error = :e
                    WHERE id = :id
                """), {"id": job.id, "e": err})
                conn.execute(text("""
                    UPDATE routine_queue SET not_before = GREATEST(not_before, now() + make_interval(secs => :w))
                    WHERE routine = :r AND status = 'queued'
                """), {"r": job.routine, "w": wait + 30})
                log(f"#{job.id} {job.routine}: предел запусков рутины → её задания ждут {wait // 60} мин")
            elif _is_quota(e):
                # квота/лимит — не вина задания: попытку не тратим, ставим на паузу ВСЮ очередь
                conn.execute(text("""
                    UPDATE routine_queue SET status = 'queued', attempts = attempts - 1, last_error = :e
                    WHERE id = :id
                """), {"id": job.id, "e": err})
                conn.execute(text("""
                    UPDATE routine_queue_state SET paused_until = now() + make_interval(mins => :m),
                           pause_reason = :e WHERE id = 1
                """), {"m": QUOTA_PAUSE_MIN, "e": err})
                _notify_once(conn, "routine:quota",
                             f"⚠️ Рутины: лимит подписки, очередь на паузе {QUOTA_PAUSE_MIN} мин\n{err[:200]}")
                log(f"#{job.id} {job.routine}: квота → пауза очереди: {err}")
            elif isinstance(e, PermissionError) or (getattr(e, "response", None) is not None
                                                    and e.response.status_code in (401, 403, 404)):
                # настройка сломана — повтор не поможет
                conn.execute(text("""
                    UPDATE routine_queue SET status = 'failed', finished_at = now(), last_error = :e
                    WHERE id = :id
                """), {"id": job.id, "e": err})
                _notify_once(conn, f"routine:config:{job.routine}",
                             f"⚠️ Рутина {job.routine} не запускается (настройка)\n{err[:200]}")
                log(f"#{job.id} {job.routine}: настройка → failed: {err}")
            else:
                # сеть / подъём контейнера — повторим позже
                conn.execute(text("""
                    UPDATE routine_queue SET
                        status = CASE WHEN attempts < max_attempts THEN 'queued' ELSE 'failed' END,
                        not_before = now() + interval '5 minutes', last_error = :e,
                        finished_at = CASE WHEN attempts < max_attempts THEN NULL ELSE now() END
                    WHERE id = :id
                """), {"id": job.id, "e": err})
                log(f"#{job.id} {job.routine}: ошибка запуска, повтор позже: {err}")
    return True


def main() -> int:
    eng = get_engine()
    started = time.monotonic()
    with eng.connect() as lock_conn:
        if not lock_conn.execute(text("SELECT pg_try_advisory_lock(:k)"), {"k": LOCK_KEY}).scalar():
            log("другой прогон диспетчера ещё идёт — выходим")
            return 0
        try:
            with eng.begin() as conn:
                expire(conn)
            while time.monotonic() - started < RUN_BUDGET_SEC - FIRE_GAP_SEC:
                if not dispatch_one(eng):
                    break
        finally:
            lock_conn.execute(text("SELECT pg_advisory_unlock(:k)"), {"k": LOCK_KEY})
    return 0


if __name__ == "__main__":
    sys.exit(main())
