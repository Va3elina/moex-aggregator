"""Общая очередь Claude Code Routines — что вызывают заводы (справки, посты, …).

Завод НЕ стреляет /fire сам: кладёт задание enqueue(), а запускает его только
signals/routine_dispatch.py — по одному, с паузой, с общим лимитом на все рутины.
Рутина в конце своей работы зовёт колбэк своего завода; колбэк закрывает задание
mark_done(queue_id). Не позвала за timeout_sec — диспетчер повторит или сдастся.

Таблицы — db/migrations/108_routine_queue_scha_jobs.sql.
"""
from __future__ import annotations

from sqlalchemy import text

_ENQUEUE = text("""
    INSERT INTO routine_queue (routine, payload, source, priority, dedup_key, max_attempts, timeout_sec)
    VALUES (:routine, :payload, :source, :priority, :dedup_key, :max_attempts, :timeout_sec)
    ON CONFLICT (dedup_key) WHERE dedup_key IS NOT NULL AND status IN ('queued', 'firing', 'running')
    DO NOTHING
    RETURNING id
""")
_ACTIVE_BY_KEY = text("""
    SELECT id FROM routine_queue
    WHERE dedup_key = :k AND status IN ('queued', 'firing', 'running')
""")


def enqueue(conn, routine: str, payload: str, source: str, *, priority: int = 100,
            dedup_key: str | None = None, max_attempts: int = 3, timeout_sec: int = 1800) -> int:
    """Поставить запуск рутины в общую очередь. Повторный вызов с тем же dedup_key, пока
    прошлое задание не закончилось, нового не создаёт — вернёт id уже стоящего."""
    row = conn.execute(_ENQUEUE, dict(routine=routine, payload=payload, source=source, priority=priority,
                                      dedup_key=dedup_key, max_attempts=max_attempts,
                                      timeout_sec=timeout_sec)).first()
    if row:
        return row[0]
    return conn.execute(_ACTIVE_BY_KEY, {"k": dedup_key}).scalar_one()


def mark_done(conn, queue_id: int) -> bool:
    """Колбэк рутины пришёл — задание выполнено. True, если это первое закрытие (повторный
    колбэк той же сессии или запоздавший после таймаута ничего не ломает)."""
    return conn.execute(text("""
        UPDATE routine_queue SET status = 'done', finished_at = now()
        WHERE id = :id AND status IN ('firing', 'running', 'queued')
    """), {"id": queue_id}).rowcount == 1
