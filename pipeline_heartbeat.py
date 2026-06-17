"""
Pipeline heartbeat — единый «пульс запуска» для всех скриптов ингеста.

Каждый скрипт (или оркестратор за него) в конце прогона вызывает
record_pipeline_run(name, success, note, duration) — это апсертит одну строку
на пайплайн в таблицу pipeline_runs. Эндпоинт /api/health/data читает её и
отдаёт рутине «кто когда отработал и не упал ли».

Зачем: индивидуальные логи скриптов разнокалиберные (часть вообще без файлов,
только stdout). Heartbeat в БД даёт РАВНОМЕРНЫЙ статус по всем — и, в паре со
свежестью данных, различает «скрипт упал» от «источник просто без новых данных».

ГЛАВНОЕ: запись heartbeat обёрнута так, что НИКОГДА не бросает исключение —
сбой пульса не должен ронять сам фетчер.
"""
import os
from datetime import datetime

from sqlalchemy import create_engine, text

DB_URL = os.getenv("DB_URL")

_engine = None
_table_ready = False

_DDL = """
CREATE TABLE IF NOT EXISTS pipeline_runs (
    pipeline           TEXT PRIMARY KEY,
    last_run_at        TIMESTAMPTZ NOT NULL,
    last_success_at    TIMESTAMPTZ,
    last_status        TEXT NOT NULL,
    last_note          TEXT,
    last_duration_sec  DOUBLE PRECISION,
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""

_UPSERT = """
INSERT INTO pipeline_runs
    (pipeline, last_run_at, last_success_at, last_status, last_note, last_duration_sec, updated_at)
VALUES
    (:p, :now, :succ_at, :status, :note, :dur, :now)
ON CONFLICT (pipeline) DO UPDATE SET
    last_run_at       = EXCLUDED.last_run_at,
    last_success_at   = COALESCE(EXCLUDED.last_success_at, pipeline_runs.last_success_at),
    last_status       = EXCLUDED.last_status,
    last_note         = EXCLUDED.last_note,
    last_duration_sec = EXCLUDED.last_duration_sec,
    updated_at        = EXCLUDED.updated_at
"""


def _get_engine():
    global _engine
    if _engine is None:
        if not DB_URL:
            raise RuntimeError("DB_URL не задан")
        # Тот же драйвер/коннект, что и у фетчеров (pg8000 → ssl_context False).
        _engine = create_engine(DB_URL, connect_args={"ssl_context": False}, pool_pre_ping=True)
    return _engine


def record_pipeline_run(pipeline: str, success: bool, note: str = "", duration_sec: float = None) -> None:
    """
    Записать факт прогона скрипта. Идемпотентно (одна строка на pipeline).

    last_success_at сохраняется через COALESCE: при падении остаётся время
    последнего успеха — видно «успешно отработал тогда-то, но последний прогон упал».

    Любая ошибка проглатывается: heartbeat не должен влиять на ингест.
    """
    global _table_ready
    try:
        now = datetime.utcnow()
        eng = _get_engine()
        with eng.begin() as conn:
            if not _table_ready:
                conn.execute(text(_DDL))
                _table_ready = True
            conn.execute(text(_UPSERT), {
                "p": pipeline,
                "now": now,
                "succ_at": now if success else None,
                "status": "ok" if success else "fail",
                "note": (note or "")[:500],
                "dur": duration_sec,
            })
    except Exception:
        # Пульс — best-effort. Молча игнорируем (фетч важнее мониторинга).
        pass
