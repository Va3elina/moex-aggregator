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
import json
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


_НАЧАЛО = """
INSERT INTO pipeline_runs
    (pipeline, last_run_at, last_status, started_at, updated_at)
VALUES
    (:p, :never, 'неизвестно', :now, :now)
ON CONFLICT (pipeline) DO UPDATE SET
    started_at = EXCLUDED.started_at,
    updated_at = EXCLUDED.updated_at
"""


def _телеграмма(фаза: str, pipeline: str, extra: dict = None) -> None:
    """
    Событие о процессе в канал data_updated → SSE → приборная панель.

    ⚠️ source='pipeline' ОБЯЗАН быть известен api/notify_listener: неизвестный
    источник там means «перестраховка» и сбрасывает ВЕСЬ Redis-кэш. Процессы
    стартуют десятки раз в час — молчаливая полная инвалидация на каждый старт
    убила бы кэш насовсем. В листенере для этого источника стоит ранний выход.

    Best-effort, как и сам пульс: телеметрия не смеет ронять ингест.
    """
    try:
        payload = {"source": "pipeline", "pipeline": pipeline, "phase": фаза,
                   "ts": datetime.utcnow().isoformat()}
        if extra:
            payload.update(extra)
        with _get_engine().begin() as conn:
            conn.execute(text("SELECT pg_notify('data_updated', :p)"),
                         {"p": json.dumps(payload, ensure_ascii=False)})
    except Exception:
        pass


def record_pipeline_start(pipeline: str) -> None:
    """Отметить НАЧАЛО прогона: панель показывает процесс идущим сразу, а не
    задним числом. Ошибки проглатываются — пульс не должен влиять на ингест."""
    global _table_ready
    try:
        now = datetime.utcnow()
        eng = _get_engine()
        with eng.begin() as conn:
            if not _table_ready:
                conn.execute(text(_DDL))
                _table_ready = True
            conn.execute(text(_НАЧАЛО),
                         {"p": pipeline, "now": now, "never": datetime(1970, 1, 1)})
    except Exception:
        pass
    _телеграмма("start", pipeline)


def record_pipeline_run(pipeline: str, success: bool, note: str = "", duration_sec: float = None,
                         degraded: bool = False) -> None:
    """
    Записать факт прогона скрипта. Идемпотентно (одна строка на pipeline).

    last_success_at сохраняется через COALESCE: при падении остаётся время
    последнего успеха — видно «успешно отработал тогда-то, но последний прогон упал».

    degraded (найдено 2026-07-17, инцидент с 401 на Anthropic fire-эндпоинте):
    success=False раньше красил статус в "fail" ОДИНАКОВО что при единичной
    ошибке среди дюжины успехов, что при полном отказе — не различить с
    первого взгляда на /api/health/data, не залезая в last_note. Пайплайны с
    батчем независимых операций (напр. content_ai_backstop — N кандидатов за
    прогон) могут передать degraded=True, когда success=False, но часть
    работы всё же прошла — статус "degraded", а не "fail". Игнорируется при
    success=True. Остальные вызывающие (без параметра) ведут себя как раньше
    — строго ok/fail.

    Любая ошибка проглатывается: heartbeat не должен влиять на ингест.
    """
    global _table_ready
    try:
        now = datetime.utcnow()
        eng = _get_engine()
        status = "ok" if success else ("degraded" if degraded else "fail")
        with eng.begin() as conn:
            if not _table_ready:
                conn.execute(text(_DDL))
                _table_ready = True
            conn.execute(text(_UPSERT), {
                "p": pipeline,
                "now": now,
                "succ_at": now if success else None,
                "status": status,
                "note": (note or "")[:500],
                "dur": duration_sec,
            })
    except Exception:
        # Пульс — best-effort. Молча игнорируем (фетч важнее мониторинга).
        pass
    _телеграмма("end", pipeline, {
        "status": "ok" if success else ("degraded" if degraded else "fail"),
        "duration_sec": duration_sec,
        "note": (note or "")[:120],
    })
