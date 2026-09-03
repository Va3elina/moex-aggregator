"""
Ручка следа агента для админского дашборда: /api/admin/agent-trace/*

Отвечает на два разных вопроса, и они намеренно разведены по эндпоинтам:
  • «почему получился ИМЕННО этот пост» — путь одного кандидата по шагам;
  • «где конвейер систематически буксует» — агрегат по всем кандидатам.

⚠️ ЧИТАЕТ, НЕ СЧИТАЕТ. Дашборд открывается часто и не должен нагружать базу: оба
запроса идут по индексам (idx_agent_trace_candidate и idx_agent_trace_source),
агрегат ограничен окном в днях. Тяжёлых соединений здесь нет и быть не должно —
если понадобится больше, это повод для снимка в Redis, а не для более умного SQL.
"""

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from api.database import get_db
from api.models import User
from api.routers.auth import require_admin

router = APIRouter(prefix="/api/admin/agent-trace", tags=["admin-agent-trace"])

# Порядок шагов в ответе — порядок работы конвейера, а не алфавит и не время
# записи: судья иногда отвечает раньше, чем дописывается след писателя.
_STEP_ORDER = {"бриф": 1, "писатель": 2, "судья": 3}

_SELECT_PATH = text("""
    SELECT step, seq, source, question, params, result_count, result_note,
           outcome, outcome_reason, duration_ms, created_at
    FROM agent_trace
    WHERE candidate_id = :id
    ORDER BY step, seq
""")

_SELECT_CANDIDATE = text("""
    SELECT id, status, headline, tickers, judge_verdict, draft_text IS NOT NULL AS has_draft
    FROM content_candidates WHERE id = :id
""")


@router.get("/summary")
def trace_summary(
    days: int = Query(30, ge=1, le=365),
    db: Session = Depends(get_db),
    _admin: User = Depends(require_admin),
):
    """Куда агент ходит, что там находит и как часто уходит ни с чем."""
    rows = db.execute(text("""
        SELECT source,
               COUNT(*) AS обращений,
               COUNT(*) FILTER (WHERE outcome = 'взято')    AS взято,
               COUNT(*) FILTER (WHERE outcome = 'не_взято') AS не_взято,
               COUNT(*) FILTER (WHERE outcome = 'пусто')    AS пусто,
               -- ::int намеренно: numeric уезжает в JSON строкой, и фронт
               -- получал бы "132" вместо 132.
               ROUND(AVG(duration_ms))::int AS среднее_мс,
               MAX(duration_ms) AS худшее_мс
        FROM agent_trace
        WHERE created_at >= now() - CAST(:days || ' days' AS interval)
        GROUP BY source ORDER BY 2 DESC
    """), {"days": days}).mappings().all()

    # Топ причин отказа — это и есть «чуйка» агента в агрегате: по ним видно, чего
    # системно не хватает в данных, а не в отдельном посте.
    reasons = db.execute(text("""
        SELECT outcome_reason AS причина, COUNT(*) AS сколько
        FROM agent_trace
        WHERE outcome <> 'взято' AND outcome_reason IS NOT NULL
          AND created_at >= now() - CAST(:days || ' days' AS interval)
        GROUP BY 1 ORDER BY 2 DESC LIMIT 10
    """), {"days": days}).mappings().all()

    totals = db.execute(text("""
        SELECT COUNT(DISTINCT candidate_id) AS кандидатов, COUNT(*) AS строк
        FROM agent_trace
        WHERE created_at >= now() - CAST(:days || ' days' AS interval)
    """), {"days": days}).mappings().first()

    return {"окно_дней": days, "итого": dict(totals or {}),
            "по_источникам": [dict(r) for r in rows],
            "причины_отказов": [dict(r) for r in reasons]}


@router.get("/{candidate_id}")
def trace_path(
    candidate_id: int,
    db: Session = Depends(get_db),
    _admin: User = Depends(require_admin),
):
    """Путь одного кандидата: что агент спросил, что нашёл, что взял в текст."""
    cand = db.execute(_SELECT_CANDIDATE, {"id": candidate_id}).mappings().first()
    if not cand:
        raise HTTPException(status_code=404, detail="Кандидат не найден")

    rows = db.execute(_SELECT_PATH, {"id": candidate_id}).mappings().all()
    # ⚠️ Пустой след — НЕ ошибка. Кандидаты, прошедшие конвейер до появления следа
    # (и те, что отсеялись раньше первого обращения к данным), его просто не имеют.
    # Отдавать 404 значило бы путать «не было такого кандидата» с «он ничего не искал».
    steps = {}
    for r in rows:
        steps.setdefault(r["step"], []).append({
            "seq": r["seq"], "источник": r["source"], "вопрос": r["question"],
            "параметры": r["params"], "найдено": r["result_count"],
            "результат": r["result_note"], "исход": r["outcome"],
            "причина": r["outcome_reason"], "мс": r["duration_ms"],
            "время": r["created_at"],
        })

    return {
        "кандидат": {"id": cand["id"], "статус": cand["status"],
                     "заголовок": cand["headline"], "тикеры": cand["tickers"],
                     "вердикт_судьи": cand["judge_verdict"],
                     "есть_черновик": cand["has_draft"]},
        "шаги": [{"шаг": name, "обращений": len(items), "строки": items}
                 for name, items in sorted(steps.items(),
                                           key=lambda kv: _STEP_ORDER.get(kv[0], 99))],
        "всего_обращений": len(rows),
        # Готовые данные для паутины поста: какие компании агент открыл и что с ними
        # сделал. Считается здесь, чтобы фронт не разбирал текст вопросов регулярками.
        "паутина": [
            {"тикер": (r["params"] or {}).get("ticker") or (r["params"] or {}).get("тикер"),
             "исход": r["outcome"], "почему": r["outcome_reason"]}
            for r in rows
            if r["source"] in ("world_facts", "news_archive")
            and ((r["params"] or {}).get("ticker") or (r["params"] or {}).get("тикер"))
        ],
    }


# ======================================================================
#        ОЧЕРЕДЬ СИГНАЛОВ СМЕНЫ ВЛАДЕНИЯ (кандидаты в рёбра графа)
# ======================================================================
# Живёт рядом со следом агента намеренно: обе ручки про НАБЛЮДЕНИЕ за тем, как
# устроен второй мозг, и обе читает один и тот же дашборд одного и того же админа.
# Заводить ради двух эндпоинтов третий роутер — плодить сущности.

signals_router = APIRouter(prefix="/api/admin/ownership-signals",
                           tags=["admin-ownership-signals"])


@signals_router.get("")
def signals_queue(
    status: str = Query("новый"),
    only_strong: bool = Query(False, description="только строгие сигналы с процентом"),
    limit: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
    _admin: User = Depends(require_admin),
):
    """
    Очередь поводов посмотреть на граф.

    Порядок неслучаен: сначала сигналы по компаниям, у которых связь УЖЕ ЕСТЬ (там
    речь об изменении известной доли — это дороже пропустить), затем строгие
    формулировки 714-П, затем свежесть.
    """
    rows = db.execute(text("""
        SELECT id, posted_at, tickers, snippet, strength, has_percent, edge_state,
               status, review_note
        FROM ownership_signals
        WHERE status = :st
          AND (:strong = false OR (strength = 'строгий' AND has_percent))
        ORDER BY (edge_state = 'есть_ребро') DESC,
                 (strength = 'строгий') DESC,
                 has_percent DESC,
                 posted_at DESC
        LIMIT :lim
    """), {"st": status, "strong": only_strong, "lim": limit}).mappings().all()

    counts = db.execute(text(
        "SELECT status, COUNT(*) FROM ownership_signals GROUP BY status"
    )).all()
    return {"очередь": [dict(r) for r in rows],
            "по_статусам": {k: v for k, v in counts}}


@signals_router.patch("/{signal_id}")
def signals_review(
    signal_id: int,
    status: str = Query(..., pattern="^(подтверждён|отклонён|новый)$"),
    note: str = Query(""),
    db: Session = Depends(get_db),
    _admin: User = Depends(require_admin),
):
    """
    Отметка разбора.

    ⚠️ Подтверждение НИЧЕГО не создаёт в графе. Оно означает «я посмотрел и завёл
    ребро руками» — ровно та граница, ради которой очередь и заведена: автомат
    предлагает, человек утверждает. Ручка, которая по нажатию писала бы ребро,
    незаметно превратила бы курируемый граф в автоматический.
    """
    res = db.execute(text("""
        UPDATE ownership_signals
        SET status = :st, review_note = NULLIF(:note, \'\'), reviewed_at = now()
        WHERE id = :id
    """), {"id": signal_id, "st": status, "note": note})
    if not res.rowcount:
        raise HTTPException(status_code=404, detail="Сигнал не найден")
    db.commit()
    return {"id": signal_id, "status": status}
