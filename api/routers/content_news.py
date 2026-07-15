"""
/api/admin/content-candidates/* — админ-Kanban состояний content-пайплайна
(«Новости», страница /admin/content-news на фронте).
/api/internal/content-news/* — callback для ИИ-шагов (Routine/прямой API), см.
блок в конце файла.

Пайплайн (сбор — отдельные скрипты, здесь только состояния + приёмка результата ИИ):
  Календарь MOEX (signals/moex_calendar_scan.py) + TG-каналы-хайп (signals/tg_hype_scan.py:
  markettwits, newssmartlab) → Шаг А (ИИ: релевантность/значимость, пишет сюда через
  /api/internal/.../step-a)
  → Шаг Б (signals/content_match.py: сверка с anomalies, БЕЗ ИИ) → Шаг В (ИИ:
  синтез черновика, пишет сюда через /api/internal/.../step-c) → ревью в
  Telegram-боте → публикация. Публикация с кнопками ревью — тоже отдельная задача.

  GET   /api/admin/content-candidates       — карточки одной колонки (пагинация)
  GET   /api/admin/content-candidates/{id}  — полная карточка + связанный тред
  PATCH /api/admin/content-candidates/{id}  — ручная смена статуса (валидация
                                               переходов по state machine ниже)

Админ-эндпоинты — require_admin (как в analytics.py). Таблица —
content_candidates (db/migrations/022_content_candidates.sql) — там же полное
описание state machine статусов.
"""
import os
from typing import Optional

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.orm import Session

from api.database import get_db
from api.models import User
from api.routers.auth import require_admin

router = APIRouter(prefix="/api/admin/content-candidates", tags=["admin-content"])

# Порядок = порядок колонок Kanban-доски на фронте.
STATUS_VALUES = (
    "candidate", "discarded", "pending", "draft_ready",
    "no_data", "in_review", "published", "rejected",
)

_COLUMNS = """
  id, status, source, headline, tickers, futures_ticker,
  event_type, importance_1_5, reasoning, matched_anomaly_id, thread_key,
  parent_candidate_id, forwards_count, created_at, updated_at, pending_expires_at,
  published_at, reviewer_action
"""


class ContentCandidateOut(BaseModel):
    id: int
    status: str
    source: Optional[str] = None
    headline: str
    tickers: list[str] = []
    futures_ticker: Optional[str] = None
    event_type: Optional[str] = None
    importance_1_5: Optional[int] = None
    reasoning: Optional[str] = None
    matched_anomaly_id: Optional[int] = None
    thread_key: Optional[str] = None
    parent_candidate_id: Optional[int] = None
    forwards_count: Optional[int] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    pending_expires_at: Optional[str] = None
    published_at: Optional[str] = None
    reviewer_action: Optional[str] = None


class ContentCandidateListOut(BaseModel):
    items: list[ContentCandidateOut]
    total: int
    limit: int
    offset: int


class ContentCandidateDetailOut(ContentCandidateOut):
    raw_text: Optional[str] = None
    draft_text: Optional[str] = None
    synth_declined_reason: Optional[str] = None
    reviewer_id: Optional[int] = None
    thread: list[ContentCandidateOut] = []  # остальные кандидаты того же thread_key


def _row_to_out(r) -> ContentCandidateOut:
    return ContentCandidateOut(
        id=r["id"], status=r["status"], source=r["source"], headline=r["headline"],
        tickers=list(r["tickers"] or []), futures_ticker=r["futures_ticker"],
        event_type=r["event_type"], importance_1_5=r["importance_1_5"],
        reasoning=r["reasoning"], matched_anomaly_id=r["matched_anomaly_id"],
        thread_key=r["thread_key"], parent_candidate_id=r["parent_candidate_id"],
        forwards_count=r["forwards_count"],
        created_at=r["created_at"].isoformat() if r["created_at"] else None,
        updated_at=r["updated_at"].isoformat() if r["updated_at"] else None,
        pending_expires_at=r["pending_expires_at"].isoformat() if r["pending_expires_at"] else None,
        published_at=r["published_at"].isoformat() if r["published_at"] else None,
        reviewer_action=r["reviewer_action"],
    )


@router.get("", response_model=ContentCandidateListOut)
def list_candidates(
    status: str = Query(..., description="одна из колонок доски — см. STATUS_VALUES"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
    _admin: User = Depends(require_admin),
):
    """Карточки одной колонки Kanban, свежие сначала. Фронт зовёт это по разу
    на каждую из 8 колонок (без общего barrier-запроса — колонки independent)."""
    if status not in STATUS_VALUES:
        raise HTTPException(status_code=422, detail=f"Неизвестный статус: {status}")

    total = db.execute(
        text("SELECT count(*) FROM content_candidates WHERE status = :status"),
        {"status": status},
    ).scalar() or 0

    rows = db.execute(
        text(f"""
            SELECT {_COLUMNS} FROM content_candidates
            WHERE status = :status
            ORDER BY created_at DESC
            LIMIT :limit OFFSET :offset
        """),
        {"status": status, "limit": limit, "offset": offset},
    ).mappings().all()

    return ContentCandidateListOut(
        items=[_row_to_out(r) for r in rows], total=total, limit=limit, offset=offset,
    )


@router.get("/{candidate_id}", response_model=ContentCandidateDetailOut)
def get_candidate(
    candidate_id: int,
    db: Session = Depends(get_db),
    _admin: User = Depends(require_admin),
):
    """Полная карточка (клик по card → модалка) + остальные кандидаты того же
    треда (thread_key), отсортированные по времени — для связки повторных
    сигналов по одному тикеру в UI."""
    row = db.execute(
        text(f"""
            SELECT {_COLUMNS}, raw_text, draft_text, synth_declined_reason, reviewer_id
            FROM content_candidates WHERE id = :id
        """),
        {"id": candidate_id},
    ).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="Кандидат не найден")

    thread: list[ContentCandidateOut] = []
    if row["thread_key"]:
        thread_rows = db.execute(
            text(f"""
                SELECT {_COLUMNS} FROM content_candidates
                WHERE thread_key = :thread_key AND id != :id
                ORDER BY created_at ASC
            """),
            {"thread_key": row["thread_key"], "id": candidate_id},
        ).mappings().all()
        thread = [_row_to_out(r) for r in thread_rows]

    base = _row_to_out(row)
    return ContentCandidateDetailOut(
        **base.model_dump(),
        raw_text=row["raw_text"], draft_text=row["draft_text"],
        synth_declined_reason=row["synth_declined_reason"], reviewer_id=row["reviewer_id"],
        thread=thread,
    )


# Разрешённые переходы — зеркало state machine из шапки db/migrations/022.
# Строго вперёд по пайплайну; терминальные статусы (discarded/no_data/published/
# rejected) переходов не имеют — ручной откат не предусмотрен в v1 (если понадобится
# чинить ошибочный статус, это прямой SQL, не API).
_ALLOWED_TRANSITIONS: dict[str, set[str]] = {
    "candidate": {"discarded", "pending"},
    "pending": {"draft_ready", "no_data"},
    "draft_ready": {"in_review"},
    "in_review": {"published", "rejected"},
}


class ContentCandidatePatch(BaseModel):
    status: str


@router.patch("/{candidate_id}", response_model=ContentCandidateOut)
def update_candidate_status(
    candidate_id: int,
    body: ContentCandidatePatch,
    db: Session = Depends(get_db),
    admin: User = Depends(require_admin),
):
    """Ручная смена статуса из админ-Kanban (напр. отбраковать кандидата не
    дожидаясь ИИ, или подтвердить публикацию, сделанную не через бот). Только
    заявленные в state machine переходы — 422 на всё остальное."""
    if body.status not in STATUS_VALUES:
        raise HTTPException(status_code=422, detail=f"Неизвестный статус: {body.status}")

    row = db.execute(
        text("SELECT status FROM content_candidates WHERE id = :id"), {"id": candidate_id},
    ).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="Кандидат не найден")

    current = row["status"]
    allowed = _ALLOWED_TRANSITIONS.get(current, set())
    if body.status not in allowed:
        raise HTTPException(
            status_code=422,
            detail=f"Переход {current} → {body.status} не допускается state machine "
                   f"(разрешено: {sorted(allowed) or 'нет — терминальный статус'})",
        )

    is_review_decision = current == "in_review" and body.status in ("published", "rejected")
    db.execute(
        text(f"""
            UPDATE content_candidates
            SET status = :status,
                updated_at = now()
                {", reviewer_action = :reviewer_action, reviewer_id = :reviewer_id" if is_review_decision else ""}
                {", published_at = now()" if body.status == "published" else ""}
            WHERE id = :id
        """),
        {
            "id": candidate_id, "status": body.status,
            **({"reviewer_action": "approved" if body.status == "published" else "rejected",
                "reviewer_id": admin.id} if is_review_decision else {}),
        },
    )
    db.commit()

    updated = db.execute(
        text(f"SELECT {_COLUMNS} FROM content_candidates WHERE id = :id"), {"id": candidate_id},
    ).mappings().first()
    return _row_to_out(updated)


# ══════════════════════════════════════════════════════════════════════════
# /api/internal/content-news/* — callback для ИИ-шагов (Routine или прямой
# API, см. signals/content_ai.py). Server-to-server: НЕ require_admin (ИИ-шаг
# не залогинен как пользователь) — проверка через shared secret в заголовке.
# Не публиковать этот путь в открытой документации/сайтмапе.
# ══════════════════════════════════════════════════════════════════════════

internal_router = APIRouter(prefix="/api/internal/content-news", tags=["internal-content-ai"])


def _require_internal_token(x_internal_token: str = Header(default="")) -> None:
    expected = os.environ.get("CONTENT_AI_INTERNAL_TOKEN", "")
    if not expected or x_internal_token != expected:
        raise HTTPException(status_code=403, detail="Неверный или отсутствующий internal token")


class StepAResult(BaseModel):
    relevant: bool
    tickers: list[str] = []
    event_type: Optional[str] = None
    importance_1_5: int
    reasoning: str


class StepCResult(BaseModel):
    draft_text: Optional[str] = None
    declined_reason: Optional[str] = None


@internal_router.patch("/{candidate_id}/step-a", dependencies=[Depends(_require_internal_token)])
def apply_step_a(candidate_id: int, body: StepAResult, db: Session = Depends(get_db)):
    """Приёмка результата Шага А (извлечение). Тут же — Шаг А2 (маппинг тикера,
    БЕЗ ИИ): резолвим первый известный тикер через ticker_futures_map. Если
    ничего не резолвилось — кандидат всё равно уходит в pending, но
    content_match.py (Шаг Б) требует futures_ticker IS NOT NULL, так что просто
    зависнет непроверенным до ручного добавления тикера в справочник."""
    row = db.execute(
        text("SELECT status FROM content_candidates WHERE id = :id"), {"id": candidate_id},
    ).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="Кандидат не найден")
    if row["status"] != "candidate":
        raise HTTPException(status_code=409, detail=f"Ожидался статус 'candidate', сейчас '{row['status']}'")

    if not body.relevant or body.importance_1_5 < 3:
        db.execute(text("""
            UPDATE content_candidates
            SET status = 'discarded', tickers = :tickers, event_type = :event_type,
                importance_1_5 = :importance, reasoning = :reasoning, updated_at = now()
            WHERE id = :id
        """), {"id": candidate_id, "tickers": body.tickers, "event_type": body.event_type,
               "importance": body.importance_1_5, "reasoning": body.reasoning})
        db.commit()
        return {"status": "discarded"}

    futures_ticker = None
    for t in body.tickers:
        m = db.execute(
            text("SELECT futures_sectype FROM ticker_futures_map WHERE stock_ticker = :t"), {"t": t},
        ).scalar()
        if m:
            futures_ticker = m
            break

    db.execute(text("""
        UPDATE content_candidates
        SET status = 'pending', tickers = :tickers, futures_ticker = :futures_ticker,
            event_type = :event_type, importance_1_5 = :importance, reasoning = :reasoning,
            thread_key = COALESCE(thread_key, :thread_key),
            pending_expires_at = now() + make_interval(days => :pending_days),
            updated_at = now()
        WHERE id = :id
    """), {
        "id": candidate_id, "tickers": body.tickers, "futures_ticker": futures_ticker,
        "event_type": body.event_type, "importance": body.importance_1_5,
        "reasoning": body.reasoning,
        "thread_key": f"ticker:{body.tickers[0]}" if body.tickers else None,
        "pending_days": 5,
    })
    db.commit()
    return {"status": "pending", "futures_ticker": futures_ticker}


@internal_router.patch("/{candidate_id}/step-c", dependencies=[Depends(_require_internal_token)])
def apply_step_c(candidate_id: int, body: StepCResult, db: Session = Depends(get_db)):
    """Приёмка результата Шага В (синтез). Если модель отказалась писать
    («недостаточно данных») — НЕ дожидаемся веры на слово, возвращаем кандидата
    в pending на повторную попытку (новые данные могут появиться до истечения
    pending_expires_at), а не тупик — ровно как обсуждали для watch-thread."""
    row = db.execute(
        text("SELECT status FROM content_candidates WHERE id = :id"), {"id": candidate_id},
    ).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="Кандидат не найден")
    if row["status"] != "draft_ready":
        raise HTTPException(status_code=409, detail=f"Ожидался статус 'draft_ready', сейчас '{row['status']}'")

    if body.draft_text:
        db.execute(text("""
            UPDATE content_candidates
            SET draft_text = :draft_text, synth_declined_reason = NULL, updated_at = now()
            WHERE id = :id
        """), {"id": candidate_id, "draft_text": body.draft_text})
        db.commit()
        return {"status": "draft_ready", "has_draft": True}

    db.execute(text("""
        UPDATE content_candidates
        SET status = 'pending', synth_declined_reason = :reason, updated_at = now()
        WHERE id = :id
    """), {"id": candidate_id, "reason": body.declined_reason or "модель не указала причину"})
    db.commit()
    return {"status": "pending", "has_draft": False}
