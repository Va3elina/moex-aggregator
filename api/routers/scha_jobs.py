"""
/api/internal/scha/jobs/* — ручки для рутины frame-scha-reader (агент читает справки о СЧА).

  GET  /api/internal/scha/jobs/{id}         задание: фонд, месяц, ссылка, ВЕСЬ текст справки,
                                            что фонд держал в прошлом месяце (для самопроверки)
  POST /api/internal/scha/jobs/{id}/result  ответ агента: бумаги, итоги, самопроверка, ok/сомнения

Server-to-server: shared secret X-Internal-Token = SCHA_INTERNAL_TOKEN — СВОЙ, не общий с заводом постов
(CONTENT_AI_INTERNAL_TOKEN публикует посты в канал). Агент читает внешний документ: если в справке
спрятана инструкция для модели, утёкший токен открывает только эти две ручки. Рутина не залогинена
как пользователь.

Конвейер — signals/scha_pipeline.py: он вынимает текст в архив (/data/manual_scha/auto, сюда
смонтирован только на чтение), ставит рутину в общую очередь и после ответа заливает/сообщает.
Ответ кладётся в scha_jobs.agent как есть; приём идемпотентен (повтор той же сессии и запоздавший
ответ после таймаута ничего не ломают) и закрывает задание общей очереди routine_queue.
"""
import logging
import os
from pathlib import Path
from typing import Any, Optional

from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.orm import Session

from api.database import get_db

logger = logging.getLogger("moex_api")

internal_router = APIRouter(prefix="/api/internal/scha", tags=["internal-scha"])

ARCHIVE_IN_API = Path("/data/manual_scha/auto")
MONTHLY_SOURCES = ["interfax_manual", "vim_sdr"]


def _require_internal_token(x_internal_token: str = Header(default="")) -> None:
    expected = os.environ.get("SCHA_INTERNAL_TOKEN", "")
    if not expected or x_internal_token != expected:
        raise HTTPException(status_code=403, detail="Неверный или отсутствующий internal token")


def _text_in_api(host_path: str) -> Path:
    """Хост пишет /opt/frame/data/manual_scha/auto/…, контейнер видит /data/manual_scha/auto/…"""
    tail = host_path.split("/manual_scha/auto/", 1)[-1]
    p = (ARCHIVE_IN_API / tail).resolve()
    if ARCHIVE_IN_API.resolve() not in p.parents:
        raise HTTPException(status_code=400, detail="путь не из архива справок")
    return p


@internal_router.get("/jobs/{job_id}", dependencies=[Depends(_require_internal_token)])
def get_job(job_id: int, db: Session = Depends(get_db)):
    job = db.execute(text("""
        SELECT j.id, j.ticker, j.period, j.url, j.status, j.text_path, f.name AS fund_name, f.fund_id
        FROM scha_jobs j LEFT JOIN funds f ON f.ticker = j.ticker WHERE j.id = :id
    """), {"id": job_id}).mappings().first()
    if not job:
        raise HTTPException(status_code=404, detail="нет такого задания")
    if not job["text_path"]:
        raise HTTPException(status_code=409, detail="текст справки ещё не готов")
    try:
        body = _text_in_api(job["text_path"]).read_text()
    except FileNotFoundError:
        raise HTTPException(status_code=410, detail="файл текста пропал из архива")
    prev = []
    if job["fund_id"]:
        prev = db.execute(text("""
            SELECT snapshot_date::text AS snapshot_date, isin, asset_name AS name,
                   positions AS quantity, amount_rub AS value_rub, weight
            FROM fund_holdings_history
            WHERE fund_id = :f AND source = ANY(:s) AND snapshot_date = (
                SELECT max(snapshot_date) FROM fund_holdings_history
                WHERE fund_id = :f AND source = ANY(:s) AND snapshot_date < CAST(:d AS date))
            ORDER BY amount_rub DESC NULLS LAST
        """), {"f": job["fund_id"], "s": MONTHLY_SOURCES, "d": job["period"] + "-01"}).mappings().all()
    return {
        "job_id": job["id"], "ticker": job["ticker"], "fund_name": job["fund_name"],
        "period": job["period"], "url": job["url"], "status": job["status"],
        "previous_month": [dict(r) for r in prev],
        "text": body,
    }


class AgentResult(BaseModel):
    queue_id: Optional[int] = None      # для журнала; закрываем ТОЛЬКО очередь своего задания
    snapshot_date: Optional[str] = None
    gross_assets_rub: Optional[float] = None
    rows: list[dict[str, Any]] = Field(default_factory=list)
    category_totals: list[dict[str, Any]] = Field(default_factory=list)
    self_check: Optional[Any] = None
    ok: bool = False                     # агент: «всё сошлось, можно лить»
    doubts: list[Any] = Field(default_factory=list)


@internal_router.post("/jobs/{job_id}/result", dependencies=[Depends(_require_internal_token)])
def post_result(job_id: int, body: AgentResult, db: Session = Depends(get_db)):
    import json
    job = db.execute(text("SELECT id, status, queue_id FROM scha_jobs WHERE id = :id FOR UPDATE"),
                     {"id": job_id}).mappings().first()
    if not job:
        raise HTTPException(status_code=404, detail="нет такого задания")
    # Ответ принимается только от задания, которое его ждёт: повтор колбэка той же сессии не
    # перетирает первый ответ, а ответ «чужого» номера не оживит закрытое/упавшее задание.
    if job["status"] != "agent_wait":
        db.commit()
        return {"status": "already", "job_status": job["status"]}
    # Закрываем ТОЛЬКО задание очереди этой справки — queue_id из тела не доверяем: иначе ответом
    # можно было бы закрыть чужую рутину (завод постов) в общей очереди.
    if job["queue_id"]:
        db.execute(text("""
            UPDATE routine_queue SET status = 'done', finished_at = now()
            WHERE id = :q AND status IN ('firing', 'running', 'queued')
        """), {"q": job["queue_id"]})
    db.execute(text("""
        UPDATE scha_jobs SET status = 'agent_done', agent = CAST(:a AS jsonb), agent_ok = :ok,
               next_try_at = now(), updated_at = now(), last_error = NULL
        WHERE id = :id
    """), {"id": job_id, "a": json.dumps(body.model_dump(), ensure_ascii=False), "ok": body.ok})
    db.execute(text("INSERT INTO scha_job_events (job_id, event, detail) VALUES (:j, 'agent', :d)"),
               {"j": job_id, "d": f"ok={body.ok}, строк {len(body.rows)}, сомнений {len(body.doubts)}"})
    db.commit()
    logger.info("scha job #%s: ответ агента ok=%s rows=%s", job_id, body.ok, len(body.rows))
    return {"status": "accepted"}
