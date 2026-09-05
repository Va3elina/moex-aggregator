"""
Второй мозг — HTTP-обёртка над api/brain_core.py (там вся логика, без FastAPI, чтобы
её мог импортировать и content_ai на хосте).

⚠️ ДОСТУП: внутренний токен (Routine-агенты) ИЛИ сессия админа (панель); решения по
очередям — только админ. Ошибки ядра (Ошибка с кодом) превращаются в HTTPException здесь.
"""

import os
from typing import Optional

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from sqlalchemy.orm import Session

from api import brain_core as ядро
from api.database import get_db
from api.models import User
from api.routers.auth import get_current_user_optional

router = APIRouter(prefix="/api/internal/brain", tags=["brain"])


def _доступ(
    x_internal_token: str = Header(default=""),
    user: Optional[User] = Depends(get_current_user_optional),
) -> str:
    ожидаемый = os.environ.get("CONTENT_AI_INTERNAL_TOKEN", "")
    if ожидаемый and x_internal_token == ожидаемый:
        return "agent"
    if user is not None and user.role == "admin":
        return "admin"
    raise HTTPException(status_code=403, detail="нужен internal token или сессия администратора")


def _только_админ(who: str = Depends(_доступ)) -> str:
    if who != "admin":
        raise HTTPException(403, "решения по очередям принимает человек")
    return who


def _вызов(f, **kw):
    try:
        return f(**kw)
    except ядро.Ошибка as e:
        raise HTTPException(e.код, e.текст)


@router.get("/stats")
def статистика(db: Session = Depends(get_db), who: str = Depends(_доступ)):
    return _вызов(ядро.статистика, db=db, _who=who)


@router.get("/top")
def самые_связанные(kind: str = Query("company"), limit: int = Query(30, ge=1, le=100),
                    db: Session = Depends(get_db), who: str = Depends(_доступ)):
    return _вызов(ядро.самые_связанные, kind=kind, limit=limit, db=db, _who=who)


@router.get("/node/{node_id:path}")
def узел(node_id: str, since: Optional[int] = Query(None, ge=1, le=3650), per_ring: int = Query(3, ge=1, le=40),
         text_: bool = Query(False, alias="text"), candidate_id: Optional[int] = Query(None),
         db: Session = Depends(get_db), who: str = Depends(_доступ)):
    return _вызов(ядро.узел, node_id=node_id, since=since, per_ring=per_ring, text_=text_, candidate_id=candidate_id, db=db, _who=who)


@router.get("/neighbors")
def соседи(id: str = Query(...), kind: Optional[str] = Query(None), since: Optional[int] = Query(None, ge=1, le=3650),
           limit: int = Query(50, ge=1, le=500), offset: int = Query(0, ge=0), candidate_id: Optional[int] = Query(None),
           db: Session = Depends(get_db), who: str = Depends(_доступ)):
    return _вызов(ядро.соседи, id=id, kind=kind, since=since, limit=limit, offset=offset, candidate_id=candidate_id, db=db, _who=who)


@router.get("/search")
def поиск(q: str = Query(..., min_length=2, max_length=300), kind: Optional[str] = Query(None), mode: str = Query("word"),
          limit: int = Query(20, ge=1, le=100), candidate_id: Optional[int] = Query(None),
          db: Session = Depends(get_db), who: str = Depends(_доступ)):
    return _вызов(ядро.поиск, q=q, kind=kind, mode=mode, limit=limit, candidate_id=candidate_id, db=db, _who=who)


@router.get("/similar")
def похожие(id: str = Query(...), kind: Optional[str] = Query(None), limit: int = Query(12, ge=1, le=60),
            candidate_id: Optional[int] = Query(None), db: Session = Depends(get_db), who: str = Depends(_доступ)):
    return _вызов(ядро.похожие, id=id, kind=kind, limit=limit, candidate_id=candidate_id, db=db, _who=who)


@router.get("/context")
def контекст(ticker: str = Query(..., min_length=1, max_length=20), days: int = Query(14, ge=1, le=365),
             candidate_id: Optional[int] = Query(None), db: Session = Depends(get_db), who: str = Depends(_доступ)):
    return _вызов(ядро.контекст, ticker=ticker, days=days, candidate_id=candidate_id, db=db, _who=who)


@router.get("/review/holders")
def очередь_держателей(status: str = Query("на_проверке"), limit: int = Query(100, ge=1, le=500),
                       db: Session = Depends(get_db), who: str = Depends(_доступ)):
    return _вызов(ядро.очередь_держателей, status=status, limit=limit, db=db, _who=who)


@router.patch("/review/holders/{holder_norm:path}")
def решить_держателя(holder_norm: str, decision: str = Query(...), company_id: Optional[str] = Query(None),
                     note: Optional[str] = Query(None), db: Session = Depends(get_db), who: str = Depends(_только_админ)):
    return _вызов(ядро.решить_держателя, holder_norm=holder_norm, decision=decision, company_id=company_id, note=note, db=db, _who=who)


@router.get("/review/names")
def правила_имён(ambiguous: Optional[bool] = Query(None), db: Session = Depends(get_db), who: str = Depends(_доступ)):
    return _вызов(ядро.правила_имён, ambiguous=ambiguous, db=db, _who=who)


@router.patch("/review/names/{rule_id}")
def править_правило(rule_id: int, enabled: Optional[bool] = Query(None), ambiguous: Optional[bool] = Query(None),
                    note: Optional[str] = Query(None), db: Session = Depends(get_db), who: str = Depends(_только_админ)):
    return _вызов(ядро.править_правило, rule_id=rule_id, enabled=enabled, ambiguous=ambiguous, note=note, db=db, _who=who)


@router.get("/path")
def путь(a: str = Query(...), b: str = Query(...), max_depth: int = Query(3, ge=1, le=4),
         candidate_id: Optional[int] = Query(None), db: Session = Depends(get_db), who: str = Depends(_доступ)):
    return _вызов(ядро.путь, a=a, b=b, max_depth=max_depth, candidate_id=candidate_id, db=db, _who=who)


@router.get("/graph")
def граф(center: Optional[str] = Query(None), depth: int = Query(2, ge=1, le=3), per_node: int = Query(40, ge=5, le=200),
         news: bool = Query(False), limit: int = Query(600, ge=20, le=1500),
         db: Session = Depends(get_db), who: str = Depends(_доступ)):
    return _вызов(ядро.граф, center=center, depth=depth, per_node=per_node, news=news, limit=limit, db=db, _who=who)
