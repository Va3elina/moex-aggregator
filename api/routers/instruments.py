"""
API endpoints для инструментов
С валидацией входных данных
"""
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session

from api.database import get_db
from api.models import Instrument
from api.schemas import InstrumentResponse, InstrumentListResponse
from api.schemas.validators import (
    InstrumentsFilterParams,
    InstrumentSearchParams,
    InstTypeType,
    validate_safe_id,
    validate_search_query
)

router = APIRouter(prefix="/api/instruments", tags=["instruments"])


@router.get("", response_model=InstrumentListResponse)
def get_instruments(
        type: InstTypeType | None = Query(None, description="Фильтр по типу: futures или stock"),
        group: str | None = Query(None, max_length=100, description="Фильтр по группе: Валюта, Акции и т.д."),
        db: Session = Depends(get_db)
):
    """Получить список всех инструментов, отсортированных по объёму торгов"""
    from sqlalchemy import text

    filters = []
    params = {}
    if type:
        filters.append("i.type = :type")
        params["type"] = type
    if group:
        filters.append("i.\"group\" = :group")
        params["group"] = group.strip()[:100]

    where_clause = "WHERE " + " AND ".join(filters) if filters else ""

    rows = db.execute(text(f"""
        SELECT i.sec_id, i.sectype, i.name, i.type, i."group", i.iss_code,
               COALESCE(v.vol, 0) as daily_volume
        FROM instruments i
        LEFT JOIN (
            SELECT sec_id, SUM(volume) as vol
            FROM candles
            WHERE interval = 24
              AND begin_time >= CURRENT_DATE - INTERVAL '3 days'
            GROUP BY sec_id
        ) v ON v.sec_id = i.sec_id
        {where_clause}
        ORDER BY daily_volume DESC
    """), params).fetchall()

    instruments = [
        InstrumentResponse(
            sec_id=r[0], sectype=r[1], name=r[2],
            type=r[3], group=r[4], iss_code=r[5],
            daily_volume=float(r[6]) if r[6] else 0
        )
        for r in rows
    ]

    return InstrumentListResponse(
        count=len(instruments),
        instruments=instruments
    )


@router.get("/groups")
def get_groups(db: Session = Depends(get_db)):
    """Получить список всех групп инструментов"""
    groups = db.query(Instrument.group).distinct().all()
    return {"groups": [g[0] for g in groups if g[0]]}


@router.get("/search", response_model=InstrumentListResponse)
def search_instruments(
        q: str = Query(..., min_length=1, max_length=100, description="Поисковый запрос"),
        db: Session = Depends(get_db)
):
    """Поиск инструментов по названию или тикеру"""

    # Валидация поискового запроса
    try:
        q = validate_search_query(q)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    # ORM автоматически экранирует параметры в ilike
    query = db.query(Instrument).filter(
        (Instrument.name.ilike(f"%{q}%")) |
        (Instrument.sectype.ilike(f"%{q}%")) |
        (Instrument.sec_id.ilike(f"%{q}%"))
    )
    instruments = query.all()

    return InstrumentListResponse(
        count=len(instruments),
        instruments=instruments
    )


@router.get("/{sec_id}", response_model=InstrumentResponse)
def get_instrument(sec_id: str, db: Session = Depends(get_db)):
    """Получить инструмент по sec_id или sectype.

    Фронтенд оперирует sectype как идентификатором актива: для фьючерсов это
    код серии («CR», «Si»), а в instruments sec_id хранит конкретный контракт
    («CRH», «SiU»). Матч только по sec_id не находил строку при резолве имени
    из URL-параметра (?instrument=CR) → в UI вместо названия оставался тикер.
    Ищем по обоим полям; точное совпадение по sec_id приоритетнее.
    """

    # Валидация sec_id
    try:
        sec_id = validate_safe_id(sec_id, "sec_id")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    instrument = (
        db.query(Instrument)
        .filter((Instrument.sec_id == sec_id) | (Instrument.sectype == sec_id))
        .order_by((Instrument.sec_id == sec_id).desc())
        .first()
    )

    if not instrument:
        raise HTTPException(status_code=404, detail=f"Инструмент {sec_id} не найден")

    return instrument