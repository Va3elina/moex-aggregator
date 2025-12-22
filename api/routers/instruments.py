"""
API endpoints для инструментов
"""
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from api.database import get_db
from api.models import Instrument
from api.schemas import InstrumentResponse, InstrumentListResponse

router = APIRouter(prefix="/api/instruments", tags=["instruments"])


@router.get("", response_model=InstrumentListResponse)
def get_instruments(
        type: str | None = Query(None, description="Фильтр по типу: futures или stock"),
        group: str | None = Query(None, description="Фильтр по группе: Валюта, Акции и т.д."),
        db: Session = Depends(get_db)
):
    """Получить список всех инструментов"""
    query = db.query(Instrument)

    if type:
        query = query.filter(Instrument.type == type)
    if group:
        query = query.filter(Instrument.group == group)

    instruments = query.all()

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
        q: str = Query(..., description="Поисковый запрос"),
        db: Session = Depends(get_db)
):
    """Поиск инструментов по названию или тикеру"""
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
    """Получить инструмент по sec_id"""
    instrument = db.query(Instrument).filter(Instrument.sec_id == sec_id).first()

    if not instrument:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail=f"Инструмент {sec_id} не найден")

    return instrument