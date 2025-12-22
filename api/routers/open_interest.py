"""
API endpoints для открытого интереса
"""
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from datetime import datetime, date

from api.database import get_db
from api.models import OpenInterest
from api.schemas import OpenInterestResponse, OpenInterestListResponse

router = APIRouter(prefix="/api/openinterest", tags=["open_interest"])


@router.get("/{sectype}", response_model=OpenInterestListResponse)
def get_open_interest(
        sectype: str,
        clgroup: str = Query("FIZ", description="Группа: FIZ (физлица) или YUR (юрлица)"),
        interval: int = Query(60, description="Таймфрейм: 5, 60 или 24 минут"),
        date_from: date | None = Query(None, description="Дата начала (YYYY-MM-DD)"),
        date_to: date | None = Query(None, description="Дата окончания (YYYY-MM-DD)"),
        limit: int = Query(10000, description="Максимум записей", le=50000),
        db: Session = Depends(get_db)
):
    """Получить открытый интерес по sectype"""

    query = db.query(OpenInterest).filter(
        OpenInterest.sectype == sectype,
        OpenInterest.clgroup == clgroup,
        OpenInterest.interval == interval
    )

    # Фильтр по датам
    if date_from:
        query = query.filter(OpenInterest.tradedate >= date_from)
    if date_to:
        query = query.filter(OpenInterest.tradedate <= date_to)

    # Сортировка и лимит
    query = query.order_by(OpenInterest.tradedate.asc(), OpenInterest.tradetime.asc()).limit(limit)

    records = query.all()

    if not records:
        raise HTTPException(status_code=404, detail=f"Данные OI для {sectype} не найдены")

    # Преобразуем в формат ответа
    oi_list = [
        OpenInterestResponse(
            time=datetime.combine(r.tradedate, r.tradetime),
            pos=r.pos,
            pos_long=r.pos_long,
            pos_short=r.pos_short,
            pos_long_num=r.pos_long_num,
            pos_short_num=r.pos_short_num
        )
        for r in records
    ]

    return OpenInterestListResponse(
        sectype=sectype,
        clgroup=clgroup,
        interval=interval,
        count=len(oi_list),
        data=oi_list
    )