"""
API endpoints для открытого интереса
С валидацией входных данных
"""
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from datetime import datetime, date

from api.database import get_db
from api.models import OpenInterest
from api.schemas import OpenInterestResponse, OpenInterestListResponse
from api.schemas.validators import ClgroupType, validate_safe_id
from api.routers.auth import get_current_user_optional
from api.security.access_control import enforce_guest_limits

router = APIRouter(prefix="/api/openinterest", tags=["open_interest"])


@router.get("/{sectype}", response_model=OpenInterestListResponse)
def get_open_interest(
        sectype: str,
        clgroup: ClgroupType = Query("FIZ", description="Группа: FIZ (физлица) или YUR (юрлица)"),
        interval: int = Query(60, description="Таймфрейм: 5, 60 или 24 минут"),
        date_from: date | None = Query(None, description="Дата начала (YYYY-MM-DD)"),
        date_to: date | None = Query(None, description="Дата окончания (YYYY-MM-DD)"),
        limit: int = Query(10000, description="Максимум записей", ge=1, le=50000),
        db: Session = Depends(get_db),
        user = Depends(get_current_user_optional)
):
    """Получить открытый интерес по sectype"""

    # Валидация interval
    if interval not in {5, 60, 24}:
        raise HTTPException(status_code=400, detail="interval должен быть 5, 60 или 24")

    # Ограничения для гостей
    enforce_guest_limits(user, interval=interval)

    # Валидация sectype
    try:
        sectype = validate_safe_id(sectype, "sectype")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    # Валидация диапазона дат
    if date_from and date_to and date_to < date_from:
        raise HTTPException(status_code=400, detail="date_to не может быть раньше date_from")

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