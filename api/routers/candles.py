"""
API endpoints для свечей
С валидацией входных данных
"""
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from datetime import datetime, date

from api.database import get_db
from api.models import Candle
from api.schemas import CandleResponse, CandleListResponse
from api.schemas.validators import validate_safe_id
from api.routers.auth import get_current_user_optional
from api.security.access_control import enforce_guest_limits

router = APIRouter(prefix="/api/candles", tags=["candles"])


@router.get("/{sec_id}", response_model=CandleListResponse)
def get_candles(
        sec_id: str,
        interval: int = Query(60, description="Таймфрейм: 5, 60 или 24 минут"),
        date_from: date | None = Query(None, description="Дата начала (YYYY-MM-DD)"),
        date_to: date | None = Query(None, description="Дата окончания (YYYY-MM-DD)"),
        limit: int = Query(10000, description="Максимум записей", ge=1, le=50000),
        db: Session = Depends(get_db),
        user = Depends(get_current_user_optional)
):
    """Получить свечи по sec_id"""

    # Валидация interval
    if interval not in {5, 60, 24}:
        raise HTTPException(status_code=400, detail="interval должен быть 5, 60 или 24")

    # Ограничения для гостей
    enforce_guest_limits(user, interval=interval)

    # Валидация sec_id
    try:
        sec_id = validate_safe_id(sec_id, "sec_id")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    # Валидация диапазона дат
    if date_from and date_to and date_to < date_from:
        raise HTTPException(status_code=400, detail="date_to не может быть раньше date_from")

    query = db.query(Candle).filter(Candle.sec_id == sec_id)

    # Фильтр по интервалу
    query = query.filter(Candle.interval == interval)

    # Фильтр по датам
    if date_from:
        query = query.filter(Candle.begin_time >= datetime.combine(date_from, datetime.min.time()))
    if date_to:
        query = query.filter(Candle.begin_time <= datetime.combine(date_to, datetime.max.time()))

    # Сортировка и лимит
    query = query.order_by(Candle.begin_time.asc()).limit(limit)

    candles = query.all()

    if not candles:
        raise HTTPException(status_code=404, detail=f"Свечи для {sec_id} не найдены")

    # Преобразуем в формат ответа
    candle_list = [
        CandleResponse(
            time=c.begin_time,
            open=float(c.open) if c.open else 0,
            high=float(c.high) if c.high else 0,
            low=float(c.low) if c.low else 0,
            close=float(c.close) if c.close else 0,
            volume=float(c.volume) if c.volume else 0
        )
        for c in candles
    ]

    return CandleListResponse(
        secid=candles[0].secid,
        sec_id=sec_id,
        interval=interval,
        count=len(candle_list),
        candles=candle_list
    )