"""
Pydantic схемы для свечей
"""
from pydantic import BaseModel
from datetime import datetime


class CandleResponse(BaseModel):
    """Схема одной свечи для API"""
    time: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float

    class Config:
        from_attributes = True


class CandleListResponse(BaseModel):
    """Схема ответа API для списка свечей"""
    secid: str
    sec_id: str
    interval: int
    count: int
    candles: list[CandleResponse]