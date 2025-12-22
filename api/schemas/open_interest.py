"""
Pydantic схемы для открытого интереса
"""
from pydantic import BaseModel
from datetime import datetime


class OpenInterestResponse(BaseModel):
    """Схема одной записи OI для API"""
    time: datetime
    pos: int | None = None
    pos_long: int | None = None
    pos_short: int | None = None
    pos_long_num: int | None = None
    pos_short_num: int | None = None

    class Config:
        from_attributes = True


class OpenInterestListResponse(BaseModel):
    """Схема ответа API для списка OI"""
    sectype: str
    clgroup: str
    interval: int
    count: int
    data: list[OpenInterestResponse]