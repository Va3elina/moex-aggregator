"""
Pydantic схемы для инструментов
"""
from pydantic import BaseModel


class InstrumentResponse(BaseModel):
    """Схема ответа API для одного инструмента"""
    sec_id: str
    sectype: str
    name: str
    type: str | None = None
    group: str | None = None
    iss_code: str | None = None
    daily_volume: float | None = None
    day_change_pct: float | None = None  # % изменения за последний торговый день (дневной ТФ)

    class Config:
        from_attributes = True  # Позволяет создавать из SQLAlchemy модели


class InstrumentListResponse(BaseModel):
    """Схема ответа API для списка инструментов"""
    count: int
    instruments: list[InstrumentResponse]