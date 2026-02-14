"""
Схемы валидации входных данных для API
=====================================

Защита от:
- SQL Injection
- Некорректных параметров
- Переполнения буфера
- Неверных диапазонов дат
"""

from pydantic import BaseModel, Field, field_validator, model_validator
from typing import Literal, Annotated
from datetime import date
import re


# ═══════════════════════════════════════════════════════════════
# Базовые валидаторы (переиспользуемые)
# ═══════════════════════════════════════════════════════════════

# Паттерн для sec_id, sectype — только буквы, цифры, _, -
SAFE_ID_PATTERN = re.compile(r'^[A-Za-z0-9_-]+$')


def validate_safe_id(value: str, field_name: str = "значение") -> str:
    """Проверка что строка содержит только безопасные символы"""
    if not value:
        raise ValueError(f"{field_name} не может быть пустым")
    if len(value) > 50:
        raise ValueError(f"{field_name} слишком длинный (макс. 50 символов)")
    if not SAFE_ID_PATTERN.match(value):
        raise ValueError(f"{field_name} содержит недопустимые символы (разрешены: A-Z, 0-9, _, -)")
    return value  # Сохраняем оригинальный регистр (Si, а не SI)


def validate_search_query(value: str) -> str:
    """Валидация поискового запроса"""
    if not value:
        raise ValueError("Поисковый запрос не может быть пустым")
    if len(value) > 100:
        raise ValueError("Поисковый запрос слишком длинный (макс. 100 символов)")
    if len(value) < 1:
        raise ValueError("Поисковый запрос слишком короткий (мин. 1 символ)")
    # Убираем опасные символы для SQL
    dangerous_chars = [';', '--', '/*', '*/', 'xp_', 'UNION', 'SELECT', 'DROP', 'DELETE', 'INSERT', 'UPDATE']
    value_upper = value.upper()
    for char in dangerous_chars:
        if char in value_upper:
            raise ValueError(f"Поисковый запрос содержит недопустимые символы")
    return value.strip()


# ═══════════════════════════════════════════════════════════════
# Типы параметров
# ═══════════════════════════════════════════════════════════════

# Допустимые значения
# Допустимые интервалы (FastAPI конвертирует строки в int автоматически)
ALLOWED_INTERVALS = {5, 60, 24}
IntervalType = int  # Валидация будет в роутерах
ClgroupType = Literal["FIZ", "YUR"]
PeriodType = Literal["1d", "1w", "1m", "3m", "6m", "1y", "all"]
InstTypeType = Literal["futures", "stock"]
SortByType = Literal["oi", "change"]
HeatmapSizeByType = Literal["value_1d", "value_1w", "value_1m", "volume_1d", "volume_1w", "volume_1m"]
HeatmapColorByType = Literal["change_1d", "change_1w", "change_1m"]
HeatmapGroupByType = Literal["sector", "none"]


# ═══════════════════════════════════════════════════════════════
# Схемы для chart.py
# ═══════════════════════════════════════════════════════════════

class ChartParams(BaseModel):
    """Параметры запроса графика"""
    sec_id: str = Field(..., min_length=1, max_length=50)
    sectype: str = Field(..., min_length=1, max_length=50)
    inst_type: InstTypeType = "futures"
    interval: IntervalType = 24
    clgroup: ClgroupType = "FIZ"
    show_oi: bool = True
    period: PeriodType = "6m"
    date_from: date | None = None
    date_to: date | None = None

    @field_validator('sec_id')
    @classmethod
    def validate_sec_id(cls, v: str) -> str:
        return validate_safe_id(v, "sec_id")

    @field_validator('sectype')
    @classmethod
    def validate_sectype(cls, v: str) -> str:
        return validate_safe_id(v, "sectype")

    @model_validator(mode='after')
    def validate_date_range(self):
        if self.date_from and self.date_to:
            if self.date_to < self.date_from:
                raise ValueError("date_to не может быть раньше date_from")
        return self


class IntervalsParams(BaseModel):
    """Параметры запроса доступных интервалов"""
    sectype: str = Field(..., min_length=1, max_length=50)
    clgroup: ClgroupType = "FIZ"

    @field_validator('sectype')
    @classmethod
    def validate_sectype(cls, v: str) -> str:
        return validate_safe_id(v, "sectype")


# ═══════════════════════════════════════════════════════════════
# Схемы для instruments.py
# ═══════════════════════════════════════════════════════════════

class InstrumentsFilterParams(BaseModel):
    """Параметры фильтрации инструментов"""
    type: InstTypeType | None = None
    group: str | None = Field(None, max_length=100)

    @field_validator('group')
    @classmethod
    def validate_group(cls, v: str | None) -> str | None:
        if v is None:
            return v
        if len(v) > 100:
            raise ValueError("Название группы слишком длинное")
        return v.strip()


class InstrumentSearchParams(BaseModel):
    """Параметры поиска инструментов"""
    q: str = Field(..., min_length=1, max_length=100)

    @field_validator('q')
    @classmethod
    def validate_query(cls, v: str) -> str:
        return validate_search_query(v)


class InstrumentByIdParams(BaseModel):
    """Параметры запроса инструмента по ID"""
    sec_id: str = Field(..., min_length=1, max_length=50)

    @field_validator('sec_id')
    @classmethod
    def validate_sec_id(cls, v: str) -> str:
        return validate_safe_id(v, "sec_id")


# ═══════════════════════════════════════════════════════════════
# Схемы для candles.py
# ═══════════════════════════════════════════════════════════════

class CandlesParams(BaseModel):
    """Параметры запроса свечей"""
    sec_id: str = Field(..., min_length=1, max_length=50)
    interval: IntervalType = 60
    date_from: date | None = None
    date_to: date | None = None
    limit: int = Field(default=10000, ge=1, le=50000)

    @field_validator('sec_id')
    @classmethod
    def validate_sec_id(cls, v: str) -> str:
        return validate_safe_id(v, "sec_id")

    @model_validator(mode='after')
    def validate_date_range(self):
        if self.date_from and self.date_to:
            if self.date_to < self.date_from:
                raise ValueError("date_to не может быть раньше date_from")
        return self


# ═══════════════════════════════════════════════════════════════
# Схемы для open_interest.py
# ═══════════════════════════════════════════════════════════════

class OpenInterestParams(BaseModel):
    """Параметры запроса открытого интереса"""
    sectype: str = Field(..., min_length=1, max_length=50)
    clgroup: ClgroupType = "FIZ"
    interval: IntervalType = 60
    date_from: date | None = None
    date_to: date | None = None
    limit: int = Field(default=10000, ge=1, le=50000)

    @field_validator('sectype')
    @classmethod
    def validate_sectype(cls, v: str) -> str:
        return validate_safe_id(v, "sectype")

    @model_validator(mode='after')
    def validate_date_range(self):
        if self.date_from and self.date_to:
            if self.date_to < self.date_from:
                raise ValueError("date_to не может быть раньше date_from")
        return self


# ═══════════════════════════════════════════════════════════════
# Схемы для heatmap.py
# ═══════════════════════════════════════════════════════════════

class HeatmapParams(BaseModel):
    """Параметры карты рынка"""
    size_by: HeatmapSizeByType = "value_1d"
    color_by: HeatmapColorByType = "change_1d"
    group_by: HeatmapGroupByType = "none"


# ═══════════════════════════════════════════════════════════════
# Схемы для stats.py
# ═══════════════════════════════════════════════════════════════

class StatsParams(BaseModel):
    """Параметры общей статистики"""
    period: PeriodType = "1w"
    clgroup: ClgroupType = "FIZ"


class TopInstrumentsParams(BaseModel):
    """Параметры топа инструментов"""
    period: PeriodType = "1w"
    clgroup: ClgroupType = "FIZ"
    limit: int = Field(default=10, ge=1, le=100)
    sort_by: SortByType = "change"


class DebugOIParams(BaseModel):
    """Параметры отладки OI"""
    sectype: str = Field(default="SR", min_length=1, max_length=50)
    clgroup: ClgroupType = "FIZ"

    @field_validator('sectype')
    @classmethod
    def validate_sectype(cls, v: str) -> str:
        return validate_safe_id(v, "sectype")


# ═══════════════════════════════════════════════════════════════
# Общие схемы пагинации
# ═══════════════════════════════════════════════════════════════

class PaginationParams(BaseModel):
    """Параметры пагинации"""
    limit: int = Field(default=100, ge=1, le=1000)
    offset: int = Field(default=0, ge=0)


class DateRangeParams(BaseModel):
    """Параметры диапазона дат"""
    date_from: date | None = None
    date_to: date | None = None

    @model_validator(mode='after')
    def validate_date_range(self):
        if self.date_from and self.date_to:
            if self.date_to < self.date_from:
                raise ValueError("date_to не может быть раньше date_from")
        return self