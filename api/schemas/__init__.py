"""
Pydantic схемы для API
"""
from api.schemas.instrument import InstrumentResponse, InstrumentListResponse
from api.schemas.candle import CandleResponse, CandleListResponse
from api.schemas.open_interest import OpenInterestResponse, OpenInterestListResponse

# Валидаторы входных данных
from api.schemas.validators import (
    # Базовые функции валидации
    validate_safe_id,
    validate_search_query,

    # Типы параметров (Literal)
    IntervalType,
    ClgroupType,
    PeriodType,
    InstTypeType,
    SortByType,
    HeatmapSizeByType,
    HeatmapColorByType,
    HeatmapGroupByType,

    # Схемы валидации
    ChartParams,
    IntervalsParams,
    InstrumentsFilterParams,
    InstrumentSearchParams,
    InstrumentByIdParams,
    CandlesParams,
    OpenInterestParams,
    HeatmapParams,
    StatsParams,
    TopInstrumentsParams,
    DebugOIParams,
    PaginationParams,
    DateRangeParams,
)

__all__ = [
    # Схемы ответов
    "InstrumentResponse",
    "InstrumentListResponse",
    "CandleResponse",
    "CandleListResponse",
    "OpenInterestResponse",
    "OpenInterestListResponse",

    # Функции валидации
    "validate_safe_id",
    "validate_search_query",

    # Типы параметров
    "IntervalType",
    "ClgroupType",
    "PeriodType",
    "InstTypeType",
    "SortByType",
    "HeatmapSizeByType",
    "HeatmapColorByType",
    "HeatmapGroupByType",

    # Схемы валидации
    "ChartParams",
    "IntervalsParams",
    "InstrumentsFilterParams",
    "InstrumentSearchParams",
    "InstrumentByIdParams",
    "CandlesParams",
    "OpenInterestParams",
    "HeatmapParams",
    "StatsParams",
    "TopInstrumentsParams",
    "DebugOIParams",
    "PaginationParams",
    "DateRangeParams",
]