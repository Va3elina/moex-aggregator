"""
API роутер для Индикатора Баффетта
- Капитализация / ВВП (классический Buffett Indicator)
- MCFTR / M2 (индекс полной доходности / денежная масса)
"""

from fastapi import APIRouter, Query
from sqlalchemy import text
from typing import Literal
from datetime import date, timedelta
import time

from api.database import get_engine
from api.logger import get_logger

router = APIRouter(prefix="/api/buffett", tags=["buffett"])
log = get_logger()

PeriodType = Literal["1y", "2y", "3y", "5y", "all"]

PERIODS = {
    "1y": 365,
    "2y": 730,
    "3y": 1095,
    "5y": 1825,
    "all": None,
}


def _interpolate_daily(sparse_data: list[tuple], daily_dates: list[date]) -> dict[date, float]:
    """
    Линейная интерполяция разреженных данных (квартальных/месячных) на ежедневную сетку.
    sparse_data: [(date, value), ...] отсортированные по дате
    daily_dates: [date, ...] целевые даты
    Возвращает {date: interpolated_value}
    """
    if not sparse_data or not daily_dates:
        return {}

    result = {}
    sparse_idx = 0

    for d in daily_dates:
        # Двигаем указатель вперёд
        while sparse_idx < len(sparse_data) - 1 and sparse_data[sparse_idx + 1][0] <= d:
            sparse_idx += 1

        if sparse_idx >= len(sparse_data) - 1:
            # За пределами — используем последнее значение
            result[d] = float(sparse_data[-1][1])
        elif d <= sparse_data[0][0]:
            # До начала — используем первое значение
            result[d] = float(sparse_data[0][1])
        else:
            # Линейная интерполяция между двумя точками
            d1, v1 = sparse_data[sparse_idx]
            d2, v2 = sparse_data[sparse_idx + 1]
            days_total = (d2 - d1).days
            if days_total == 0:
                result[d] = float(v1)
            else:
                t = (d - d1).days / days_total
                result[d] = float(v1) + t * (float(v2) - float(v1))

    return result


def _ema(values: list[float], span: int) -> list[float]:
    """Экспоненциальное скользящее среднее."""
    if not values:
        return []
    alpha = 2 / (span + 1)
    result = [values[0]]
    for v in values[1:]:
        result.append(alpha * v + (1 - alpha) * result[-1])
    return result


@router.get("/cap-gdp")
async def get_buffett_cap_gdp(
    period: PeriodType = Query("3y", description="Период"),
    smooth: bool = Query(True, description="Сглаживание EMA(60)"),
):
    """
    Индикатор Баффетта: 100 × Капитализация / ВВП (TTM).
    GDP_TTM = скользящая сумма 4 последних кварталов, линейно интерполированная на ежедневную сетку.
    """
    start_time = time.time()
    engine = get_engine()

    days = PERIODS.get(period)
    date_from = date.today() - timedelta(days=days) if days else date(2000, 1, 1)

    with engine.connect() as conn:
        # 1. Капитализация IMOEX
        cap_rows = conn.execute(text("""
            SELECT trade_date, capitalization
            FROM index_data
            WHERE secid = 'IMOEX'
              AND trade_date >= :date_from
              AND capitalization IS NOT NULL
              AND capitalization > 0
            ORDER BY trade_date
        """), {"date_from": date_from}).fetchall()

        # 2. GDP квартальный (берём всю историю для расчёта TTM)
        gdp_rows = conn.execute(text("""
            SELECT period_date, value
            FROM macro_data
            WHERE indicator = 'GDP_QUARTERLY'
            ORDER BY period_date
        """)).fetchall()

    if not cap_rows or len(gdp_rows) < 4:
        return {"data": [], "period": period}

    # 3. Рассчитать GDP_TTM (сумма 4 последних кварталов)
    gdp_ttm_points = []
    for i in range(3, len(gdp_rows)):
        ttm = sum(float(gdp_rows[j][1]) for j in range(i - 3, i + 1))
        gdp_ttm_points.append((gdp_rows[i][0], ttm))

    # 4. Интерполяция GDP_TTM на ежедневную сетку
    daily_dates = [row[0] for row in cap_rows]
    gdp_daily = _interpolate_daily(gdp_ttm_points, daily_dates)

    # 5. Рассчитать buffett = 100 * cap / gdp_ttm
    # Капитализация в рублях, GDP в млрд руб → приведение: cap / (gdp * 1e9) * 100
    raw_values = []
    data_points = []
    for row in cap_rows:
        d = row[0]
        cap = float(row[1])
        gdp_ttm = gdp_daily.get(d)
        if gdp_ttm and gdp_ttm > 0:
            buffett_raw = 100.0 * cap / (gdp_ttm * 1e9)
            raw_values.append(buffett_raw)
            data_points.append({
                "date": d.isoformat(),
                "buffett_raw": round(buffett_raw, 2),
                "cap": round(cap / 1e12, 3),  # трлн руб
                "gdp_ttm": round(gdp_ttm / 1e3, 2),  # трлн руб
            })

    # 6. Сглаживание EMA(60)
    if smooth and raw_values:
        smoothed = _ema(raw_values, 60)
        for i, point in enumerate(data_points):
            point["buffett"] = round(smoothed[i], 2)
    else:
        for point in data_points:
            point["buffett"] = point["buffett_raw"]

    duration = time.time() - start_time
    log.info(f"GET /buffett/cap-gdp period={period} smooth={smooth} -> {len(data_points)} points, {duration:.2f}s")

    return {"data": data_points, "period": period}


@router.get("/mcftr-m2")
async def get_buffett_mcftr_m2(
    period: PeriodType = Query("3y", description="Период"),
    smooth: bool = Query(True, description="Сглаживание EMA(60)"),
):
    """
    MCFTR / M2: индекс полной доходности / денежная масса.
    M2 линейно интерполирована на ежедневную сетку.
    """
    start_time = time.time()
    engine = get_engine()

    days = PERIODS.get(period)
    date_from = date.today() - timedelta(days=days) if days else date(2000, 1, 1)

    with engine.connect() as conn:
        # 1. MCFTR close
        mcftr_rows = conn.execute(text("""
            SELECT trade_date, close
            FROM index_data
            WHERE secid = 'MCFTR'
              AND trade_date >= :date_from
              AND close IS NOT NULL
              AND close > 0
            ORDER BY trade_date
        """), {"date_from": date_from}).fetchall()

        # 2. M2 месячный (вся история для интерполяции)
        m2_rows = conn.execute(text("""
            SELECT period_date, value
            FROM macro_data
            WHERE indicator = 'M2_MONTHLY'
            ORDER BY period_date
        """)).fetchall()

    if not mcftr_rows or not m2_rows:
        return {"data": [], "period": period}

    # 3. Интерполяция M2 на ежедневную сетку
    daily_dates = [row[0] for row in mcftr_rows]
    m2_sparse = [(row[0], float(row[1])) for row in m2_rows]
    m2_daily = _interpolate_daily(m2_sparse, daily_dates)

    # 4. Рассчитать ratio = mcftr / m2
    data_points = []
    for row in mcftr_rows:
        d = row[0]
        mcftr = float(row[1])
        m2 = m2_daily.get(d)
        if m2 and m2 > 0:
            ratio = mcftr / m2
            data_points.append({
                "date": d.isoformat(),
                "ratio": round(ratio, 6),
                "mcftr": round(mcftr, 2),
                "m2": round(m2, 1),
            })

    # 5. Сглаживание EMA(60)
    if smooth and data_points:
        raw_ratios = [p["ratio"] for p in data_points]
        smoothed = _ema(raw_ratios, 60)
        for i, point in enumerate(data_points):
            point["ratio_raw"] = point["ratio"]
            point["ratio"] = round(smoothed[i], 6)

    duration = time.time() - start_time
    log.info(f"GET /buffett/mcftr-m2 period={period} smooth={smooth} -> {len(data_points)} points, {duration:.2f}s")

    return {"data": data_points, "period": period}
