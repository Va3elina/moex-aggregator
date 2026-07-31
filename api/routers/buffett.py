"""
API роутер для Индикатора Баффетта
- Капитализация / ВВП (классический Buffett Indicator)
- MCFTR / M2 (индекс полной доходности / денежная масса)
"""

from fastapi import APIRouter, Query, Depends
from sqlalchemy import text
from typing import Literal
from datetime import date, timedelta
import time

from api.database import get_engine
from api.logger import get_logger
from api.routers.auth import get_current_user_optional
from api.security.access_control import enforce_tier_limits

router = APIRouter(prefix="/api/buffett", tags=["buffett"])
log = get_logger()

PeriodType = Literal["1m", "1y", "2y", "3y", "5y", "10y", "20y", "all"]

PERIODS = {
    "1m": 30,
    "1y": 365,
    "2y": 730,
    "3y": 1095,
    "5y": 1825,
    "10y": 3650,
    "20y": 7300,
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


def _ema_warmup_days(timeframe: str, span: int) -> int:
    """Дней истории для прогрева EMA(span) ПЕРЕД запрошенным периодом.

    _ema сидится первой точкой окна: без прогрева левый край сглаженной кривой —
    артефакт холодного старта, и значение на одной и той же дате зависело от
    выбранного периода (1y vs 3y давали разные числа). 3×span точек достаточно
    (вес первой точки < 5%). timeframe определяет «дней на точку»."""
    unit = {"1d": 1, "1w": 7}.get(timeframe, 30)
    return span * unit * 3


def _trim_to_period(data_points: list[dict], date_from_req) -> list[dict]:
    """Обрезает warmup-буфер: наружу уходит только запрошенный период."""
    cut = date_from_req.isoformat()
    return [p for p in data_points if p["date"] >= cut]


@router.get("/cap-gdp")
async def get_buffett_cap_gdp(
    period: PeriodType = Query("3y", description="Период"),
    smooth: bool = Query(True, description="Сглаживание EMA(60)"),
    timeframe: str = Query("1m", description="Таймфрейм агрегации: 1w или 1m"),
    user = Depends(get_current_user_optional),
):
    """
    Индикатор Баффетта: 100 × Капитализация / ВВП (TTM).
    GDP_TTM = скользящая сумма 4 последних кварталов, линейно интерполированная на ежедневную сетку.
    """
    # Tier: с 2026-07 индикатор бесплатен целиком — оба режима на всех тирах,
    # max_history_days=None, поэтому period здесь фактически НЕ проверяется и
    # вызов сейчас no-op. Оставлен точкой гейта на случай возврата ограничений.
    # Источник истины — INDICATOR_FEATURES["buffett"] в api/billing/features.py.
    enforce_tier_limits(user, "buffett", mode="cap-gdp", period=period)

    start_time = time.time()
    engine = get_engine()

    days = PERIODS.get(period)
    date_from_req = date.today() - timedelta(days=days) if days else date(1997, 1, 1)
    date_from = (date_from_req - timedelta(days=_ema_warmup_days(timeframe, 12))
                 if days else date_from_req)

    with engine.connect() as conn:
        # 1. Полная капитализация рынка РФ (млрд руб)
        cap_rows = conn.execute(text("""
            SELECT period_date, value
            FROM macro_data
            WHERE indicator = 'MARKET_CAP_TOTAL'
              AND period_date >= :date_from
              AND value IS NOT NULL
              AND value > 0
            ORDER BY period_date
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

    # 3. Агрегация дневных данных cap по периодам (день, неделя или месяц)
    if timeframe == "1d":
        # Линейная интерполяция между точками (до 2024 данные месячные)
        raw = [(row[0], float(row[1])) for row in cap_rows]
        if len(raw) >= 2:
            cap_monthly = []
            for i in range(len(raw) - 1):
                d0, v0 = raw[i]
                d1, v1 = raw[i + 1]
                gap = (d1 - d0).days
                cap_monthly.append((d0, v0))
                if gap > 2:
                    for day_offset in range(1, gap):
                        t = day_offset / gap
                        interp_date = d0 + timedelta(days=day_offset)
                        interp_val = v0 + (v1 - v0) * t
                        if interp_date.weekday() < 5:  # пн-пт
                            cap_monthly.append((interp_date, interp_val))
            cap_monthly.append(raw[-1])
        else:
            cap_monthly = raw
    else:
        period_cap: dict = {}
        for row in cap_rows:
            d = row[0]
            if timeframe == "1w":
                period_key = d.isocalendar()[:2]
            else:
                period_key = (d.year, d.month)
            period_cap[period_key] = (d, float(row[1]))
        cap_monthly = sorted(period_cap.values(), key=lambda x: x[0])

    if not cap_monthly:
        return {"data": [], "period": period}

    # 4. Рассчитать GDP_TTM (сумма 4 последних кварталов)
    gdp_ttm_points = []
    for i in range(3, len(gdp_rows)):
        ttm = sum(float(gdp_rows[j][1]) for j in range(i - 3, i + 1))
        gdp_ttm_points.append((gdp_rows[i][0], ttm))

    # 5. Интерполяция GDP_TTM на месячную сетку
    monthly_dates = [row[0] for row in cap_monthly]
    gdp_monthly = _interpolate_daily(gdp_ttm_points, monthly_dates)

    # 6. Рассчитать buffett = 100 * cap / gdp_ttm на месячных данных
    raw_values = []
    data_points = []
    for d, cap in cap_monthly:
        gdp_ttm = gdp_monthly.get(d)
        if gdp_ttm and gdp_ttm > 0:
            buffett_raw = 100.0 * cap / gdp_ttm
            raw_values.append(buffett_raw)
            data_points.append({
                "date": d.isoformat(),
                "buffett_raw": round(buffett_raw, 2),
                "cap": round(cap / 1e3, 3),  # трлн руб
                "gdp_ttm": round(gdp_ttm / 1e3, 2),  # трлн руб
            })

    # 7. Сглаживание EMA(12) — 12 месяцев = 1 год (для месячных данных)
    if smooth and raw_values:
        smoothed = _ema(raw_values, 12)
        for i, point in enumerate(data_points):
            point["buffett"] = round(smoothed[i], 2)
    else:
        for point in data_points:
            point["buffett"] = point["buffett_raw"]

    if days:
        data_points = _trim_to_period(data_points, date_from_req)

    duration = time.time() - start_time
    log.info(f"GET /buffett/cap-gdp period={period} smooth={smooth} -> {len(data_points)} points, {duration:.2f}s")

    return {"data": data_points, "period": period}


@router.get("/mcftr-m2")
async def get_buffett_mcftr_m2(
    period: PeriodType = Query("3y", description="Период"),
    smooth: bool = Query(True, description="Сглаживание EMA(60)"),
    user = Depends(get_current_user_optional),
):
    """
    MCFTR / M2: индекс полной доходности / денежная масса.
    M2 линейно интерполирована на ежедневную сетку.
    """
    # Tier: как cap-gdp/cap-m2 — открыт на всех тирах, глубина не ограничена.
    # До 2026-07-31 сидел на легаси-гейте enforce_guest_limits и резал гостю период
    # по GUEST_MAX_PERIOD=1y, пока соседние режимы отдавали всю историю. Асимметрия
    # была наследием, а не замыслом — снята переводом на общую матрицу тиров.
    enforce_tier_limits(user, "buffett", mode="mcftr-m2", period=period)

    start_time = time.time()
    engine = get_engine()

    days = PERIODS.get(period)
    date_from_req = date.today() - timedelta(days=days) if days else date(2000, 1, 1)
    # EMA(60) на дневных → прогрев 180 дней
    date_from = (date_from_req - timedelta(days=_ema_warmup_days("1d", 60))
                 if days else date_from_req)

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

    if days:
        data_points = _trim_to_period(data_points, date_from_req)

    duration = time.time() - start_time
    log.info(f"GET /buffett/mcftr-m2 period={period} smooth={smooth} -> {len(data_points)} points, {duration:.2f}s")

    return {"data": data_points, "period": period}


@router.get("/cap-m2")
async def get_buffett_cap_m2(
    period: PeriodType = Query("3y", description="Период"),
    smooth: bool = Query(True, description="Сглаживание EMA(12)"),
    timeframe: str = Query("1m", description="Таймфрейм: 1d, 1w, 1m"),
    user = Depends(get_current_user_optional),
):
    """
    Капитализация / M2: рыночная капитализация / денежная масса.
    M2 линейно интерполирована на сетку дат капитализации.
    """
    # Tier: cap-m2 открыт на всех тирах, включая free/гостя (см. cap-gdp выше)
    enforce_tier_limits(user, "buffett", mode="cap-m2", period=period)

    start_time = time.time()
    engine = get_engine()

    days = PERIODS.get(period)
    date_from_req = date.today() - timedelta(days=days) if days else date(1997, 1, 1)
    date_from = (date_from_req - timedelta(days=_ema_warmup_days(timeframe, 12))
                 if days else date_from_req)

    with engine.connect() as conn:
        cap_rows = conn.execute(text("""
            SELECT period_date, value
            FROM macro_data
            WHERE indicator = 'MARKET_CAP_TOTAL'
              AND period_date >= :date_from
              AND value IS NOT NULL AND value > 0
            ORDER BY period_date
        """), {"date_from": date_from}).fetchall()

        m2_rows = conn.execute(text("""
            SELECT period_date, value
            FROM macro_data
            WHERE indicator = 'M2_MONTHLY'
            ORDER BY period_date
        """)).fetchall()

    if not cap_rows or not m2_rows:
        return {"data": [], "period": period}

    # Агрегация по таймфрейму (как в cap-gdp)
    if timeframe == "1d":
        raw = [(row[0], float(row[1])) for row in cap_rows]
        if len(raw) >= 2:
            cap_agg = []
            for i in range(len(raw) - 1):
                d0, v0 = raw[i]
                d1, v1 = raw[i + 1]
                gap = (d1 - d0).days
                cap_agg.append((d0, v0))
                if gap > 2:
                    for day_offset in range(1, gap):
                        t = day_offset / gap
                        interp_date = d0 + timedelta(days=day_offset)
                        interp_val = v0 + (v1 - v0) * t
                        if interp_date.weekday() < 5:
                            cap_agg.append((interp_date, interp_val))
            cap_agg.append(raw[-1])
        else:
            cap_agg = raw
    else:
        period_cap: dict = {}
        for row in cap_rows:
            d = row[0]
            if timeframe == "1w":
                period_key = d.isocalendar()[:2]
            else:
                period_key = (d.year, d.month)
            period_cap[period_key] = (d, float(row[1]))
        cap_agg = sorted(period_cap.values(), key=lambda x: x[0])

    if not cap_agg:
        return {"data": [], "period": period}

    agg_dates = [d for d, _ in cap_agg]
    m2_sparse = [(row[0], float(row[1])) for row in m2_rows]
    m2_interp = _interpolate_daily(m2_sparse, agg_dates)

    data_points = []
    for d, cap in cap_agg:
        m2 = m2_interp.get(d)
        if m2 and m2 > 0:
            ratio = cap / m2
            data_points.append({
                "date": d.isoformat(),
                "ratio": round(ratio, 4),
                "cap": round(cap / 1000, 2),
                "m2": round(m2, 1),
            })

    if smooth and data_points:
        raw_ratios = [p["ratio"] for p in data_points]
        smoothed = _ema(raw_ratios, 12)
        for i, point in enumerate(data_points):
            point["ratio_raw"] = point["ratio"]
            point["ratio"] = round(smoothed[i], 4)

    if days:
        data_points = _trim_to_period(data_points, date_from_req)

    duration = time.time() - start_time
    log.info(f"GET /buffett/cap-m2 period={period} smooth={smooth} tf={timeframe} -> {len(data_points)} points, {duration:.2f}s")

    return {"data": data_points, "period": period}
