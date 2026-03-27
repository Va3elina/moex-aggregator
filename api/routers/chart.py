"""
API endpoint для получения свечей и данных OI
С валидацией входных данных и защитой от SQL injection

ИСПРАВЛЕНО: добавлено поле net_position = pos_long + pos_short
(pos_short в БД уже отрицательный, поэтому используем ПЛЮС)
"""

from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from datetime import datetime, date, time as dt_time, timedelta
from api.cache import get_or_set, DEFAULT_TTL
from typing import Optional
from pydantic import BaseModel
import time

from api.logger import get_logger

log = get_logger()

from api.database import get_db
from api.models import Instrument
from api.schemas import CandleResponse, OpenInterestResponse
from api.schemas.validators import (
    ChartParams,
    IntervalsParams,
    ClgroupType,
    PeriodType,
    InstTypeType,
    validate_safe_id
)
from api.routers.auth import get_current_user_optional
from api.security.access_control import enforce_guest_limits

router = APIRouter(prefix='/api/chart', tags=['chart'])


class ChartResponse(BaseModel):
    sec_id: str
    sectype: str
    interval: int
    clgroup: str
    candles_count: int
    oi_count: int
    candles: list[CandleResponse]
    open_interest: list[OpenInterestResponse]
    oi_start_date: str | None = None
    oi_end_date: str | None = None
    candles_start_date: str | None = None
    candles_end_date: str | None = None
    has_oi_data: bool = False
    contracts: list[str] = []
    mode: str = "price_and_oi"
    period: str = "6m"
    data_start: str | None = None
    data_end: str | None = None
    available_intervals: list[int] = []


class AvailableIntervalsResponse(BaseModel):
    sectype: str
    intervals: list[dict]


PERIODS = {
    "1d": 1, "1w": 7, "1m": 30, "3m": 90,
    "6m": 180, "1y": 365, "all": 10000
}


@router.get("/intervals/{sectype}", response_model=AvailableIntervalsResponse)
def get_available_intervals(
    sectype: str,
    clgroup: ClgroupType = Query("FIZ"),
    db: Session = Depends(get_db)
):
    """Получить доступные интервалы OI для инструмента"""

    try:
        sectype = validate_safe_id(sectype, "sectype")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    query = text("""
        SELECT 
            interval,
            COUNT(*) as cnt,
            MIN(tradedate) as start_date,
            MAX(tradedate) as end_date
        FROM open_interest 
        WHERE sectype = :sectype 
          AND clgroup = :clgroup
        GROUP BY interval
        ORDER BY interval
    """)

    result = db.execute(query, {"sectype": sectype, "clgroup": clgroup}).fetchall()

    intervals = []
    for row in result:
        intervals.append({
            "interval": row.interval,
            "count": row.cnt,
            "start": str(row.start_date) if row.start_date else None,
            "end": str(row.end_date) if row.end_date else None,
        })

    return AvailableIntervalsResponse(
        sectype=sectype,
        intervals=intervals
    )


@router.get("/{sec_id}", response_model=ChartResponse)
def get_chart_data(
        sec_id: str,
        sectype: str = Query(...),
        inst_type: InstTypeType = Query("futures"),
        interval: int = Query(24),
        clgroup: ClgroupType = Query("FIZ"),
        show_oi: bool = Query(True),
        period: PeriodType = Query("6m"),
        date_from: Optional[date] = Query(None),
        date_to: Optional[date] = Query(None),
        db: Session = Depends(get_db),
        user = Depends(get_current_user_optional)
):
    """Получить данные графика с валидацией"""

    if interval not in {5, 60, 24}:
        raise HTTPException(status_code=400, detail="interval должен быть 5, 60 или 24")

    # Ограничения для гостей (временно отключено для отладки)
    # enforce_guest_limits(user, interval=interval, period=period)

    try:
        sec_id = validate_safe_id(sec_id, "sec_id")
        sectype = validate_safe_id(sectype, "sectype")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    if date_from and date_to and date_to < date_from:
        raise HTTPException(status_code=400, detail="date_to не может быть раньше date_from")

    # Кеширование (TTL 30мин — инкрементально обновляется при NOTIFY)
    cache_key = f"chart:{sec_id}:{sectype}:{inst_type}:{interval}:{clgroup}:{show_oi}:{period}:{date_from}:{date_to}"
    cached = get_or_set(cache_key)
    if cached is not None:
        return cached

    log.info(f"REQUEST: {sec_id}, sectype={sectype}, interval={interval}, period={period}")
    total_start = time.time()

    # 1. Получаем sec_ids
    t0 = time.time()
    sec_ids_result = db.execute(text("""
        SELECT DISTINCT sec_id FROM instruments 
        WHERE sectype = :sectype AND type = :inst_type
    """), {"sectype": sectype, "inst_type": inst_type}).fetchall()
    sec_ids = [r[0] for r in sec_ids_result] or [sec_id]
    log.debug(f"[1] sec_ids: {(time.time()-t0)*1000:.0f} мс | {sec_ids}")

    # 2. Границы свечей (один запрос вместо 2*N)
    t0 = time.time()
    bounds_row = db.execute(text("""
        SELECT MIN(begin_time), MAX(begin_time) FROM candles
        WHERE sec_id = ANY(:sec_ids) AND interval = :interval
    """), {"sec_ids": sec_ids, "interval": interval}).fetchone()

    c_start = bounds_row[0] if bounds_row else None
    c_end = bounds_row[1] if bounds_row else None

    if c_start:
        c_start = c_start.date() if hasattr(c_start, 'date') else c_start
    if c_end:
        c_end = c_end.date() if hasattr(c_end, 'date') else c_end

    log.debug(f"[2] candles bounds: {(time.time()-t0)*1000:.0f} мс | {c_start} - {c_end}")

    # 3. Границы OI
    t0 = time.time()
    oi_bounds = db.execute(text("""
        SELECT MIN(tradedate), MAX(tradedate) FROM open_interest 
        WHERE sectype = :sectype AND clgroup = :clgroup AND interval = :interval
    """), {"sectype": sectype, "clgroup": clgroup, "interval": interval}).fetchone()
    oi_start, oi_end = oi_bounds if oi_bounds else (None, None)
    log.debug(f"[3] OI bounds: {(time.time()-t0)*1000:.0f} мс | {oi_start} - {oi_end}")

    if not c_end:
        log.warning("[!] Нет данных свечей!")
        return ChartResponse(
            sec_id=sec_id, sectype=sectype, interval=interval, clgroup=clgroup,
            candles_count=0, oi_count=0, candles=[], open_interest=[],
            has_oi_data=False, contracts=sec_ids, mode="price_only", period=period
        )

    has_oi_data = oi_end is not None

    # 4. Рабочий период
    if show_oi and has_oi_data:
        data_start = max(c_start, oi_start)
        data_end = min(c_end, oi_end)
        mode = "price_and_oi"
    else:
        data_start = c_start
        data_end = c_end
        mode = "price_only"

    if date_from and date_to:
        work_start = max(data_start, date_from)
        work_end = min(data_end, date_to)
    else:
        work_end = data_end
        days = PERIODS.get(period, 180)
        period_start = work_end - timedelta(days=days)
        work_start = max(data_start, period_start)

    log.debug(f"[4] work period: {work_start} - {work_end}")

    # 5. Запрос свечей (включаем sec_id для умной дедупликации)
    t0 = time.time()
    candles_raw = db.execute(text("""
        SELECT begin_time, open, high, low, close, volume, sec_id
        FROM candles
        WHERE sec_id = ANY(:sec_ids)
          AND interval = :interval
          AND begin_time >= :start_time
          AND begin_time <= :end_time
          AND close > 0
        ORDER BY begin_time
    """), {
        "sec_ids": sec_ids,
        "interval": interval,
        "start_time": datetime.combine(work_start, dt_time.min),
        "end_time": datetime.combine(work_end, dt_time.max)
    }).fetchall()
    log.debug(f"[5] candles query: {(time.time()-t0)*1000:.0f} мс | rows: {len(candles_raw)}")

    # 6. Склейка контрактов: для каждого ДНЯ выбираем самый ликвидный контракт
    # Это позволяет плавно переходить между контрактами при ролловере
    t0 = time.time()
    
    # Группируем свечи по дням и контрактам
    daily_volume = {}  # {date: {sec_id: total_volume}}
    for c in candles_raw:
        day = c[0].date()
        sec_id = c[6] if len(c) > 6 else 'unknown'
        vol = float(c[5] or 0)
        
        if day not in daily_volume:
            daily_volume[day] = {}
        daily_volume[day][sec_id] = daily_volume[day].get(sec_id, 0) + vol
    
    # Определяем лучший контракт для каждого дня
    best_contract_by_day = {}
    for day, contracts in daily_volume.items():
        if contracts:
            best_contract_by_day[day] = max(contracts, key=contracts.get)
    
    # Фильтруем: для каждого begin_time оставляем свечу лучшего контракта этого дня
    # (per-day, а не per-timestamp — иначе забор при близких объёмах двух контрактов)
    best_by_time = {}  # {begin_time: candle_row}
    for c in candles_raw:
        bt = c[0]
        vol = float(c[5] or 0)
        # Для интрадей: пропускаем фейковые свечи (volume=0, zero-fill артефакты)
        if interval != 24 and vol == 0:
            continue
        day = bt.date()
        sec_id_c = c[6] if len(c) > 6 else 'unknown'
        best_sid = best_contract_by_day.get(day)
        if best_sid and sec_id_c == best_sid:
            best_by_time[bt] = c
        elif bt not in best_by_time:
            # Fallback: если нет best для дня, берём первую попавшуюся
            best_by_time[bt] = c

    sorted_candles = sorted(best_by_time.values(), key=lambda x: x[0])
    log.debug(f"[6] chain: {(time.time()-t0)*1000:.0f} мс | candles: {len(sorted_candles)}")

    # 7. Запрос OI
    oi_raw = []
    if show_oi and has_oi_data and sorted_candles:
        t0 = time.time()
        actual_start = sorted_candles[0][0].date()
        actual_end = sorted_candles[-1][0].date()

        oi_raw = db.execute(text("""
            SELECT 
                tradedate, 
                tradetime, 
                pos,
                pos_long, 
                pos_short, 
                pos_long_num, 
                pos_short_num
            FROM open_interest
            WHERE sectype = :sectype
              AND clgroup = :clgroup
              AND interval = :interval
              AND tradedate >= :start_date
              AND tradedate <= :end_date
            ORDER BY tradedate, tradetime
        """), {
            "sectype": sectype,
            "clgroup": clgroup,
            "interval": interval,
            "start_date": actual_start,
            "end_date": actual_end
        }).fetchall()
        log.debug(f"[7] OI query: {(time.time()-t0)*1000:.0f} мс | rows: {len(oi_raw)}")

    # 8. Формируем ответ
    t0 = time.time()
    candles_list = [
        CandleResponse(
            time=c[0],
            open=float(c[1] or 0),
            high=float(c[2] or 0),
            low=float(c[3] or 0),
            close=float(c[4] or 0),
            volume=float(c[5] or 0)
        ) for c in sorted_candles
    ]

    # ============================================================
    # ИСПРАВЛЕНО: Вычисляем net_position = pos_long + pos_short
    # pos_short в БД уже ОТРИЦАТЕЛЬНЫЙ, поэтому используем ПЛЮС!
    #
    # Пример: pos_long=128694, pos_short=-26535
    #   net_position = 128694 + (-26535) = 102159 (физики в лонге)
    # ============================================================
    oi_list = []
    for r in oi_raw:
        pos_long = r[3] or 0
        pos_short = r[4] or 0

        # ПРАВИЛЬНАЯ ФОРМУЛА!
        net_position = pos_long + pos_short

        oi_list.append(OpenInterestResponse(
            time=datetime.combine(r[0], r[1]),
            pos=r[2],
            pos_long=pos_long,
            pos_short=pos_short,
            pos_long_num=r[5],
            pos_short_num=r[6],
            net_position=net_position
        ))

    log.debug(f"[8] build response: {(time.time()-t0)*1000:.0f} мс")

    total_ms = (time.time() - total_start) * 1000
    log.info(f"TOTAL: {sec_id} {total_ms:.0f} мс")

    # 9. Получаем доступные интервалы OI
    available_intervals_query = text("""
        SELECT DISTINCT interval 
        FROM open_interest 
        WHERE sectype = :sectype AND clgroup = :clgroup
        ORDER BY interval
    """)
    available_intervals = [
        row[0] for row in db.execute(
            available_intervals_query,
            {"sectype": sectype, "clgroup": clgroup}
        ).fetchall()
    ]

    response = ChartResponse(
        sec_id=sec_id,
        sectype=sectype,
        interval=interval,
        clgroup=clgroup,
        candles_count=len(candles_list),
        oi_count=len(oi_list),
        candles=candles_list,
        open_interest=oi_list,
        oi_start_date=str(oi_raw[0][0]) if oi_raw else None,
        oi_end_date=str(oi_raw[-1][0]) if oi_raw else None,
        candles_start_date=str(sorted_candles[0][0].date()) if sorted_candles else None,
        candles_end_date=str(sorted_candles[-1][0].date()) if sorted_candles else None,
        has_oi_data=has_oi_data,
        contracts=sec_ids,
        mode=mode,
        period=period,
        data_start=str(data_start) if data_start else None,
        data_end=str(data_end) if data_end else None,
        available_intervals=available_intervals
    )

    get_or_set(cache_key, response, ttl=DEFAULT_TTL)
    return response