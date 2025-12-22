"""
API endpoint для получения свечей и данных OI
"""

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import text
from datetime import datetime, date, time as dt_time, timedelta
from typing import Optional
from pydantic import BaseModel
import time

from api.database import get_db
from api.models import Instrument
from api.schemas import CandleResponse, OpenInterestResponse

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
    intervals: list[dict]  # [{interval: 5, count: 1000, start: "2020-01-01", end: "2025-12-01"}, ...]


PERIODS = {
    "1d": 1, "1w": 7, "1m": 30, "3m": 90,
    "6m": 180, "1y": 365, "all": 10000
}


@router.get("/intervals/{sectype}", response_model=AvailableIntervalsResponse)
def get_available_intervals(
    sectype: str,
    clgroup: str = Query("FIZ"),
    db: Session = Depends(get_db)
):
    """
    Получить доступные интервалы OI для инструмента
    """
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
        inst_type: str = Query("futures"),
        interval: int = Query(24),
        clgroup: str = Query("FIZ"),
        show_oi: bool = Query(True),
        period: str = Query("6m"),
        date_from: Optional[date] = Query(None),
        date_to: Optional[date] = Query(None),
        db: Session = Depends(get_db)
):
    print(f"\n{'='*60}")
    print(f"REQUEST: {sec_id}, sectype={sectype}, interval={interval}, period={period}")
    total_start = time.time()

    # 1. Получаем sec_ids
    t0 = time.time()
    sec_ids_result = db.execute(text("""
        SELECT DISTINCT sec_id FROM instruments 
        WHERE sectype = :sectype AND type = :inst_type
    """), {"sectype": sectype, "inst_type": inst_type}).fetchall()
    sec_ids = [r[0] for r in sec_ids_result] or [sec_id]
    print(f"[1] sec_ids: {(time.time()-t0)*1000:.0f} мс | {sec_ids}")

    # 2. Границы свечей — быстрые отдельные запросы
    t0 = time.time()

    c_start = None
    c_end = None

    for sid in sec_ids:
        # MIN
        row = db.execute(text("""
            SELECT begin_time FROM candles 
            WHERE sec_id = :sec_id AND interval = :interval
            ORDER BY begin_time ASC LIMIT 1
        """), {"sec_id": sid, "interval": interval}).fetchone()
        if row:
            if c_start is None or row[0] < c_start:
                c_start = row[0]

        # MAX
        row = db.execute(text("""
            SELECT begin_time FROM candles 
            WHERE sec_id = :sec_id AND interval = :interval
            ORDER BY begin_time DESC LIMIT 1
        """), {"sec_id": sid, "interval": interval}).fetchone()
        if row:
            if c_end is None or row[0] > c_end:
                c_end = row[0]

    if c_start:
        c_start = c_start.date() if hasattr(c_start, 'date') else c_start
    if c_end:
        c_end = c_end.date() if hasattr(c_end, 'date') else c_end

    print(f"[2] candles bounds: {(time.time()-t0)*1000:.0f} мс | {c_start} - {c_end}")

    # 3. Границы OI
    t0 = time.time()
    oi_bounds = db.execute(text("""
        SELECT MIN(tradedate), MAX(tradedate) FROM open_interest 
        WHERE sectype = :sectype AND clgroup = :clgroup AND interval = :interval
    """), {"sectype": sectype, "clgroup": clgroup, "interval": interval}).fetchone()
    oi_start, oi_end = oi_bounds if oi_bounds else (None, None)
    print(f"[3] OI bounds: {(time.time()-t0)*1000:.0f} мс | {oi_start} - {oi_end}")

    if not c_end:
        print(f"[!] Нет данных свечей!")
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

    print(f"[4] work period: {work_start} - {work_end}")

    # 5. Запрос свечей
    t0 = time.time()
    sec_ids_sql = ",".join(f"'{s}'" for s in sec_ids)
    candles_raw = db.execute(text(f"""
        SELECT begin_time, open, high, low, close, volume
        FROM candles
        WHERE sec_id IN ({sec_ids_sql})
          AND interval = :interval
          AND begin_time >= :start_time
          AND begin_time <= :end_time
          AND close > 0
        ORDER BY begin_time
    """), {
        "interval": interval,
        "start_time": datetime.combine(work_start, dt_time.min),
        "end_time": datetime.combine(work_end, dt_time.max)
    }).fetchall()
    print(f"[5] candles query: {(time.time()-t0)*1000:.0f} мс | rows: {len(candles_raw)}")

    # 6. Дедупликация
    t0 = time.time()
    candles_map = {}
    for c in candles_raw:
        t = c[0]
        if t not in candles_map or (c[5] or 0) > (candles_map[t][5] or 0):
            candles_map[t] = c
    sorted_candles = sorted(candles_map.values(), key=lambda x: x[0])
    print(f"[6] dedup: {(time.time()-t0)*1000:.0f} мс | unique: {len(sorted_candles)}")

    # 7. Запрос OI (ИСПРАВЛЕНО: добавлено поле pos)
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
        print(f"[7] OI query: {(time.time()-t0)*1000:.0f} мс | rows: {len(oi_raw)}")

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

    # ИСПРАВЛЕНО: правильные расчёты OI
    # r[0] = tradedate
    # r[1] = tradetime
    # r[2] = pos (чистая позиция = pos_long + pos_short)
    # r[3] = pos_long
    # r[4] = pos_short (отрицательное в БД!)
    # r[5] = pos_long_num
    # r[6] = pos_short_num
    oi_list = [
        OpenInterestResponse(
            time=datetime.combine(r[0], r[1]),
            pos=r[2],  # чистая позиция из БД
            pos_long=r[3],  # позиции лонг
            pos_short=r[4],  # позиции шорт (отрицательное!)
            pos_long_num=r[5],  # количество покупателей
            pos_short_num=r[6]  # количество продавцов
        ) for r in oi_raw
    ]
    print(f"[8] build response: {(time.time()-t0)*1000:.0f} мс")

    total_ms = (time.time() - total_start) * 1000
    print(f"{'='*60}")
    print(f"TOTAL: {total_ms:.0f} мс")
    print(f"{'='*60}\n")

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

    return ChartResponse(
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