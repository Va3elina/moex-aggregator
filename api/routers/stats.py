"""
API endpoint для общей статистики открытого интереса
"""
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import text
from datetime import datetime, timedelta

from api.database import get_db

router = APIRouter(prefix="/api/stats", tags=["stats"])


def get_period_days(period: str) -> int:
    """Преобразует период в количество дней"""
    periods = {
        '1d': 1,
        '1w': 7,
        '1m': 30,
        '3m': 90,
        '6m': 180,
        '1y': 365,
        'all': 3650
    }
    return periods.get(period, 30)


@router.get("")
async def get_stats(
    period: str = Query(default="1w", description="Период: 1d, 1w, 1m, 3m, 6m, 1y, all"),
    clgroup: str = Query(default="FIZ", description="Группа: FIZ или YUR"),
    db: Session = Depends(get_db)
):
    """
    Получить общую статистику OI за период
    """
    days = get_period_days(period)
    start_date = datetime.now() - timedelta(days=days)

    # Для 1 дня используем часовые данные
    interval_val = 60 if period == '1d' else 24

    # Агрегированные данные OI по дням
    query = text("""
        SELECT 
            tradedate as date,
            SUM(pos_long) as total_long,
            SUM(ABS(pos_short)) as total_short,
            SUM(pos_long) + SUM(ABS(pos_short)) as total_oi,
            SUM(pos) as net_position,
            SUM(pos_long_num) as total_long_num,
            SUM(pos_short_num) as total_short_num
        FROM open_interest 
        WHERE interval = :interval_val
          AND tradedate >= :start_date
          AND clgroup = :clgroup
        GROUP BY tradedate
        ORDER BY tradedate
    """)

    result = db.execute(query, {
        "interval_val": interval_val,
        "start_date": start_date,
        "clgroup": clgroup
    }).fetchall()

    # Форматируем данные
    chart_data = []
    for row in result:
        chart_data.append({
            "date": row.date.strftime("%Y-%m-%d") if hasattr(row.date, 'strftime') else str(row.date),
            "total_oi": int(row.total_oi or 0),
            "total_long": int(row.total_long or 0),
            "total_short": int(row.total_short or 0),
            "net_position": int(row.net_position or 0),
            "total_long_num": int(row.total_long_num or 0),
            "total_short_num": int(row.total_short_num or 0),
        })

    # Вычисляем изменения
    if len(chart_data) >= 2:
        current = chart_data[-1]
        previous = chart_data[0]

        def calc_change(curr, prev):
            return ((curr - prev) / prev * 100) if prev > 0 else 0

        oi_change = calc_change(current["total_oi"], previous["total_oi"])
        long_change = calc_change(current["total_long"], previous["total_long"])
        short_change = calc_change(current["total_short"], previous["total_short"])

        current_participants = current["total_long_num"] + current["total_short_num"]
        previous_participants = previous["total_long_num"] + previous["total_short_num"]
        participants_change = calc_change(current_participants, previous_participants)
    else:
        current = chart_data[-1] if chart_data else {"total_oi": 0, "total_long": 0, "total_short": 0, "total_long_num": 0, "total_short_num": 0}
        oi_change = long_change = short_change = participants_change = 0
        current_participants = current.get("total_long_num", 0) + current.get("total_short_num", 0)

    # Количество инструментов
    instruments_query = text("""
        SELECT COUNT(DISTINCT sectype) as count
        FROM open_interest 
        WHERE interval = :interval_val
          AND tradedate >= :start_date
          AND clgroup = :clgroup
    """)
    instruments_count = db.execute(instruments_query, {
        "interval_val": interval_val,
        "start_date": start_date,
        "clgroup": clgroup
    }).fetchone()

    return {
        "period": period,
        "clgroup": clgroup,
        "stats": {
            "total_oi": current.get("total_oi", 0) if isinstance(current, dict) else current["total_oi"],
            "oi_change": round(oi_change, 2),
            "total_long": current.get("total_long", 0) if isinstance(current, dict) else current["total_long"],
            "long_change": round(long_change, 2),
            "total_short": current.get("total_short", 0) if isinstance(current, dict) else current["total_short"],
            "short_change": round(short_change, 2),
            "participants": current_participants,
            "participants_change": round(participants_change, 2),
            "instruments_count": instruments_count.count if instruments_count else 0,
        },
        "chart_data": chart_data,
        "data_points": len(chart_data),
    }


@router.get("/top")
async def get_top_instruments(
    period: str = Query(default="1w", description="Период"),
    clgroup: str = Query(default="FIZ", description="Группа"),
    limit: int = Query(default=10, description="Количество"),
    sort_by: str = Query(default="change", description="Сортировка: oi, change"),
    db: Session = Depends(get_db)
):
    """
    Получить топ инструментов по изменению OI за период
    """
    days = get_period_days(period)
    start_date = datetime.now() - timedelta(days=days)

    # Получаем текущие и начальные значения OI для каждого инструмента
    query = text("""
        WITH current_oi AS (
            SELECT DISTINCT ON (sectype) 
                sectype,
                tradedate as current_date,
                pos_long as current_long,
                ABS(pos_short) as current_short,
                pos_long + ABS(pos_short) as current_oi
            FROM open_interest 
            WHERE interval = 24 
              AND clgroup = :clgroup
            ORDER BY sectype, tradedate DESC
        ),
        previous_oi AS (
            SELECT DISTINCT ON (sectype) 
                sectype,
                tradedate as previous_date,
                pos_long + ABS(pos_short) as previous_oi
            FROM open_interest 
            WHERE interval = 24 
              AND clgroup = :clgroup
              AND tradedate >= :start_date
            ORDER BY sectype, tradedate ASC
        )
        SELECT 
            c.sectype,
            c.current_oi,
            c.current_long,
            c.current_short,
            p.previous_oi,
            CASE 
                WHEN p.previous_oi > 0 
                THEN ROUND(((c.current_oi - p.previous_oi)::numeric / p.previous_oi * 100), 2)
                ELSE 0 
            END as oi_change_pct,
            c.current_oi - COALESCE(p.previous_oi, c.current_oi) as oi_change_abs
        FROM current_oi c
        LEFT JOIN previous_oi p ON c.sectype = p.sectype
        WHERE c.current_oi > 1000
        ORDER BY 
            CASE WHEN :sort_by = 'change' 
                THEN ABS(CASE WHEN p.previous_oi > 0 
                    THEN ((c.current_oi - p.previous_oi)::numeric / p.previous_oi * 100)
                    ELSE 0 END)
                ELSE c.current_oi 
            END DESC
        LIMIT :limit_val
    """)

    result = db.execute(query, {
        "clgroup": clgroup,
        "start_date": start_date,
        "sort_by": sort_by,
        "limit_val": limit
    }).fetchall()

    top_instruments = []
    for row in result:
        top_instruments.append({
            "sectype": row.sectype,
            "current_oi": int(row.current_oi or 0),
            "current_long": int(row.current_long or 0),
            "current_short": int(row.current_short or 0),
            "previous_oi": int(row.previous_oi or 0),
            "oi_change": float(row.oi_change_pct or 0),
            "oi_change_abs": int(row.oi_change_abs or 0),
        })

    # Сортируем по абсолютному изменению (по модулю) если sort_by == 'change'
    if sort_by == 'change':
        top_instruments.sort(key=lambda x: abs(x['oi_change']), reverse=True)

    return {
        "period": period,
        "clgroup": clgroup,
        "sort_by": sort_by,
        "instruments": top_instruments,
    }


@router.get("/debug")
async def debug_oi(
    sectype: str = Query(default="SR"),
    clgroup: str = Query(default="FIZ"),
    db: Session = Depends(get_db)
):
    """
    Отладочный endpoint для проверки данных OI
    """
    query = text("""
        SELECT 
            tradedate,
            pos,
            pos_long,
            pos_short,
            pos_long_num,
            pos_short_num
        FROM open_interest 
        WHERE sectype = :sectype
          AND clgroup = :clgroup
          AND interval = 24
        ORDER BY tradedate DESC
        LIMIT 5
    """)

    result = db.execute(query, {
        "sectype": sectype,
        "clgroup": clgroup
    }).fetchall()

    data = []
    for row in result:
        data.append({
            "date": str(row.tradedate),
            "pos": row.pos,
            "pos_long": row.pos_long,
            "pos_short": row.pos_short,
            "abs_pos_short": abs(row.pos_short) if row.pos_short else 0,
            "calculated_oi": (row.pos_long or 0) + abs(row.pos_short or 0),
            "pos_long_num": row.pos_long_num,
            "pos_short_num": row.pos_short_num,
        })

    return {
        "sectype": sectype,
        "clgroup": clgroup,
        "data": data,
    }