"""
Market Breadth API — Сила рынка
Рассчитывает % акций торгующихся выше EMA

/history — читает из pre-computed таблицы breadth_history (мгновенно)
/current — считает на лету для текущей даты (быстро, 42 тикера)
"""
from fastapi import APIRouter, Query, HTTPException
from sqlalchemy import text
from datetime import date, timedelta
import pandas as pd
import time

from api.database import get_engine
from api.cache import get_or_set

router = APIRouter(prefix="/api/breadth", tags=["breadth"])


def get_stock_tickers() -> list[str]:
    """Получает список тикеров акций из БД (без фьючерсов и индексов)"""
    engine = get_engine()
    query = text("""
        SELECT DISTINCT secid
        FROM candles
        WHERE interval = 24
          AND begin_time > CURRENT_DATE - 30
          AND secid NOT SIMILAR TO '%[0-9]%'
          AND secid NOT IN ('IMOEX', 'IMOEXF', 'RGBI', 'USDRUBF', 'CNYRUBF', 'EURRUBF', 'GLDRUBF', 'GAZPF', 'SBERF')
        ORDER BY secid
    """)
    with engine.connect() as conn:
        result = conn.execute(query)
        return [row[0] for row in result]


def calculate_ema(prices: list[float], period: int) -> list[float]:
    """Рассчёт EMA с использованием pandas"""
    if not prices or len(prices) < period:
        return []
    series = pd.Series(prices)
    ema = series.ewm(span=period, adjust=False).mean()
    return ema.tolist()


@router.get("/current")
async def get_current_breadth(
    ema_period: int = Query(200, ge=10, le=500, description="Период EMA"),
):
    """
    Возвращает текущее значение Market Breadth:
    - percent_above: % акций выше EMA
    - count_above: количество акций выше EMA
    - count_total: всего акций
    - stocks: детали по каждой акции
    """
    cache_key = f"breadth:current:{ema_period}"
    cached = get_or_set(cache_key)
    if cached is not None:
        return cached

    start_time = time.time()
    print(f"REQUEST: /breadth/current ema_period={ema_period}")

    engine = get_engine()
    stock_tickers = get_stock_tickers()

    stocks_data = []
    count_above = 0

    for ticker in stock_tickers:
        try:
            query = text("""
                SELECT begin_time::date as date, close
                FROM candles
                WHERE secid = :ticker
                  AND interval = 24
                  AND type = 'stock'
                ORDER BY begin_time DESC
                LIMIT :limit
            """)
            with engine.connect() as conn:
                result = conn.execute(query, {"ticker": ticker, "limit": ema_period + 50})
                rows = result.fetchall()

            if len(rows) < ema_period:
                continue

            rows = list(reversed(rows))
            prices = [float(r[1]) for r in rows if r[1]]

            if len(prices) < ema_period:
                continue

            ema_values = calculate_ema(prices, ema_period)
            current_price = prices[-1]
            current_ema = ema_values[-1]
            is_above = current_price > current_ema

            if is_above:
                count_above += 1

            stocks_data.append({
                "ticker": ticker,
                "price": round(current_price, 2),
                "ema": round(current_ema, 2),
                "is_above": is_above,
                "diff_percent": round((current_price - current_ema) / current_ema * 100, 2)
            })

        except Exception as e:
            print(f"Error processing {ticker}: {e}")
            continue

    count_total = len(stocks_data)
    percent_above = round((count_above / count_total) * 100, 1) if count_total > 0 else 0

    if percent_above >= 70:
        classification = "overbought"
    elif percent_above >= 50:
        classification = "bullish"
    elif percent_above >= 30:
        classification = "neutral"
    else:
        classification = "oversold"

    duration = time.time() - start_time
    print(f"DONE: /breadth/current {count_total} stocks, {duration:.2f}s")

    result = {
        "percent_above": percent_above,
        "count_above": count_above,
        "count_total": count_total,
        "ema_period": ema_period,
        "classification": classification,
        "stocks": sorted(stocks_data, key=lambda x: x["diff_percent"], reverse=True)
    }
    get_or_set(cache_key, result, ttl=300)  # 5 минут
    return result


@router.get("/history")
async def get_breadth_history(
    ema_period: int = Query(200, ge=10, le=500, description="Период EMA"),
    days: int = Query(365, ge=30, le=9000, description="Количество дней истории"),
):
    """
    Возвращает историю Market Breadth из pre-computed таблицы breadth_history.
    Для каждой даты: % акций выше EMA + данные IMOEX для наложения.
    """
    cache_key = f"breadth:history:{ema_period}:{days}"
    cached = get_or_set(cache_key)
    if cached is not None:
        return cached

    start_time = time.time()
    print(f"REQUEST: /breadth/history ema={ema_period}, days={days}")

    engine = get_engine()
    date_from = date.today() - timedelta(days=days)

    # ── 1. Читаем из pre-computed таблицы ──────────────────────────────────
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT trade_date, percent_above, count_above, count_total
            FROM breadth_history
            WHERE ema_period = :ema_period
              AND trade_date >= :date_from
            ORDER BY trade_date
        """), {"ema_period": ema_period, "date_from": date_from}).fetchall()

    history = [
        {
            "date": str(row[0]),
            "percent_above": float(row[1]),
            "count_above": int(row[2]),
            "count_total": int(row[3]),
        }
        for row in rows
    ]

    # ── 2. IMOEX для наложения (из index_data — данные с 1997 года) ────────
    imoex_data = []
    try:
        with engine.connect() as conn:
            imoex_rows = conn.execute(text("""
                SELECT trade_date as date, close
                FROM index_data
                WHERE secid = 'IMOEX'
                  AND trade_date >= :date_from
                  AND close IS NOT NULL
                ORDER BY trade_date
            """), {"date_from": date_from}).fetchall()

        imoex_by_date = {str(row[0]): float(row[1]) for row in imoex_rows if row[1]}
        for point in history:
            if point["date"] in imoex_by_date:
                point["imoex"] = imoex_by_date[point["date"]]

        imoex_data = [
            {"date": str(row[0]), "close": float(row[1])}
            for row in imoex_rows if row[1]
        ]
    except Exception as e:
        print(f"Error fetching IMOEX: {e}")

    duration = time.time() - start_time
    print(f"DONE: /breadth/history {len(history)} points, {duration:.2f}s")

    result = {
        "ema_period": ema_period,
        "data": history,
        "imoex": imoex_data,
        "precomputed": True,
    }
    get_or_set(cache_key, result, ttl=3600)  # 1 час
    return result
