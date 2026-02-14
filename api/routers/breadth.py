"""
Market Breadth API — Сила рынка
Рассчитывает % акций торгующихся выше EMA
"""
from fastapi import APIRouter, Query, HTTPException
from sqlalchemy import text
from datetime import date, timedelta
import pandas as pd
import numpy as np
import time

from api.database import get_engine

router = APIRouter(prefix="/api/breadth", tags=["breadth"])


def get_stock_tickers() -> list[str]:
    """Получает список тикеров акций из БД (без фьючерсов и индексов)"""
    engine = get_engine()
    query = text("""
        SELECT DISTINCT secid 
        FROM candles 
        WHERE interval = 24 
          AND begin_time > CURRENT_DATE - 30
          AND secid !~ '[0-9]'  -- исключаем фьючерсы (содержат цифры)
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
    start_time = time.time()
    print(f"REQUEST: /breadth/current ema_period={ema_period}")
    
    engine = get_engine()
    
    # Динамически получаем список акций
    stock_tickers = get_stock_tickers()
    
    stocks_data = []
    count_above = 0
    
    for ticker in stock_tickers:
        try:
            # Получаем последние N+50 дней для расчёта EMA
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
            
            # Разворачиваем (от старых к новым)
            rows = list(reversed(rows))
            prices = [float(r[1]) for r in rows if r[1]]
            
            if len(prices) < ema_period:
                continue
            
            # EMA
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
    
    # Определение состояния рынка
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
    
    return {
        "percent_above": percent_above,
        "count_above": count_above,
        "count_total": count_total,
        "ema_period": ema_period,
        "classification": classification,
        "stocks": sorted(stocks_data, key=lambda x: x["diff_percent"], reverse=True)
    }


@router.get("/history")
async def get_breadth_history(
    ema_period: int = Query(200, ge=10, le=500, description="Период EMA"),
    days: int = Query(365, ge=30, le=1000, description="Количество дней истории"),
):
    """
    Возвращает историю Market Breadth для графика.
    Для каждой даты рассчитывается % акций выше EMA.
    """
    start_time = time.time()
    print(f"REQUEST: /breadth/history ema={ema_period}, days={days}")
    
    engine = get_engine()
    
    # Динамически получаем список акций
    stock_tickers = get_stock_tickers()
    
    # Получаем все данные разом для оптимизации
    query = text("""
        SELECT secid, begin_time::date as date, close
        FROM candles
        WHERE secid = ANY(:tickers)
          AND interval = 24
          AND begin_time >= CURRENT_DATE - :total_days * INTERVAL '1 day'
        ORDER BY secid, begin_time
    """)
    
    total_days = days + ema_period + 50
    
    with engine.connect() as conn:
        result = conn.execute(query, {"tickers": stock_tickers, "total_days": total_days})
        rows = result.fetchall()
    
    # Группируем по тикеру
    ticker_data: dict[str, list[tuple]] = {}
    for row in rows:
        ticker = row[0]
        if ticker not in ticker_data:
            ticker_data[ticker] = []
        ticker_data[ticker].append((row[1], float(row[2]) if row[2] else None))
    
    # Рассчитываем EMA для каждого тикера
    ticker_above: dict[str, dict[date, bool]] = {}
    
    for ticker, data in ticker_data.items():
        # Фильтруем None
        data = [(d, p) for d, p in data if p is not None]
        if len(data) < ema_period:
            continue
        
        dates = [d for d, _ in data]
        prices = [p for _, p in data]
        ema_values = calculate_ema(prices, ema_period)
        
        ticker_above[ticker] = {}
        for i in range(ema_period - 1, len(prices)):
            ticker_above[ticker][dates[i]] = prices[i] > ema_values[i]
    
    # Собираем все уникальные даты
    all_dates = set()
    for ticker_dict in ticker_above.values():
        all_dates.update(ticker_dict.keys())
    
    sorted_dates = sorted(all_dates)
    
    # Для каждой даты считаем %
    history = []
    for d in sorted_dates[-days:]:  # Только последние N дней
        above = 0
        total = 0
        for ticker, date_dict in ticker_above.items():
            if d in date_dict:
                total += 1
                if date_dict[d]:
                    above += 1
        
        if total > 0:
            percent = round((above / total) * 100, 1)
            history.append({
                "date": str(d),
                "percent_above": percent,
                "count_above": above,
                "count_total": total
            })
    
    # Получаем данные IMOEX для наложения
    imoex_query = text("""
        SELECT begin_time::date as date, close
        FROM candles
        WHERE secid = 'IMOEXF'
          AND interval = 24
          AND begin_time >= CURRENT_DATE - :total_days * INTERVAL '1 day'
        ORDER BY begin_time
    """)
    
    imoex_data = []
    try:
        with engine.connect() as conn:
            result = conn.execute(imoex_query, {"total_days": total_days})
            imoex_rows = result.fetchall()
        
        # Создаём словарь для быстрого поиска
        imoex_by_date = {str(row[0]): float(row[1]) for row in imoex_rows if row[1]}
        
        # Добавляем IMOEX к каждой дате
        for point in history:
            if point["date"] in imoex_by_date:
                point["imoex"] = imoex_by_date[point["date"]]
        
        # Отдельный массив для совместимости
        imoex_data = [
            {"date": str(row[0]), "close": float(row[1])} 
            for row in imoex_rows if row[1]
        ]
    except Exception as e:
        print(f"Error fetching IMOEX: {e}")
    
    duration = time.time() - start_time
    print(f"DONE: /breadth/history {len(history)} points, {duration:.2f}s")
    
    return {
        "ema_period": ema_period,
        "data": history,
        "imoex": imoex_data
    }
