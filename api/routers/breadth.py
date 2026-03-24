"""
Market Breadth API — Сила рынка
Рассчитывает % акций торгующихся выше EMA

/history — читает из pre-computed таблицы breadth_history (мгновенно)
/current — считает на лету для текущей даты (быстро, 42 тикера)
"""
from fastapi import APIRouter, Query, HTTPException, Depends
from sqlalchemy import text
from datetime import date, timedelta
import pandas as pd
import time
import httpx

from api.database import get_engine
from api.cache import get_or_set
from api.logger import get_logger
from api.routers.auth import get_current_user_optional
from api.security.access_control import enforce_guest_limits

log = get_logger()

router = APIRouter(prefix="/api/breadth", tags=["breadth"])

IMOEX_ISS_URL = "https://iss.moex.com/iss/statistics/engines/stock/markets/index/analytics/IMOEX.json?limit=100"


async def get_imoex_tickers() -> set[str]:
    """Получает список тикеров, входящих в индекс IMOEX. Кеш 1 час."""
    cache_key = "imoex_tickers_set"
    cached = get_or_set(cache_key)
    if cached is not None:
        return set(cached)

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(IMOEX_ISS_URL)
            resp.raise_for_status()
            data = resp.json()
        cols = data["analytics"]["columns"]
        rows_data = data["analytics"]["data"]
        idx_ticker = cols.index("ticker")
        tickers = [row[idx_ticker] for row in rows_data]
        get_or_set(cache_key, tickers, ttl=3600)
        return set(tickers)
    except Exception as e:
        log.warning(f"Failed to fetch IMOEX tickers: {e}")
        return set()


def get_stock_tickers() -> list[dict]:
    """Получает список тикеров акций из БД с секторами (без фьючерсов и индексов)"""
    engine = get_engine()
    query = text("""
        SELECT DISTINCT c.secid, COALESCE(i.sector, 'Другое') as sector
        FROM candles c
        LEFT JOIN instruments i ON i.sec_id = c.secid AND i.type = 'stock'
        WHERE c.interval = 24
          AND c.begin_time > CURRENT_DATE - 30
          AND c.secid NOT SIMILAR TO '%[0-9]%'
          AND c.secid NOT IN ('IMOEX', 'IMOEXF', 'RGBI', 'USDRUBF', 'CNYRUBF', 'EURRUBF', 'GLDRUBF', 'GAZPF', 'SBERF')
        ORDER BY c.secid
    """)
    with engine.connect() as conn:
        result = conn.execute(query)
        return [{"ticker": row[0], "sector": row[1]} for row in result]


def calculate_ema(prices: list[float], period: int) -> list[float]:
    """Рассчёт EMA с использованием pandas"""
    if not prices or len(prices) < period:
        return []
    series = pd.Series(prices)
    ema = series.ewm(span=period, adjust=False).mean()
    return ema.tolist()


def _compute_breadth_for_tickers(engine, tickers: list[str], ema_period: int, date_from: date) -> list[dict]:
    """
    Вычисляет историю breadth на лету для заданного списка тикеров.
    Возвращает [{date, percent_above, count_above, count_total}, ...]
    """
    from collections import defaultdict

    # Нужно загрузить данные с запасом для расчёта EMA
    buffer_days = int(ema_period * 1.8)
    fetch_from = date_from - timedelta(days=buffer_days)

    # Для каждого тикера: date → is_above_ema
    daily_above: defaultdict[str, dict[str, bool]] = defaultdict(dict)

    for ticker in tickers:
        try:
            with engine.connect() as conn:
                rows = conn.execute(text("""
                    SELECT begin_time::date as d, close
                    FROM candles
                    WHERE secid = :ticker AND interval = 24 AND type = 'stock'
                      AND begin_time::date >= :fetch_from
                    ORDER BY begin_time
                """), {"ticker": ticker, "fetch_from": fetch_from.isoformat()}).fetchall()

            if len(rows) < ema_period:
                continue

            prices = [float(r[1]) for r in rows if r[1]]
            dates = [str(r[0]) for r in rows if r[1]]

            if len(prices) < ema_period:
                continue

            ema_values = calculate_ema(prices, ema_period)

            for i in range(ema_period - 1, len(prices)):
                d = dates[i]
                daily_above[d][ticker] = prices[i] > ema_values[i]
        except Exception:
            continue

    # Агрегация по датам
    date_from_str = date_from.isoformat()
    result = []
    for d in sorted(daily_above.keys()):
        if d < date_from_str:
            continue
        stocks = daily_above[d]
        total = len(stocks)
        above = sum(1 for v in stocks.values() if v)
        pct = round((above / total) * 100, 1) if total > 0 else 0
        result.append({
            "date": d,
            "percent_above": pct,
            "count_above": above,
            "count_total": total,
        })

    return result


@router.get("/current")
async def get_current_breadth(
    ema_period: int = Query(200, ge=10, le=500, description="Период EMA"),
    universe: str = Query("all", description="Вселенная: all — все акции, imoex — только индекс MOEX"),
):
    """
    Возвращает текущее значение Market Breadth:
    - percent_above: % акций выше EMA
    - count_above: количество акций выше EMA
    - count_total: всего акций
    - stocks: детали по каждой акции
    """
    if universe not in ("all", "imoex"):
        universe = "all"

    cache_key = f"breadth:current:{ema_period}:{universe}"
    cached = get_or_set(cache_key)
    if cached is not None:
        return cached

    start_time = time.time()
    log.info(f"REQUEST: /breadth/current ema_period={ema_period}")

    engine = get_engine()
    stock_entries = get_stock_tickers()

    # Фильтрация по вселенной
    if universe == "imoex":
        imoex_tickers = await get_imoex_tickers()
        stock_entries = [e for e in stock_entries if e["ticker"] in imoex_tickers]

    stocks_data = []
    count_above = 0

    for entry in stock_entries:
        ticker = entry["ticker"]
        sector = entry["sector"]
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
                "sector": sector,
                "price": round(current_price, 2),
                "ema": round(current_ema, 2),
                "is_above": is_above,
                "diff_percent": round((current_price - current_ema) / current_ema * 100, 2)
            })

        except Exception as e:
            log.debug(f"Error processing {ticker}: {e}")
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
    log.info(f"DONE: /breadth/current {count_total} stocks, {duration:.2f}s")

    result = {
        "percent_above": percent_above,
        "count_above": count_above,
        "count_total": count_total,
        "ema_period": ema_period,
        "universe": universe,
        "classification": classification,
        "stocks": sorted(stocks_data, key=lambda x: x["diff_percent"], reverse=True)
    }
    get_or_set(cache_key, result, ttl=300)  # 5 минут
    return result


@router.get("/history")
async def get_breadth_history(
    ema_period: int = Query(200, ge=10, le=500, description="Период EMA"),
    days: int = Query(365, ge=30, le=9000, description="Количество дней истории"),
    universe: str = Query("all", description="Вселенная: all или imoex"),
    user = Depends(get_current_user_optional),
):
    """
    Возвращает историю Market Breadth.
    universe=all — из pre-computed таблицы breadth_history.
    universe=imoex — вычисляется на лету для тикеров индекса.
    """
    if universe not in ("all", "imoex"):
        universe = "all"

    # Ограничения для гостей
    enforce_guest_limits(user, days=days)
    cache_key = f"breadth:history:{ema_period}:{days}:{universe}"
    cached = get_or_set(cache_key)
    if cached is not None:
        return cached

    start_time = time.time()
    log.info(f"REQUEST: /breadth/history ema={ema_period}, days={days}, universe={universe}")

    engine = get_engine()
    date_from = date.today() - timedelta(days=days)

    # Оба варианта читаются из pre-computed таблицы breadth_history
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT trade_date, percent_above, count_above, count_total
            FROM breadth_history
            WHERE ema_period = :ema_period
              AND universe = :universe
              AND trade_date >= :date_from
            ORDER BY trade_date
        """), {"ema_period": ema_period, "universe": universe, "date_from": date_from}).fetchall()

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
        log.error(f"Error fetching IMOEX: {e}")

    duration = time.time() - start_time
    log.info(f"DONE: /breadth/history {len(history)} points, {duration:.2f}s")

    result = {
        "ema_period": ema_period,
        "universe": universe,
        "data": history,
        "imoex": imoex_data,
    }
    # IMOEX на лету — кешируем 30 мин (тяжёлый запрос), all — 1 час
    ttl = 1800 if universe == "imoex" else 3600
    get_or_set(cache_key, result, ttl=ttl)
    return result
