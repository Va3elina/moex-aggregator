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


# ════════════════════════════════════════════════════════════════════════════
# Split-adjustment registry
#
# 5-минутные свечи MOEX retroactively не пересчитываются при сплитах. Если
# акция получила split 1:N, то pre-split close (например 3200₽) и post-split
# close (320₽) лежат в БД как есть. Любая EMA/breadth-аналитика на смеси даст
# wrong result: EMA200 будет где-то посередине, current price «ниже EMA» →
# breadth классифицирует акцию как oversold хоть на самом деле она просто
# получила сплит.
#
# Тот же registry применён в db/mv_heatmap_stocks.sql (Heatmap уже починен).
# Добавлять новые сплиты при появлении (детектировать по аномальному
# day-over-day change в daily candles).
# ════════════════════════════════════════════════════════════════════════════
from datetime import date as _date_cls

KNOWN_SPLITS: dict[str, tuple[_date_cls, float]] = {
    # secid : (split_date, ratio).  Сколько новых акций на 1 старую.
    # Применяется ТОЛЬКО к close < split_date (retroactive adjustment):
    #   adjusted_close = raw_close / ratio
    'T':    (_date_cls(2026, 4, 2), 10.0),     # Т-Технологии (бывш. Tinkoff) 1:10
    'SFIN': (_date_cls(2025, 12, 25), 1.93),   # СФИ — split 1.93
}


def _adjust_for_split(ticker: str, dated_prices: list[tuple]) -> list[tuple]:
    """
    Если ticker в KNOWN_SPLITS — делит pre-split цены на ratio. Иначе no-op.
    dated_prices: [(date, price), ...] в хронологическом порядке.
    """
    info = KNOWN_SPLITS.get(ticker)
    if not info:
        return dated_prices
    split_date, ratio = info
    return [
        (d, p / ratio) if (d if isinstance(d, _date_cls) else _date_cls.fromisoformat(str(d))) < split_date else (d, p)
        for d, p in dated_prices
    ]

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


def _load_usd_rates(engine, date_from: date) -> dict[date, float]:
    """
    Загружает курс USD/RUB с forward-fill (для выходных/праздников).
    - До 2024-06-11: USD000UTSTOM из index_data (спот)
    - С 2024-06-11: USDRUBF из candles (вечный фьючерс)
    Возвращает {date: rate} с заполненными пропусками.
    """
    from datetime import date as _date

    cache_key = f"usd_rates:{date_from.isoformat()}"
    cached = get_or_set(cache_key)
    if cached is not None:
        return {_date.fromisoformat(k): v for k, v in cached.items()}

    SWITCH_DATE = _date(2024, 6, 11)
    raw_rates: dict[_date, float] = {}

    with engine.connect() as conn:
        rows_spot = conn.execute(text("""
            SELECT trade_date, close FROM index_data
            WHERE secid = 'USD000UTSTOM' AND trade_date >= :date_from AND trade_date < :switch
              AND close IS NOT NULL AND close > 0
            ORDER BY trade_date
        """), {"date_from": date_from, "switch": SWITCH_DATE}).fetchall()
        for d, close in rows_spot:
            raw_rates[d] = float(close)

        rows_fut = conn.execute(text("""
            SELECT begin_time::date, close FROM candles
            WHERE secid = 'USDRUBF' AND interval = 24
              AND begin_time::date >= :switch
              AND close IS NOT NULL AND close > 0
            ORDER BY begin_time
        """), {"switch": SWITCH_DATE}).fetchall()
        for d, close in rows_fut:
            raw_rates[d] = float(close)

    # Forward-fill для выходных/праздников
    filled: dict[_date, float] = {}
    if raw_rates:
        sorted_dates = sorted(raw_rates.keys())
        d = sorted_dates[0]
        end = sorted_dates[-1]
        last_rate = None
        while d <= end:
            if d in raw_rates:
                last_rate = raw_rates[d]
            if last_rate is not None:
                filled[d] = last_rate
            d += timedelta(days=1)

    # Кеш 1 час (курс меняется нечасто)
    get_or_set(cache_key, {k.isoformat(): v for k, v in filled.items()}, ttl=3600)
    return filled


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

            # Split-adjustment: pre-split цены делим на ratio (см. KNOWN_SPLITS).
            # Иначе EMA для T/SFIN считается на смеси старых и новых цен
            # → bogus результат (T выглядит как oversold после split 1:10).
            dated = [(r[0], float(r[1])) for r in rows if r[1]]
            dated = _adjust_for_split(ticker, dated)
            prices = [p for _, p in dated]
            dates = [str(d) for d, _ in dated]

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
    universe: str = Query("all", description="Вселенная: all, imoex, all_usd, imoex_usd"),
):
    """
    Возвращает текущее значение Market Breadth:
    - percent_above: % акций выше EMA
    - count_above: количество акций выше EMA
    - count_total: всего акций
    - stocks: детали по каждой акции

    universe=all/imoex → рублёвые цены и EMA.
    universe=all_usd/imoex_usd → цены конвертируются через USDRUB (спот до
    2024-06-11, фьючерс USDRUBF после), EMA считается на USD-ценах.
    """
    if universe not in ("all", "imoex", "all_usd", "imoex_usd"):
        universe = "all"
    is_usd = universe.endswith("_usd")
    universe_base = universe[:-4] if is_usd else universe

    cache_key = f"breadth:current:{ema_period}:{universe}"
    cached = get_or_set(cache_key)
    if cached is not None:
        return cached

    start_time = time.time()
    log.info(f"REQUEST: /breadth/current ema_period={ema_period} universe={universe}")

    engine = get_engine()
    stock_entries = get_stock_tickers()

    # Фильтрация по вселенной (all vs imoex)
    if universe_base == "imoex":
        imoex_tickers = await get_imoex_tickers()
        stock_entries = [e for e in stock_entries if e["ticker"] in imoex_tickers]

    # USD: загружаем курс на весь период прогрева EMA (тот же что у свечей).
    # Иначе первые дни свечей будут без курса и выпадут из EMA-расчёта.
    usd_rates: dict = {}
    if is_usd:
        rate_from = date.today() - timedelta(days=ema_period * 2 + 200)
        usd_rates = _load_usd_rates(engine, rate_from)

    stocks_data = []
    count_above = 0

    for entry in stock_entries:
        ticker = entry["ticker"]
        sector = entry["sector"]
        try:
            # Окно прогрева EMA. Теоретически при N точках первая точка весит
            # ~13%, но на практике акции не делают экстремальных скачков и при
            # 2×N+100 дней точность ~0.1% (проверено эмпирически на SBER).
            # Должно совпадать с warmup в compute_breadth_history.py.
            warmup_limit = ema_period * 2 + 100
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
                result = conn.execute(query, {"ticker": ticker, "limit": warmup_limit})
                rows = result.fetchall()

            if len(rows) < ema_period:
                continue

            rows = list(reversed(rows))
            raw_prices = [(r[0], float(r[1])) for r in rows if r[1]]

            # Split-adjustment: pre-split цены делим на ratio. Без этого
            # T (split 1:10 на 2026-04-02) выглядит как oversold —
            # EMA200 на смеси 3200₽/320₽ ~1500₽, current 320 < EMA → false.
            raw_prices = _adjust_for_split(ticker, raw_prices)

            # USD конверсия: пропускаем дни без курса
            if is_usd:
                prices_pairs = []
                for d, p in raw_prices:
                    rate = usd_rates.get(d)
                    if rate and rate > 0:
                        prices_pairs.append((d, p / rate))
            else:
                prices_pairs = raw_prices

            if len(prices_pairs) < ema_period:
                continue
            prices = [p for _, p in prices_pairs]

            ema_values = calculate_ema(prices, ema_period)
            current_price = prices[-1]
            current_ema = ema_values[-1]
            is_above = current_price > current_ema

            if is_above:
                count_above += 1

            stocks_data.append({
                "ticker": ticker,
                "sector": sector,
                "price": round(current_price, 4 if is_usd else 2),
                "ema": round(current_ema, 4 if is_usd else 2),
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
    universe: str = Query("all", description="Вселенная: all, imoex, all_usd, imoex_usd"),
    user = Depends(get_current_user_optional),
):
    """
    Возвращает историю Market Breadth.
    universe=all/imoex — рублёвый breadth.
    universe=all_usd/imoex_usd — долларовый breadth (цены акций / USD).
    """
    if universe not in ("all", "imoex", "all_usd", "imoex_usd"):
        universe = "all"
    is_usd = universe.endswith("_usd")

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

    # ── 2. Индекс для наложения: IMOEX (₽) или RTSI ($) ────────
    overlay_secid = "RTSI" if is_usd else "IMOEX"
    imoex_data = []
    try:
        with engine.connect() as conn:
            imoex_rows = conn.execute(text("""
                SELECT trade_date as date, close
                FROM index_data
                WHERE secid = :secid
                  AND trade_date >= :date_from
                  AND close IS NOT NULL
                ORDER BY trade_date
            """), {"secid": overlay_secid, "date_from": date_from}).fetchall()

        imoex_by_date = {str(row[0]): float(row[1]) for row in imoex_rows if row[1]}
        for point in history:
            if point["date"] in imoex_by_date:
                point["imoex"] = imoex_by_date[point["date"]]

        imoex_data = [
            {"date": str(row[0]), "close": float(row[1])}
            for row in imoex_rows if row[1]
        ]
    except Exception as e:
        log.error(f"Error fetching {overlay_secid}: {e}")

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
