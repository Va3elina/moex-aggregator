#!/usr/bin/env python3
"""
Предвычисление Market Breadth (% акций выше EMA).

Загружает дневные свечи акций, считает EMA(50/100/200) для каждого тикера
и сохраняет результаты в таблицу breadth_history для мгновенного API-доступа.

Поддерживает две вселенные:
- all: все акции из БД
- imoex: только акции из индекса IMOEX

Таблица: breadth_history(trade_date, ema_period, universe, percent_above, count_above, count_total)

Запуск:
    python compute_breadth_history.py           # инкрементальное обновление
    python compute_breadth_history.py --full    # полный пересчёт (первый запуск)
    python compute_breadth_history.py --days 90 # пересчитать последние 90 дней
    python compute_breadth_history.py --once --force  # для оркестратора
"""

import sys
import logging
import argparse
import numpy as np
import httpx
from datetime import date, timedelta
from pathlib import Path

from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

sys.path.insert(0, str(Path(__file__).parent.parent))
load_dotenv(Path(__file__).parent.parent / ".env")

DB_URL = os.getenv("DB_URL", "postgresql+pg8000://postgres:1803@localhost:5432/moex_db")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger(__name__)

# Периоды EMA которые предвычисляем
EMA_PERIODS = [50, 100, 200]

# Тикеры для исключения (индексы, фьючерсы на валюты)
EXCLUDED = {
    'IMOEX', 'IMOEXF', 'RGBI', 'USDRUBF', 'CNYRUBF', 'EURRUBF',
    'GLDRUBF', 'GAZPF', 'SBERF',
}

IMOEX_ISS_URL = "https://iss.moex.com/iss/statistics/engines/stock/markets/index/analytics/IMOEX.json?limit=100"


# ──────────────────────────────────────────────────────────────────────────────
#  БД: создание таблицы и индексов
# ──────────────────────────────────────────────────────────────────────────────

def create_schema(engine) -> None:
    """Создаёт таблицу breadth_history и индексы если не существуют."""
    with engine.connect() as conn:
        # Добавляем колонку universe если таблица уже есть
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS breadth_history (
                trade_date    DATE     NOT NULL,
                ema_period    SMALLINT NOT NULL,
                universe      VARCHAR(10) NOT NULL DEFAULT 'all',
                percent_above REAL     NOT NULL,
                count_above   SMALLINT NOT NULL,
                count_total   SMALLINT NOT NULL,
                PRIMARY KEY (trade_date, ema_period, universe)
            )
        """))

        # Миграция: если старая таблица без universe — добавить колонку и пересоздать PK
        try:
            conn.execute(text("""
                ALTER TABLE breadth_history ADD COLUMN IF NOT EXISTS universe VARCHAR(10) NOT NULL DEFAULT 'all'
            """))
        except Exception:
            pass

        # Попытка пересоздать PK (если он без universe)
        try:
            conn.execute(text("""
                ALTER TABLE breadth_history DROP CONSTRAINT IF EXISTS breadth_history_pkey
            """))
            conn.execute(text("""
                ALTER TABLE breadth_history ADD PRIMARY KEY (trade_date, ema_period, universe)
            """))
        except Exception:
            pass

        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_breadth_history_lookup
            ON breadth_history(ema_period, universe, trade_date DESC)
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_candles_breadth
            ON candles(interval, secid, begin_time, close)
            WHERE interval = 24
        """))
        conn.commit()
    log.info("Схема breadth_history готова (с universe)")


def get_last_computed_date(engine, universe: str = "all") -> date | None:
    """Возвращает максимальную дату в breadth_history для данной вселенной."""
    with engine.connect() as conn:
        row = conn.execute(text(
            "SELECT MAX(trade_date) FROM breadth_history WHERE ema_period = 200 AND universe = :universe"
        ), {"universe": universe}).fetchone()
        return row[0] if row and row[0] else None


# ──────────────────────────────────────────────────────────────────────────────
#  Данные: тикеры и свечи
# ──────────────────────────────────────────────────────────────────────────────

def get_stock_tickers(engine) -> list[str]:
    """Список акций из БД (активные за последние 30 дней, без фьючерсов)."""
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT DISTINCT secid
            FROM candles
            WHERE interval = 24
              AND begin_time > CURRENT_DATE - 30
              AND secid NOT SIMILAR TO '%[0-9]%'
            ORDER BY secid
        """)).fetchall()
    tickers = [r[0] for r in rows if r[0] not in EXCLUDED]
    log.info(f"Тикеры (all): {len(tickers)} акций")
    return tickers


def get_imoex_tickers() -> list[str]:
    """Получает список тикеров из индекса IMOEX через ISS API."""
    try:
        resp = httpx.get(IMOEX_ISS_URL, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        cols = data["analytics"]["columns"]
        rows_data = data["analytics"]["data"]
        idx_ticker = cols.index("ticker")
        tickers = [row[idx_ticker] for row in rows_data]
        log.info(f"Тикеры (imoex): {len(tickers)} акций")
        return tickers
    except Exception as e:
        log.error(f"Ошибка загрузки тикеров IMOEX: {e}")
        return []


def load_usd_rates(engine, date_from: date) -> dict[date, float]:
    """
    Загружает составной курс USD/RUB:
    - До 2024-06-11: USD000UTSTOM из index_data (спот)
    - С 2024-06-11:  USDRUBF из candles (вечный фьючерс)
    Возвращает {date: rate}
    """
    SWITCH_DATE = date(2024, 6, 11)
    rates = {}

    with engine.connect() as conn:
        # Спот до switch date
        rows_spot = conn.execute(text("""
            SELECT trade_date, close FROM index_data
            WHERE secid = 'USD000UTSTOM' AND trade_date >= :date_from AND trade_date < :switch
              AND close IS NOT NULL AND close > 0
            ORDER BY trade_date
        """), {"date_from": date_from, "switch": SWITCH_DATE}).fetchall()
        for d, close in rows_spot:
            rates[d] = float(close)

        # Фьючерс с switch date
        rows_fut = conn.execute(text("""
            SELECT begin_time::date, close FROM candles
            WHERE secid = 'USDRUBF' AND interval = 24
              AND begin_time::date >= :switch
              AND close IS NOT NULL AND close > 0
            ORDER BY begin_time
        """), {"switch": SWITCH_DATE}).fetchall()
        for d, close in rows_fut:
            rates[d] = float(close)

    log.info(f"Курс USD/RUB: {len(rates)} дней (спот: {len(rows_spot)}, фьючерс: {len(rows_fut)})")
    return rates


def convert_to_usd(
    ticker_data: dict[str, tuple[list, list]],
    usd_rates: dict[date, float],
) -> dict[str, tuple[list, list]]:
    """
    Конвертирует рублёвые цены в USD.
    Пропускает дни без курса. Использует forward-fill для пропущенных дней.
    """
    # Forward-fill курса (выходные/праздники)
    sorted_dates = sorted(usd_rates.keys())
    filled_rates = {}
    last_rate = None
    if sorted_dates:
        # Заполняем все даты от первой до последней
        d = sorted_dates[0]
        end = sorted_dates[-1]
        while d <= end:
            if d in usd_rates:
                last_rate = usd_rates[d]
            if last_rate:
                filled_rates[d] = last_rate
            d += timedelta(days=1)

    result = {}
    for ticker, (dates, prices) in ticker_data.items():
        usd_dates = []
        usd_prices = []
        for d, p in zip(dates, prices):
            rate = filled_rates.get(d)
            if rate and rate > 0:
                usd_dates.append(d)
                usd_prices.append(p / rate)
        if usd_dates:
            result[ticker] = (usd_dates, usd_prices)
    return result


def load_candles(engine, tickers: list[str], date_from: date) -> dict[str, tuple[list, list]]:
    """
    Загружает дневные свечи одним запросом.
    Возвращает {secid: ([dates...], [prices...])} отсортированный по дате.
    """
    log.info(f"Загрузка свечей с {date_from}…")
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT secid, begin_time::date AS d, close
            FROM candles
            WHERE secid = ANY(:tickers)
              AND interval = 24
              AND begin_time::date >= :date_from
              AND close IS NOT NULL
              AND close > 0
            ORDER BY secid, begin_time
        """), {"tickers": tickers, "date_from": date_from}).fetchall()

    log.info(f"Загружено {len(rows):,} строк")

    ticker_data: dict[str, tuple[list, list]] = {}
    for secid, d, close in rows:
        if secid not in ticker_data:
            ticker_data[secid] = ([], [])
        ticker_data[secid][0].append(d)
        ticker_data[secid][1].append(float(close))

    return ticker_data


# ──────────────────────────────────────────────────────────────────────────────
#  Вычисления
# ──────────────────────────────────────────────────────────────────────────────

def ema_numpy(prices: np.ndarray, span: int) -> np.ndarray:
    """EMA через numpy (без overhead pandas, ~3× быстрее для коротких серий)."""
    alpha = 2.0 / (span + 1)
    out = np.empty(len(prices), dtype=np.float64)
    out[0] = prices[0]
    for i in range(1, len(prices)):
        out[i] = alpha * prices[i] + (1.0 - alpha) * out[i - 1]
    return out


def compute_all(
    ticker_data: dict[str, tuple[list, list]],
    periods: list[int],
) -> dict[int, dict[date, list[int]]]:
    """
    Для каждого периода и каждой даты считает [above, total].

    Returns:
        {period: {date: [above_count, total_count]}}
    """
    from collections import defaultdict

    max_period = max(periods)
    # {period: {date: [above, total]}}
    counts: dict[int, dict] = {p: defaultdict(lambda: [0, 0]) for p in periods}

    processed = skipped = 0
    for ticker, (dates, prices_list) in ticker_data.items():
        if len(prices_list) < max_period:
            skipped += 1
            continue

        prices = np.array(prices_list, dtype=np.float64)

        for period in periods:
            if len(prices) < period:
                continue

            ema = ema_numpy(prices, period)

            # Считаем от (period-1) — EMA «прогрелась»
            for i in range(period - 1, len(dates)):
                d = dates[i]
                counts[period][d][1] += 1  # total
                if prices[i] > ema[i]:
                    counts[period][d][0] += 1  # above

        processed += 1

    log.info(f"Обработано: {processed} тикеров, пропущено (мало данных): {skipped}")
    return counts


def build_records(counts: dict[int, dict], universe: str = "all") -> list[dict]:
    """Преобразует counts в список словарей для upsert."""
    records = []
    for period, date_counts in counts.items():
        for d, (above, total) in date_counts.items():
            if total == 0:
                continue
            records.append({
                "trade_date": d,
                "ema_period": period,
                "universe": universe,
                "percent_above": round(above / total * 100, 2),
                "count_above": above,
                "count_total": total,
            })
    log.info(f"Записей для upsert ({universe}): {len(records):,}")
    return records


# ──────────────────────────────────────────────────────────────────────────────
#  Сохранение
# ──────────────────────────────────────────────────────────────────────────────

def upsert(engine, records: list[dict]) -> None:
    """Bulk upsert в breadth_history (INSERT … ON CONFLICT UPDATE)."""
    if not records:
        log.warning("Нет записей для сохранения")
        return

    chunk_size = 1000
    total = 0
    with engine.connect() as conn:
        for i in range(0, len(records), chunk_size):
            chunk = records[i: i + chunk_size]
            conn.execute(text("""
                INSERT INTO breadth_history
                    (trade_date, ema_period, universe, percent_above, count_above, count_total)
                VALUES (:trade_date, :ema_period, :universe, :percent_above, :count_above, :count_total)
                ON CONFLICT (trade_date, ema_period, universe) DO UPDATE SET
                    percent_above = EXCLUDED.percent_above,
                    count_above   = EXCLUDED.count_above,
                    count_total   = EXCLUDED.count_total
            """), chunk)
            total += len(chunk)
        conn.commit()

    log.info(f"Сохранено {total:,} записей в breadth_history")


# ──────────────────────────────────────────────────────────────────────────────
#  Точка входа
# ──────────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Предвычисление Market Breadth")
    parser.add_argument("--full", action="store_true",
                        help="Полный пересчёт (с 2007)")
    parser.add_argument("--days", type=int, default=None,
                        help="Пересчитать последние N дней")
    parser.add_argument("--once", action="store_true",
                        help="Однократный запуск (для оркестратора)")
    parser.add_argument("--force", action="store_true",
                        help="Не проверять торговый день")
    args = parser.parse_args()

    import time
    t0 = time.time()

    engine = create_engine(DB_URL, connect_args={"ssl_context": False})
    create_schema(engine)

    # ── 1. Все акции (universe=all) ──
    all_tickers = get_stock_tickers(engine)
    if not all_tickers:
        log.error("Нет тикеров в БД — выход")
        return

    # ── 2. IMOEX тикеры ──
    imoex_tickers = get_imoex_tickers()

    max_period = max(EMA_PERIODS)
    warmup = max_period + 50  # дней для «прогрева» EMA

    if args.full:
        date_from = date(2007, 1, 1) - timedelta(days=warmup)
        log.info(f"Режим: полный пересчёт (с {date_from})")
    elif args.days:
        date_from = date.today() - timedelta(days=args.days + warmup)
        log.info(f"Режим: последние {args.days} дней (загрузка с {date_from})")
    else:
        # Инкрементальный: обновляем последние 30 дней
        last = get_last_computed_date(engine, "all")
        if last:
            date_from = last - timedelta(days=30 + warmup)
            log.info(f"Режим: инкрементальный, последняя дата={last}, загрузка с {date_from}")
        else:
            date_from = date(2007, 1, 1) - timedelta(days=warmup)
            log.info(f"Режим: первый запуск — полный расчёт с {date_from}")

    # Загружаем все свечи одним запросом (union тикеров)
    all_unique_tickers = list(set(all_tickers) | set(imoex_tickers))
    all_candle_data = load_candles(engine, all_unique_tickers, date_from)

    # ── Вычисление для all ──
    log.info("=== Вычисление breadth для ALL ===")
    all_ticker_data = {t: all_candle_data[t] for t in all_tickers if t in all_candle_data}
    counts_all = compute_all(all_ticker_data, EMA_PERIODS)
    records_all = build_records(counts_all, "all")
    upsert(engine, records_all)

    # ── Вычисление для IMOEX ──
    if imoex_tickers:
        log.info("=== Вычисление breadth для IMOEX ===")
        imoex_ticker_data = {t: all_candle_data[t] for t in imoex_tickers if t in all_candle_data}
        counts_imoex = compute_all(imoex_ticker_data, EMA_PERIODS)
        records_imoex = build_records(counts_imoex, "imoex")
        upsert(engine, records_imoex)
    else:
        log.warning("Не удалось получить тикеры IMOEX — пропускаем")

    # ── Вычисление для USD (все акции пересчитанные в доллары) ──
    usd_rates = load_usd_rates(engine, date_from)
    if usd_rates:
        log.info("=== Вычисление breadth для ALL_USD ===")
        all_usd_data = convert_to_usd(all_ticker_data, usd_rates)
        counts_all_usd = compute_all(all_usd_data, EMA_PERIODS)
        records_all_usd = build_records(counts_all_usd, "all_usd")
        upsert(engine, records_all_usd)

        if imoex_tickers:
            log.info("=== Вычисление breadth для IMOEX_USD ===")
            imoex_usd_data = convert_to_usd(
                {t: all_candle_data[t] for t in imoex_tickers if t in all_candle_data},
                usd_rates
            )
            counts_imoex_usd = compute_all(imoex_usd_data, EMA_PERIODS)
            records_imoex_usd = build_records(counts_imoex_usd, "imoex_usd")
            upsert(engine, records_imoex_usd)
    else:
        log.warning("Нет курса USD/RUB — пропускаем USD universes")

    engine.dispose()
    log.info(f"Готово за {time.time() - t0:.1f}с")


if __name__ == "__main__":
    main()
