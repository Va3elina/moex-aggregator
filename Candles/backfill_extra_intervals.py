#!/usr/bin/env python3
"""
Бэкфилл часовых (60) и недельных (7) свечей акций через Algopack API.

Использование:
  python Candles/backfill_extra_intervals.py                    # Все тикеры, часовые + недельные
  python Candles/backfill_extra_intervals.py --interval 7       # Только недельные
  python Candles/backfill_extra_intervals.py --interval 60      # Только часовые
  python Candles/backfill_extra_intervals.py --ticker SIBN      # Конкретный тикер
  python Candles/backfill_extra_intervals.py --dry-run          # Только показать план
"""

import asyncio
import aiohttp
import logging
import os
import argparse
from pathlib import Path
from datetime import datetime, date
from typing import List, Tuple, Optional

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

PROJECT_DIR = Path(__file__).parent.parent
load_dotenv(PROJECT_DIR / ".env")

DB_URL = os.getenv("DB_URL")
ALGOPACK_API_KEY = os.getenv("ALGOPACK_API_KEY", "")
BASE_URL = "https://apim.moex.com/iss/engines/stock/markets/shares/boards/tqbr/securities"

MAX_CONCURRENT = 3  # Меньше чем для дневок — часовые тяжелее

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger(__name__)


def get_engine():
    return create_engine(DB_URL, pool_pre_ping=True)


def get_stocks(engine) -> List[Tuple[str, str]]:
    """Все акции из instruments."""
    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT sec_id, name FROM instruments WHERE type = 'stock' ORDER BY sec_id"
        )).fetchall()
    return [(r[0], r[1]) for r in rows]


def get_last_candle_date(engine, secid: str, interval: int) -> Optional[date]:
    """Последняя свеча для тикера и интервала."""
    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT MAX(begin_time)::date FROM candles
            WHERE secid = :secid AND type = 'stock' AND interval = :interval
        """), {'secid': secid, 'interval': interval}).scalar()
    return row


def get_candle_count(engine, secid: str, interval: int) -> int:
    """Количество свечей для тикера и интервала."""
    with engine.connect() as conn:
        return conn.execute(text("""
            SELECT COUNT(*) FROM candles
            WHERE secid = :secid AND type = 'stock' AND interval = :interval
        """), {'secid': secid, 'interval': interval}).scalar() or 0


async def fetch_candles(session, ticker: str, interval: int,
                        from_date: str, till_date: str) -> list:
    """Загружает свечи за период с пагинацией."""
    all_rows = []
    start = 0

    while True:
        url = (
            f"{BASE_URL}/{ticker}/candles.json"
            f"?interval={interval}&from={from_date}&till={till_date}&start={start}"
        )
        headers = {'Authorization': f'Bearer {ALGOPACK_API_KEY}'}

        try:
            async with session.get(url, headers=headers,
                                   timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status != 200:
                    break
                data = await resp.json()
                rows = data.get('candles', {}).get('data', [])
                if not rows:
                    break
                all_rows.extend(rows)
                if len(rows) < 500:
                    break
                start += len(rows)
        except Exception as e:
            log.warning(f"  {ticker} {from_date}-{till_date} ошибка: {e}")
            break

    return all_rows


def save_candles(engine, ticker: str, rows: list, interval: int) -> int:
    """Сохраняет свечи в БД."""
    if not rows:
        return 0

    values = []
    for r in rows:
        values.append({
            'secid': ticker,
            'open': r[0],
            'close': r[1],
            'high': r[2],
            'low': r[3],
            'value': r[4],
            'volume': r[5],
            'begin_time': r[6],
            'end_time': r[7],
            'interval': interval,
            'type': 'stock',
        })

    inserted = 0
    with engine.connect() as conn:
        for v in values:
            result = conn.execute(text("""
                INSERT INTO candles (secid, open, close, high, low, value, volume,
                                    begin_time, end_time, interval, type)
                VALUES (:secid, :open, :close, :high, :low, :value, :volume,
                        :begin_time, :end_time, :interval, :type)
                ON CONFLICT (secid, begin_time, interval, type) DO NOTHING
            """), v)
            inserted += result.rowcount
        conn.commit()

    return inserted


async def backfill_ticker(session, engine, ticker: str, name: str, interval: int):
    """Загружает историю для одного тикера и интервала."""
    current_year = datetime.now().year

    # Для часовых начинаем с 2010, для недельных с 2007
    start_year = 2007 if interval == 7 else 2010
    total = 0

    for year in range(start_year, current_year + 1):
        rows = await fetch_candles(session, ticker, interval,
                                   f"{year}-01-01", f"{year}-12-31")
        if not rows:
            continue

        inserted = save_candles(engine, ticker, rows, interval)
        total += inserted
        if inserted > 0:
            log.info(f"  {ticker} {year}: +{inserted} свечей (interval={interval})")

    return total


async def main():
    parser = argparse.ArgumentParser(description='Backfill hourly/weekly candles')
    parser.add_argument('--ticker', type=str, help='Конкретный тикер')
    parser.add_argument('--interval', type=int, choices=[7, 60],
                        help='Интервал (7=недельные, 60=часовые). По умолчанию оба')
    parser.add_argument('--dry-run', action='store_true', help='Только показать план')
    args = parser.parse_args()

    intervals = [args.interval] if args.interval else [7, 60]
    engine = get_engine()
    stocks = get_stocks(engine)

    if args.ticker:
        stocks = [(s, n) for s, n in stocks if s == args.ticker]
        if not stocks:
            log.error(f"Тикер {args.ticker} не найден")
            return

    for interval in intervals:
        interval_name = 'недельные' if interval == 7 else 'часовые'
        log.info(f"{'=' * 60}")
        log.info(f"BACKFILL {interval_name.upper()} (interval={interval})")
        log.info(f"Тикеров: {len(stocks)}")
        log.info(f"{'=' * 60}")

        # Показать текущее состояние
        to_backfill = []
        for ticker, name in stocks:
            count = get_candle_count(engine, ticker, interval)
            last = get_last_candle_date(engine, ticker, interval)
            to_backfill.append((ticker, name, count, last))

        if args.dry_run:
            for ticker, name, count, last in to_backfill:
                last_str = str(last) if last else 'нет данных'
                log.info(f"  {ticker:8s} ({name:15s}) — свечей: {count:>6}, последняя: {last_str}")
            continue

        async with aiohttp.ClientSession() as session:
            semaphore = asyncio.Semaphore(MAX_CONCURRENT)
            grand_total = 0

            async def process(ticker, name):
                async with semaphore:
                    return await backfill_ticker(session, engine, ticker, name, interval)

            for i in range(0, len(to_backfill), MAX_CONCURRENT):
                batch = to_backfill[i:i + MAX_CONCURRENT]
                tasks = [process(t, n) for t, n, _, _ in batch]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                for j, r in enumerate(results):
                    ticker = batch[j][0]
                    if isinstance(r, Exception):
                        log.error(f"  {ticker}: ОШИБКА {r}")
                    elif isinstance(r, int):
                        grand_total += r
                        if r > 0:
                            log.info(f"  ✅ {ticker}: +{r}")

            log.info(f"{'=' * 60}")
            log.info(f"ИТОГО {interval_name}: +{grand_total} свечей")
            log.info(f"{'=' * 60}")


if __name__ == '__main__':
    asyncio.run(main())
