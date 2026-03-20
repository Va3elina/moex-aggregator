#!/usr/bin/env python3
"""
Бэкфилл исторических дневных свечей для новых акций через Algopack API.
Загружает данные от 2007 года до текущей даты.

Использование:
  python Candles/backfill_daily_history.py              # Все тикеры без данных
  python Candles/backfill_daily_history.py --ticker SIBN # Конкретный тикер
  python Candles/backfill_daily_history.py --dry-run     # Только показать что будет загружено
"""

import asyncio
import aiohttp
import json
import logging
import sys
import os
import argparse
from pathlib import Path
from datetime import datetime, timedelta, date
from typing import List, Tuple

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# === Пути ===
PROJECT_DIR = Path(__file__).parent.parent
load_dotenv(PROJECT_DIR / ".env")

DB_URL = os.getenv("DB_URL", "postgresql+pg8000://postgres:1803@localhost:5432/moex_db")
ALGOPACK_API_KEY = os.getenv("ALGOPACK_API_KEY", "")

BASE_URL = "https://apim.moex.com/iss/engines/stock/markets/shares/boards/tqbr/securities"
START_DATE = "2007-01-01"
MAX_CONCURRENT = 5  # Не слишком агрессивно

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger(__name__)


def get_engine():
    return create_engine(DB_URL, pool_pre_ping=True)


def get_stocks_without_daily(engine) -> List[Tuple[str, str]]:
    """Возвращает акции, у которых нет дневных свечей или мало данных."""
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT i.sec_id, i.name,
                   MIN(c.begin_time) as first_candle,
                   COUNT(c.*) as candle_count
            FROM instruments i
            LEFT JOIN candles c ON c.secid = i.sec_id AND c.type = 'stock' AND c.interval = 24
            WHERE i.type = 'stock'
            GROUP BY i.sec_id, i.name
            ORDER BY candle_count ASC, i.sec_id
        """)).fetchall()
    return [(r[0], r[1], r[2], r[3]) for r in rows]


async def fetch_candles_for_year(session, ticker: str, year: int) -> list:
    """Загружает дневные свечи за один год."""
    all_rows = []
    start = 0

    while True:
        url = (
            f"{BASE_URL}/{ticker}/candles.json"
            f"?interval=24&from={year}-01-01&till={year}-12-31&start={start}"
        )
        headers = {'Authorization': f'Bearer {ALGOPACK_API_KEY}'}

        try:
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=30)) as resp:
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
            log.warning(f"  {ticker}/{year} ошибка: {e}")
            break

    return all_rows


async def backfill_ticker(session, engine, ticker: str, name: str):
    """Загружает всю историю для одного тикера."""
    current_year = datetime.now().year
    total_candles = 0

    for year in range(2007, current_year + 1):
        rows = await fetch_candles_for_year(session, ticker, year)
        if not rows:
            continue

        # Колонки: open, close, high, low, value, volume, begin, end
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
                'interval': 24,
                'type': 'stock',
            })

        if values:
            with engine.connect() as conn:
                for v in values:
                    conn.execute(text("""
                        INSERT INTO candles (secid, open, close, high, low, value, volume,
                                            begin_time, end_time, interval, type)
                        VALUES (:secid, :open, :close, :high, :low, :value, :volume,
                                :begin_time, :end_time, :interval, :type)
                        ON CONFLICT DO NOTHING
                    """), v)
                conn.commit()

            total_candles += len(values)
            log.info(f"  {ticker} {year}: +{len(values)} свечей")

    return total_candles


async def main():
    parser = argparse.ArgumentParser(description='Backfill daily candles from Algopack')
    parser.add_argument('--ticker', type=str, help='Конкретный тикер')
    parser.add_argument('--dry-run', action='store_true', help='Только показать план')
    parser.add_argument('--only-new', action='store_true', default=True,
                        help='Только тикеры без свечей (по умолчанию)')
    args = parser.parse_args()

    engine = get_engine()
    stocks = get_stocks_without_daily(engine)

    if args.ticker:
        stocks = [(s[0], s[1], s[2], s[3]) for s in stocks if s[0] == args.ticker]
        if not stocks:
            log.error(f"Тикер {args.ticker} не найден в instruments")
            return

    # Фильтруем — те, у которых мало дневных свечей (< 2000 = ~8 лет)
    # Это покрывает новые тикеры И тикеры с неполной историей
    if args.only_new:
        to_backfill = [(s[0], s[1]) for s in stocks if s[3] < 2000]
    else:
        to_backfill = [(s[0], s[1]) for s in stocks]

    log.info(f"{'='*60}")
    log.info(f"BACKFILL ДНЕВНЫХ СВЕЧЕЙ")
    log.info(f"Тикеров для загрузки: {len(to_backfill)}")
    log.info(f"Период: {START_DATE} — {date.today()}")
    log.info(f"{'='*60}")

    if args.dry_run:
        for ticker, name in to_backfill:
            existing = next((s[3] for s in stocks if s[0] == ticker), 0)
            log.info(f"  {ticker:8s} ({name}) — текущих свечей: {existing}")
        return

    async with aiohttp.ClientSession() as session:
        semaphore = asyncio.Semaphore(MAX_CONCURRENT)
        grand_total = 0

        async def process(ticker, name):
            async with semaphore:
                return await backfill_ticker(session, engine, ticker, name)

        # Последовательно по группам по 5
        for i in range(0, len(to_backfill), MAX_CONCURRENT):
            batch = to_backfill[i:i+MAX_CONCURRENT]
            tasks = [process(t, n) for t, n in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for j, r in enumerate(results):
                ticker = batch[j][0]
                if isinstance(r, Exception):
                    log.error(f"  {ticker}: ОШИБКА {r}")
                elif isinstance(r, int):
                    grand_total += r
                    log.info(f"  ✅ {ticker}: итого {r} свечей")

    log.info(f"{'='*60}")
    log.info(f"ИТОГО загружено: {grand_total} дневных свечей")
    log.info(f"{'='*60}")


if __name__ == '__main__':
    asyncio.run(main())
