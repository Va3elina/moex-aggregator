#!/usr/bin/env python3
"""
Бэкфилл исторических дневных свечей через ISS API (публичный).
ISS возвращает полные дневные свечи со всех сессий (основная + вечерняя + аукционы).

Использование:
  python Candles/backfill_daily_history.py              # Только тикеры с <2000 свечей
  python Candles/backfill_daily_history.py --all         # ВСЕ тикеры (перезаливка)
  python Candles/backfill_daily_history.py --ticker SBER # Конкретный тикер
  python Candles/backfill_daily_history.py --dry-run     # Только показать что будет загружено
"""

import asyncio
import aiohttp
import logging
import sys
import os
import argparse
from pathlib import Path
from datetime import datetime, date
from typing import List, Tuple

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# === Пути ===
PROJECT_DIR = Path(__file__).parent.parent
load_dotenv(PROJECT_DIR / ".env")

DB_URL = os.getenv("DB_URL", "postgresql+pg8000://postgres:1803@localhost:5432/moex_db")

# ISS — публичный API, не требует авторизации.
# Возвращает полные дневные свечи (все сессии за сутки).
ISS_URL = "https://iss.moex.com/iss/engines/stock/markets/shares/boards/tqbr/securities"
START_DATE = "2007-01-01"
MAX_CONCURRENT = 5

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger(__name__)


def get_engine():
    return create_engine(DB_URL, pool_pre_ping=True)


def get_all_stocks(engine) -> List[Tuple[str, str, int]]:
    """Возвращает все акции с количеством дневных свечей."""
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT i.sec_id, i.name, COUNT(c.*) as candle_count
            FROM instruments i
            LEFT JOIN candles c ON c.secid = i.sec_id AND c.type = 'stock' AND c.interval = 24
            WHERE i.type = 'stock'
            GROUP BY i.sec_id, i.name
            ORDER BY i.sec_id
        """)).fetchall()
    return [(r[0], r[1], r[2]) for r in rows]


async def fetch_candles_for_year(session, ticker: str, year: int) -> list:
    """Загружает дневные свечи за один год через ISS (без авторизации)."""
    all_rows = []
    start = 0

    while True:
        url = (
            f"{ISS_URL}/{ticker}/candles.json"
            f"?interval=24&from={year}-01-01&till={year}-12-31&start={start}"
            f"&iss.meta=off"
        )

        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status == 429:
                    log.warning(f"  {ticker}/{year} rate limit, ждём 5 сек...")
                    await asyncio.sleep(5)
                    continue
                if resp.status != 200:
                    log.warning(f"  {ticker}/{year} HTTP {resp.status}")
                    break
                data = await resp.json()
                rows = data.get('candles', {}).get('data', [])
                if not rows:
                    break
                all_rows.extend(rows)
                if len(rows) < 500:
                    break
                start += len(rows)
                await asyncio.sleep(0.1)  # Не нагружаем ISS
        except Exception as e:
            log.warning(f"  {ticker}/{year} ошибка: {e}")
            break

    return all_rows


async def backfill_ticker(session, engine, ticker: str, name: str, start_year: int = 2007):
    """Загружает историю для одного тикера из ISS начиная с start_year."""
    current_year = datetime.now().year
    total_candles = 0

    for year in range(start_year, current_year + 1):
        rows = await fetch_candles_for_year(session, ticker, year)
        if not rows:
            continue

        # ISS колонки: open, close, high, low, value, volume, begin, end
        values = []
        for r in rows:
            values.append({
                'secid': ticker,
                'sec_id': ticker,
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
                        INSERT INTO candles (secid, sec_id, open, close, high, low, value, volume,
                                            begin_time, end_time, interval, type)
                        VALUES (:secid, :sec_id, :open, :close, :high, :low, :value, :volume,
                                :begin_time, :end_time, :interval, :type)
                        ON CONFLICT (secid, begin_time, interval, type) DO UPDATE SET
                            open = EXCLUDED.open, close = EXCLUDED.close,
                            high = EXCLUDED.high, low = EXCLUDED.low,
                            value = EXCLUDED.value, volume = EXCLUDED.volume,
                            end_time = EXCLUDED.end_time
                    """), v)
                conn.commit()

            total_candles += len(values)
            log.info(f"  {ticker} {year}: {len(values)} свечей")

        await asyncio.sleep(0.2)  # Пауза между годами

    return total_candles


async def main():
    parser = argparse.ArgumentParser(description='Backfill daily candles from ISS API')
    parser.add_argument('--ticker', type=str, help='Конкретный тикер')
    parser.add_argument('--all', action='store_true',
                        help='Перезалить ВСЕ тикеры (обновить существующие)')
    parser.add_argument('--years', type=float, default=None,
                        help='Сколько лет назад загружать (например 1.5). По умолчанию — вся история с 2007')
    parser.add_argument('--dry-run', action='store_true', help='Только показать план')
    args = parser.parse_args()

    engine = get_engine()
    stocks = get_all_stocks(engine)

    if args.ticker:
        stocks = [s for s in stocks if s[0] == args.ticker]
        if not stocks:
            log.error(f"Тикер {args.ticker} не найден в instruments")
            return

    # --all: все тикеры. Без флага: только тикеры с < 2000 свечей (новые / неполные)
    if args.all or args.ticker:
        to_backfill = [(s[0], s[1]) for s in stocks]
    else:
        to_backfill = [(s[0], s[1]) for s in stocks if s[2] < 2000]

    # Определяем стартовый год
    if args.years:
        from datetime import timedelta
        start_year = (date.today() - timedelta(days=int(args.years * 365))).year
    else:
        start_year = 2007

    log.info(f"{'='*60}")
    log.info(f"BACKFILL ДНЕВНЫХ СВЕЧЕЙ (ISS API)")
    log.info(f"Тикеров для загрузки: {len(to_backfill)}")
    log.info(f"Период: {start_year}-01-01 — {date.today()}")
    log.info(f"Режим: {'ПОЛНАЯ ПЕРЕЗАЛИВКА' if args.all else 'НОВЫЕ/НЕПОЛНЫЕ'}")
    log.info(f"{'='*60}")

    if args.dry_run:
        for ticker, name in to_backfill:
            existing = next((s[2] for s in stocks if s[0] == ticker), 0)
            log.info(f"  {ticker:8s} ({name}) — текущих свечей: {existing}")
        return

    async with aiohttp.ClientSession() as session:
        grand_total = 0
        semaphore = asyncio.Semaphore(MAX_CONCURRENT)

        async def process(ticker, name):
            async with semaphore:
                return await backfill_ticker(session, engine, ticker, name, start_year)

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
    log.info(f"ИТОГО загружено/обновлено: {grand_total} дневных свечей")
    log.info(f"{'='*60}")


if __name__ == '__main__':
    asyncio.run(main())
