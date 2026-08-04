#!/usr/bin/env python3
"""
Бэкфилл дневных свечей ДЕЛИСТИНГОВАННЫХ бумаг, которые держали фонды.

Зачем. «Карта сделок» в «Потоках по компании» рисует сделки фондов на линии
цены. У переехавших в РФ компаний история до редомициляции лежит на бирже под
СТАРЫМ тикером (HEAD ← HHRU, YDEX ← YNDX, X5 ← FIVE, RAGR ← AGRO, CNRU ← CIAN,
VKCO ← MAIL), а у делистингованных (POGR, POLY, RSTI, DSKY) — только под их
собственным. Ни тех, ни других нет в instruments, поэтому обычный
backfill_daily_history.py (он идёт по instruments) их не качает, и на карте
зияли дыры до 46 месяцев.

Что делает. Берёт из securities_ref бумаги, которые (а) встречаются в составах
whitelist-фондов акций, (б) имеют secid, (в) НЕ торгуются сейчас (is_traded =
false), и докачивает их дневные свечи с ISS в candles (type='stock').

ВАЖНО: в instruments эти бумаги НЕ добавляются — иначе делистингованный YNDX
всплыл бы в пикерах сезонности и остальных индикаторов как живой актив. Карта
рынка строится FROM instruments, ширина рынка берёт свечи за последние 30 дней,
поэтому старые свечи в candles им безразличны. Склейку старой и новой серии
делает /api/fund-trades/price-weekly по securities_ref.canonical_isin.

Использование:
  python Candles/backfill_delisted_history.py            # докачать недостающее
  python Candles/backfill_delisted_history.py --all      # перезалить всё
  python Candles/backfill_delisted_history.py --dry-run  # только показать план
"""

import asyncio
import aiohttp
import logging
import os
import sys
import argparse
from pathlib import Path
from datetime import date

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

PROJECT_DIR = Path(__file__).parent.parent
load_dotenv(PROJECT_DIR / ".env")
sys.path.insert(0, str(PROJECT_DIR))

DB_URL = os.getenv("DB_URL")

# Свечи делистингованных бумаг живут на securities-уровне (без борда): доска
# TQBR у них уже снята, поэтому boards/tqbr/... отдаёт пусто.
ISS_URL = "https://iss.moex.com/iss/engines/stock/markets/shares/securities"
MAX_CONCURRENT = 3
START_YEAR = 2007

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger(__name__)


def get_engine():
    return create_engine(DB_URL, pool_pre_ping=True)


def get_delisted_holdings(engine) -> list[tuple[str, str, int]]:
    """Делистингованные бумаги из составов фондов: (secid, short_name, свечей)."""
    from api.routers.fund_trades import WHITELIST_TICKERS, MONTHLY_SOURCES
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT sr.secid,
                   MAX(sr.short_name) AS nm,
                   (SELECT COUNT(*) FROM candles c
                     WHERE c.secid = sr.secid AND c.type = 'stock' AND c.interval = 24) AS cnt
            FROM securities_ref sr
            WHERE sr.secid IS NOT NULL
              AND sr.is_traded IS FALSE
              AND sr.sec_type IN ('common_share', 'preferred_share', 'depositary_receipt')
              AND sr.isin IN (
                    SELECT DISTINCT h.isin
                    FROM fund_holdings_history h
                    JOIN funds f ON f.fund_id = h.fund_id
                    WHERE f.ticker = ANY(:tickers) AND f.category = 'stocks'
                      AND h.source = ANY(:sources)
              )
            GROUP BY sr.secid
            ORDER BY sr.secid
        """), {"tickers": list(WHITELIST_TICKERS), "sources": list(MONTHLY_SOURCES)}).fetchall()
    return [(r[0], r[1], r[2]) for r in rows]


async def fetch_candles_for_year(session, secid: str, year: int) -> list:
    """Дневные свечи за год через ISS (публичный, без авторизации)."""
    all_rows = []
    start = 0
    while True:
        url = (f"{ISS_URL}/{secid}/candles.json"
               f"?interval=24&from={year}-01-01&till={year}-12-31&start={start}&iss.meta=off")
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status == 429:
                    log.warning(f"  {secid}/{year} rate limit, ждём 5 сек...")
                    await asyncio.sleep(5)
                    continue
                if resp.status != 200:
                    log.warning(f"  {secid}/{year} HTTP {resp.status}")
                    break
                data = await resp.json()
                rows = data.get('candles', {}).get('data', [])
                if not rows:
                    break
                all_rows.extend(rows)
                if len(rows) < 500:
                    break
                start += len(rows)
                await asyncio.sleep(0.1)
        except Exception as e:
            log.warning(f"  {secid}/{year} ошибка: {e}")
            break
    return all_rows


async def backfill_secid(session, engine, secid: str, name: str) -> int:
    """Вся доступная история одной делистингованной бумаги."""
    log.info(f"{secid} ({name}): качаем историю с {START_YEAR}")
    total = 0
    for year in range(START_YEAR, date.today().year + 1):
        rows = await fetch_candles_for_year(session, secid, year)
        if not rows:
            continue
        # ISS: open, close, high, low, value, volume, begin, end.
        # Фантомы (OHLC=NULL за неторговые дни) пропускаем — как в backfill_daily_history.
        values = []
        for r in rows:
            if r[0] is None or r[1] is None or r[2] is None or r[3] is None:
                continue
            values.append({
                'secid': secid, 'sec_id': secid,
                'open': r[0], 'close': r[1], 'high': r[2], 'low': r[3],
                'value': r[4], 'volume': r[5],
                'begin_time': r[6], 'end_time': r[7],
                'interval': 24, 'type': 'stock',
            })
        if not values:
            continue
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
        total += len(values)
        log.info(f"  {secid} {year}: {len(values)} свечей")
        await asyncio.sleep(0.2)
    return total


async def main():
    parser = argparse.ArgumentParser(description='Backfill daily candles for delisted fund holdings')
    parser.add_argument('--secid', type=str, help='Конкретная бумага')
    parser.add_argument('--all', action='store_true', help='Перезалить все, а не только пустые')
    parser.add_argument('--dry-run', action='store_true', help='Только показать план')
    args = parser.parse_args()

    engine = get_engine()
    secs = get_delisted_holdings(engine)
    if args.secid:
        secs = [s for s in secs if s[0] == args.secid]
        if not secs:
            log.error(f"{args.secid} не найден среди делистингованных бумаг фондов")
            return
    # Без флагов докачиваем только тех, у кого свечей нет вовсе: у остальных
    # история уже полная (докачивать нечего — бумага больше не торгуется).
    todo = secs if (args.all or args.secid) else [s for s in secs if s[2] == 0]

    log.info("=" * 60)
    log.info("BACKFILL ДЕЛИСТИНГОВАННЫХ БУМАГ ИЗ СОСТАВОВ ФОНДОВ (ISS)")
    log.info(f"Кандидатов всего: {len(secs)}, к загрузке: {len(todo)}")
    log.info("=" * 60)
    if args.dry_run:
        for secid, nm, cnt in secs:
            mark = "→ качаем" if (secid, nm, cnt) in todo else "  пропуск"
            log.info(f"  {mark} {secid:8s} ({nm}) — свечей сейчас: {cnt}")
        return

    async with aiohttp.ClientSession() as session:
        grand_total = 0
        semaphore = asyncio.Semaphore(MAX_CONCURRENT)

        async def process(secid, nm):
            async with semaphore:
                return await backfill_secid(session, engine, secid, nm)

        results = await asyncio.gather(
            *[process(s[0], s[1]) for s in todo], return_exceptions=True
        )
        for i, r in enumerate(results):
            secid = todo[i][0]
            if isinstance(r, Exception):
                log.error(f"  {secid}: ОШИБКА {r}")
            elif isinstance(r, int):
                grand_total += r
                log.info(f"  ✅ {secid}: итого {r} свечей")

    log.info("=" * 60)
    log.info(f"ИТОГО загружено/обновлено: {grand_total} дневных свечей")
    log.info("=" * 60)


if __name__ == '__main__':
    asyncio.run(main())
