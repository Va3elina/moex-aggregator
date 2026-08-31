#!/usr/bin/env python3
"""
Бэкфилл дневных свечей ДЕЛИСТИНГОВАННЫХ бумаг: холдинги фондов и бывшие
участники базы расчёта индекса МосБиржи.

Зачем (фонды). «Карта сделок» в «Потоках по компании» рисует сделки фондов на
линии цены. У переехавших в РФ компаний история до редомициляции лежит на бирже под
СТАРЫМ тикером (HEAD ← HHRU, YDEX ← YNDX, X5 ← FIVE, RAGR ← AGRO, CNRU ← CIAN,
VKCO ← MAIL), а у делистингованных (POGR, POLY, RSTI, DSKY) — только под их
собственным. Ни тех, ни других нет в instruments, поэтому обычный
backfill_daily_history.py (он идёт по instruments) их не качает, и на карте
зияли дыры до 46 месяцев.

Зачем (индекс). «Сила рынка» со вселенной imoex считает широту по составу
индекса НА КАЖДУЮ ДАТУ (index_composition). Бумаги, выбывшие из индекса и с
биржи — Уралкалий, Мегафон, РАО ЕЭС, Дикси, ОГК-3 — в candles не лежат, и
знаменатель ранних лет получался вдвое меньше реального состава: в 2008 году
в индексе было 30 бумаг, а в широте участвовало 14.

Что делает. Берёт кандидатов из двух источников (--source):
  funds — securities_ref: бумаги из составов whitelist-фондов акций, у которых
          есть secid и is_traded = false;
  index — index_composition: бумаги, входившие когда-либо в базу расчёта
          индекса, но не имеющие ни одной дневной свечи в candles.
Докачивает их дневные свечи с ISS в candles (type='stock') и записывает сами
бумаги в реестр delisted_securities — пометку «этот secid в candles не
действующий инструмент, а история».

ВАЖНО: в instruments эти бумаги НЕ добавляются — иначе делистингованный YNDX
всплыл бы в пикерах сезонности и остальных индикаторов как живой актив. Карта
рынка строится FROM instruments, вселенная «все акции» у ширины рынка берёт
свечи за последние 30 дней, поэтому старые свечи в candles им безразличны.
Пометка о делистинге живёт в delisted_securities. Склейку старой и новой серии
делает /api/fund-trades/price-weekly по securities_ref.canonical_isin.

Использование:
  python Candles/backfill_delisted_history.py                  # фонды (как раньше)
  python Candles/backfill_delisted_history.py --source index   # бывшие участники IMOEX
  python Candles/backfill_delisted_history.py --source all     # и те, и другие
  python Candles/backfill_delisted_history.py --all            # перезалить всё
  python Candles/backfill_delisted_history.py --dry-run        # только показать план

После загрузки бумаг источника index пересчитать широту:
  python Candles/compute_breadth_history.py --full
"""

import asyncio
import aiohttp
import json
import logging
import os
import sys
import argparse
import urllib.request
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

# Метаданные бумаги (короткое имя, ISIN) для реестра delisted_securities.
ISS_SECURITY = "https://iss.moex.com/iss/securities/{secid}.json?iss.meta=off&iss.only=description"

# Индекс, чью историческую базу расчёта разбираем при --source index.
INDEX_ID = "IMOEX"

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
              -- Суффикс -ME (ETLN-ME, TCS-ME) — внебиржевая доска RPMA: свечей
              -- по ним ISS не отдаёт, а биржевая история этих бумаг и так цела
              -- под основным тикером.
              AND sr.secid NOT LIKE '%-ME'
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


def get_index_delisted(engine, index_id: str = INDEX_ID) -> list[tuple[str, str, int]]:
    """Бывшие участники базы расчёта индекса без единой дневной свечи.

    Возвращает (secid, short_name, свечей=0) — тот же кортеж, что и у
    источника funds, чтобы дальше обрабатывать их одинаково. Имя берём с ISS
    (в index_composition хранятся только тикеры и веса).
    """
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT ic.ticker, COUNT(DISTINCT ic.trade_date) AS days
            FROM index_composition ic
            WHERE ic.index_id = :idx
              AND NOT EXISTS (
                    SELECT 1 FROM candles c
                    WHERE c.secid = ic.ticker AND c.type = 'stock' AND c.interval = 24
              )
            GROUP BY ic.ticker
            ORDER BY days DESC
        """), {"idx": index_id}).fetchall()

    out = []
    for secid, days in rows:
        name, _ = fetch_security_meta(secid)
        log.info(f"  кандидат {secid:8s} ({name}) — дней в индексе: {days}")
        out.append((secid, name, 0))
    return out


def fetch_security_meta(secid: str) -> tuple[str, str]:
    """(short_name, isin) бумаги с ISS. Пустые строки, если ISS промолчал."""
    try:
        req = urllib.request.Request(ISS_SECURITY.format(secid=secid),
                                     headers={"User-Agent": "Mozilla/5.0 (compatible; FrameBot/1.0)"})
        with urllib.request.urlopen(req, timeout=30) as r:
            data = json.load(r)
        cols = data["description"]["columns"]
        i_name, i_val = cols.index("name"), cols.index("value")
        meta = {row[i_name]: row[i_val] for row in data["description"]["data"]}
        return (meta.get("SHORTNAME") or meta.get("NAME") or secid, meta.get("ISIN") or "")
    except Exception as e:
        log.warning(f"  {secid}: метаданные ISS недоступны ({e})")
        return (secid, "")


def ensure_registry(engine) -> None:
    """Схема delisted_securities — как в db/migrations/053 (идемпотентно)."""
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS delisted_securities (
                secid         VARCHAR(16) PRIMARY KEY,
                short_name    VARCHAR(120),
                isin          VARCHAR(20),
                source        VARCHAR(32),
                first_candle  DATE,
                last_candle   DATE,
                candles_count INTEGER,
                note          TEXT,
                updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
            )
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_delisted_securities_last
            ON delisted_securities(last_candle DESC)
        """))


def mark_delisted(engine, secid: str, name: str, source: str) -> None:
    """Отмечает бумагу в реестре историй: границы серии берём из candles.

    Вызывается ПОСЛЕ загрузки свечей — иначе границы окажутся пустыми.
    """
    _, isin = fetch_security_meta(secid)
    with engine.begin() as conn:
        row = conn.execute(text("""
            SELECT MIN(begin_time)::date, MAX(begin_time)::date, COUNT(*)
            FROM candles
            WHERE secid = :s AND type = 'stock' AND interval = 24
        """), {"s": secid}).fetchone()
        first, last, cnt = (row[0], row[1], row[2]) if row else (None, None, 0)
        conn.execute(text("""
            INSERT INTO delisted_securities
                (secid, short_name, isin, source, first_candle, last_candle, candles_count, updated_at)
            VALUES (:s, :n, :i, :src, :f, :l, :c, now())
            ON CONFLICT (secid) DO UPDATE SET
                short_name    = COALESCE(EXCLUDED.short_name, delisted_securities.short_name),
                isin          = COALESCE(NULLIF(EXCLUDED.isin, ''), delisted_securities.isin),
                source        = EXCLUDED.source,
                first_candle  = EXCLUDED.first_candle,
                last_candle   = EXCLUDED.last_candle,
                candles_count = EXCLUDED.candles_count,
                updated_at    = now()
        """), {"s": secid, "n": name, "i": isin, "src": source,
               "f": first, "l": last, "c": cnt})
    log.info(f"  ✓ {secid} помечен делистингованным: {first} … {last}, свечей {cnt}")


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
    parser.add_argument('--source', choices=['funds', 'index', 'all'], default='funds',
                        help='Источник кандидатов: холдинги фондов, база расчёта индекса или оба')
    parser.add_argument('--all', action='store_true', help='Перезалить все, а не только пустые')
    parser.add_argument('--dry-run', action='store_true', help='Только показать план')
    args = parser.parse_args()

    engine = get_engine()
    ensure_registry(engine)

    # Источник помним по бумаге: он идёт в delisted_securities.source.
    source_of: dict[str, str] = {}
    secs: list = []
    if args.source in ('funds', 'all'):
        for row in get_delisted_holdings(engine):
            source_of[row[0]] = 'fund_holdings'
            secs.append(row)
    if args.source in ('index', 'all'):
        log.info(f"Кандидаты из истории базы расчёта {INDEX_ID}:")
        known = {r[0] for r in secs}
        for row in get_index_delisted(engine):
            source_of.setdefault(row[0], 'index_composition')
            if row[0] not in known:
                secs.append(row)

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
                loaded = await backfill_secid(session, engine, secid, nm)
                # Пометка в реестре — после загрузки: границы серии читаются
                # из candles. Ставим её и когда свечей не прибавилось: строка
                # реестра нужна как факт «это историческая бумага».
                mark_delisted(engine, secid, nm, source_of.get(secid, 'unknown'))
                return loaded

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
