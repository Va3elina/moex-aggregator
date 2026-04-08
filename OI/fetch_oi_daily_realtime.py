#!/usr/bin/env python3
"""
Реалтайм обновление Open Interest (24 ч) через Algopack API.

Источник: Algopack API (FUTOI)
Интервал: 24 часа (агрегация из 5мин данных — последняя запись дня)

Расписание (МСК):
- Раз в день: 00:10:00 (после закрытия торгов)
- Выходные (Сб, Вс) и праздники MOEX: пропускаются

При старте: докачка с последней даты в БД

Endpoint:
GET https://apim.moex.com/iss/analyticalproducts/futoi/securities/{ticker}.json
"""

import asyncio
import aiohttp
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta, date
from typing import Optional, List, Tuple, Dict
import logging
import sys
from pathlib import Path

from dotenv import load_dotenv
import os

sys.path.insert(0, str(Path(__file__).parent.parent))
load_dotenv(Path(__file__).parent.parent / ".env")

DB_URL = os.getenv("DB_URL")
ALGOPACK_API_KEY = os.getenv("ALGOPACK_API_KEY")

try:
    from moex_calendar import (
        get_moscow_time,
        is_trading_day,
        get_trading_dates
    )
except ImportError as e:
    print(f"Ошибка импорта: {e}")
    sys.exit(1)


MAX_CONCURRENT = 10
DAILY_UPDATE_HOUR = 0
DAILY_UPDATE_MINUTE = 10
GAP_CHECK_INTERVAL = 3600

LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)


def setup_logging():
    detailed_fmt = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(funcName)s:%(lineno)d | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_fmt = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(message)s',
        datefmt='%H:%M:%S'
    )
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    root.handlers.clear()

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(console_fmt)
    root.addHandler(ch)

    fh = logging.FileHandler(LOG_DIR / f"oi_daily_{datetime.now():%Y%m%d}.log", encoding='utf-8')
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(detailed_fmt)
    root.addHandler(fh)

    return logging.getLogger(__name__)


log = setup_logging()


class AlgopackOIDailyFetcher:
    """Загрузчик дневного OI через Algopack FUTOI API"""

    BASE_URL = "https://apim.moex.com/iss/analyticalproducts/futoi/securities"

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.headers = {'Authorization': f'Bearer {api_key}'}
        self.stats = {'requests': 0, 'success': 0, 'errors': 0, 'empty': 0}

    def get_stats(self) -> dict:
        return self.stats.copy()

    def reset_stats(self):
        for k in self.stats:
            self.stats[k] = 0

    async def fetch_oi_day(
        self,
        session: aiohttp.ClientSession,
        ticker: str,
        trade_date: date
    ) -> Optional[List[dict]]:
        """
        Загружает дневной OI для одного тикера за конкретную дату.
        Возвращает последние записи FIZ и YUR за этот день.
        """
        url = f"{self.BASE_URL}/{ticker}.json"
        day_str = trade_date.strftime('%Y-%m-%d')
        params = {'from': day_str, 'till': day_str}

        self.stats['requests'] += 1

        for attempt in range(3):
            try:
                async with session.get(
                    url, headers=self.headers, params=params,
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as resp:
                    if resp.status == 401:
                        log.error(f"[{ticker}] Ошибка авторизации (401). Проверьте ALGOPACK_API_KEY")
                        self.stats['errors'] += 1
                        return None
                    if resp.status == 404:
                        self.stats['empty'] += 1
                        return None
                    if resp.status == 429:
                        log.warning(f"[{ticker}] Rate limit (429), ждём 30 сек...")
                        await asyncio.sleep(30)
                        continue
                    if resp.status != 200:
                        if resp.status >= 500:
                            log.warning(f"[{ticker}] Ошибка сервера {resp.status}, попытка {attempt+1}/3")
                            await asyncio.sleep(2)
                            continue
                        log.warning(f"[{ticker}] HTTP {resp.status}")
                        self.stats['errors'] += 1
                        return None

                    data = await resp.json()
                    if "futoi" not in data:
                        self.stats['empty'] += 1
                        return None

                    futoi = data["futoi"]
                    columns = futoi.get("columns", [])
                    rows = futoi.get("data", [])

                    if not rows:
                        self.stats['empty'] += 1
                        return None

                    records = [dict(zip(columns, row)) for row in rows]

                    # Берём последние записи для каждой clgroup (fiz/yur) — это EOD данные
                    daily = {}
                    for rec in records:
                        cg = rec.get('clgroup', '').lower()
                        if cg in ('fiz', 'yur'):
                            daily[cg] = rec  # перезаписываем — последняя запись дня

                    self.stats['success'] += 1
                    return list(daily.values()) if daily else None

            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                if attempt < 2:
                    await asyncio.sleep(1 + attempt)
                    continue
                log.error(f"[{ticker}] Ошибка сети после 3 попыток: {e}")
                self.stats['errors'] += 1
                return None

        self.stats['errors'] += 1
        return None


class OIDailyUpdater:
    """Обновление дневных данных OI через Algopack"""

    def __init__(self, db_url: str):
        log.info("Инициализация OIDailyUpdater (Algopack)...")

        if not ALGOPACK_API_KEY:
            log.critical("ALGOPACK_API_KEY не задан в .env!")
            raise ValueError("ALGOPACK_API_KEY required")

        self.engine = create_engine(db_url, connect_args={"ssl_context": False})
        with self.engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        log.info("  DB подключена")

        self.fetcher = AlgopackOIDailyFetcher(ALGOPACK_API_KEY)
        self.instruments = self._load_instruments()

        self.last_daily_update = None
        self.last_gap_check = None
        self.session_stats = {
            'start_time': datetime.now(),
            'updates': 0,
            'records_saved': 0,
            'errors': 0
        }

        log.info(f"  Инструментов: {len(self.instruments)}")
        log.info("  OIDailyUpdater (Algopack) готов")

    def _load_instruments(self) -> List[Dict]:
        """Загружает фьючерсные тикеры из БД"""
        with self.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT DISTINCT sectype, name
                FROM instruments
                WHERE type = 'futures'
                ORDER BY name
            """))
            return [{'sectype': r[0], 'name': r[1]} for r in result.fetchall()]

    def get_last_date(self, sectype: str) -> Optional[date]:
        with self.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT MAX(tradedate)
                FROM open_interest
                WHERE sectype = :sectype AND interval = 24
            """), {'sectype': sectype})
            row = result.fetchone()
            return row[0] if row and row[0] else None

    def save_records(self, records: List[dict], sectype: str, trade_date: date) -> int:
        if not records:
            return 0

        inserted = 0
        with self.engine.connect() as conn:
            for rec in records:
                clgroup = rec.get('clgroup', '').upper()
                if clgroup not in ('FIZ', 'YUR'):
                    continue

                pos_long = int(rec.get('pos_long', 0) or 0)
                pos_short = int(rec.get('pos_short', 0) or 0)

                try:
                    result = conn.execute(text("""
                        INSERT INTO open_interest
                        (sectype, tradedate, tradetime, clgroup, pos, pos_long, pos_short,
                         pos_long_num, pos_short_num, interval)
                        VALUES
                        (:sectype, :tradedate, '00:00:00', :clgroup, :pos, :pos_long, :pos_short,
                         :pos_long_num, :pos_short_num, 24)
                        ON CONFLICT (sectype, tradedate, tradetime, clgroup, interval) DO UPDATE SET
                         pos = EXCLUDED.pos,
                         pos_long = EXCLUDED.pos_long,
                         pos_short = EXCLUDED.pos_short,
                         pos_long_num = EXCLUDED.pos_long_num,
                         pos_short_num = EXCLUDED.pos_short_num
                    """), {
                        'sectype': sectype,
                        'tradedate': trade_date,
                        'clgroup': clgroup,
                        'pos': pos_long + pos_short,
                        'pos_long': pos_long,
                        'pos_short': -abs(pos_short),
                        'pos_long_num': int(rec.get('pos_long_num', 0) or 0),
                        'pos_short_num': int(rec.get('pos_short_num', 0) or 0),
                    })
                    inserted += result.rowcount
                except Exception as e:
                    log.debug(f"Ошибка вставки: {e}")

            conn.commit()
        return inserted

    async def update_instrument(self, session: aiohttp.ClientSession, instr: Dict) -> int:
        sectype = instr['sectype']
        last_date = self.get_last_date(sectype)
        start_date = (last_date + timedelta(days=1)) if last_date else date(2022, 6, 24)
        end_date = get_moscow_time().date()

        if start_date > end_date:
            return 0

        trading_dates = get_trading_dates(start_date, end_date)
        if not trading_dates:
            return 0

        total_inserted = 0
        for trade_date in trading_dates:
            records = await self.fetcher.fetch_oi_day(session, sectype, trade_date)
            if records:
                inserted = self.save_records(records, sectype, trade_date)
                total_inserted += inserted
            await asyncio.sleep(0.05)

        return total_inserted

    async def update_all(self, session: aiohttp.ClientSession) -> Tuple[int, int]:
        total_instruments = 0
        total_inserted = 0
        semaphore = asyncio.Semaphore(MAX_CONCURRENT)

        async def process(instr):
            async with semaphore:
                return await self.update_instrument(session, instr)

        log.info(f"Обновление {len(self.instruments)} инструментов...")
        tasks = [process(instr) for instr in self.instruments]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for i, r in enumerate(results):
            if isinstance(r, int) and r > 0:
                total_instruments += 1
                total_inserted += r
                log.debug(f"  {self.instruments[i]['sectype']}: +{r}")
            elif isinstance(r, Exception):
                log.error(f"Ошибка: {r}")

        self.session_stats['records_saved'] += total_inserted
        return total_instruments, total_inserted

    async def check_data_gaps(self):
        log.info("Проверка пропусков OI (daily)...")
        today = date.today()
        gaps = []
        for instr in self.instruments[:20]:
            last = self.get_last_date(instr['sectype'])
            if last is None:
                gaps.append(f"{instr['sectype']}: НЕТ ДАННЫХ")
            elif (today - last).days > 3:
                gaps.append(f"{instr['sectype']}: {(today - last).days} дней")
        if gaps:
            log.warning(f"Пропуски ({len(gaps)}):")
            for g in gaps[:5]:
                log.warning(f"  {g}")
        else:
            log.info("Пропусков нет")

    def print_stats(self):
        uptime = datetime.now() - self.session_stats['start_time']
        api = self.fetcher.get_stats()
        log.info("=" * 50)
        log.info("СТАТИСТИКА OI (дневной, Algopack)")
        log.info(f"  Время работы: {uptime}")
        log.info(f"  Обновлений: {self.session_stats['updates']}")
        log.info(f"  Записей: {self.session_stats['records_saved']}")
        log.info(f"  API: {api['requests']} запросов, {api['success']} ok, {api['errors']} err")
        log.info("=" * 50)

    async def run_forever(self):
        log.info("=" * 60)
        log.info("ЗАПУСК OI DAILY (Algopack FUTOI)")
        log.info(f"  Инструментов: {len(self.instruments)}")
        log.info(f"  Расписание: {DAILY_UPDATE_HOUR:02d}:{DAILY_UPDATE_MINUTE:02d} МСК")
        log.info("=" * 60)

        connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT)
        retry_count = 0

        while True:
            try:
                async with aiohttp.ClientSession(connector=connector) as session:
                    log.info("Начальная синхронизация...")
                    instr_count, ins = await self.update_all(session)
                    log.info(f"  Синхронизировано: {instr_count} инструментов, +{ins} записей")
                    self.last_daily_update = get_moscow_time().date()
                    self.last_gap_check = datetime.now()
                    retry_count = 0

                    while True:
                        now = get_moscow_time()
                        today = now.date()

                        is_trading, reason = is_trading_day()
                        if not is_trading:
                            await asyncio.sleep(3600)
                            continue

                        if (today != self.last_daily_update and
                            now.hour == DAILY_UPDATE_HOUR and
                            now.minute >= DAILY_UPDATE_MINUTE):
                            log.info(f"[{now:%H:%M} МСК] Дневное обновление OI...")
                            instr_count, ins = await self.update_all(session)
                            self.last_daily_update = today
                            self.session_stats['updates'] += 1
                            log.info(f"  +{ins} записей ({instr_count} инструментов)")
                            self.print_stats()

                        if (datetime.now() - self.last_gap_check).seconds >= GAP_CHECK_INTERVAL:
                            await self.check_data_gaps()
                            self.last_gap_check = datetime.now()

                        await asyncio.sleep(60)

            except aiohttp.ClientError as e:
                retry_count += 1
                log.error(f"Ошибка сети: {e}, переподключение {retry_count}/5")
                if retry_count >= 5:
                    raise
                await asyncio.sleep(30 * retry_count)
            except KeyboardInterrupt:
                self.print_stats()
                break
            except Exception as e:
                log.critical(f"Критическая ошибка: {e}", exc_info=True)
                await asyncio.sleep(60)


async def main():
    import argparse
    parser = argparse.ArgumentParser(description='OI daily через Algopack FUTOI')
    parser.add_argument('--once', action='store_true')
    parser.add_argument('--force', action='store_true')
    args = parser.parse_args()

    log.info(f"OI Daily (Algopack) | МСК: {get_moscow_time()} | {'once' if args.once else 'daemon'}")

    if args.once and not args.force:
        is_trading, reason = is_trading_day()
        if not is_trading:
            log.info(f"Пропуск: {reason} (--force для принудительного)")
            return

    updater = OIDailyUpdater(DB_URL)

    if args.once:
        connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT)
        async with aiohttp.ClientSession(connector=connector) as session:
            instr_count, ins = await updater.update_all(session)
            log.info(f"Готово: {instr_count} инструментов, +{ins} записей")
    else:
        await updater.run_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Завершено")
    except Exception as e:
        log.critical(f"Ошибка: {e}")
        sys.exit(1)
