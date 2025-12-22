#!/usr/bin/env python3
"""
Реалтайм обновление Open Interest (24 ч) через ISS MOEX.

Источник: ISS MOEX (бесплатный, без API ключа)
Тикеров: 133 (все с iss_code)
Интервал: 24 часа (дневной)

Расписание (МСК):
- Раз в день: 00:10:00 (после закрытия торгов)
- Выходные (Сб, Вс) и праздники MOEX: пропускаются

При старте: докачка с последней даты в БД

Endpoint:
GET https://iss.moex.com/iss/statistics/engines/futures/markets/forts/openpositions/{iss_code}.json
"""

import asyncio
import aiohttp
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta, date
from typing import Optional, List, Tuple, Dict
import logging
import sys
from pathlib import Path

# === Импорт из корневой папки ===
from dotenv import load_dotenv
import os

sys.path.insert(0, str(Path(__file__).parent.parent))
load_dotenv(Path(__file__).parent.parent / ".env")

DB_URL = os.getenv("DB_URL")

try:
    from moex_calendar import (
        get_moscow_time,
        is_trading_day,
        get_trading_dates
    )
except ImportError as e:
    print(f"❌ Ошибка импорта: {e}")
    print("Убедитесь, что moex_calendar.py находится в корневой папке MOEX/")
    sys.exit(1)


# === ⚙️ НАСТРОЙКИ ===

# Параллельность
MAX_CONCURRENT = 20

# Время обновления дневных данных (МСК)
DAILY_UPDATE_HOUR = 0
DAILY_UPDATE_MINUTE = 10

# Проверка пропусков (секунды)
GAP_CHECK_INTERVAL = 3600

# Директория логов
LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

# Дата начала данных ISS
ISS_START_DATE = date(2022, 6, 24)


# ======================================================================
#                         ЛОГИРОВАНИЕ
# ======================================================================

def setup_logging():
    """Настройка логирования в файл и консоль"""

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

    # Консоль (INFO+)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(console_fmt)
    root.addHandler(ch)

    # Файл всех логов (DEBUG+)
    fh_all = logging.FileHandler(
        LOG_DIR / f"oi_daily_{datetime.now():%Y%m%d}.log",
        encoding='utf-8'
    )
    fh_all.setLevel(logging.DEBUG)
    fh_all.setFormatter(detailed_fmt)
    root.addHandler(fh_all)

    # Файл ошибок (WARNING+)
    fh_err = logging.FileHandler(
        LOG_DIR / f"oi_daily_errors_{datetime.now():%Y%m%d}.log",
        encoding='utf-8'
    )
    fh_err.setLevel(logging.WARNING)
    fh_err.setFormatter(detailed_fmt)
    root.addHandler(fh_err)

    return logging.getLogger(__name__)


log = setup_logging()


# ======================================================================
#                    ISS OI FETCHER
# ======================================================================

class ISSOIFetcher:
    """Загрузчик дневного OI через ISS MOEX"""

    BASE_URL = "https://iss.moex.com/iss/statistics/engines/futures/markets/forts/openpositions"

    def __init__(self):
        self.stats = {
            'requests': 0,
            'success': 0,
            'errors': 0,
            'empty': 0
        }

    def get_stats(self) -> dict:
        return self.stats.copy()

    def reset_stats(self):
        for k in self.stats:
            self.stats[k] = 0

    async def fetch_oi(
        self,
        session: aiohttp.ClientSession,
        iss_code: str,
        trade_date: date
    ) -> Optional[List[dict]]:
        """
        Загружает дневной OI для одного инструмента.

        Returns: список записей или None
        """
        url = f"{self.BASE_URL}/{iss_code}.json"
        params = {'date': trade_date.strftime('%Y-%m-%d')}

        self.stats['requests'] += 1

        try:
            async with session.get(
                url,
                params=params,
                timeout=aiohttp.ClientTimeout(total=30)
            ) as resp:

                if resp.status != 200:
                    log.debug(f"[{iss_code}] HTTP {resp.status}")
                    self.stats['errors'] += 1
                    return None

                data = await resp.json()

                rows = data.get('open_positions', {}).get('data', [])
                columns = data.get('open_positions', {}).get('columns', [])

                if not rows:
                    self.stats['empty'] += 1
                    return None

                self.stats['success'] += 1
                return [dict(zip(columns, row)) for row in rows]

        except asyncio.TimeoutError:
            log.error(f"[{iss_code}] Таймаут")
            self.stats['errors'] += 1
            return None
        except Exception as e:
            log.debug(f"[{iss_code}] Ошибка: {e}")
            self.stats['errors'] += 1
            return None


# ======================================================================
#                    OI DAILY UPDATER
# ======================================================================

class OIDailyUpdater:
    """Обновление дневных данных OI"""

    def __init__(self, db_url: str):
        log.info("Инициализация OIDailyUpdater...")

        try:
            self.engine = create_engine(db_url)
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            log.info("✓ БД подключена")
        except Exception as e:
            log.critical(f"Ошибка подключения к БД: {e}")
            raise

        self.fetcher = ISSOIFetcher()
        self.instruments = self._load_instruments()

        self.last_daily_update = None
        self.last_gap_check = None

        self.session_stats = {
            'start_time': datetime.now(),
            'updates': 0,
            'records_saved': 0,
            'errors': 0
        }

        log.info(f"✓ Инструментов: {len(self.instruments)}")
        log.info("✓ OIDailyUpdater готов")

    def _load_instruments(self) -> List[Dict]:
        """Загружает инструменты с iss_code из БД"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT DISTINCT sectype, name, iss_code
                    FROM instruments
                    WHERE type = 'futures' AND iss_code IS NOT NULL
                    ORDER BY name
                """))
                return [
                    {'sectype': r[0], 'name': r[1], 'iss_code': r[2]}
                    for r in result.fetchall()
                ]
        except Exception as e:
            log.error(f"Ошибка загрузки инструментов: {e}")
            return []

    def get_last_date(self, sectype: str) -> Optional[date]:
        """Последняя дата OI для инструмента"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT MAX(tradedate)
                    FROM open_interest
                    WHERE sectype = :sectype AND interval = 24
                """), {'sectype': sectype})
                row = result.fetchone()
                return row[0] if row and row[0] else None
        except Exception as e:
            log.error(f"Ошибка получения последней даты: {e}")
            return None

    def save_records(self, records: List[dict], sectype: str, trade_date: date) -> int:
        """Сохраняет дневные записи OI"""
        if not records:
            return 0

        inserted = 0

        try:
            with self.engine.connect() as conn:
                for rec in records:
                    clgroup = 'FIZ' if rec.get('is_fiz') == 1 else 'YUR'
                    pos_long = rec.get('open_position_long', 0) or 0
                    pos_short = rec.get('open_position_short', 0) or 0

                    try:
                        result = conn.execute(text("""
                            INSERT INTO open_interest 
                            (sectype, tradedate, tradetime, clgroup, pos, pos_long, pos_short,
                             pos_long_num, pos_short_num, interval)
                            VALUES 
                            (:sectype, :tradedate, '00:00:00', :clgroup, :pos, :pos_long, :pos_short,
                             :pos_long_num, :pos_short_num, 24)
                            ON CONFLICT (sectype, tradedate, tradetime, clgroup, interval) DO NOTHING
                        """), {
                            'sectype': sectype,
                            'tradedate': trade_date,
                            'clgroup': clgroup,
                            'pos': pos_long + pos_short,
                            'pos_long': pos_long,
                            'pos_short': -abs(pos_short),
                            'pos_long_num': rec.get('persons_long', 0) or 0,
                            'pos_short_num': rec.get('persons_short', 0) or 0,
                        })
                        inserted += result.rowcount
                    except Exception as e:
                        log.debug(f"Ошибка вставки: {e}")

                conn.commit()

        except Exception as e:
            log.error(f"Ошибка сохранения: {e}")
            self.session_stats['errors'] += 1

        return inserted

    async def update_instrument(
        self,
        session: aiohttp.ClientSession,
        instr: Dict
    ) -> int:
        """Обновляет OI для одного инструмента"""

        sectype = instr['sectype']
        iss_code = instr['iss_code']

        last_date = self.get_last_date(sectype)
        start_date = (last_date + timedelta(days=1)) if last_date else ISS_START_DATE
        end_date = date.today() - timedelta(days=1)

        if start_date > end_date:
            return 0

        # Используем календарь MOEX для получения торговых дней
        trading_dates = get_trading_dates(start_date, end_date)
        if not trading_dates:
            return 0

        total_inserted = 0

        for trade_date in trading_dates:
            records = await self.fetcher.fetch_oi(session, iss_code, trade_date)
            if records:
                inserted = self.save_records(records, sectype, trade_date)
                total_inserted += inserted

        return total_inserted

    async def update_all(self, session: aiohttp.ClientSession) -> Tuple[int, int]:
        """Обновляет OI для всех инструментов"""

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
        """Проверяет пропуски в данных"""
        log.info("🔍 Проверка пропусков OI (daily)...")

        today = date.today()
        gaps = []

        for instr in self.instruments[:20]:
            last = self.get_last_date(instr['sectype'])

            if last is None:
                gaps.append(f"{instr['sectype']}: НЕТ ДАННЫХ")
            elif (today - last).days > 3:
                gaps.append(f"{instr['sectype']}: {(today - last).days} дней")

        if gaps:
            log.warning(f"⚠️ Пропуски ({len(gaps)}):")
            for g in gaps[:5]:
                log.warning(f"  {g}")
        else:
            log.info("✓ Пропусков нет")

    def _get_day_slot(self) -> date:
        now = get_moscow_time()
        return now.date()

    def print_stats(self):
        """Статистика сессии"""
        uptime = datetime.now() - self.session_stats['start_time']
        api = self.fetcher.get_stats()

        log.info("=" * 50)
        log.info("📊 СТАТИСТИКА OI (дневной)")
        log.info(f"  Время работы: {uptime}")
        log.info(f"  Обновлений: {self.session_stats['updates']}")
        log.info(f"  Записей сохранено: {self.session_stats['records_saved']}")
        log.info(f"  Ошибок: {self.session_stats['errors']}")
        log.info(f"  API: {api['requests']} запросов, {api['success']} успешных, {api['errors']} ошибок")
        log.info("=" * 50)

    async def run_forever(self):
        """Основной цикл"""

        log.info("=" * 60)
        log.info("🚀 ЗАПУСК РЕАЛТАЙМ ОБНОВЛЕНИЯ OI (дневной)")
        log.info(f"  API: ISS MOEX (бесплатный)")
        log.info(f"  Инструментов: {len(self.instruments)}")
        log.info(f"  Параллельность: {MAX_CONCURRENT}")
        log.info(f"  Расписание: {DAILY_UPDATE_HOUR:02d}:{DAILY_UPDATE_MINUTE:02d} МСК (Пн-Пт, кроме праздников)")
        log.info(f"  Логи: {LOG_DIR.absolute()}")
        log.info("=" * 60)

        connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT)
        timeout = aiohttp.ClientTimeout(total=300)

        retry_count = 0
        max_retries = 5

        while True:
            try:
                async with aiohttp.ClientSession(
                    connector=connector,
                    timeout=timeout
                ) as session:

                    # === Начальная синхронизация ===
                    log.info("🔄 Начальная синхронизация...")
                    instr_count, ins = await self.update_all(session)
                    log.info(f"  ✓ Инструментов обновлено: {instr_count}, записей: +{ins}")

                    self.last_daily_update = self._get_day_slot()
                    self.last_gap_check = datetime.now()

                    log.info("✅ Синхронизация завершена")
                    log.info("=" * 60)

                    retry_count = 0

                    # === Основной цикл ===
                    while True:
                        now = get_moscow_time()
                        today = now.date()

                        # === Проверка торгового дня (с учётом праздников) ===
                        is_trading, reason = is_trading_day()
                        if not is_trading:
                            log.debug(f"Вне торгов: {reason}")
                            await asyncio.sleep(3600)  # Спим час
                            continue

                        # === Дневное обновление в 00:10 ===
                        if (today != self.last_daily_update and
                            now.hour == DAILY_UPDATE_HOUR and
                            now.minute >= DAILY_UPDATE_MINUTE):

                            log.info(f"📊 [{now:%H:%M:%S} МСК] Дневное обновление OI...")
                            instr_count, ins = await self.update_all(session)
                            self.last_daily_update = today
                            self.session_stats['updates'] += 1

                            log.info(f"  ✓ Инструментов: {instr_count}, записей: +{ins}")
                            self.print_stats()

                        # === Проверка пропусков ===
                        if (datetime.now() - self.last_gap_check).seconds >= GAP_CHECK_INTERVAL:
                            await self.check_data_gaps()
                            self.last_gap_check = datetime.now()

                        await asyncio.sleep(60)

            except aiohttp.ClientError as e:
                retry_count += 1
                log.error(f"❌ Ошибка сети: {e}")
                log.warning(f"  Переподключение {retry_count}/{max_retries}...")

                if retry_count >= max_retries:
                    log.critical("❌ Превышено число попыток!")
                    self.print_stats()
                    raise

                await asyncio.sleep(30 * retry_count)

            except KeyboardInterrupt:
                log.info("🛑 Остановка...")
                self.print_stats()
                break

            except Exception as e:
                log.critical(f"❌ Критическая ошибка: {e}", exc_info=True)
                self.session_stats['errors'] += 1
                await asyncio.sleep(60)


# ======================================================================
#                         ENTRY POINT
# ======================================================================

async def main():
    import argparse

    parser = argparse.ArgumentParser(description='Загрузка дневных OI через ISS MOEX')
    parser.add_argument('--once', action='store_true', help='Однократное обновление')
    parser.add_argument('--force', action='store_true', help='Игнорировать проверку торгового дня')
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("ЗАПУСК СКРИПТА OI (дневной) - ISS MOEX")
    log.info(f"Время: {datetime.now()}")
    log.info(f"МСК: {get_moscow_time()}")
    log.info(f"Режим: {'однократный' if args.once else 'daemon'}")
    log.info("=" * 60)

    # === Проверка торгового дня в режиме --once ===
    if args.once and not args.force:
        is_trading, reason = is_trading_day()
        if not is_trading:
            log.info(f"⏭️ Пропуск: {reason}")
            log.info("  (используйте --force для принудительного запуска)")
            return

    try:
        updater = OIDailyUpdater(DB_URL)

        if args.once:
            # Однократное обновление
            connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT)
            async with aiohttp.ClientSession(connector=connector) as session:
                instr_count, ins = await updater.update_all(session)
                log.info(f"✓ Инструментов: {instr_count}, записей: +{ins}")
        else:
            # Режим daemon
            await updater.run_forever()

    except Exception as e:
        log.critical(f"❌ Фатальная ошибка: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Завершено")
    except Exception as e:
        log.critical(f"Ошибка: {e}")
        sys.exit(1)