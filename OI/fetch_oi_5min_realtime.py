#!/usr/bin/env python3
"""
Реалтайм обновление Open Interest (5 мин) через Algopack API.

Источник: Algopack API
Тикеров: 65
Интервал: 5 минут

Расписание (МСК):
- Каждые 5 минут: XX:00:30, XX:05:30, XX:10:30...

Торговые часы: 07:00-24:00 МСК, Пн-Пт (кроме праздников MOEX)

При старте: докачка с последней даты в БД

Endpoint:
GET https://apim.moex.com/iss/analyticalproducts/futoi/securities/{ticker}.json
"""

import asyncio
import json
import aiohttp
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
from typing import Optional, List, Tuple
import logging
import sys
from pathlib import Path

# === Импорт из корневой папки ===
from dotenv import load_dotenv
import os

sys.path.insert(0, str(Path(__file__).parent.parent))
load_dotenv(Path(__file__).parent.parent / ".env")

DB_URL = os.getenv("DB_URL")
ALGOPACK_API_KEY = os.getenv("ALGOPACK_API_KEY")

# Цепочка Минцифры: apim.moex.com живёт на Russian Trusted Root CA, которого
# нет в certifi. См. moex_tls.py.
from moex_tls import moex_ssl_context

try:
    from moex_calendar import (
        get_moscow_time,
        is_trading_hours,
        is_trading_day,
        is_holiday,
        TRADING_START_HOUR,
        TRADING_END_HOUR
    )
except ImportError as e:
    print(f"❌ Ошибка импорта: {e}")
    print("Убедитесь, что moex_calendar.py находится в корневой папке MOEX/")
    sys.exit(1)


# === ⚙️ НАСТРОЙКИ ===

# Параллельность (backfill — щадящий режим, realtime — побыстрее)
MAX_CONCURRENT_BACKFILL = 3
MAX_CONCURRENT_REALTIME = 10

# Буфер после закрытия 5-минутки (секунды)
BUFFER_SECONDS = 30

# Проверка пропусков (секунды)
GAP_CHECK_INTERVAL = 3600

# Директория логов
LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

# 65 тикеров Algopack OI
ALGOPACK_OI_TICKERS = [
    "CR", "CNYRUBF", "Si", "Eu", "IB", "VB", "USDRUBF", "GZ", "IMOEXF", "RB",
    "CC", "GL", "GLDRUBF", "NA", "NR", "ED", "GK", "SV", "SS", "X5",
    "MX", "MM", "NG", "GD", "SR", "SF", "GAZPF", "MN", "YD", "BR",
    "SE", "TN", "PT", "AF", "KC", "FF", "AL", "EURRUBF", "SBERF", "CE",
    "HS", "NK", "RI", "RL", "LK", "UC", "PD", "NM", "MC", "RM",
    "RN", "SP", "SN", "ME", "HY", "BM", "TT", "OJ", "MG", "W4",
    "DX", "CH", "MY", "VI", "AU",
]


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
        LOG_DIR / f"oi_5min_{datetime.now():%Y%m%d}.log",
        encoding='utf-8'
    )
    fh_all.setLevel(logging.DEBUG)
    fh_all.setFormatter(detailed_fmt)
    root.addHandler(fh_all)

    # Файл ошибок (WARNING+)
    fh_err = logging.FileHandler(
        LOG_DIR / f"oi_5min_errors_{datetime.now():%Y%m%d}.log",
        encoding='utf-8'
    )
    fh_err.setLevel(logging.WARNING)
    fh_err.setFormatter(detailed_fmt)
    root.addHandler(fh_err)

    return logging.getLogger(__name__)


log = setup_logging()


# ======================================================================
#                    ALGOPACK OI FETCHER
# ======================================================================

class AlgopackOIFetcher:
    """Загрузчик Open Interest через Algopack API"""

    BASE_URL = "https://apim.moex.com/iss/analyticalproducts/futoi/securities"

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.headers = {'Authorization': f'Bearer {api_key}'}

        self.stats = {
            'requests': 0,
            'success': 0,
            'errors': 0,
            'empty': 0,
            # Отдельный счётчик для 401 — сигнализирует о проблеме с API-ключом
            # (expired/revoked). main() читает его и завершается с exit-1, чтобы
            # orchestrator увидел реальную ошибку, а не silent ✓ при 0 inserted.
            'auth_failures': 0,
        }

    def get_stats(self) -> dict:
        return self.stats.copy()

    def reset_stats(self):
        for k in self.stats:
            self.stats[k] = 0

    async def fetch_oi(
        self,
        session: aiohttp.ClientSession,
        ticker: str,
        from_date: str,
        till_date: str,
        latest: bool = False
    ) -> Optional[List[dict]]:
        """
        Загружает OI для одного тикера.
        Algopack futoi НЕ поддерживает пагинацию через start —
        поэтому разбиваем запросы по дням.

        Returns: список записей или None
        """
        import time as _time

        url = f"{self.BASE_URL}/{ticker}.json"

        # Для latest — один запрос
        if latest:
            return await self._fetch_oi_single(session, ticker, url, from_date, till_date, latest=True)

        # Разбиваем диапазон по дням
        from datetime import date
        d_from = datetime.strptime(from_date, '%Y-%m-%d').date()
        d_till = datetime.strptime(till_date, '%Y-%m-%d').date()

        all_records: List[dict] = []
        t_start = _time.time()
        current = d_from

        while current <= d_till:
            day_str = current.strftime('%Y-%m-%d')
            day_records = await self._fetch_oi_single(session, ticker, url, day_str, day_str)
            if day_records:
                all_records.extend(day_records)
            current += timedelta(days=1)
            await asyncio.sleep(0.05)

        if all_records:
            elapsed = _time.time() - t_start
            log.info(f"  [{ticker}] {len(all_records)} records, {(d_till - d_from).days + 1} days ({elapsed:.1f}s)")
        return all_records if all_records else None

    async def _fetch_oi_single(
        self,
        session: aiohttp.ClientSession,
        ticker: str,
        url: str,
        from_date: str,
        till_date: str,
        latest: bool = False
    ) -> Optional[List[dict]]:
        """Один запрос к API (один день или latest)"""
        params = {'from': from_date, 'till': till_date}
        if latest:
            params['latest'] = 1

        self.stats['requests'] += 1

        for attempt in range(3):
            try:
                async with session.get(
                    url,
                    headers=self.headers,
                    params=params,
                    timeout=aiohttp.ClientTimeout(total=45)
                ) as resp:

                    if resp.status == 401:
                        log.error(f"[{ticker}] Ошибка авторизации (401)")
                        self.stats['errors'] += 1
                        self.stats['auth_failures'] += 1
                        return None

                    if resp.status == 404:
                        self.stats['empty'] += 1
                        return None

                    if resp.status == 429:
                        log.warning(f"[{ticker}] Лимит запросов (429), ждём 60 сек...")
                        await asyncio.sleep(60)
                        continue

                    if resp.status != 200:
                        if resp.status >= 500:
                            log.warning(f"[{ticker}] Ошибка сервера {resp.status}, попытка {attempt+1}/3")
                            await asyncio.sleep(2)
                            continue
                        else:
                            log.warning(f"[{ticker}] HTTP {resp.status}")
                            self.stats['errors'] += 1
                            return None

                    try:
                        data = await resp.json()
                    except Exception as json_err:
                        log.error(f"[{ticker}] Ошибка парсинга JSON: {json_err}")
                        self.stats['errors'] += 1
                        return None

                    if "futoi" not in data:
                        self.stats['empty'] += 1
                        return None

                    futoi = data["futoi"]
                    columns = futoi.get("columns", [])
                    rows = futoi.get("data", [])
                    self.stats['success'] += 1
                    return [dict(zip(columns, row)) for row in rows]

            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                if attempt < 2:
                    await asyncio.sleep(1 + attempt)
                    continue
                log.error(f"[{ticker}] Ошибка сети после 3 попыток: {e}")
                self.stats['errors'] += 1
                return None
            except Exception as e:
                log.error(f"[{ticker}] Неизвестная ошибка: {e}")
                self.stats['errors'] += 1
                return None
        return None


# ======================================================================
#                    OI 5MIN UPDATER
# ======================================================================

class OI5minUpdater:
    """Обновление 5-минутных данных OI"""

    def __init__(self, db_url: str, api_key: str):
        log.info("Инициализация OI5minUpdater...")

        try:
            self.engine = create_engine(db_url, connect_args={"ssl_context": False})
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            log.info("✓ БД подключена")
        except Exception as e:
            log.critical(f"Ошибка подключения к БД: {e}")
            raise

        self.fetcher = AlgopackOIFetcher(api_key)
        self.tickers = ALGOPACK_OI_TICKERS

        self.last_5min_update = None
        self.last_gap_check = None

        self.session_stats = {
            'start_time': datetime.now(),
            'cycles': 0,
            'records_saved': 0,
            'errors': 0
        }

        log.info(f"✓ Тикеров: {len(self.tickers)}")
        log.info("✓ OI5minUpdater готов")

    def get_last_datetime(self, sectype: str) -> Optional[datetime]:
        """Последняя дата+время OI для тикера"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT MAX(tradedate + tradetime) as last_dt
                    FROM open_interest
                    WHERE sectype = :sectype AND interval = 5
                """), {'sectype': sectype})
                row = result.fetchone()
                return row[0] if row and row[0] else None
        except Exception as e:
            log.error(f"Ошибка получения последней даты: {e}")
            return None

    @staticmethod
    def _is_non_trading_date(d) -> bool:
        """Дата, за которую OI хранить НЕ нужно. ТОЛЬКО праздники MOEX.

        Выходные (сб/вс) ТЕПЕРЬ могут быть торговыми (торги выходного дня MOEX) —
        НЕ фильтруем их по weekday, иначе потеряем futoi выходных сессий.
        Атрибуция даты: храним по фактической `tradedate` (=суббота); поле
        `trade_session_date` (=след. пн) НЕ используем — так фьючерсная свеча и OI
        выравниваются по одной календарной дате (signals/features.py,
        api/routers/chart.py джойнят по date()). В неторговый день источник и так
        отдаёт пусто, поэтому фильтр праздников — лёгкая подстраховка."""
        if isinstance(d, str):
            d = datetime.strptime(d, '%Y-%m-%d').date()
        if hasattr(d, 'date'):
            d = d.date()
        return is_holiday(d)

    def save_records(self, records: List[dict]) -> Tuple[int, int]:
        """
        Сохраняет записи OI в БД.
        Фильтрует выходные и праздники MOEX.

        Returns: (попыток, вставлено)
        """
        if not records:
            return 0, 0

        # Отфильтровываем выходные и праздники
        records = [r for r in records if not self._is_non_trading_date(r.get('tradedate'))]
        if not records:
            return 0, 0

        try:
            raw_conn = self.engine.raw_connection()
            cursor = raw_conn.cursor()

            inserted = 0

            def to_int(v):
                if v is None or v == "":
                    return 0
                if isinstance(v, bool):
                    return int(v)
                if isinstance(v, (int, float)):
                    return int(v)
                try:
                    return int(float(str(v).replace(',', '.')))
                except Exception:
                    return 0

            # Подготовка всех значений
            values_list = []
            for rec in records:
                try:
                    tradedate = rec.get('tradedate')
                    if isinstance(tradedate, str):
                        tradedate = datetime.strptime(tradedate, '%Y-%m-%d').date()

                    values_list.append((
                        rec.get('ticker'),
                        tradedate,
                        rec.get('tradetime'),
                        rec.get('clgroup'),
                        to_int(rec.get('pos', 0)),
                        to_int(rec.get('pos_long', 0)),
                        to_int(rec.get('pos_short', 0)),
                        to_int(rec.get('pos_long_num', 0)),
                        to_int(rec.get('pos_short_num', 0)),
                        rec.get('systime'),
                    ))
                except Exception as e:
                    log.debug(f"Ошибка подготовки записи: {e}")

            # Batch INSERT по 500 записей
            BATCH_SIZE = 500
            for i in range(0, len(values_list), BATCH_SIZE):
                batch = values_list[i:i + BATCH_SIZE]
                try:
                    placeholders = ','.join(
                        ['(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,5)'] * len(batch)
                    )
                    flat_values = [v for row in batch for v in row]
                    cursor.execute(f"""
                        INSERT INTO open_interest
                        (sectype, tradedate, tradetime, clgroup, pos, pos_long, pos_short,
                         pos_long_num, pos_short_num, systime, interval)
                        VALUES {placeholders}
                        ON CONFLICT (sectype, tradedate, tradetime, clgroup, interval) DO NOTHING
                    """, flat_values)
                    inserted += cursor.rowcount
                    raw_conn.commit()
                except Exception as e:
                    log.warning(f"Batch insert ошибка, fallback на row-by-row: {e}")
                    try:
                        raw_conn.rollback()
                    except Exception:
                        pass
                    # Fallback: вставляем по одной
                    for row in batch:
                        try:
                            cursor.execute("""
                                INSERT INTO open_interest
                                (sectype, tradedate, tradetime, clgroup, pos, pos_long, pos_short,
                                 pos_long_num, pos_short_num, systime, interval)
                                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,5)
                                ON CONFLICT (sectype, tradedate, tradetime, clgroup, interval) DO NOTHING
                            """, row)
                            inserted += cursor.rowcount
                        except Exception as row_e:
                            try:
                                raw_conn.rollback()
                            except Exception:
                                pass
                            log.debug(f"Ошибка вставки строки: {row_e}")
                    raw_conn.commit()

            cursor.close()
            raw_conn.close()

            log.info(f"Batch insert: {inserted}/{len(values_list)} записей")
            return len(records), inserted

        except Exception as e:
            log.error(f"Ошибка сохранения: {e}")
            self.session_stats['errors'] += 1
            return 0, 0

    def _fill_gaps(self, records: List[dict], ticker: str) -> List[dict]:
        """Заполняет пропуски в данных (forward fill)"""
        if not records:
            return []
            
        try:
            # Используем pandas для удобной работы с временными рядами
            import pandas as pd
            
            df = pd.DataFrame(records)
            
            # Собираем datetime колонку
            df['ts'] = pd.to_datetime(df['tradedate'].astype(str) + ' ' + df['tradetime'].astype(str))
            
            # Разделяем по группам (FIZ/YUR)
            filled_records = []
            
            for clgroup in df['clgroup'].unique():
                group_df = df[df['clgroup'] == clgroup].copy()
                group_df = group_df.sort_values('ts').set_index('ts')
                
                # Создаем полный индекс (5 мин)
                # Берем начало и конец из данных + округляем до 5 мин
                start_ts = group_df.index.min().floor('5min')
                end_ts = group_df.index.max().floor('5min')
                
                full_range = pd.date_range(start=start_ts, end=end_ts, freq='5min')
                # Только торговые часы MOEX (07:00–23:50)
                full_range = full_range[full_range.map(lambda t: 7 <= t.hour <= 23)]

                # Реиндексация
                group_df = group_df.reindex(full_range)
                
                # Forward Fill значений
                cols_to_fill = ['pos', 'pos_long', 'pos_short', 'pos_long_num', 'pos_short_num']
                group_df[cols_to_fill] = group_df[cols_to_fill].ffill(limit=6)  # макс 30 мин (6 интервалов)
                
                # Заполняем метаданные
                group_df['ticker'] = ticker
                group_df['clgroup'] = clgroup
                group_df['sectype'] = ticker # на всякий случай
                
                # Восстанавливаем tradedate/tradetime
                group_df = group_df.reset_index().rename(columns={'index': 'ts'})
                group_df = group_df.dropna(subset=['pos']) # Удаляем начальные NaN если есть
                
                group_df['tradedate'] = group_df['ts'].dt.date
                group_df['tradetime'] = group_df['ts'].dt.time
                
                # Заполняем systime текущим временем для новых записей
                group_df['systime'] = group_df['systime'].fillna(datetime.now().isoformat())
                
                # Обратно в dict
                recs = group_df.to_dict('records')
                # date/time объекты могут не сериализоваться, но save_records принимает их нормально
                filled_records.extend(recs)
                
            return filled_records
            
        except ImportError:
            log.warning("Pandas не установлен, пропуск заполнения гэпов")
            return records
        except Exception as e:
            log.debug(f"Пропуск fill_gaps для {ticker}: {e}")
            return records

    async def update_ticker(
        self,
        session: aiohttp.ClientSession,
        ticker: str,
        from_date: str,
        till_date: str
    ) -> Tuple[int, int]:
        """Обновляет OI для одного тикера"""
        import time

        t0 = time.time()
        records = await self.fetcher.fetch_oi(session, ticker, from_date, till_date)
        t_fetch = time.time() - t0

        if not records:
            return 0, 0

        # Фильтруем: оставляем только записи НОВЕЕ last_datetime (как в Candles)
        last_dt = await asyncio.to_thread(self.get_last_datetime, ticker)
        if last_dt:
            before_filter = len(records)
            records = [
                r for r in records
                if datetime.strptime(f"{r['tradedate']} {r['tradetime']}", '%Y-%m-%d %H:%M:%S') > last_dt
            ]
            log.info(f"  [{ticker}] filtered {before_filter}→{len(records)} (after {last_dt})")
            if not records:
                return 0, 0

        t1 = time.time()
        filled_records = self._fill_gaps(records, ticker)
        t_gap = time.time() - t1

        t2 = time.time()
        result = await asyncio.to_thread(self.save_records, filled_records)
        t_save = time.time() - t2

        log.info(f"  [{ticker}] fetch={t_fetch:.1f}s gap_fill={t_gap:.1f}s save={t_save:.1f}s records={len(records)}→{len(filled_records)}")
        return result

    async def update_all(
        self,
        session: aiohttp.ClientSession,
        from_date: str = None,
        till_date: str = None
    ) -> Tuple[int, int]:
        """Обновляет OI для всех тикеров"""

        now = get_moscow_time()

        if till_date is None:
            till_date = now.strftime('%Y-%m-%d')

        total_attempted = 0
        total_inserted = 0

        semaphore = asyncio.Semaphore(MAX_CONCURRENT_BACKFILL)

        async def process(ticker):
            async with semaphore:
                # Определяем from_date для каждого тикера
                if from_date:
                    ticker_from = from_date
                else:
                    last_dt = await asyncio.to_thread(self.get_last_datetime, ticker)
                    if last_dt:
                        ticker_from = (last_dt - timedelta(days=1)).strftime('%Y-%m-%d')
                    else:
                        ticker_from = (now - timedelta(days=7)).strftime('%Y-%m-%d')

                return await self.update_ticker(session, ticker, ticker_from, till_date)

        tasks = [process(ticker) for ticker in self.tickers]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for r in results:
            if isinstance(r, tuple):
                total_attempted += r[0]
                total_inserted += r[1]
            elif isinstance(r, Exception):
                log.error(f"Ошибка: {r}")

        self.session_stats['records_saved'] += total_inserted
        return total_attempted, total_inserted

    async def update_latest(self, session: aiohttp.ClientSession) -> Tuple[int, int]:
        """Загружает только последнюю 5-минутку"""

        now = get_moscow_time()
        today = now.strftime('%Y-%m-%d')

        total_attempted = 0
        total_inserted = 0

        semaphore = asyncio.Semaphore(MAX_CONCURRENT_REALTIME)

        async def process(ticker):
            async with semaphore:
                records = await self.fetcher.fetch_oi(
                    session, ticker, today, today, latest=True
                )
                if records:
                    return self.save_records(records)
                return 0, 0

        tasks = [process(ticker) for ticker in self.tickers]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for r in results:
            if isinstance(r, tuple):
                total_attempted += r[0]
                total_inserted += r[1]

        self.session_stats['records_saved'] += total_inserted
        return total_attempted, total_inserted

    async def check_data_gaps(self):
        """Проверяет пропуски в данных"""
        log.info("🔍 Проверка пропусков OI...")

        now = get_moscow_time()
        gaps = []

        for ticker in self.tickers[:20]:
            last_dt = self.get_last_datetime(ticker)

            if last_dt is None:
                gaps.append(f"{ticker}: НЕТ ДАННЫХ")
            elif (now - last_dt) > timedelta(hours=2):
                gap = now - last_dt
                gaps.append(f"{ticker}: {gap}")

        if gaps:
            log.warning(f"⚠️ Пропуски ({len(gaps)}):")
            for g in gaps[:5]:
                log.warning(f"  {g}")
        else:
            log.info("✓ Пропусков нет")

    def _get_5min_slot(self) -> datetime:
        now = get_moscow_time()
        return now.replace(second=0, microsecond=0, minute=(now.minute // 5) * 5)

    def print_stats(self):
        """Статистика сессии"""
        uptime = datetime.now() - self.session_stats['start_time']
        api = self.fetcher.get_stats()

        log.info("=" * 50)
        log.info("📊 СТАТИСТИКА OI (5 мин)")
        log.info(f"  Время работы: {uptime}")
        log.info(f"  Циклов: {self.session_stats['cycles']}")
        log.info(f"  Записей сохранено: {self.session_stats['records_saved']}")
        log.info(f"  Ошибок: {self.session_stats['errors']}")
        log.info(f"  API: {api['requests']} запросов, {api['success']} успешных, {api['errors']} ошибок")
        log.info("=" * 50)

    async def run_forever(self):
        """Основной цикл"""

        log.info("=" * 60)
        log.info("🚀 ЗАПУСК РЕАЛТАЙМ ОБНОВЛЕНИЯ OI (5 мин)")
        log.info(f"  API: Algopack")
        log.info(f"  Тикеров: {len(self.tickers)}")
        log.info(f"  Параллельность: backfill={MAX_CONCURRENT_BACKFILL}, realtime={MAX_CONCURRENT_REALTIME}")
        log.info(f"  Торговые часы: {TRADING_START_HOUR}:00 - {TRADING_END_HOUR}:00 МСК")
        log.info(f"  Расписание: XX:00:30, XX:05:30...")
        log.info(f"  Логи: {LOG_DIR.absolute()}")
        log.info("=" * 60)

        connector = aiohttp.TCPConnector(
            limit=MAX_CONCURRENT_REALTIME, ssl=moex_ssl_context()
        )
        timeout = aiohttp.ClientTimeout(total=120)

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
                    att, ins = await self.update_all(session)
                    log.info(f"  ✓ Загружено: {att}, сохранено: +{ins}")

                    self.last_5min_update = self._get_5min_slot()
                    self.last_gap_check = datetime.now()

                    log.info("✅ Синхронизация завершена")
                    log.info("=" * 60)

                    retry_count = 0

                    # === Основной цикл ===
                    while True:
                        now = get_moscow_time()

                        is_trading, reason = is_trading_hours()
                        if not is_trading:
                            log.debug(f"Вне торгов: {reason}")
                            await asyncio.sleep(60)
                            continue

                        slot = self._get_5min_slot()

                        # Обновляем каждые 5 минут
                        if slot != self.last_5min_update and now.second >= BUFFER_SECONDS:
                            log.info(f"📊 [{now:%H:%M:%S} МСК] Обновление OI...")
                            att, ins = await self.update_latest(session)
                            self.last_5min_update = slot
                            self.session_stats['cycles'] += 1

                            log.info(f"  ✓ +{ins} новых")

                        # Проверка пропусков
                        if (datetime.now() - self.last_gap_check).seconds >= GAP_CHECK_INTERVAL:
                            await self.check_data_gaps()
                            self.last_gap_check = datetime.now()

                        await asyncio.sleep(1)

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

    parser = argparse.ArgumentParser(description='Загрузка OI 5-минутных данных через Algopack')
    parser.add_argument('--once', action='store_true', help='Однократное обновление')
    parser.add_argument('--force', action='store_true', help='Игнорировать проверку торговых часов')
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("ЗАПУСК СКРИПТА OI (5 минут) - ALGOPACK")
    log.info(f"Время: {datetime.now()}")
    log.info(f"МСК: {get_moscow_time()}")
    log.info(f"Режим: {'однократный' if args.once else 'daemon'}")
    log.info("=" * 60)

    # === Проверка торгового дня в режиме --once ===
    if args.once and not args.force:
        is_trading, reason = is_trading_day()
        if not is_trading:
            log.info(f"⏭️ Пропуск: {reason}")
            print(json.dumps({"пропуск": reason}, ensure_ascii=False))
            log.info("  (используйте --force для принудительного запуска)")
            return

    try:
        updater = OI5minUpdater(DB_URL, ALGOPACK_API_KEY)

        if args.once:
            # Однократное обновление
            connector = aiohttp.TCPConnector(
                limit=MAX_CONCURRENT_BACKFILL, ssl=moex_ssl_context()
            )
            async with aiohttp.ClientSession(connector=connector) as session:
                att, ins = await updater.update_all(session)
                log.info(f"✓ Загружено: {att}, сохранено: +{ins}")

            # Hard fail на 401: если хоть один запрос вернул unauthorized,
            # значит API-ключ Algopack expired/revoked. Без этого блока скрипт
            # завершается с exit-0, orchestrator видит "✓ OI 5м (6.7с)" и
            # silent failure длится дни (так и было: 2 дня без OI данных,
            # пока коллега не заметил визуально). С exit-1 orchestrator
            # залогирует "✗ OI 5м: ..." и проблему видно сразу.
            auth_fails = updater.fetcher.stats.get('auth_failures', 0)
            if auth_fails > 0:
                log.critical(
                    f"❌ Algopack 401 Unauthorized — API-ключ expired/revoked "
                    f"({auth_fails} запросов). Обнови ALGOPACK_API_KEY и перезапусти orchestrator."
                )
                sys.exit(1)
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