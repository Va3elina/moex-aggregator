#!/usr/bin/env python3
"""
Реалтайм обновление свечей АКЦИЙ через Algopack API.

Таймфреймы:
- 5 минут (агрегация из 1-минутных)
- 60 минут
- 24 часа (дневные)

Расписание (МСК):
- 5м: каждые 5 минут (XX:00:10, XX:05:10...)
- 60м: каждый час (XX:00:15)
- 24ч: в полночь (00:01:00)

Торговые часы: 10:00-18:50 МСК (основная сессия)
               19:05-23:50 МСК (вечерняя сессия)

Режимы запуска:
- daemon (по умолчанию): работает постоянно
- --once: однократное обновление всех таймфреймов
- --force: игнорировать проверку торгового дня
"""

import asyncio
import aiohttp
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Tuple
import logging
import sys
from pathlib import Path

# === Импорт из корневой папки ===
from dotenv import load_dotenv
import os

sys.path.insert(0, str(Path(__file__).parent.parent))
load_dotenv(Path(__file__).parent.parent / ".env")

DB_URL = os.getenv("DB_URL", "postgresql+pg8000://postgres:1803@localhost:5432/moex_db")
ALGOPACK_API_KEY = os.getenv("ALGOPACK_API_KEY", "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJaVHA2Tjg1ekE4YTBFVDZ5SFBTajJ2V0ZldzNOc2xiSVR2bnVaYWlSNS1NIn0.eyJleHAiOjE3NjkwMTUxNTYsImlhdCI6MTc2NjQyMzE1NiwianRpIjoiZjBjODFmNDEtZTE3NC00NmRlLWIwMGUtZjAzZGQxY2I2YjhmIiwiaXNzIjoiaHR0cHM6Ly9zc28yLm1vZXguY29tL2F1dGgvcmVhbG1zL2NyYW1sIiwiYXVkIjpbImFjY291bnQiLCJpc3MiXSwic3ViIjoiZjowYmE2YThmMC1jMzhhLTQ5ZDYtYmEwZS04NTZmMWZlNGJmN2U6ZmI2YzRhMTMtMmEyOS00Nzk5LTljZTYtNDQyMTJkN2I5N2UzIiwidHlwIjoiQmVhcmVyIiwiYXpwIjoiaXNzIiwic2lkIjoiYmM0ZDYwMTYtYTg4MS00MDc2LThlNGEtNzY3NzAyMGI4NzkyIiwiYWNyIjoiMSIsImFsbG93ZWQtb3JpZ2lucyI6WyIvKiJdLCJyZWFsbV9hY2Nlc3MiOnsicm9sZXMiOlsib2ZmbGluZV9hY2Nlc3MiLCJ1bWFfYXV0aG9yaXphdGlvbiJdfSwicmVzb3VyY2VfYWNjZXNzIjp7ImFjY291bnQiOnsicm9sZXMiOlsibWFuYWdlLWFjY291bnQiLCJ2aWV3LXByb2ZpbGUiXX19LCJzY29wZSI6Im9wZW5pZCBpc3NfYWxnb3BhY2sgcHJvZmlsZSBvZmZsaW5lX2FjY2VzcyBlbWFpbCBiYWNrd2FyZHNfY29tcGF0aWJsZSIsImVtYWlsX3ZlcmlmaWVkIjpmYWxzZSwiaXNzX3Blcm1pc3Npb25zIjoiMTM3LCAxMzgsIDEzOSwgMTQwLCAxNjUsIDE2NiwgMTY3LCAxNjgsIDMyOSwgNDIxIiwibmFtZSI6ItCQ0LvQtdC60YHQsNC90LTRgCDQotC-0YDQuNGPIiwicHJlZmVycmVkX3VzZXJuYW1lIjoiZmI2YzRhMTMtMmEyOS00Nzk5LTljZTYtNDQyMTJkN2I5N2UzIiwiZ2l2ZW5fbmFtZSI6ItCQ0LvQtdC60YHQsNC90LTRgCIsInNlc3Npb25fc3RhdGUiOiJiYzRkNjAxNi1hODgxLTQwNzYtOGU0YS03Njc3MDIwYjg3OTIiLCJmYW1pbHlfbmFtZSI6ItCi0L7RgNC40Y8ifQ.ht68EDUCuDP_dweBnZalCQlwrkyEXtzfCxRwkO3V6H0zHtveqHh7S0AqIs2KDo57IepE83P20H2aZqWIHHOHlk66DhMn0EDu2V6CJLKHV8InWaoW_uKoinni1tND1b829VcnP5Bd2AdgHif8EWuUOg78P4u7EiRApf1CTMpVg_s2WKdIRmMdRSEFOlWi52oG5uYjqNdGsAT7J-HTzoSPqfQWiRKArnNp_tfPqB2lFkO2-hQgyx79c0ltQ4fQ2PtLyJxC4w25_R8bArpUrhUwvL8XhG4rlfRdC12RTdzJgvNptI_imm0LDgDe4km9oTYWYUn1av5HVW1Wg3sTvMkZcA")

try:
    from moex_calendar import (
        get_moscow_time,
        is_trading_hours,
        is_trading_day,
        TRADING_START_HOUR,
        TRADING_END_HOUR
    )
    CONFIG_LOADED = True
except ImportError as e:
    print(f"⚠️ Ошибка импорта из корня: {e}")
    print("Используем локальные настройки...")
    CONFIG_LOADED = False

    # Fallback настройки
    TRADING_START_HOUR = 7
    TRADING_END_HOUR = 24
    TRADING_DAYS = [0, 1, 2, 3, 4]

    def get_moscow_time() -> datetime:
        try:
            import pytz
            msk = pytz.timezone('Europe/Moscow')
            return datetime.now(msk).replace(tzinfo=None)
        except ImportError:
            return datetime.utcnow() + timedelta(hours=3)

    def is_trading_hours() -> Tuple[bool, str]:
        now = get_moscow_time()
        if now.weekday() not in TRADING_DAYS:
            return False, f"Выходной ({now.strftime('%A')})"
        if now.hour < TRADING_START_HOUR:
            return False, f"До начала торгов ({TRADING_START_HOUR}:00 МСК)"
        if now.hour >= TRADING_END_HOUR:
            return False, "Торги завершены"
        # Перерыв между сессиями 18:50-19:05
        if now.hour == 18 and now.minute >= 50:
            return False, "Перерыв (18:50-19:05)"
        if now.hour == 19 and now.minute < 5:
            return False, "Перерыв (18:50-19:05)"
        return True, "Торговая сессия"

    def is_trading_day(check_date=None) -> Tuple[bool, str]:
        check = check_date or get_moscow_time().date()
        if check.weekday() not in TRADING_DAYS:
            return False, "Выходной"
        return True, "Торговый день"


# === ⚙️ НАСТРОЙКИ ===

# Параллельность
MAX_CONCURRENT_FETCH = 10  # было 20, снижено для экономии CPU

# Буфер после закрытия свечи (секунды)
BUFFER_SECONDS = 10

# Проверка пропусков (секунды)
GAP_CHECK_INTERVAL = 3600

# Директория логов
LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)


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
        LOG_DIR / f"candles_stocks_{datetime.now():%Y%m%d}.log",
        encoding='utf-8'
    )
    fh_all.setLevel(logging.DEBUG)
    fh_all.setFormatter(detailed_fmt)
    root.addHandler(fh_all)

    # Файл ошибок (WARNING+)
    fh_err = logging.FileHandler(
        LOG_DIR / f"candles_stocks_errors_{datetime.now():%Y%m%d}.log",
        encoding='utf-8'
    )
    fh_err.setLevel(logging.WARNING)
    fh_err.setFormatter(detailed_fmt)
    root.addHandler(fh_err)

    return logging.getLogger(__name__)


log = setup_logging()


# ======================================================================
#                    STOCKS MANAGER
# ======================================================================

class StocksManager:
    """
    Простой менеджер акций.
    Загружает список тикеров из БД — без ротации.
    """

    def __init__(self, engine):
        self.engine = engine
        self._cache = []
        self._cache_time = None
        self._cache_ttl = timedelta(hours=24)  # Обновляем раз в день

        log.debug("StocksManager инициализирован")

    def load_stocks(self) -> List[Tuple[str, str]]:
        """
        Загружает список акций из БД.

        Returns: [(secid, name), ...]
        """
        now = datetime.now()

        # Проверяем кэш
        if self._cache_time and (now - self._cache_time) < self._cache_ttl and self._cache:
            log.debug(f"Кэш акций: {len(self._cache)} шт")
            return self._cache

        log.info("📋 Загрузка списка акций из БД...")

        try:
            with self.engine.connect() as conn:
                df = pd.read_sql(text("""
                                      SELECT sec_id, name
                                      FROM instruments
                                      WHERE type = 'stock'
                                      ORDER BY sec_id
                                      """), conn)

            stocks = [(row['sec_id'], row['name']) for _, row in df.iterrows()]

            # Обновляем кэш
            self._cache = stocks
            self._cache_time = now

            log.info(f"✓ Загружено {len(stocks)} акций")
            return stocks

        except Exception as e:
            log.error(f"Ошибка загрузки акций: {e}")
            return self._cache if self._cache else []

    def invalidate_cache(self):
        """Сброс кэша"""
        self._cache = []
        self._cache_time = None


# ======================================================================
#                    ALGOPACK FETCHER (для акций)
# ======================================================================

class AlgopackStocksFetcher:
    """Загрузчик свечей акций через Algopack API"""

    BASE_URL = "https://apim.moex.com/iss/engines/stock/markets/shares/boards/tqbr/securities"

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.headers = {'Authorization': f'Bearer {api_key}'}

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

    async def fetch_candles(
            self,
            session: aiohttp.ClientSession,
            ticker: str,
            interval: int,
            from_date: str,
            till_date: str
    ) -> Optional[pd.DataFrame]:
        """Загружает свечи с пагинацией"""

        url = f"{self.BASE_URL}/{ticker}/candles.json"
        params = {
            'interval': interval,
            'from': from_date,
            'till': till_date
        }

        all_candles = []
        columns = None
        start = 0

        while True:
            params['start'] = start
            self.stats['requests'] += 1

            try:
                async with session.get(
                        url,
                        headers=self.headers,
                        params=params,
                        timeout=aiohttp.ClientTimeout(total=30)
                ) as resp:

                    if resp.status == 401:
                        log.error(f"[{ticker}] Ошибка авторизации (401)")
                        self.stats['errors'] += 1
                        return None

                    if resp.status == 429:
                        log.warning(f"[{ticker}] Лимит запросов (429), ждём 60 сек...")
                        await asyncio.sleep(60)
                        continue

                    if resp.status != 200:
                        if resp.status == 500:
                            log.error(f"[{ticker}] Ошибка сервера (500). MOEX API временно недоступен.")
                        elif resp.status == 502:
                            log.error(f"[{ticker}] Bad Gateway (502). Проблемы с MOEX.")
                        elif resp.status == 503:
                            log.error(f"[{ticker}] Сервис недоступен (503). Техработы на MOEX.")
                        else:
                            log.warning(f"[{ticker}] HTTP {resp.status}")
                        self.stats['errors'] += 1
                        return None

                    try:
                        data = await resp.json()
                    except Exception as json_err:
                        log.error(f"[{ticker}] Ошибка парсинга JSON: {json_err}. API изменился?")
                        text = await resp.text()
                        log.debug(f"    Ответ: {text[:300]}")
                        self.stats['errors'] += 1
                        return None

                    if "candles" not in data or not data["candles"].get("data"):
                        if start == 0:
                            self.stats['empty'] += 1
                        break

                    candles = data["candles"]["data"]
                    columns = data["candles"]["columns"]
                    all_candles.extend(candles)

                    if len(candles) < 500:
                        break

                    start += len(candles)
                    await asyncio.sleep(0.05)

            except asyncio.TimeoutError:
                log.error(f"[{ticker}] Таймаут соединения (30с). MOEX API не отвечает.")
                self.stats['errors'] += 1
                return None
            except aiohttp.ClientConnectorError as e:
                log.error(f"[{ticker}] Ошибка соединения: {e}. Проверьте интернет.")
                self.stats['errors'] += 1
                return None
            except aiohttp.ServerDisconnectedError:
                log.error(f"[{ticker}] Сервер разорвал соединение.")
                self.stats['errors'] += 1
                return None
            except Exception as e:
                log.error(f"[{ticker}] Неизвестная ошибка: {type(e).__name__}: {e}")
                log.debug(f"    Traceback:", exc_info=True)
                self.stats['errors'] += 1
                return None

        if not all_candles or not columns:
            return None

        self.stats['success'] += 1

        df = pd.DataFrame(all_candles, columns=columns)
        df["begin_time"] = pd.to_datetime(df["begin"]).dt.tz_localize(None)
        df["end_time"] = pd.to_datetime(df["end"]).dt.tz_localize(None)
        df["interval"] = interval
        df["secid"] = ticker
        df["sec_id"] = ticker  # Для акций secid = sec_id
        df["type"] = "stock"
        df = df.drop(["begin", "end"], axis=1)

        return df


# ======================================================================
#                    АГРЕГАЦИЯ 1М → 5М
# ======================================================================

def aggregate_to_5min(df_1min: pd.DataFrame) -> pd.DataFrame:
    """Агрегирует 1-минутные свечи в 5-минутные"""

    if df_1min.empty:
        return pd.DataFrame()

    df = df_1min.copy().sort_values('begin_time')
    df['interval_5min'] = df['begin_time'].dt.floor('5min')

    secid = df['secid'].iloc[0]
    sec_id = df['sec_id'].iloc[0] if 'sec_id' in df.columns else secid

    agg = {
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum',
        'value': 'sum',
    }

    df_5min = df.groupby('interval_5min').agg(agg).reset_index()
    df_5min.rename(columns={'interval_5min': 'begin_time'}, inplace=True)
    df_5min['end_time'] = df_5min['begin_time'] + pd.Timedelta(minutes=5)
    df_5min['interval'] = 5
    df_5min['secid'] = secid
    df_5min['sec_id'] = sec_id
    df_5min['type'] = 'stock'

    return df_5min


# ======================================================================
#                    CANDLES UPDATER (STOCKS)
# ======================================================================

class StocksCandlesUpdater:
    """Основной класс обновления свечей акций"""

    def __init__(self, db_url: str, api_key: str):
        log.info("Инициализация StocksCandlesUpdater...")

        try:
            self.engine = create_engine(db_url, connect_args={"ssl_context": False})
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            log.info("✓ БД подключена")
        except Exception as e:
            log.critical(f"Ошибка подключения к БД: {e}")
            raise

        self.api_key = api_key
        self.fetcher = AlgopackStocksFetcher(api_key)
        self.stocks_manager = StocksManager(self.engine)

        self.last_5min_update = None
        self.last_60min_update = None
        self.last_daily_update = None
        self.last_weekly_update = None
        self.last_gap_check = None

        self.session_stats = {
            'start_time': datetime.now(),
            'cycles': 0,
            'candles_saved': {5: 0, 60: 0, 24: 0, 7: 0},
            'errors': 0
        }

        log.info("✓ StocksCandlesUpdater готов")

    def get_last_candle_time(self, secid: str, interval: int) -> Optional[datetime]:
        """Время последней свечи в БД"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("""
                                           SELECT MAX(begin_time)
                                           FROM candles
                                           WHERE secid = :secid
                                             AND "interval" = :interval
                                             AND type = 'stock'
                                           """), {'secid': secid, 'interval': interval})
                row = result.fetchone()
                return row[0] if row and row[0] else None
        except Exception as e:
            log.error(f"Ошибка получения последней свечи: {e}")
            return None

    def save_candles(self, df: pd.DataFrame) -> Tuple[int, int]:
        """Сохраняет свечи. Возвращает (попыток, вставлено)"""

        if df.empty:
            return 0, 0

        for col in ['value', 'volume', 'open', 'close', 'high', 'low']:
            if col in df.columns:
                df[col] = df[col].fillna(0)

        try:
            raw_conn = self.engine.raw_connection()
            cursor = raw_conn.cursor()

            columns = [
                'secid', 'begin_time', 'interval', 'type', 'end_time',
                'open', 'close', 'high', 'low', 'value', 'volume', 'sec_id'
            ]

            for col in columns:
                if col not in df.columns:
                    df[col] = None

            records = [tuple(x) for x in df[columns].to_numpy()]

            upsert_suffix = """
                           ON CONFLICT (secid, begin_time, interval, type) DO UPDATE SET
                               open = EXCLUDED.open, close = EXCLUDED.close,
                               high = EXCLUDED.high, low = EXCLUDED.low,
                               value = EXCLUDED.value, volume = EXCLUDED.volume,
                               end_time = EXCLUDED.end_time
                           WHERE EXCLUDED.volume > candles.volume
                           """
            insert_query = """
                           INSERT INTO candles (secid, begin_time, interval, type, end_time,
                                                open, close, high, low, value, volume, sec_id)
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                           """ + upsert_suffix

            inserted = 0
            BATCH_SIZE = 500
            for i in range(0, len(records), BATCH_SIZE):
                batch = records[i:i + BATCH_SIZE]
                try:
                    placeholders = ','.join(
                        ['(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'] * len(batch)
                    )
                    flat_values = [v for row in batch for v in row]
                    cursor.execute(f"""
                        INSERT INTO candles (secid, begin_time, interval, type, end_time,
                                             open, close, high, low, value, volume, sec_id)
                        VALUES {placeholders}
                        {upsert_suffix}
                    """, flat_values)
                    inserted += cursor.rowcount
                    raw_conn.commit()
                except Exception as e:
                    log.warning(f"Batch insert ошибка, fallback на row-by-row: {e}")
                    try:
                        raw_conn.rollback()
                    except Exception:
                        pass
                    for record in batch:
                        try:
                            cursor.execute(insert_query, record)
                            inserted += cursor.rowcount
                        except Exception:
                            try:
                                raw_conn.rollback()
                            except Exception:
                                pass
                    raw_conn.commit()

            cursor.close()
            raw_conn.close()

            log.info(f"Batch insert: {inserted}/{len(records)} свечей")
            return len(records), inserted

        except Exception as e:
            log.error(f"Ошибка сохранения: {e}")
            self.session_stats['errors'] += 1
            return 0, 0

    async def update_5min(
            self,
            session: aiohttp.ClientSession,
            stocks: List[Tuple[str, str]]
    ) -> Tuple[int, int]:
        """Обновляет 5-минутные свечи"""

        now = get_moscow_time()
        till_date = now.strftime('%Y-%m-%d')
        total_attempted = 0
        total_inserted = 0

        semaphore = asyncio.Semaphore(MAX_CONCURRENT_FETCH)

        async def process(ticker, name):
            async with semaphore:
                last_time = self.get_last_candle_time(ticker, 5)

                if last_time:
                    from_date = last_time.strftime('%Y-%m-%d')
                else:
                    from_date = (now - timedelta(days=7)).strftime('%Y-%m-%d')

                df_1min = await self.fetcher.fetch_candles(
                    session, ticker, 1, from_date, till_date
                )

                if df_1min is None or df_1min.empty:
                    return 0, 0

                df_5min = aggregate_to_5min(df_1min)

                if not df_5min.empty and last_time:
                    df_5min = df_5min[df_5min['begin_time'] > last_time]

                if df_5min.empty:
                    return 0, 0

                return await asyncio.to_thread(self.save_candles, df_5min)

        tasks = [process(ticker, name) for ticker, name in stocks]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for r in results:
            if isinstance(r, tuple):
                total_attempted += r[0]
                total_inserted += r[1]

        self.session_stats['candles_saved'][5] += total_inserted
        return total_attempted, total_inserted

    async def update_60min(
            self,
            session: aiohttp.ClientSession,
            stocks: List[Tuple[str, str]]
    ) -> Tuple[int, int]:
        """Обновляет часовые свечи"""

        now = get_moscow_time()
        till_date = now.strftime('%Y-%m-%d')
        total_attempted = 0
        total_inserted = 0

        semaphore = asyncio.Semaphore(MAX_CONCURRENT_FETCH)

        async def process(ticker, name):
            async with semaphore:
                last_time = self.get_last_candle_time(ticker, 60)

                if last_time:
                    from_date = last_time.strftime('%Y-%m-%d')
                else:
                    from_date = (now - timedelta(days=30)).strftime('%Y-%m-%d')

                df = await self.fetcher.fetch_candles(
                    session, ticker, 60, from_date, till_date
                )

                if df is None or df.empty:
                    return 0, 0

                if last_time:
                    df = df[df['begin_time'] > last_time]

                if df.empty:
                    return 0, 0

                return await asyncio.to_thread(self.save_candles, df)

        tasks = [process(ticker, name) for ticker, name in stocks]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for r in results:
            if isinstance(r, tuple):
                total_attempted += r[0]
                total_inserted += r[1]

        self.session_stats['candles_saved'][60] += total_inserted
        return total_attempted, total_inserted

    async def update_daily(
            self,
            session: aiohttp.ClientSession,
            stocks: List[Tuple[str, str]]
    ) -> Tuple[int, int]:
        """Обновляет дневные свечи"""

        now = get_moscow_time()
        till_date = now.strftime('%Y-%m-%d')
        total_attempted = 0
        total_inserted = 0

        semaphore = asyncio.Semaphore(MAX_CONCURRENT_FETCH)

        async def process(ticker, name):
            async with semaphore:
                last_time = self.get_last_candle_time(ticker, 24)

                if last_time:
                    from_date = last_time.strftime('%Y-%m-%d')
                else:
                    from_date = (now - timedelta(days=365)).strftime('%Y-%m-%d')

                df = await self.fetcher.fetch_candles(
                    session, ticker, 24, from_date, till_date
                )

                if df is None or df.empty:
                    return 0, 0

                if last_time:
                    df = df[df['begin_time'] > last_time]

                if df.empty:
                    return 0, 0

                return await asyncio.to_thread(self.save_candles, df)

        tasks = [process(ticker, name) for ticker, name in stocks]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for r in results:
            if isinstance(r, tuple):
                total_attempted += r[0]
                total_inserted += r[1]

        self.session_stats['candles_saved'][24] += total_inserted
        return total_attempted, total_inserted

    async def update_weekly(
            self,
            session: aiohttp.ClientSession,
            stocks: List[Tuple[str, str]]
    ) -> Tuple[int, int]:
        """Обновляет недельные свечи"""

        now = get_moscow_time()
        till_date = now.strftime('%Y-%m-%d')
        total_attempted = 0
        total_inserted = 0

        semaphore = asyncio.Semaphore(MAX_CONCURRENT_FETCH)

        async def process(ticker, name):
            async with semaphore:
                last_time = self.get_last_candle_time(ticker, 7)

                if last_time:
                    from_date = last_time.strftime('%Y-%m-%d')
                else:
                    from_date = (now - timedelta(days=365)).strftime('%Y-%m-%d')

                df = await self.fetcher.fetch_candles(
                    session, ticker, 7, from_date, till_date
                )

                if df is None or df.empty:
                    return 0, 0

                if last_time:
                    df = df[df['begin_time'] > last_time]

                if df.empty:
                    return 0, 0

                return await asyncio.to_thread(self.save_candles, df)

        tasks = [process(ticker, name) for ticker, name in stocks]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for r in results:
            if isinstance(r, tuple):
                total_attempted += r[0]
                total_inserted += r[1]

        self.session_stats['candles_saved'][7] += total_inserted
        return total_attempted, total_inserted

    async def check_data_gaps(self, stocks: List[Tuple[str, str]]):
        """Проверяет пропуски в данных"""
        log.info("🔍 Проверка пропусков...")

        now = get_moscow_time()
        gaps = []

        for ticker, name in stocks[:20]:
            last = self.get_last_candle_time(ticker, 5)

            if last is None:
                gaps.append(f"{ticker}: НЕТ ДАННЫХ")
            elif (now - last) > timedelta(hours=2):
                gap = now - last
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

    def _get_hour_slot(self) -> datetime:
        now = get_moscow_time()
        return now.replace(second=0, microsecond=0, minute=0)

    def _get_day_slot(self) -> datetime:
        now = get_moscow_time()
        return now.replace(second=0, microsecond=0, minute=0, hour=0)

    def print_stats(self):
        """Статистика сессии"""
        uptime = datetime.now() - self.session_stats['start_time']
        api = self.fetcher.get_stats()

        log.info("=" * 50)
        log.info("📊 СТАТИСТИКА (АКЦИИ)")
        log.info(f"  Время работы: {uptime}")
        log.info(f"  Циклов: {self.session_stats['cycles']}")
        log.info(f"  Свечей сохранено:")
        log.info(f"    5м: {self.session_stats['candles_saved'][5]}")
        log.info(f"    60м: {self.session_stats['candles_saved'][60]}")
        log.info(f"    24ч: {self.session_stats['candles_saved'][24]}")
        log.info(f"    7д: {self.session_stats['candles_saved'][7]}")
        log.info(f"  API: {api['requests']} запросов, {api['success']} успешных")
        log.info("=" * 50)

    async def run_once(self, session: aiohttp.ClientSession) -> Dict[int, int]:
        """
        Однократное обновление всех таймфреймов.
        Возвращает словарь {interval: inserted_count}
        """
        results = {}

        # Загружаем список акций
        stocks = self.stocks_manager.load_stocks()
        log.info(f"📋 Акций: {len(stocks)}")

        if not stocks:
            log.warning("⚠️ Нет акций в БД!")
            return results

        # 5-минутки
        log.info("🔄 Обновление 5м...")
        att, ins = await self.update_5min(session, stocks)
        results[5] = ins
        log.info(f"  ✓ 5м: +{ins} новых")

        # Часовые
        log.info("🔄 Обновление 60м...")
        att, ins = await self.update_60min(session, stocks)
        results[60] = ins
        log.info(f"  ✓ 60м: +{ins} новых")

        # Дневные
        log.info("🔄 Обновление 24ч...")
        att, ins = await self.update_daily(session, stocks)
        results[24] = ins
        log.info(f"  ✓ 24ч: +{ins} новых")

        # Недельные
        log.info("🔄 Обновление 7д...")
        att, ins = await self.update_weekly(session, stocks)
        results[7] = ins
        log.info(f"  ✓ 7д: +{ins} новых")

        return results

    async def run_forever(self):
        """Основной цикл"""

        log.info("=" * 60)
        log.info("🚀 ЗАПУСК РЕАЛТАЙМ ОБНОВЛЕНИЯ СВЕЧЕЙ АКЦИЙ")
        log.info(f"  API: Algopack")
        log.info(f"  Параллельность: {MAX_CONCURRENT_FETCH}")
        log.info(f"  Торговые часы: {TRADING_START_HOUR}:00 - {TRADING_END_HOUR}:00 МСК")
        log.info(f"  Расписание:")
        log.info(f"    5м: XX:00:10, XX:05:10...")
        log.info(f"    60м: XX:00:15")
        log.info(f"    24ч: 00:01:00")
        log.info(f"  Логи: {LOG_DIR.absolute()}")
        log.info("=" * 60)

        connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_FETCH)
        timeout = aiohttp.ClientTimeout(total=120)

        retry_count = 0
        max_retries = 5

        while True:
            try:
                async with aiohttp.ClientSession(
                        connector=connector,
                        timeout=timeout
                ) as session:

                    # === Загрузка списка акций ===
                    stocks = self.stocks_manager.load_stocks()
                    log.info(f"📋 Акций: {len(stocks)}")

                    if not stocks:
                        log.error("❌ Нет акций в БД!")
                        return

                    # === Начальная синхронизация ===
                    log.info("🔄 Начальная синхронизация...")

                    att, ins = await self.update_5min(session, stocks)
                    log.info(f"  ✓ 5м: +{ins} новых")

                    att, ins = await self.update_60min(session, stocks)
                    log.info(f"  ✓ 60м: +{ins} новых")

                    att, ins = await self.update_daily(session, stocks)
                    log.info(f"  ✓ 24ч: +{ins} новых")

                    self.last_5min_update = self._get_5min_slot()
                    self.last_60min_update = self._get_hour_slot()
                    self.last_daily_update = self._get_day_slot()
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

                        slot_5min = self._get_5min_slot()
                        slot_hour = self._get_hour_slot()
                        slot_day = self._get_day_slot()

                        # === 5-минутки ===
                        if slot_5min != self.last_5min_update and now.second >= BUFFER_SECONDS:
                            log.info(f"📊 [{now:%H:%M:%S} МСК] 5-минутки...")
                            att, ins = await self.update_5min(session, stocks)
                            self.last_5min_update = slot_5min
                            self.session_stats['cycles'] += 1

                            log.info(f"  ✓ +{ins} новых")

                        # === Часовые ===
                        if slot_hour != self.last_60min_update and now.minute == 0 and now.second >= 15:
                            log.info(f"📊 [{now:%H:%M:%S} МСК] Часовые...")
                            att, ins = await self.update_60min(session, stocks)
                            self.last_60min_update = slot_hour

                            log.info(f"  ✓ +{ins} новых")

                        # === Дневные ===
                        if slot_day != self.last_daily_update and now.hour == 0 and now.minute >= 1:
                            log.info(f"📊 [{now:%H:%M:%S} МСК] Дневные...")
                            att, ins = await self.update_daily(session, stocks)
                            self.last_daily_update = slot_day

                            log.info(f"  ✓ +{ins} новых")
                            self.print_stats()

                        # === Проверка пропусков ===
                        if (datetime.now() - self.last_gap_check).seconds >= GAP_CHECK_INTERVAL:
                            await self.check_data_gaps(stocks)
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

    parser = argparse.ArgumentParser(description='Загрузка свечей акций через Algopack')
    parser.add_argument('--once', action='store_true', help='Однократное обновление')
    parser.add_argument('--force', action='store_true', help='Игнорировать проверку торгового дня')
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("ЗАПУСК СКРИПТА ОБНОВЛЕНИЯ СВЕЧЕЙ АКЦИЙ")
    log.info(f"Время: {datetime.now()}")
    log.info(f"МСК: {get_moscow_time()}")
    log.info(f"Режим: {'однократный' if args.once else 'daemon'}")
    if CONFIG_LOADED:
        log.info("✓ Конфиг загружен из корневой папки")
    else:
        log.warning("⚠️ Используются локальные настройки")
    log.info("=" * 60)

    # === Проверка API ключа ===
    if not ALGOPACK_API_KEY or ALGOPACK_API_KEY == "":
        log.critical("❌ Укажи API ключ в config.py!")
        return

    # === Проверка торгового дня в режиме --once ===
    if args.once and not args.force:
        is_trading, reason = is_trading_day()
        if not is_trading:
            log.info(f"⏭️ Пропуск: {reason}")
            log.info("  (используйте --force для принудительного запуска)")
            return

    try:
        updater = StocksCandlesUpdater(DB_URL, ALGOPACK_API_KEY)

        if args.once:
            # Однократное обновление
            connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_FETCH)
            timeout = aiohttp.ClientTimeout(total=120)
            async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
                results = await updater.run_once(session)
                log.info(f"✅ Готово: 5м=+{results.get(5, 0)}, 60м=+{results.get(60, 0)}, 24ч=+{results.get(24, 0)}")
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