#!/usr/bin/env python3
"""
Главный оркестратор MOEX данных.

Управляет всеми скриптами:
=== OI (Open Interest) ===
1. OI/fetch_oi_5min_realtime.py — 5-минутные данные (Algopack, 65 тикеров)
2. OI/aggregate_oi_hourly.py — агрегация 5м → 60м
3. OI/fetch_oi_daily_realtime.py — дневные данные (ISS MOEX, 133 инструмента)

=== Candles (Свечи) ===
4. Candles/fetch_candles_futures_realtime.py — свечи фьючерсов (Algopack)
5. Candles/fetch_candles_spot_realtime.py — свечи акций (Algopack)

=== Macro (Макроданные) ===
6. Macro/fetch_macro_realtime.py — M2 с ЦБ РФ (автоматически)

=== Материализованные представления ===
- mv_heatmap_stocks — карта рынка акций
- mv_oi_daily_stats — статистика открытого интереса

Расписание (МСК):
- Каждые 5 минут (XX:00:10, XX:05:10...):
    → OI 5м
    → Candles Futures
    → Candles Spot
    → REFRESH mv_heatmap_stocks, mv_oi_daily_stats
- Каждый час (XX:02:00):
    → Агрегация OI 5м → 60м
- Раз в день (00:10):
    → OI Daily

Торговые часы: 07:00-24:00 МСК (фьючерсы), 10:00-24:00 МСК (акции)
Выходные и праздники MOEX: скрипты не запускаются

Запуск:
  python main_orchestrator.py
"""

import asyncio
import sys
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Tuple, Dict, List
import signal

# === Импорт для БД и представлений ===
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

load_dotenv()
DB_URL = os.getenv("DB_URL", "postgresql+pg8000://postgres:1803@localhost:5432/moex_db")

# === Импорт календаря MOEX ===
try:
    from moex_calendar import (
        get_moscow_time,
        is_trading_hours,
        is_trading_day,
        MOEX_HOLIDAYS,
        TRADING_START_HOUR,
        TRADING_END_HOUR
    )

    CALENDAR_AVAILABLE = True
except ImportError:
    print("⚠️ moex_calendar.py не найден, используем встроенные функции")
    CALENDAR_AVAILABLE = False
    MOEX_HOLIDAYS = set()
    TRADING_START_HOUR = 7
    TRADING_END_HOUR = 24
    TRADING_DAYS = [0, 1, 2, 3, 4]


    def get_moscow_time():
        try:
            import pytz
            msk = pytz.timezone('Europe/Moscow')
            return datetime.now(msk).replace(tzinfo=None)
        except ImportError:
            return datetime.utcnow() + timedelta(hours=3)


    def is_trading_hours(check_time=None):
        now = check_time or get_moscow_time()
        if now.weekday() not in TRADING_DAYS:
            return False, f"Выходной ({now.strftime('%A')})"
        if now.hour < TRADING_START_HOUR:
            return False, f"До начала торгов ({TRADING_START_HOUR}:00)"
        if now.hour >= TRADING_END_HOUR:
            return False, "Торги завершены"
        return True, "Торговая сессия"


    def is_trading_day(check_date=None):
        check = check_date or get_moscow_time().date()
        if check.weekday() not in TRADING_DAYS:
            return False, "Выходной"
        return True, "Торговый день"

# === ⚙️ НАСТРОЙКИ ===

# Базовая директория (где лежит этот скрипт)
BASE_DIR = Path(__file__).parent

# Пути к скриптам
SCRIPTS = {
    # OI скрипты
    'oi_5min': BASE_DIR / 'OI' / 'fetch_oi_5min_realtime.py',
    'oi_aggregate': BASE_DIR / 'OI' / 'aggregate_oi_hourly.py',
    'oi_daily': BASE_DIR / 'OI' / 'fetch_oi_daily_realtime.py',
    # Candles скрипты
    'candles_futures': BASE_DIR / 'Candles' / 'fetch_candles_futures_realtime.py',
    'candles_spot': BASE_DIR / 'Candles' / 'fetch_candles_spot_realtime.py',
    # Funds скрипты
    'funds_daily': BASE_DIR / 'Funds' / 'fetch_funds_realtime.py',
    # Indices скрипты
    'indices_daily': BASE_DIR / 'Funds' / 'fetch_indices_realtime.py',
    # Macro скрипты (M2, GDP)
    'macro_daily': BASE_DIR / 'Macro' / 'fetch_macro_realtime.py',
    # Market Cap (полная капитализация рынка)
    'market_cap_daily': BASE_DIR / 'Macro' / 'fetch_market_cap.py',
    # Market Breadth (% акций выше EMA — предвычисление)
    'breadth_daily': BASE_DIR / 'Candles' / 'compute_breadth_history.py',
}

# Материализованные представления
MATERIALIZED_VIEWS = {
    'mv_heatmap_stocks': 'Карта рынка акций',
    'mv_oi_daily_stats': 'Статистика OI',
}

# Время дневного обновления OI
DAILY_UPDATE_HOUR = 0
DAILY_UPDATE_MINUTE = 10

# Буферы (секунды после закрытия интервала)
BUFFER_5MIN = 10  # После закрытия 5-минутки
BUFFER_HOUR = 120  # 2 минуты после часа для агрегации

# Таймауты (секунды)
TIMEOUTS = {
    'oi_5min': 300,  # 5 минут
    'oi_aggregate': 600,  # 10 минут
    'oi_daily': 1800,  # 30 минут
    'candles_futures': 300,  # 5 минут
    'candles_spot': 300,  # 5 минут
    'funds_daily': 600,  # 10 минут
    'indices_daily': 300,  # 5 минут
    'macro_daily': 120,  # 2 минуты
    'market_cap_daily': 120,  # 2 минуты
    'breadth_daily': 600,  # 10 минут (полный пересчёт ~2 мин)
}

# Директория логов
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)


# ======================================================================
#                         ЛОГИРОВАНИЕ
# ======================================================================

def setup_logging():
    """Настройка логирования"""

    detailed_fmt = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    console_fmt = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(message)s',
        datefmt='%H:%M:%S'
    )

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    root.handlers.clear()

    # Консоль
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(console_fmt)
    root.addHandler(ch)

    # Файл всех логов
    fh_all = logging.FileHandler(
        LOG_DIR / f"main_orchestrator_{datetime.now():%Y%m%d}.log",
        encoding='utf-8'
    )
    fh_all.setLevel(logging.DEBUG)
    fh_all.setFormatter(detailed_fmt)
    root.addHandler(fh_all)

    # Файл ошибок
    fh_err = logging.FileHandler(
        LOG_DIR / f"main_orchestrator_errors_{datetime.now():%Y%m%d}.log",
        encoding='utf-8'
    )
    fh_err.setLevel(logging.WARNING)
    fh_err.setFormatter(detailed_fmt)
    root.addHandler(fh_err)

    return logging.getLogger(__name__)


log = setup_logging()


# ======================================================================
#                    ОБНОВЛЕНИЕ МАТЕРИАЛИЗОВАННЫХ ПРЕДСТАВЛЕНИЙ
# ======================================================================

def refresh_materialized_views(views: List[str] = None) -> Dict[str, Tuple[bool, float]]:
    """
    Обновляет материализованные представления.

    Args:
        views: список представлений для обновления.
               None = все представления

    Returns:
        {'view_name': (success, duration_sec), ...}
    """
    if views is None:
        views = list(MATERIALIZED_VIEWS.keys())

    results = {}

    try:
        engine = create_engine(DB_URL)

        with engine.connect() as conn:
            for view_name in views:
                try:
                    start = datetime.now()
                    conn.execute(text(f"REFRESH MATERIALIZED VIEW {view_name}"))
                    conn.commit()
                    duration = (datetime.now() - start).total_seconds()
                    results[view_name] = (True, duration)
                    desc = MATERIALIZED_VIEWS.get(view_name, view_name)
                    log.info(f"    ✅ {desc} ({duration:.1f}с)")
                except Exception as e:
                    results[view_name] = (False, 0)
                    log.warning(f"    ⚠️ {view_name}: {e}")

        engine.dispose()

    except Exception as e:
        log.error(f"Ошибка подключения к БД: {e}")

    return results


# ======================================================================
#                         ЗАПУСК СКРИПТОВ
# ======================================================================

async def run_script(script_key: str, args: List[str] = None, timeout: int = None) -> Tuple[bool, str, float]:
    """
    Асинхронно запускает Python скрипт.

    Args:
        script_key: Ключ скрипта из SCRIPTS
        args: Дополнительные аргументы
        timeout: Таймаут в секундах

    Returns:
        (success, message, duration_seconds)
    """
    if args is None:
        args = []

    if timeout is None:
        timeout = TIMEOUTS.get(script_key, 300)

    script_path = SCRIPTS.get(script_key)

    if script_path is None:
        return False, f"Неизвестный скрипт: {script_key}", 0

    if not script_path.exists():
        return False, f"Скрипт не найден: {script_path}", 0

    cmd = [sys.executable, str(script_path)] + args

    log.debug(f"Запуск: {' '.join(cmd)}")

    start_time = datetime.now()

    try:
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=str(script_path.parent)
        )

        try:
            stdout, stderr = await asyncio.wait_for(
                process.communicate(),
                timeout=timeout
            )
        except asyncio.TimeoutError:
            process.kill()
            await process.wait()
            duration = (datetime.now() - start_time).total_seconds()
            return False, f"Таймаут ({timeout} сек)", duration

        duration = (datetime.now() - start_time).total_seconds()

        if process.returncode == 0:
            return True, "OK", duration
        else:
            error_msg = stderr.decode()[-500:] if stderr else "Unknown error"
            return False, f"Exit code {process.returncode}: {error_msg}", duration

    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds()
        return False, f"Ошибка: {e}", duration


# ======================================================================
#                         ОРКЕСТРАТОР
# ======================================================================

class MainOrchestrator:
    """Главный оркестратор всех скриптов"""

    def __init__(self):
        self.last_5min_update = None
        self.last_hourly_aggregate = None
        self.last_daily_update = None

        self.stats = {
            'start_time': datetime.now(),
            'cycles': 0,
            # OI
            'oi_5min_runs': 0,
            'oi_5min_success': 0,
            'oi_aggregate_runs': 0,
            'oi_aggregate_success': 0,
            'oi_daily_runs': 0,
            'oi_daily_success': 0,
            # Candles
            'candles_futures_runs': 0,
            'candles_futures_success': 0,
            'candles_spot_runs': 0,
            'candles_spot_success': 0,
            # Funds
            'funds_daily_runs': 0,
            'funds_daily_success': 0,
            # Indices
            'indices_daily_runs': 0,
            'indices_daily_success': 0,
            # Macro
            'macro_daily_runs': 0,
            'macro_daily_success': 0,
            # Market Cap
            'market_cap_daily_runs': 0,
            'market_cap_daily_success': 0,
            # Breadth
            'breadth_daily_runs': 0,
            'breadth_daily_success': 0,
            # Представления
            'views_refresh_runs': 0,
            'views_refresh_success': 0,
            # Общее
            'errors': 0,
            'total_duration': 0,
        }

        self.running = True

        log.info("MainOrchestrator инициализирован")

    def _get_5min_slot(self) -> datetime:
        now = get_moscow_time()
        return now.replace(second=0, microsecond=0, minute=(now.minute // 5) * 5)

    def _get_hour_slot(self) -> datetime:
        now = get_moscow_time()
        return now.replace(second=0, microsecond=0, minute=0)

    def _get_day_slot(self) -> datetime:
        now = get_moscow_time()
        return now.replace(second=0, microsecond=0, minute=0, hour=0)

    async def run_5min_cycle(self) -> Dict[str, bool]:
        """
        Запускает все 5-минутные скрипты последовательно.

        Returns:
            Словарь {script_key: success}
        """
        results = {}
        total_start = datetime.now()

        # 1. OI 5-минутки
        log.info("  📊 OI 5м...")
        self.stats['oi_5min_runs'] += 1
        success, msg, dur = await run_script('oi_5min', ['--once', '--force'])
        results['oi_5min'] = success
        if success:
            self.stats['oi_5min_success'] += 1
            log.info(f"    ✓ OI 5м ({dur:.1f}с)")
        else:
            self.stats['errors'] += 1
            log.error(f"    ✗ OI 5м: {msg}")

        # 2. Candles Futures
        log.info("  📊 Candles Futures...")
        self.stats['candles_futures_runs'] += 1
        success, msg, dur = await run_script('candles_futures', ['--once', '--force'])
        results['candles_futures'] = success
        if success:
            self.stats['candles_futures_success'] += 1
            log.info(f"    ✓ Candles Futures ({dur:.1f}с)")
        else:
            self.stats['errors'] += 1
            log.error(f"    ✗ Candles Futures: {msg}")

        # 3. Candles Spot
        log.info("  📊 Candles Spot...")
        self.stats['candles_spot_runs'] += 1
        success, msg, dur = await run_script('candles_spot', ['--once', '--force'])
        results['candles_spot'] = success
        if success:
            self.stats['candles_spot_success'] += 1
            log.info(f"    ✓ Candles Spot ({dur:.1f}с)")
        else:
            self.stats['errors'] += 1
            log.error(f"    ✗ Candles Spot: {msg}")

        # 4. Обновление материализованных представлений
        log.info("  🔄 Обновление представлений...")
        self.stats['views_refresh_runs'] += 1
        view_results = refresh_materialized_views()

        # Считаем успешные обновления
        all_success = all(r[0] for r in view_results.values())
        if all_success:
            self.stats['views_refresh_success'] += 1
        results['views'] = all_success

        total_duration = (datetime.now() - total_start).total_seconds()
        self.stats['total_duration'] += total_duration
        self.stats['cycles'] += 1

        log.info(f"  ⏱️ Цикл завершён за {total_duration:.1f}с")

        return results

    async def run_hourly_aggregate(self) -> bool:
        """Запускает агрегацию OI 5м → 60м"""
        log.info("  📊 Агрегация OI...")
        self.stats['oi_aggregate_runs'] += 1

        success, msg, dur = await run_script('oi_aggregate', ['--last-hour', '--force'])

        if success:
            self.stats['oi_aggregate_success'] += 1
            log.info(f"    ✓ Агрегация OI ({dur:.1f}с)")
        else:
            self.stats['errors'] += 1
            log.error(f"    ✗ Агрегация OI: {msg}")

        return success

    async def run_daily_update(self) -> bool:
        """Запускает дневное обновление OI"""
        log.info("  📊 OI Daily...")
        self.stats['oi_daily_runs'] += 1

        success, msg, dur = await run_script('oi_daily', ['--once', '--force'])

        if success:
            self.stats['oi_daily_success'] += 1
            log.info(f"    ✓ OI Daily ({dur:.1f}с)")
        else:
            self.stats['errors'] += 1
            log.error(f"    ✗ OI Daily: {msg}")

        return success

    async def run_funds_update(self) -> bool:
        """Запускает дневное обновление фондов"""
        log.info("  📊 Funds Daily...")
        self.stats['funds_daily_runs'] += 1

        success, msg, dur = await run_script('funds_daily', ['--once', '--force'])

        if success:
            self.stats['funds_daily_success'] += 1
            log.info(f"    ✓ Funds Daily ({dur:.1f}с)")
        else:
            self.stats['errors'] += 1
            log.error(f"    ✗ Funds Daily: {msg}")

        return success

    async def run_indices_update(self) -> bool:
        """Запускает дневное обновление индексов MOEX"""
        log.info("  📊 Indices Daily...")
        self.stats['indices_daily_runs'] += 1

        success, msg, dur = await run_script('indices_daily', ['--once'])

        if success:
            self.stats['indices_daily_success'] += 1
            log.info(f"    ✓ Indices Daily ({dur:.1f}с)")
        else:
            self.stats['errors'] += 1
            log.error(f"    ✗ Indices Daily: {msg}")

        return success

    async def run_macro_update(self) -> bool:
        """Запускает обновление макроданных (M2 с ЦБ)"""
        log.info("  📊 Macro Daily (M2)...")
        self.stats['macro_daily_runs'] += 1

        success, msg, dur = await run_script('macro_daily', ['--once', '--force'])

        if success:
            self.stats['macro_daily_success'] += 1
            log.info(f"    ✓ Macro Daily ({dur:.1f}с)")
        else:
            self.stats['errors'] += 1
            log.error(f"    ✗ Macro Daily: {msg}")

        return success

    async def run_breadth_update(self) -> bool:
        """Запускает предвычисление Market Breadth (инкрементально)"""
        log.info("  📊 Breadth Daily...")
        self.stats['breadth_daily_runs'] += 1

        success, msg, dur = await run_script('breadth_daily', ['--once', '--force'])

        if success:
            self.stats['breadth_daily_success'] += 1
            log.info(f"    ✓ Breadth Daily ({dur:.1f}с)")
        else:
            self.stats['errors'] += 1
            log.error(f"    ✗ Breadth Daily: {msg}")

        return success

    async def run_market_cap_update(self) -> bool:
        """Запускает обновление полной капитализации рынка (SmartLab)"""
        log.info("  📊 Market Cap Daily...")
        self.stats['market_cap_daily_runs'] += 1

        success, msg, dur = await run_script('market_cap_daily', ['--once', '--force'])

        if success:
            self.stats['market_cap_daily_success'] += 1
            log.info(f"    ✓ Market Cap Daily ({dur:.1f}с)")
        else:
            self.stats['errors'] += 1
            log.error(f"    ✗ Market Cap Daily: {msg}")

        return success

    def print_stats(self):
        """Выводит статистику"""
        uptime = datetime.now() - self.stats['start_time']
        avg_cycle = self.stats['total_duration'] / max(self.stats['cycles'], 1)

        log.info("=" * 60)
        log.info("📊 СТАТИСТИКА ОРКЕСТРАТОРА")
        log.info(f"  Время работы: {uptime}")
        log.info(f"  Циклов: {self.stats['cycles']}, среднее время: {avg_cycle:.1f}с")
        log.info("  --- OI ---")
        log.info(f"    5м: {self.stats['oi_5min_success']}/{self.stats['oi_5min_runs']}")
        log.info(f"    Агрегация: {self.stats['oi_aggregate_success']}/{self.stats['oi_aggregate_runs']}")
        log.info(f"    Daily: {self.stats['oi_daily_success']}/{self.stats['oi_daily_runs']}")
        log.info("  --- Candles ---")
        log.info(f"    Futures: {self.stats['candles_futures_success']}/{self.stats['candles_futures_runs']}")
        log.info(f"    Spot: {self.stats['candles_spot_success']}/{self.stats['candles_spot_runs']}")
        log.info("  --- Funds ---")
        log.info(f"    Daily: {self.stats['funds_daily_success']}/{self.stats['funds_daily_runs']}")
        log.info("  --- Macro ---")
        log.info(f"    Daily: {self.stats['macro_daily_success']}/{self.stats['macro_daily_runs']}")
        log.info("  --- Market Cap ---")
        log.info(f"    Daily: {self.stats['market_cap_daily_success']}/{self.stats['market_cap_daily_runs']}")
        log.info("  --- Breadth ---")
        log.info(f"    Daily: {self.stats['breadth_daily_success']}/{self.stats['breadth_daily_runs']}")
        log.info("  --- Представления ---")
        log.info(f"    Обновлений: {self.stats['views_refresh_success']}/{self.stats['views_refresh_runs']}")
        log.info(f"  Ошибок: {self.stats['errors']}")
        log.info("=" * 60)

    def stop(self):
        """Останавливает оркестратор"""
        self.running = False
        log.info("🛑 Получен сигнал остановки...")

    async def run_forever(self):
        """Основной цикл оркестратора"""

        log.info("=" * 60)
        log.info("🚀 ЗАПУСК ГЛАВНОГО ОРКЕСТРАТОРА MOEX")
        log.info(f"  Базовая директория: {BASE_DIR}")
        log.info("  Скрипты:")
        for key, path in SCRIPTS.items():
            exists = "✓" if path.exists() else "✗"
            log.info(f"    [{exists}] {key}: {path.name}")
        log.info("  Материализованные представления:")
        for view, desc in MATERIALIZED_VIEWS.items():
            log.info(f"    • {view}: {desc}")
        log.info(f"  Торговые часы: {TRADING_START_HOUR}:00 - {TRADING_END_HOUR}:00 МСК")
        log.info(f"  Расписание:")
        log.info(f"    5м цикл: XX:00:{BUFFER_5MIN:02d}, XX:05:{BUFFER_5MIN:02d}...")
        log.info(f"    Агрегация: XX:02:00")
        log.info(f"    Daily: {DAILY_UPDATE_HOUR:02d}:{DAILY_UPDATE_MINUTE:02d}")
        log.info(f"  Календарь MOEX: {'✓ загружен' if CALENDAR_AVAILABLE else '✗ не найден'}")
        if CALENDAR_AVAILABLE:
            holidays_2025 = len([d for d in MOEX_HOLIDAYS if d.year == 2025])
            log.info(f"  Праздников 2025: {holidays_2025}")
        log.info(f"  Логи: {LOG_DIR.absolute()}")
        log.info("=" * 60)

        # === Проверка наличия скриптов ===
        missing = [k for k, p in SCRIPTS.items() if not p.exists()]
        if missing:
            log.critical(f"❌ Скрипты не найдены: {missing}")
            return

        # === Начальная синхронизация ===
        log.info("🔄 Начальная синхронизация...")

        is_trading, reason = is_trading_day()
        if is_trading:
            # 5-минутный цикл
            results = await self.run_5min_cycle()

            # Агрегация (если OI 5м успешно)
            if results.get('oi_5min'):
                await self.run_hourly_aggregate()

            # Daily (OI + Funds + Indices + Macro + Market Cap + Breadth)
            await self.run_daily_update()
            await self.run_funds_update()
            await self.run_indices_update()
            await self.run_macro_update()
            await self.run_market_cap_update()
            await self.run_breadth_update()

            # Обновляем все представления после полной синхронизации
            log.info("  🔄 Финальное обновление представлений...")
            refresh_materialized_views()
        else:
            log.info(f"  ⏭️ Пропуск синхронизации: {reason}")

        self.last_5min_update = self._get_5min_slot()
        self.last_hourly_aggregate = self._get_hour_slot()
        self.last_daily_update = self._get_day_slot()

        log.info("✅ Начальная синхронизация завершена")
        log.info("=" * 60)

        # === Основной цикл ===
        while self.running:
            try:
                now = get_moscow_time()

                # Проверка торговых часов
                is_trading, reason = is_trading_hours()

                slot_5min = self._get_5min_slot()
                slot_hour = self._get_hour_slot()
                slot_day = self._get_day_slot()

                # === 5-минутный цикл (только в торговые часы) ===
                if is_trading:
                    if slot_5min != self.last_5min_update and now.second >= BUFFER_5MIN:
                        log.info(f"⏰ [{now:%H:%M:%S} МСК] 5-минутный цикл...")

                        results = await self.run_5min_cycle()
                        self.last_5min_update = slot_5min

                        # Агрегация (после 5м, если новый час и минута >= 2)
                        if results.get('oi_5min') and now.minute >= 2:
                            if slot_hour != self.last_hourly_aggregate:
                                await self.run_hourly_aggregate()
                                self.last_hourly_aggregate = slot_hour
                else:
                    log.debug(f"Вне торгов: {reason}")

                # === Дневное обновление OI (в 00:10, только в торговые дни) ===
                is_trade_day, _ = is_trading_day()

                if is_trade_day:
                    if (slot_day != self.last_daily_update and
                            now.hour == DAILY_UPDATE_HOUR and
                            now.minute >= DAILY_UPDATE_MINUTE):
                        log.info(f"⏰ [{now:%H:%M:%S} МСК] Дневное обновление...")
                        await self.run_daily_update()
                        await self.run_funds_update()
                        await self.run_indices_update()
                        await self.run_macro_update()
                        await self.run_market_cap_update()
                        await self.run_breadth_update()
                        self.last_daily_update = slot_day
                        self.print_stats()

                await asyncio.sleep(1)

            except asyncio.CancelledError:
                log.info("Цикл отменён")
                break

            except Exception as e:
                log.error(f"❌ Ошибка в цикле: {e}", exc_info=True)
                self.stats['errors'] += 1
                await asyncio.sleep(60)

        self.print_stats()
        log.info("🛑 Оркестратор остановлен")


# ======================================================================
#                         ENTRY POINT
# ======================================================================

async def main():
    log.info("=" * 60)
    log.info("ЗАПУСК ГЛАВНОГО ОРКЕСТРАТОРА MOEX")
    log.info(f"Время: {datetime.now()}")
    log.info(f"МСК: {get_moscow_time()}")
    log.info("=" * 60)

    orchestrator = MainOrchestrator()

    # Обработка сигналов
    loop = asyncio.get_event_loop()

    def signal_handler():
        orchestrator.stop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, signal_handler)
        except NotImplementedError:
            # Windows не поддерживает add_signal_handler
            pass

    try:
        await orchestrator.run_forever()
    except KeyboardInterrupt:
        orchestrator.stop()
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