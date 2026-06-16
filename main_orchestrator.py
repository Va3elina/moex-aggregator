#!/usr/bin/env python3
"""
Главный оркестратор MOEX данных.

Управляет всеми скриптами:
=== OI (Open Interest) ===
1. OI/fetch_oi_5min_realtime.py — 5-минутные данные (Algopack, 65 тикеров)
2. OI/aggregate_oi_hourly.py — агрегация 5м → 60м
3. OI/fetch_oi_daily_realtime.py — дневные данные (Algopack FUTOI, обновляется каждые 5 мин)

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
DB_URL = os.getenv("DB_URL")

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
    # Рублёвый оборот фьючерсов (VALTODAY с ISS → candles.value): у фьючерсных
    # свечей оборот = 0, в поиске «Объём» нужен в рублях, а не контрактах.
    'futures_turnover': BASE_DIR / 'Candles' / 'fetch_futures_turnover.py',
    # Funds скрипты
    'funds_daily': BASE_DIR / 'Funds' / 'fetch_funds_realtime.py',
    # Indices скрипты
    'indices_daily': BASE_DIR / 'Funds' / 'fetch_indices_realtime.py',
    'index_candles_hourly': BASE_DIR / 'Funds' / 'fetch_index_candles_hourly.py',
    # Macro скрипты (M2, GDP)
    'macro_daily': BASE_DIR / 'Macro' / 'fetch_macro_realtime.py',
    # Market Cap (полная капитализация рынка)
    'market_cap_daily': BASE_DIR / 'Macro' / 'fetch_market_cap.py',
    # Market Breadth (% акций выше EMA — предвычисление)
    'breadth_daily': BASE_DIR / 'Candles' / 'compute_breadth_history.py',
    # Дивиденды (загрузка с ISS + определение экс-дат)
    'dividends_daily': BASE_DIR / 'Candles' / 'fetch_dividends.py',
    # Сырьевые товары (Yahoo Finance: BRENT/GOLD/SILVER/...) — для seasonality
    'commodity_daily': BASE_DIR / 'Commodity' / 'fetch_commodity_realtime.py',
    # ОРФР ЦБ (cbr_orfr_flows) убран из расписания: cbr.ru таймаутит с сервера,
    # формат отчёта нестабилен (ЦБ переименовывает листы). Перешли на ручной
    # ингест — файл присылается вручную, грузится через
    #   python3 -m CBR.fetch_orfr_flows --xlsx <path>
    # (скрипт CBR/fetch_orfr_flows.py остаётся как ручной инструмент).
}

# Материализованные представления.
# concurrent=True → REFRESH CONCURRENTLY (не берёт ACCESS EXCLUSIVE lock,
# читатели продолжают видеть старую версию во время ~10-13с пересборки).
# Требует UNIQUE-индекс на MV. mv_oi_daily_stats его НЕ имеет → обычный REFRESH.
MATERIALIZED_VIEWS = {
    'mv_heatmap_stocks': 'Карта рынка акций',
    'mv_oi_daily_stats': 'Статистика OI',
}

# Какие MV можно обновлять CONCURRENTLY (есть UNIQUE-индекс).
# mv_heatmap_stocks: UNIQUE INDEX на (sec_id) — см. db/mv_heatmap_stocks.sql.
# REFRESH CONCURRENTLY ~12.7с раньше блокировал /api/heatmap/stocks на весь
# этот срок (ACCESS EXCLUSIVE), из-за чего «Все акции» грузились 10-20с.
CONCURRENT_REFRESH_VIEWS = {'mv_heatmap_stocks'}

# Время дневного обновления всех источников.
# 19:10 МСК — после закрытия торгов (18:45) + время УК на расчёт NAV.
# Раньше было 00:10, но Cbonds публикует NAV за день только к 15-17:00 МСК,
# поэтому fund_data отставали на день (загрузка за T делалась в T+1 00:10,
# видела только до T-1). 19:10 МСК → данные появляются в день T, не T+1.
# OI / candles / macro / dividends к 19:10 тоже уже точно опубликованы.
DAILY_UPDATE_HOUR = 19
DAILY_UPDATE_MINUTE = 10

# Ранний прогон ТОЛЬКО для funds — Cbonds публикует NAV с ~11-14 МСК,
# 14:30 ловит большинство УК. Поздний 19:10 догоняет опоздавших.
# UPSERT на (fund_id, trade_date) → безопасный повторный прогон.
FUNDS_EARLY_UPDATE_HOUR = 14
FUNDS_EARLY_UPDATE_MINUTE = 30

# Commodity — Yahoo Finance daily close. US market закрыт в 23:00 EST = 07:00 МСК
# (или 06:00 в DST). 08:00 МСК — гарантированно после закрытия, дневные close
# уже окончательные. UPSERT на (secid, trade_date) → безопасно если запустим
# раньше или дважды.
COMMODITY_UPDATE_HOUR = 8
COMMODITY_UPDATE_MINUTE = 0

# ОРФР ЦБ — авто-расписание убрано (ручной ингест через --xlsx, см. SCRIPTS).

# Буферы (секунды после закрытия интервала)
BUFFER_5MIN = 10  # После закрытия 5-минутки
BUFFER_HOUR = 120  # 2 минуты после часа для агрегации

# Таймауты (секунды)
TIMEOUTS = {
    'oi_5min': 900,  # 15 минут (нужно для backfill после простоя)
    'oi_aggregate': 600,  # 10 минут
    'oi_daily': 1800,  # 30 минут
    'candles_futures': 300,  # 5 минут
    'candles_spot': 300,  # 5 минут
    'futures_turnover': 60,  # 1 минута (один ISS-запрос + bulk UPDATE)
    'funds_daily': 600,  # 10 минут
    'indices_daily': 300,  # 5 минут
    'index_candles_hourly': 600,  # 10 минут (бэкфилл с 2011 — ~25k свечей)
    'macro_daily': 120,  # 2 минуты
    'market_cap_daily': 120,  # 2 минуты
    'breadth_daily': 600,  # 10 минут (полный пересчёт ~2 мин)
    'dividends_daily': 900,  # 15 минут (много HTTP-запросов к ISS)
    'commodity_daily': 600,  # 10 минут (9 тикеров × Yahoo HTTP, обычно <1 мин)
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
#                    POSTGRESQL NOTIFY (SSE)
# ======================================================================

def send_data_notify(source: str, tables: list = None):
    """Отправить PostgreSQL NOTIFY о обновлении данных."""
    import json as _json
    payload = _json.dumps({
        "source": source,
        "tables": tables or [],
        "ts": datetime.now().isoformat()
    })
    try:
        engine = create_engine(DB_URL, connect_args={"ssl_context": False})
        with engine.connect() as conn:
            conn.execute(text("SELECT pg_notify('data_updated', :payload)"), {"payload": payload})
            conn.commit()
        engine.dispose()
        log.info(f"NOTIFY sent: source={source}")
    except Exception as e:
        log.error(f"NOTIFY failed: {e}")


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
        engine = create_engine(DB_URL, connect_args={"ssl_context": False})

        # AUTOCOMMIT обязателен для REFRESH ... CONCURRENTLY — он не может
        # выполняться внутри транзакционного блока.
        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            for view_name in views:
                concurrent = view_name in CONCURRENT_REFRESH_VIEWS
                try:
                    start = datetime.now()
                    if concurrent:
                        conn.execute(text(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {view_name}"))
                    else:
                        conn.execute(text(f"REFRESH MATERIALIZED VIEW {view_name}"))
                    duration = (datetime.now() - start).total_seconds()
                    results[view_name] = (True, duration)
                    desc = MATERIALIZED_VIEWS.get(view_name, view_name)
                    log.info(f"    ✅ {desc} ({duration:.1f}с){' [concurrent]' if concurrent else ''}")
                except Exception as e:
                    # Фоллбэк: если CONCURRENTLY не сработал (например пропал
                    # UNIQUE-индекс после миграции) — пробуем обычный REFRESH,
                    # чтобы данные хотя бы обновились (ценой блокировки чтения).
                    if concurrent:
                        try:
                            start = datetime.now()
                            conn.execute(text(f"REFRESH MATERIALIZED VIEW {view_name}"))
                            duration = (datetime.now() - start).total_seconds()
                            results[view_name] = (True, duration)
                            log.warning(f"    ⚠️ {view_name}: CONCURRENTLY не удался ({e}), fallback на обычный REFRESH ({duration:.1f}с)")
                            continue
                        except Exception as e2:
                            e = e2
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
        self.last_funds_early_update = None  # ранний funds-only прогон в 14:30
        self.last_commodity_update = None    # commodity daily — 08:00 МСК после US close
        self.last_weekend_catchup = None
        self.last_billing_hourly = None      # billing renewal + expire — раз в час
        self.last_expire_quarter = None      # лёгкий expire-only — каждые 15 мин

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
            'index_candles_hourly_runs': 0,
            'index_candles_hourly_success': 0,
            # Macro
            'macro_daily_runs': 0,
            'macro_daily_success': 0,
            # Market Cap
            'market_cap_daily_runs': 0,
            'market_cap_daily_success': 0,
            # Breadth
            'breadth_daily_runs': 0,
            'breadth_daily_success': 0,
            # Dividends
            'dividends_daily_runs': 0,
            'dividends_daily_success': 0,
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

        # Пауза между скриптами для снижения нагрузки на CPU
        await asyncio.sleep(3)

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

        # 2b. Рублёвый оборот фьючерсов (VALTODAY с ISS → candles.value).
        # Best-effort: отдельный процесс ПОСЛЕ свечей (нужна сегодняшняя дневная
        # свеча). Падение не трогает свечи и не считается ошибкой цикла — поиск
        # просто покажет прежнее значение оборота.
        self.stats.setdefault('futures_turnover_runs', 0)
        self.stats.setdefault('futures_turnover_success', 0)
        self.stats['futures_turnover_runs'] += 1
        success, msg, dur = await run_script('futures_turnover', ['--force'])
        if success:
            self.stats['futures_turnover_success'] += 1
            log.info(f"    ✓ Futures turnover ({dur:.1f}с)")
        else:
            log.warning(f"    ⚠️ Futures turnover: {msg}")

        # Пауза между скриптами для снижения нагрузки на CPU
        await asyncio.sleep(3)

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
        view_results = await asyncio.to_thread(refresh_materialized_views)

        # Считаем успешные обновления
        all_success = all(r[0] for r in view_results.values())
        if all_success:
            self.stats['views_refresh_success'] += 1
        results['views'] = all_success

        # NOTIFY: уведомляем API о новых данных
        send_data_notify("5min", ["candles", "open_interest"])
        send_data_notify("mv_refresh", ["mv_heatmap_stocks", "mv_oi_daily_stats"])

        total_duration = (datetime.now() - total_start).total_seconds()
        self.stats['total_duration'] += total_duration
        self.stats['cycles'] += 1

        log.info(f"  ⏱️ Цикл завершён за {total_duration:.1f}с")

        return results

    async def run_expire_only(self) -> int:
        """Лёгкий expire-only проход (каждые 15 мин, без T-Bank renewal).

        Tier-gating читает users.role, которую sync_user_role обновляет, а
        expire_overdue понижает у истёкших подписок. Раньше это шло только в
        часовом billing-job → между истечением подписки и кроном юзер до часа
        сохранял платную роль. expire_overdue — дешёвый UPDATE и ИДЕМПОТЕНТЕН,
        гасит ТОЛЬКО реально истёкшие (expires_at < now) → безопасно гонять
        чаще, окно устаревшей роли сжимается с ≤60 до ≤15 мин. Auto-renewal
        (T-Bank HTTP) остаётся часовым — его учащать не нужно."""
        try:
            from api.database import SessionLocal
            from api.billing.service import expire_overdue
        except Exception as e:
            log.error("expire-only: import failed: %s", e)
            return 0

        def _do():
            db = SessionLocal()
            try:
                return expire_overdue(db)
            finally:
                db.close()
        try:
            n = await asyncio.to_thread(_do)
            if n:
                log.info("  💳 expire-only: погашено %d истёкших подписок", n)
            return n
        except Exception as e:
            log.error("expire-only failed: %s", e, exc_info=True)
            return 0

    async def run_billing_hourly(self) -> dict:
        """Billing hourly job: expire overdue + auto-renewal через T-Bank Charge.

        Сначала expire_overdue — переводит истекшие active sub'ы в expired
        (для тех у кого нет рекуррентной карты, или charge failed на прошлых
        итерациях). Затем renew_expiring_subs — для sub'ов с привязанной
        картой и без cancelled_at пытается продлить через /v2/Charge.

        Безопасно: каждая функция использует свой db.commit. При ошибке в
        одной — другая отрабатывает.

        Возвращает {expired, renewed, failed, skipped, checked}.
        """
        log.info("  💳 Billing hourly: expire + auto-renewal...")
        try:
            from api.database import SessionLocal
            from api.billing.service import expire_overdue, renew_expiring_subs
        except Exception as e:
            log.error("billing: import failed: %s", e)
            return {"error": str(e)}

        result = {"expired": 0, "renewed": 0, "failed": 0, "skipped": 0, "checked": 0}

        # expire_overdue — синхронный, оборачиваем в thread
        def _do_expire():
            db = SessionLocal()
            try:
                return expire_overdue(db)
            finally:
                db.close()
        try:
            result["expired"] = await asyncio.to_thread(_do_expire)
        except Exception as e:
            log.error("billing.expire_overdue failed: %s", e, exc_info=True)

        # renew_expiring_subs — тоже синхронный + делает T-Bank HTTP
        def _do_renew():
            db = SessionLocal()
            try:
                return renew_expiring_subs(db)
            finally:
                db.close()
        try:
            renew_summary = await asyncio.to_thread(_do_renew)
            result.update({
                "renewed": renew_summary.get("renewed", 0),
                "failed": renew_summary.get("failed", 0),
                "skipped": renew_summary.get("skipped", 0),
                "checked": renew_summary.get("checked", 0),
            })
        except Exception as e:
            log.error("billing.renew_expiring_subs failed: %s", e, exc_info=True)

        log.info("  💳 Billing hourly result: %s", result)
        return result

    async def run_hourly_aggregate(self, recent_days: int = None) -> bool:
        """Запускает агрегацию OI 5м → 60м.

        recent_days=None — только последний час (для оркестратора в реалтайме)
        recent_days=N    — последние N дней (для докачки)
        """
        if recent_days:
            log.info(f"  📊 Агрегация OI (последние {recent_days} дней)...")
            args = ['--recent', str(recent_days), '--force']
        else:
            log.info("  📊 Агрегация OI (последний час)...")
            args = ['--last-hour', '--force']

        self.stats['oi_aggregate_runs'] += 1
        success, msg, dur = await run_script('oi_aggregate', args)

        if success:
            self.stats['oi_aggregate_success'] += 1
            log.info(f"    ✓ Агрегация OI ({dur:.1f}с)")
            send_data_notify("hourly", ["open_interest"])
        else:
            self.stats['errors'] += 1
            log.error(f"    ✗ Агрегация OI: {msg}")

        return success

    def _get_oi_hourly_gap_days(self) -> int:
        """Возвращает кол-во дней с последней часовой агрегации OI (минимум 1, максимум 90)."""
        try:
            engine = create_engine(DB_URL, connect_args={"ssl_context": False})
            with engine.connect() as conn:
                result = conn.execute(text(
                    "SELECT MAX(tradedate) FROM open_interest WHERE interval = 60"
                ))
                last_date = result.scalar()
            engine.dispose()
            if last_date is None:
                return 90
            gap = (get_moscow_time().date() - last_date).days + 1
            return max(1, min(gap, 90))
        except Exception as e:
            log.warning(f"  ⚠️ Не удалось определить период дыры OI: {e}, используем 7 дней")
            return 7

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

    async def run_analytics_cleanup(self) -> bool:
        """Cleanup analytics_events старше 180 дней (раз в день).

        Запускается inline через SQL DELETE — отдельного скрипта не нужно.
        180 дней — достаточно для seasonality (week-over-week, month-over-month
        comparisons), но не overkill (~100K rows / месяц для текущей нагрузки).
        """
        log.info("  🧹 Analytics cleanup (events > 180d)...")
        self.stats.setdefault('analytics_cleanup_runs', 0)
        self.stats.setdefault('analytics_cleanup_success', 0)
        self.stats['analytics_cleanup_runs'] += 1

        try:
            from sqlalchemy import create_engine, text
            engine = create_engine(DB_URL)
            with engine.begin() as conn:
                result = conn.execute(text(
                    "DELETE FROM analytics_events WHERE server_ts < NOW() - INTERVAL '180 days'"
                ))
                deleted = result.rowcount
            self.stats['analytics_cleanup_success'] += 1
            log.info(f"    ✓ Analytics cleanup: удалено {deleted} строк")
            return True
        except Exception as e:
            self.stats['errors'] += 1
            log.error(f"    ✗ Analytics cleanup failed: {e}")
            return False

    async def run_commodity_update(self) -> bool:
        """Дневные цены сырья (Yahoo Finance) для Сезонности."""
        log.info("  🛢️  Commodity Daily (Yahoo)...")
        self.stats.setdefault('commodity_daily_runs', 0)
        self.stats.setdefault('commodity_daily_success', 0)
        self.stats['commodity_daily_runs'] += 1

        success, msg, dur = await run_script('commodity_daily', [])

        if success:
            self.stats['commodity_daily_success'] += 1
            log.info(f"    ✓ Commodity Daily ({dur:.1f}с)")
        else:
            self.stats['errors'] += 1
            log.error(f"    ✗ Commodity Daily: {msg}")

        return success

    async def run_index_candles_hourly_update(self) -> bool:
        """Часовые свечи индексов (для intraday-сезонности IMOEX)."""
        log.info("  📊 Index Candles Hourly...")
        self.stats['index_candles_hourly_runs'] += 1

        success, msg, dur = await run_script('index_candles_hourly', ['--once'])

        if success:
            self.stats['index_candles_hourly_success'] += 1
            log.info(f"    ✓ Index Candles Hourly ({dur:.1f}с)")
        else:
            self.stats['errors'] += 1
            log.error(f"    ✗ Index Candles Hourly: {msg}")

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

    async def run_dividends_update(self) -> bool:
        """Запускает загрузку дивидендов с ISS MOEX"""
        log.info("  📊 Dividends Daily...")
        self.stats['dividends_daily_runs'] += 1

        success, msg, dur = await run_script('dividends_daily', ['--once', '--force'])

        if success:
            self.stats['dividends_daily_success'] += 1
            log.info(f"    ✓ Dividends Daily ({dur:.1f}с)")
        else:
            self.stats['errors'] += 1
            log.error(f"    ✗ Dividends Daily: {msg}")

        return success

    async def run_weekend_catchup(self) -> bool:
        """
        Докачивает все данные с момента последней записи в БД до текущего момента.
        Каждый скрипт сам определяет from_date по последней записи в БД.
        Запускается один раз в сутки в выходной/праздник.
        """
        log.info("=" * 60)
        log.info("📅 ВЫХОДНОЙ — докачка всех пропущенных данных (с последней записи в БД)")
        log.info("=" * 60)

        any_success = False

        # 1. 5-минутные данные (OI + свечи)
        log.info("  📊 5-минутные данные...")
        for key, args in [
            ('oi_5min', ['--once', '--force']),
            ('candles_futures', ['--once', '--force']),
            ('candles_spot', ['--once', '--force']),
        ]:
            self.stats[f'{key}_runs'] += 1
            success, msg, dur = await run_script(key, args)
            if success:
                self.stats[f'{key}_success'] += 1
                log.info(f"    ✓ {key} ({dur:.1f}с)")
                any_success = True
            else:
                self.stats['errors'] += 1
                log.error(f"    ✗ {key}: {msg}")

        # 2. Агрегация OI 5м → 60м (только докаченный период)
        gap_days = self._get_oi_hourly_gap_days()
        log.info(f"  📊 Агрегация OI 5м → 60м (последние {gap_days} дней)...")
        agg_ok = await self.run_hourly_aggregate(recent_days=gap_days)
        if agg_ok:
            any_success = True

        # 3. Дневные данные
        log.info("  📊 Дневные данные...")
        for fn in [
            self.run_daily_update,
            self.run_funds_update,
            self.run_indices_update,
            self.run_index_candles_hourly_update,
            self.run_macro_update,
            self.run_market_cap_update,
            self.run_breadth_update,
            self.run_dividends_update,
            self.run_commodity_update,
            self.run_analytics_cleanup,
        ]:
            success = await fn()
            if success:
                any_success = True

        # 4. Обновление представлений
        log.info("  🔄 Обновление представлений после докачки...")
        refresh_materialized_views()

        self.last_weekend_catchup = get_moscow_time().date()
        log.info("✅ Докачка пропущенных данных завершена")
        log.info("=" * 60)
        return any_success

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
        log.info("  --- Index Candles (intraday) ---")
        log.info(f"    Hourly: {self.stats['index_candles_hourly_success']}/{self.stats['index_candles_hourly_runs']}")
        log.info("  --- Macro ---")
        log.info(f"    Daily: {self.stats['macro_daily_success']}/{self.stats['macro_daily_runs']}")
        log.info("  --- Market Cap ---")
        log.info(f"    Daily: {self.stats['market_cap_daily_success']}/{self.stats['market_cap_daily_runs']}")
        log.info("  --- Breadth ---")
        log.info(f"    Daily: {self.stats['breadth_daily_success']}/{self.stats['breadth_daily_runs']}")
        log.info("  --- Dividends ---")
        log.info(f"    Daily: {self.stats['dividends_daily_success']}/{self.stats['dividends_daily_runs']}")
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
        log.info(f"    Commodity: {COMMODITY_UPDATE_HOUR:02d}:{COMMODITY_UPDATE_MINUTE:02d}")
        log.info(f"    Funds early: {FUNDS_EARLY_UPDATE_HOUR:02d}:{FUNDS_EARLY_UPDATE_MINUTE:02d}")
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

            # Агрегация OI 5м → 60м (заполняем все пропущенные часы при старте)
            if results.get('oi_5min'):
                gap_days = self._get_oi_hourly_gap_days()
                await self.run_hourly_aggregate(recent_days=gap_days)

            # Daily (OI + Funds + Indices + Macro + Market Cap + Breadth + Commodity + Analytics cleanup)
            await self.run_daily_update()
            await self.run_funds_update()
            await self.run_indices_update()
            await self.run_index_candles_hourly_update()
            await self.run_macro_update()
            await self.run_market_cap_update()
            await self.run_breadth_update()
            await self.run_dividends_update()
            await self.run_commodity_update()
            await self.run_analytics_cleanup()

            # Обновляем все представления после полной синхронизации
            log.info("  🔄 Финальное обновление представлений...")
            refresh_materialized_views()
        else:
            log.info(f"  ⏭️ Выходной/праздник: {reason} — запускаем докачку пропущенных данных...")
            await self.run_weekend_catchup()

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

                is_trade_day, trade_day_reason = is_trading_day()

                # === Billing hourly — РАНЬШЕ проверки is_trade_day ===
                # Подписки истекают и в выходные. expire + auto-renewal должны
                # работать каждый час 24/7, независимо от торговых часов MOEX.
                if slot_hour != self.last_billing_hourly and now.minute < 2:
                    await self.run_billing_hourly()
                    self.last_billing_hourly = slot_hour

                # === Лёгкий expire-only каждые 15 мин (сжать окно tier-gating) ===
                # На :00 покрывается часовым billing-job выше, поэтому здесь — на
                # :15/:30/:45 (now.minute >= 2 отсекает повтор сразу после часового).
                quarter_slot = (now.hour, now.minute // 15)
                if quarter_slot != self.last_expire_quarter and now.minute >= 2:
                    await self.run_expire_only()
                    self.last_expire_quarter = quarter_slot

                # === Выходной / праздник ===
                if not is_trade_day:
                    today = now.date()
                    if self.last_weekend_catchup != today:
                        log.info(f"⏰ [{now:%H:%M:%S} МСК] Выходной ({trade_day_reason}) — докачка...")
                        await self.run_weekend_catchup()
                    else:
                        log.debug(f"Выходной, докачка уже выполнена сегодня")
                    await asyncio.sleep(300)  # Проверяем раз в 5 минут
                    continue

                # === 5-минутный цикл (только в торговые часы) ===
                if is_trading:
                    if slot_5min != self.last_5min_update and now.second >= BUFFER_5MIN:
                        log.info(f"⏰ [{now:%H:%M:%S} МСК] 5-минутный цикл...")

                        results = await self.run_5min_cycle()
                        self.last_5min_update = slot_5min

                        # OI Daily — обновляем текущий день раз в час (не каждые 5 мин — экономим ресурсы)
                        if results.get('oi_5min') and now.minute < 5:
                            if slot_hour != getattr(self, '_last_oi_daily_hour', None):
                                await self.run_daily_update()
                                self._last_oi_daily_hour = slot_hour

                        # Агрегация (после 5м, если новый час и минута >= 2)
                        if results.get('oi_5min') and now.minute >= 2:
                            if slot_hour != self.last_hourly_aggregate:
                                await self.run_hourly_aggregate()
                                self.last_hourly_aggregate = slot_hour

                        # Часовые свечи индексов (для intraday-сезонности).
                        # Раз в час, когда новая часовая свеча уже закрылась.
                        if now.minute >= 2 and slot_hour != getattr(self, '_last_index_candles_hour', None):
                            await self.run_index_candles_hourly_update()
                            self._last_index_candles_hour = slot_hour
                else:
                    log.debug(f"Вне торгов: {reason}")

                # === Ранний funds-only прогон (14:30 МСК) ===
                # Cbonds публикует большинство NAV к 14 часам — ловим раньше
                # чем основной daily в 19:10, чтобы user видел свежие потоки сразу.
                if (slot_day != self.last_funds_early_update and
                        now.hour == FUNDS_EARLY_UPDATE_HOUR and
                        now.minute >= FUNDS_EARLY_UPDATE_MINUTE):
                    log.info(f"⏰ [{now:%H:%M:%S} МСК] Ранний funds update...")
                    await self.run_funds_update()
                    self.last_funds_early_update = slot_day

                # === Commodity (08:00 МСК) ===
                # US market закрывается в 23:00 EST = 07:00 МСК. К 08:00 дневные
                # close уже окончательные на Yahoo. Запускается ежедневно (включая
                # выходные — Yahoo не закрывается на выходные MOEX).
                if (slot_day != self.last_commodity_update and
                        now.hour == COMMODITY_UPDATE_HOUR and
                        now.minute >= COMMODITY_UPDATE_MINUTE):
                    log.info(f"⏰ [{now:%H:%M:%S} МСК] Commodity update...")
                    await self.run_commodity_update()
                    self.last_commodity_update = slot_day

                # === Дневное обновление (в 19:10, только в торговые дни) ===
                if (slot_day != self.last_daily_update and
                        now.hour == DAILY_UPDATE_HOUR and
                        now.minute >= DAILY_UPDATE_MINUTE):
                    log.info(f"⏰ [{now:%H:%M:%S} МСК] Дневное обновление...")
                    await self.run_daily_update()
                    await self.run_funds_update()
                    await self.run_indices_update()
                    await self.run_index_candles_hourly_update()
                    await self.run_macro_update()
                    await self.run_market_cap_update()
                    await self.run_breadth_update()
                    await self.run_dividends_update()
                    await self.run_analytics_cleanup()
                    send_data_notify("daily")
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