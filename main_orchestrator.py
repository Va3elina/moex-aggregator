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
        is_weekend_session,
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


    def is_weekend_session(check_time=None):
        # Fallback: торги выходного дня (сб/вс в торговые часы, не праздник).
        now = check_time or get_moscow_time()
        if now.weekday() < 5:
            return False, "Будний день"
        if now.date() in MOEX_HOLIDAYS:
            return False, "Праздник"
        if now.hour < TRADING_START_HOUR or now.hour >= TRADING_END_HOUR:
            return False, "Вне часов"
        return True, "Сессия выходного дня"

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
    # Календарь экспираций фьючерсов (ISS) — источник КАЛЕНДАРНОГО ролловера
    'contract_calendar': BASE_DIR / 'Candles' / 'fetch_contract_calendar.py',
    # Funds скрипты
    'funds_daily': BASE_DIR / 'Funds' / 'fetch_funds_realtime.py',
    # Indices скрипты
    'indices_daily': BASE_DIR / 'Funds' / 'fetch_indices_realtime.py',
    'index_candles_hourly': BASE_DIR / 'Funds' / 'fetch_index_candles_hourly.py',
    # Внутридневное текущее значение индексов (IMOEX/RTSI) — держит сегодняшнюю
    # строку index_data свежей для индикаторов («Сила рынка»). Гоняется в 5-мин цикле.
    'index_intraday': BASE_DIR / 'Funds' / 'fetch_index_intraday.py',
    # Macro скрипты (M2, GDP)
    'macro_daily': BASE_DIR / 'Macro' / 'fetch_macro_realtime.py',
    # Market Cap (полная капитализация рынка)
    'market_cap_daily': BASE_DIR / 'Macro' / 'fetch_market_cap.py',
    # Free-float капитализация по бумагам (MOEXBMI analytics, помесячно) —
    # знаменатель режима «От капитализации» в «Потоках по компании»
    'freefloat_cap_daily': BASE_DIR / 'Macro' / 'fetch_freefloat_cap.py',
    # Историческая база расчёта IMOEX (index_composition) — состав индекса на
    # каждый торговый день, знаменатель вселенной imoex у «Силы рынка».
    # ДОЛЖЕН идти перед breadth_daily: тот читает свежий состав из БД.
    'index_composition_daily': BASE_DIR / 'Candles' / 'fetch_index_composition.py',
    # Market Breadth (% акций выше EMA — предвычисление)
    'breadth_daily': BASE_DIR / 'Candles' / 'compute_breadth_history.py',
    # Дивиденды (загрузка с ISS + определение экс-дат)
    'dividends_daily': BASE_DIR / 'Candles' / 'fetch_dividends.py',
    # Сырьевые товары (Yahoo Finance: BRENT/GOLD/SILVER/...) — для seasonality
    'commodity_daily': BASE_DIR / 'Commodity' / 'fetch_commodity_realtime.py',
    # Карточки компаний со smart-lab: отчётность, дивиденды, акционеры, документы.
    # ⚠️ ДВА РЕЖИМА, потому что данные живут с разной скоростью. Полный обход —
    # 7 страниц на компанию, 560 запросов, раз в неделю: отчётность меняется четыре
    # раза в год. Лёгкий — 2 страницы, 160 запросов, ежедневно: дивиденды объявляют
    # внезапно, а смену акционера важно поймать быстро.
    'company_cards': BASE_DIR / 'Company' / 'fetch_company_cards.py',
    # Рёбра владения между эмитентами (страницы акционеров) → world_facts.
    # Пишет только изменившееся, поэтому лишний прогон безвреден.
    'ownership_scan': BASE_DIR / 'Company' / 'ownership_scan.py',
    # Детектор смены владения по архиву новостей → очередь ownership_signals.
    # Дёшево (один полнотекстовый запрос по GIN-индексу), поэтому ежедневно.
    'ownership_detect': BASE_DIR / 'Company' / 'ownership_detect.py',
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

# Ранний прогон indices — MOEX публикует history по MCFTR/EUR_RUB__TOM только
# ночью (T+1), вечерний 19:10 их день T не видит (остальные серии успевают —
# либо history к 19:10, либо use_candles). Утренний прогон добирает вчерашние
# строки → лаг этих серий не превышает 1 торговый день.
# UPSERT на (secid, trade_date) → безопасный повторный прогон.
INDICES_EARLY_UPDATE_HOUR = 9
INDICES_EARLY_UPDATE_MINUTE = 0

# Commodity — Yahoo Finance daily close. US market закрыт в 23:00 EST = 07:00 МСК
# (или 06:00 в DST). 08:00 МСК — гарантированно после закрытия, дневные close
# уже окончательные. UPSERT на (secid, trade_date) → безопасно если запустим
# раньше или дважды.
COMMODITY_UPDATE_HOUR = 8
COMMODITY_UPDATE_MINUTE = 0

# Карточки компаний. Полный обход — ночью с воскресенья на понедельник: он идёт
# 20+ минут и занимает одно ядро из четырёх, а в 04:00 сервер свободен и биржа
# закрыта. Лёгкий проход (дивиденды и акционеры) — ежедневно в 05:00, после
# полного, чтобы в понедельник они не спорили за источник.
CARDS_FULL_WEEKDAY = 0        # понедельник
CARDS_FULL_HOUR = 4
CARDS_LIGHT_HOUR = 5
CARDS_MINUTE = 0

# Граф владения — сразу после полного обхода карточек: он читает те же страницы
# акционеров, и к этому моменту они уже в кэше источника.
OWNERSHIP_WEEKDAY = 0
OWNERSHIP_HOUR = 6
OWNERSHIP_MINUTE = 0

# Детектор сигналов — ежедневно, сразу после лёгкого прохода карточек. Окно 7 дней
# при суточной частоте: перекрытие безвредно (повторы схлопываются), а разрыв из-за
# пропущенного прогона не теряет сигналы.
DETECT_HOUR = 5
DETECT_MINUTE = 30
DETECT_WINDOW_DAYS = 7

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
    'contract_calendar': 120,  # 2 минуты (1 bulk-запрос ISS + upsert ~566 строк)
    'funds_daily': 600,  # 10 минут
    'indices_daily': 300,  # 5 минут
    'index_candles_hourly': 600,  # 10 минут (бэкфилл с 2011 — ~25k свечей)
    'index_intraday': 60,  # 1 минута (2 HTTP-запроса ISS marketdata + upsert)
    'macro_daily': 120,  # 2 минуты
    'market_cap_daily': 120,  # 2 минуты
    # Полный обход 80 компаний измерен: 20 минут при паузе 0,5 с между запросами.
    # 45 минут — запас на повторы после таймаутов, не на «вдруг медленно».
    'company_cards': 2700,
    'ownership_scan': 900,   # 80 страниц акционеров, замер — 1,5 минуты
    'ownership_detect': 180,  # один запрос к news_archive по GIN, замер — секунды
    'freefloat_cap_daily': 300,  # 5 минут (обычно 2-4 ISS-запроса, бэкфилл дольше)
    'index_composition_daily': 300,  # 5 минут (инкремент — единицы ISS-запросов)
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

# Дневные пайплайны, которые РЕТРАИМ при транзиентном сбое (блипы ISS/Algopack):
# для них одно падение = потеря дня данных до следующих суток. 5-мин фетчеры
# (oi_5min, candles_futures/spot, futures_turnover, oi_aggregate) сюда НЕ входят —
# они и так повторяются каждые 5 минут, ретрай только добавил бы латентность.
RETRYABLE_DAILY = {
    'oi_daily', 'funds_daily', 'indices_daily', 'contract_calendar',
    'index_candles_hourly', 'macro_daily', 'market_cap_daily',
    'freefloat_cap_daily', 'index_composition_daily', 'breadth_daily',
    'dividends_daily', 'commodity_daily',
    # ⚠️ company_cards в ретраях НЕТ намеренно. Если smart-lab попросил остановиться
    # (429/403), скрипт прекращает обход сам — и автоматический повтор через 30 секунд
    # был бы ровно тем поведением, за которое банят по IP. Повтор здесь — решение
    # человека, а не оркестратора.
    'ownership_scan', 'ownership_detect',
}
RETRY_BACKOFF = [30, 120]  # сек между попытками (итого до 3 попыток)


async def run_script(script_key: str, args: List[str] = None, timeout: int = None) -> Tuple[bool, str, float]:
    """
    Обёртка над _run_script_impl: после прогона пишет heartbeat в pipeline_runs
    (единый пульс запуска для мониторинга /api/health/data). Запись best-effort —
    сбой пульса НЕ влияет на фетч.

    Для дневных пайплайнов (RETRYABLE_DAILY) при падении делает до 2 ретраев с
    backoff — транзиентный сбой источника не должен терять день данных. Heartbeat
    пишется ОДИН раз по ФИНАЛЬНОМУ исходу (монитор алертит только если не помогли
    и ретраи).
    """
    ok, msg, dur = await _run_script_impl(script_key, args, timeout)
    if not ok and script_key in RETRYABLE_DAILY:
        for i, backoff in enumerate(RETRY_BACKOFF, 1):
            log.warning(f"    ↻ {script_key} упал ({(msg or '')[:80]}) — "
                        f"ретрай {i}/{len(RETRY_BACKOFF)} через {backoff}с")
            await asyncio.sleep(backoff)
            ok, msg, dur = await _run_script_impl(script_key, args, timeout)
            if ok:
                log.info(f"    ✓ {script_key} удался с ретрая {i}")
                break
    try:
        import pipeline_heartbeat
        pipeline_heartbeat.record_pipeline_run(script_key, ok, msg, dur)
    except Exception:
        pass
    return ok, msg, dur


async def _run_script_impl(script_key: str, args: List[str] = None, timeout: int = None) -> Tuple[bool, str, float]:
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
        self.last_indices_early_update = None  # ранний indices-прогон в 09:00 (T+1-публикации ISS)
        self.last_commodity_update = None
        # Карточки и граф владения ведут свои отметки: у них своё расписание,
        # не привязанное к торговому дню.
        self.last_cards_full = None
        self.last_cards_light = None
        self.last_ownership = None
        self.last_detect = None        # ежедневный детектор сигналов владения
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
            # Free-float Cap
            'freefloat_cap_daily_runs': 0,
            'freefloat_cap_daily_success': 0,
            # Index Composition
            'index_composition_daily_runs': 0,
            'index_composition_daily_success': 0,
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

        # 3b. Внутридневное текущее значение индексов (IMOEX/RTSI). Держит
        # сегодняшнюю строку index_data свежей, чтобы «Сила рынка» показывала
        # последнее значение индекса, а не вчерашнее закрытие. Best-effort:
        # падение НЕ считается ошибкой цикла (индикатор просто покажет прежнее
        # значение), вне сессии скрипт сам пишет 0 строк и выходит успешно.
        await asyncio.sleep(2)
        self.stats.setdefault('index_intraday_runs', 0)
        self.stats.setdefault('index_intraday_success', 0)
        self.stats['index_intraday_runs'] += 1
        success, msg, dur = await run_script('index_intraday', ['--force'])
        if success:
            self.stats['index_intraday_success'] += 1
            log.info(f"    ✓ Index intraday ({dur:.1f}с)")
        else:
            log.warning(f"    ⚠️ Index intraday: {msg}")

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

    async def run_weekend_5min_cycle(self) -> dict:
        """5-мин цикл для ТОРГОВ ВЫХОДНОГО ДНЯ — урезанная версия run_5min_cycle:
        фьючерсы + OI (+ оборот) + СПОТ + карта рынка.

        СПОТ собираем и в выходные: MOEX торгует ликвидными акциями по сб/вс
        (09:55–18:50 МСК), и карта рынка должна показывать актуальный ход
        выходной сессии, а не пятничный снимок. mv_heatmap_stocks обновляем здесь
        же, чтобы /api/heatmap отдавал свежий снимок (раньше спот+вью обновлялись
        лишь раз в сутки в run_weekend_catchup ≈ в полночь, ДО начала сессии →
        суббота весь день показывала пятницу, воскресенье — субботу).

        Сезонность от этого НЕ страдает — её запросы фильтруют ISODOW 1-5
        (api/routers/seasonality.py), как и prev_close в mv_heatmap_stocks
        (DOW 1-5): эталон дневного изменения остаётся последним БУДНИМ закрытием
        (сб/вс считаются vs пятница — кумулятивный ход выходных).

        OI/фьючерсная свеча хранятся по фактической дате (=суббота) → пара
        свеча↔OI остаётся выровненной по дате."""
        results = {}
        total_start = datetime.now()

        # 1. OI 5-минутки (futoi выходного дня; tradedate=суббота)
        log.info("  📊 [выходной] OI 5м...")
        self.stats['oi_5min_runs'] += 1
        success, msg, dur = await run_script('oi_5min', ['--once', '--force'])
        results['oi_5min'] = success
        if success:
            self.stats['oi_5min_success'] += 1
            log.info(f"    ✓ OI 5м ({dur:.1f}с)")
        else:
            self.stats['errors'] += 1
            log.error(f"    ✗ OI 5м: {msg}")

        await asyncio.sleep(3)

        # 2. Candles Futures (валютные/вечные фьючерсы выходного дня; до их запуска
        #    источник отдаёт пусто → no-op, после — субботние свечи автоматически)
        log.info("  📊 [выходной] Candles Futures...")
        self.stats['candles_futures_runs'] += 1
        success, msg, dur = await run_script('candles_futures', ['--once', '--force'])
        results['candles_futures'] = success
        if success:
            self.stats['candles_futures_success'] += 1
            log.info(f"    ✓ Candles Futures ({dur:.1f}с)")
        else:
            self.stats['errors'] += 1
            log.error(f"    ✗ Candles Futures: {msg}")

        # 2b. Рублёвый оборот фьючерсов (best-effort)
        self.stats.setdefault('futures_turnover_runs', 0)
        self.stats.setdefault('futures_turnover_success', 0)
        self.stats['futures_turnover_runs'] += 1
        success, msg, dur = await run_script('futures_turnover', ['--force'])
        if success:
            self.stats['futures_turnover_success'] += 1
            log.info(f"    ✓ Futures turnover ({dur:.1f}с)")
        else:
            log.warning(f"    ⚠️ Futures turnover: {msg}")

        # 2c. Внутридневное значение индексов в выходной день. IMOEX и RTSI по
        # выходным не торгуются (fetch вернёт None → пропуск), но IMOEX2 живой —
        # держим его сегодняшнюю строку index_data свежей, чтобы рублёвая «Сила
        # рынка» рисовала верхний график и по выходным. Best-effort.
        self.stats.setdefault('index_intraday_runs', 0)
        self.stats.setdefault('index_intraday_success', 0)
        self.stats['index_intraday_runs'] += 1
        success, msg, dur = await run_script('index_intraday', ['--force'])
        if success:
            self.stats['index_intraday_success'] += 1
            log.info(f"    ✓ [выходной] Index intraday ({dur:.1f}с)")
        else:
            log.warning(f"    ⚠️ [выходной] Index intraday: {msg}")

        await asyncio.sleep(3)

        # 3. Candles Spot (акции выходной сессии MOEX; до открытия источник отдаёт
        #    пусто → no-op, в сессию — субботние/воскресные 5-мин свечи). Тот же
        #    скрипт, что и в будни (run_5min_cycle) и в run_weekend_catchup.
        log.info("  📊 [выходной] Candles Spot...")
        self.stats['candles_spot_runs'] += 1
        success, msg, dur = await run_script('candles_spot', ['--once', '--force'])
        results['candles_spot'] = success
        if success:
            self.stats['candles_spot_success'] += 1
            log.info(f"    ✓ Candles Spot ({dur:.1f}с)")
        else:
            self.stats['errors'] += 1
            log.error(f"    ✗ Candles Spot: {msg}")

        # 4. Карта рынка — обновляем mv_heatmap_stocks (CONCURRENTLY, читатели не
        #    блокируются). Снимок per-секция сам определит дату: есть 5-мин свеча
        #    за сегодня → snap=сегодня (is_live), иначе → последний торговый день
        #    (см. db/mv_heatmap_stocks.sql). Только эта вью — OI-вью на выходных
        #    не трогаем (поведение OI без изменений).
        log.info("  🔄 [выходной] Карта рынка (mv_heatmap_stocks)...")
        self.stats['views_refresh_runs'] += 1
        view_results = await asyncio.to_thread(
            refresh_materialized_views, ['mv_heatmap_stocks']
        )
        if all(r[0] for r in view_results.values()):
            self.stats['views_refresh_success'] += 1

        # NOTIFY: новые OI/свечи + обновлённая карта. mv_refresh инвалидирует
        # серверный кэш heatmap: (api/notify_listener.py) и триггерит SSE-reload
        # карты на фронте (useRealtimeData(['5min','mv_refresh'])).
        send_data_notify("5min", ["candles", "open_interest"])
        send_data_notify("mv_refresh", ["mv_heatmap_stocks"])

        total_duration = (datetime.now() - total_start).total_seconds()
        self.stats['total_duration'] += total_duration
        self.stats['cycles'] += 1
        log.info(f"  ⏱️ [выходной] Цикл завершён за {total_duration:.1f}с")
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
        except Exception as e:
            log.error("expire-only failed: %s", e, exc_info=True)
            n = 0

        # Фоллбэк: добить триал-привязки, чей редирект сломался (T-Bank AddCard
        # ErrorCode 9 не возвращает юзера на /trial-success, но карта биндится).
        # Активируем server-side через GetAddCardState. Каждые 15 мин.
        def _complete_trials():
            from api.billing.trial import complete_pending_trials
            db = SessionLocal()
            try:
                return complete_pending_trials(db)
            finally:
                db.close()
        try:
            ct = await asyncio.to_thread(_complete_trials)
            if ct.get("completed"):
                log.info("  💳 trial-fallback: активировано %d привязок", ct["completed"])
        except Exception as e:
            log.error("trial-complete fallback failed: %s", e, exc_info=True)

        # СБП-привязки: дотянуть AccountToken+BankMemberId после оплаты по
        # RequestKey (GetAddAccountQrState). Включает автопродление, либо помечает
        # подписку разовой если юзер не привязал счёт. Каждые 15 мин.
        def _resolve_sbp():
            from api.billing.service import resolve_sbp_bindings
            db = SessionLocal()
            try:
                return resolve_sbp_bindings(db)
            finally:
                db.close()
        try:
            rs = await asyncio.to_thread(_resolve_sbp)
            if rs.get("bound") or rs.get("gave_up"):
                log.info("  💳 sbp-reconcile: bound=%d gave_up=%d pending=%d",
                         rs.get("bound", 0), rs.get("gave_up", 0), rs.get("pending", 0))
        except Exception as e:
            log.error("sbp-reconcile failed: %s", e, exc_info=True)

        return n

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

        # T-1 напоминания об окончании пробного периода (best practice, не закон).
        # Идемпотентно через trial_reminder_sent; no-op если триалов нет / флаг выкл.
        def _do_trial_reminders():
            from api.billing.trial import send_trial_reminders
            db = SessionLocal()
            try:
                return send_trial_reminders(db)
            finally:
                db.close()
        try:
            tr = await asyncio.to_thread(_do_trial_reminders)
            result["trial_reminders_sent"] = tr.get("sent", 0)
        except Exception as e:
            log.error("billing.send_trial_reminders failed: %s", e, exc_info=True)

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

    async def run_contract_calendar_update(self) -> bool:
        """Обновляет календарь экспираций фьючерсов (futures_contracts из ISS).

        Источник КАЛЕНДАРНОГО ролловера. Дёшево (1 bulk-запрос ISS + upsert).
        Запускается ПЕРЕД циклом свечей, чтобы фетчер выбрал фронт по lsttrade.
        """
        log.info("  📅 Контракт-календарь...")
        success, msg, dur = await run_script('contract_calendar', ['--once', '--force'])
        if success:
            log.info(f"    ✓ Контракт-календарь ({dur:.1f}с)")
        else:
            self.stats['errors'] += 1
            log.error(f"    ✗ Контракт-календарь: {msg}")
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

    async def run_company_cards(self, light: bool) -> bool:
        """Карточки компаний со smart-lab.

        light=True — только дивиденды и акционеры (2 страницы из 7, ~160 запросов);
        light=False — полный обход (~560 запросов, 20+ минут).

        ⚠️ Скрипт сам прекращает обход, если источник ответил 429/403, и сам пишет
        об этом в Telegram. Оркестратору остаётся не мешать: ретраев тут нет.
        """
        имя = "Карточки (лёгкий)" if light else "Карточки (полный)"
        log.info(f"  📇 {имя}...")
        self.stats.setdefault('company_cards_runs', 0)
        self.stats.setdefault('company_cards_success', 0)
        self.stats['company_cards_runs'] += 1

        args = ['--once']
        if light:
            args.append('--light')
        success, msg, dur = await run_script('company_cards', args)

        if success:
            self.stats['company_cards_success'] += 1
            log.info(f"    ✓ {имя} ({dur:.1f}с)")
        else:
            self.stats['errors'] += 1
            log.error(f"    ✗ {имя}: {msg}")
        return success

    async def run_ownership_detect(self) -> bool:
        """Детектор смены владения: наполняет очередь поводов посмотреть.

        ⚠️ Ничего не пишет в граф. Автомат предлагает, человек утверждает — попытка
        выводить рёбра из текста новостей автоматически уже давала уверенный мусор.
        """
        log.info("  🔎 Сигналы смены владения...")
        self.stats.setdefault('ownership_detect_runs', 0)
        self.stats['ownership_detect_runs'] += 1
        success, msg, dur = await run_script(
            'ownership_detect', ['--days', str(DETECT_WINDOW_DAYS)])
        if success:
            log.info(f"    ✓ Сигналы ({dur:.1f}с)")
        else:
            self.stats['errors'] += 1
            log.error(f"    ✗ Сигналы: {msg}")
        return success

    async def run_ownership_scan(self) -> bool:
        """Рёбра владения между эмитентами → world_facts."""
        log.info("  🕸  Граф владения...")
        self.stats.setdefault('ownership_runs', 0)
        self.stats.setdefault('ownership_success', 0)
        self.stats['ownership_runs'] += 1

        success, msg, dur = await run_script('ownership_scan', ['--all', '--load'])

        if success:
            self.stats['ownership_success'] += 1
            log.info(f"    ✓ Граф владения ({dur:.1f}с)")
        else:
            self.stats['errors'] += 1
            log.error(f"    ✗ Граф владения: {msg}")
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

    async def run_index_composition_update(self) -> bool:
        """Догоняет историю базы расчёта IMOEX (index_composition).

        Запускать ДО run_breadth_update: широта по вселенной imoex берёт
        состав индекса на каждую дату именно оттуда.
        """
        log.info("  🧾 Index Composition Daily...")
        self.stats['index_composition_daily_runs'] += 1

        success, msg, dur = await run_script('index_composition_daily', ['--once'])

        if success:
            self.stats['index_composition_daily_success'] += 1
            log.info(f"    ✓ Index Composition Daily ({dur:.1f}с)")
        else:
            self.stats['errors'] += 1
            log.error(f"    ✗ Index Composition Daily: {msg}")

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
            self.run_contract_calendar_update,
            self.run_daily_update,
            self.run_funds_update,
            self.run_indices_update,
            self.run_index_candles_hourly_update,
            self.run_macro_update,
            self.run_market_cap_update,
            self.run_index_composition_update,
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

    async def run_freefloat_cap_update(self) -> bool:
        """Free-float капитализация по бумагам (MOEXBMI). Скрипт идемпотентен:
        дозаполняет пропущенные месяцы и освежает текущий, --force не нужен."""
        log.info("  📊 Free-float Cap...")
        self.stats['freefloat_cap_daily_runs'] += 1

        success, msg, dur = await run_script('freefloat_cap_daily', ['--once'])

        if success:
            self.stats['freefloat_cap_daily_success'] += 1
            log.info(f"    ✓ Free-float Cap ({dur:.1f}с)")
        else:
            self.stats['errors'] += 1
            log.error(f"    ✗ Free-float Cap: {msg}")

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
        log.info("  --- Free-float Cap ---")
        log.info(f"    Daily: {self.stats['freefloat_cap_daily_success']}/{self.stats['freefloat_cap_daily_runs']}")
        log.info("  --- Index Composition ---")
        log.info(f"    Daily: {self.stats['index_composition_daily_success']}/{self.stats['index_composition_daily_runs']}")
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

        # Календарь экспираций фьючерсов — ДО первого цикла свечей, чтобы фетчер
        # выбрал фронт-контракт по lsttrade (а не по объёму).
        await self.run_contract_calendar_update()

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
            await self.run_freefloat_cap_update()
            await self.run_index_composition_update()
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

                    # === ТОРГИ ВЫХОДНОГО ДНЯ: фьючерсы+OI+спот 5-мин цикл в часы сессии ===
                    # Спот собираем и в выходные (карта рынка показывает живой ход
                    # сб/вс — см. run_weekend_5min_cycle). В часы сессии опрашиваем
                    # часто (1с, как будни) для точного попадания в 5-мин слот; вне
                    # сессии — раз в 5 минут.
                    wknd_session, wknd_reason = is_weekend_session(now)
                    if wknd_session:
                        if slot_5min != self.last_5min_update and now.second >= BUFFER_5MIN:
                            log.info(f"⏰ [{now:%H:%M:%S} МСК] Выходная сессия — фьючерсы+OI+спот...")
                            results = await self.run_weekend_5min_cycle()
                            self.last_5min_update = slot_5min
                            # Агрегация OI 5м→60м (новый час, минута >= 2)
                            if results.get('oi_5min') and now.minute >= 2:
                                if slot_hour != self.last_hourly_aggregate:
                                    await self.run_hourly_aggregate()
                                    self.last_hourly_aggregate = slot_hour
                        await asyncio.sleep(1)
                    else:
                        await asyncio.sleep(300)  # вне выходной сессии — реже
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

                # === Ранний indices-прогон (09:00 МСК) ===
                # MCFTR/EUR_RUB__TOM публикуются в ISS history ночью (T+1) —
                # добираем утром, не дожидаясь вечернего 19:10. В выходные
                # скрипт сам скипает прогон (--once + is_trading_day).
                if (slot_day != self.last_indices_early_update and
                        now.hour == INDICES_EARLY_UPDATE_HOUR and
                        now.minute >= INDICES_EARLY_UPDATE_MINUTE):
                    log.info(f"⏰ [{now:%H:%M:%S} МСК] Ранний indices update...")
                    await self.run_indices_update()
                    self.last_indices_early_update = slot_day

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

                # === Карточки компаний ===
                # Полный обход — понедельник 04:00, лёгкий — ежедневно 05:00.
                # ⚠️ Ночью и в выходные тоже: smart-lab не биржа, он не закрывается,
                # а мы в это время никому не мешаем — ни торгам, ни себе по CPU.
                if (slot_day != self.last_cards_full and
                        now.weekday() == CARDS_FULL_WEEKDAY and
                        now.hour == CARDS_FULL_HOUR and now.minute >= CARDS_MINUTE):
                    log.info(f"⏰ [{now:%H:%M:%S} МСК] Карточки компаний, полный обход...")
                    await self.run_company_cards(light=False)
                    self.last_cards_full = slot_day

                if (slot_day != self.last_cards_light and
                        now.hour == CARDS_LIGHT_HOUR and now.minute >= CARDS_MINUTE):
                    log.info(f"⏰ [{now:%H:%M:%S} МСК] Карточки компаний, лёгкий проход...")
                    await self.run_company_cards(light=True)
                    self.last_cards_light = slot_day

                if (slot_day != self.last_detect and
                        now.hour == DETECT_HOUR and now.minute >= DETECT_MINUTE):
                    log.info(f"⏰ [{now:%H:%M:%S} МСК] Сигналы смены владения...")
                    await self.run_ownership_detect()
                    self.last_detect = slot_day

                if (slot_day != self.last_ownership and
                        now.weekday() == OWNERSHIP_WEEKDAY and
                        now.hour == OWNERSHIP_HOUR and now.minute >= OWNERSHIP_MINUTE):
                    log.info(f"⏰ [{now:%H:%M:%S} МСК] Граф владения...")
                    await self.run_ownership_scan()
                    self.last_ownership = slot_day

                # === Дневное обновление (в 19:10, только в торговые дни) ===
                if (slot_day != self.last_daily_update and
                        now.hour == DAILY_UPDATE_HOUR and
                        now.minute >= DAILY_UPDATE_MINUTE):
                    log.info(f"⏰ [{now:%H:%M:%S} МСК] Дневное обновление...")
                    await self.run_contract_calendar_update()
                    await self.run_daily_update()
                    await self.run_funds_update()
                    await self.run_indices_update()
                    await self.run_index_candles_hourly_update()
                    await self.run_macro_update()
                    await self.run_market_cap_update()
                    await self.run_freefloat_cap_update()
                    await self.run_index_composition_update()
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