#!/usr/bin/env python3
"""
Полная капитализация фондового рынка РФ.

Источники:
- SmartLab (ежедневно): парсинг HTML https://smart-lab.ru/q/shares_fundamental/?field=market_cap
- GuruFocus/WebArchive (исторические, одноразовый импорт): 321 точка, 1997-2024

Хранение: macro_data, indicator = 'MARKET_CAP_TOTAL', значения в млрд руб.

Использование:
  python Macro/fetch_market_cap.py --once               # Парсит SmartLab, сохраняет сегодня
  python Macro/fetch_market_cap.py --once --force        # Принудительно (даже в выходной)
  python Macro/fetch_market_cap.py --import-historical FILE  # Импорт GuruFocus HTML
  python Macro/fetch_market_cap.py --check               # Проверка целостности данных
"""

import asyncio
import argparse
import logging
import sys
import os
import re
import json
import statistics
import urllib.request
from pathlib import Path
from datetime import datetime, date, timedelta, timezone
from typing import List, Tuple, Dict

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# === Пути ===
BASE_DIR = Path(__file__).parent
PROJECT_DIR = BASE_DIR.parent

load_dotenv(PROJECT_DIR / ".env")
DB_URL = os.getenv("DB_URL")

LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

# === Лимиты валидации ===
CAP_MIN_VALUE = 10          # Минимум 10 млрд руб (1998 кризис)
CAP_MAX_VALUE = 200_000     # Максимум 200 трлн руб (200 000 млрд)
CAP_MIN_DATE = date(1997, 1, 1)
CAP_MAX_JUMP_PCT = 50       # Максимальный скачок между соседними точками (%)
# Капа одной бумаги, которой нет в нашем реестре instruments, выше которой считаем её
# ошибкой источника и вычитаем из «Всего» (см. subtract_bogus_company_caps). Инцидент
# 2026-06-17: новому IPO INCB SmartLab присвоил 8 трлн руб → раздул total на ~15%.
UNKNOWN_TICKER_CAP_CEILING = 1500  # млрд руб: больше любого реального нового IPO, ниже топ-эшелона

# === SmartLab URL ===
SMARTLAB_URL = "https://smart-lab.ru/q/shares_fundamental/?field=market_cap"

# === ISS TQBR — для gap-safe расчёта total = Σ(close × ISSUESIZE) из свечей ===
ISS_TQBR_URL = ("https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/"
                "securities.json?iss.meta=off&iss.only=securities")

# === Календарь MOEX ===
sys.path.insert(0, str(PROJECT_DIR))
try:
    from moex_calendar import get_moscow_time, is_trading_day
except ImportError:
    def get_moscow_time():
        return datetime.utcnow() + timedelta(hours=3)
    def is_trading_day(check_date=None):
        check = check_date or get_moscow_time().date()
        if check.weekday() in [5, 6]:
            return False, "Выходной"
        return True, "Торговый день"


# ======================================================================
#                         ЛОГИРОВАНИЕ
# ======================================================================

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

    if sys.platform == 'win32':
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(console_fmt)
    root.addHandler(ch)

    fh_all = logging.FileHandler(
        LOG_DIR / f"market_cap_{datetime.now():%Y%m%d}.log",
        encoding='utf-8'
    )
    fh_all.setLevel(logging.DEBUG)
    fh_all.setFormatter(detailed_fmt)
    root.addHandler(fh_all)

    fh_err = logging.FileHandler(
        LOG_DIR / f"market_cap_errors_{datetime.now():%Y%m%d}.log",
        encoding='utf-8'
    )
    fh_err.setLevel(logging.WARNING)
    fh_err.setFormatter(detailed_fmt)
    root.addHandler(fh_err)

    return logging.getLogger(__name__)


log = setup_logging()


# ======================================================================
#                         БАЗА ДАННЫХ
# ======================================================================

def get_engine():
    if not DB_URL:
        raise ValueError("DB_URL не установлен в .env")
    return create_engine(DB_URL, connect_args={"ssl_context": False})


def ensure_table(engine):
    """Убедиться что таблицы macro/macro_data/stock_market_cap существуют и зарегистрировать индикатор."""
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS macro (
                id SERIAL PRIMARY KEY,
                indicator VARCHAR(50) UNIQUE NOT NULL,
                name VARCHAR(255) NOT NULL,
                frequency VARCHAR(20) NOT NULL,
                source VARCHAR(100),
                start_date DATE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS macro_data (
                id SERIAL PRIMARY KEY,
                indicator VARCHAR(50) NOT NULL,
                period_date DATE NOT NULL,
                value NUMERIC(24, 2) NOT NULL,
                source VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(indicator, period_date)
            )
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_macro_data_indicator_date
            ON macro_data(indicator, period_date)
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS stock_market_cap (
                id SERIAL PRIMARY KEY,
                sec_id VARCHAR(20) NOT NULL,
                period_date DATE NOT NULL,
                market_cap NUMERIC(24, 2) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(sec_id, period_date)
            )
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_stock_market_cap_secid
            ON stock_market_cap(sec_id, period_date DESC)
        """))
        conn.execute(text("""
            INSERT INTO macro (indicator, name, frequency, source)
            VALUES ('MARKET_CAP_TOTAL', 'Полная капитализация фондового рынка РФ', 'daily', 'SMARTLAB')
            ON CONFLICT (indicator) DO UPDATE SET
                name = EXCLUDED.name,
                frequency = EXCLUDED.frequency,
                source = EXCLUDED.source
        """))
        conn.commit()


# ======================================================================
#                         ВАЛИДАЦИЯ
# ======================================================================

def validate_value(value: float) -> Tuple[bool, str]:
    if value is None:
        return False, "значение None"
    if value <= 0:
        return False, f"отрицательное или нулевое: {value}"
    if value < CAP_MIN_VALUE:
        return False, f"слишком мало: {value} < {CAP_MIN_VALUE} млрд"
    if value > CAP_MAX_VALUE:
        return False, f"слишком велико: {value} > {CAP_MAX_VALUE} млрд"
    return True, "OK"


def validate_date(period_date: date) -> Tuple[bool, str]:
    today = date.today()
    if period_date > today + timedelta(days=7):
        return False, f"дата в будущем: {period_date}"
    if period_date < CAP_MIN_DATE:
        return False, f"дата до {CAP_MIN_DATE}: {period_date}"
    return True, "OK"


def validate_series_jumps(data: List[Tuple[date, float]]) -> List[str]:
    warnings = []
    sorted_data = sorted(data, key=lambda x: x[0])
    for i in range(1, len(sorted_data)):
        prev_date, prev_val = sorted_data[i - 1]
        curr_date, curr_val = sorted_data[i]
        if prev_val == 0:
            continue
        change_pct = abs(curr_val - prev_val) / prev_val * 100
        if change_pct > CAP_MAX_JUMP_PCT:
            warnings.append(
                f"Скачок {change_pct:.1f}% между {prev_date} ({prev_val:,.0f}) "
                f"и {curr_date} ({curr_val:,.0f})"
            )
    return warnings


# ======================================================================
#               SmartLab: ежедневная капитализация
# ======================================================================

def parse_per_company_caps(html_clean: str) -> List[Tuple[str, float]]:
    """
    Парсит HTML SmartLab и извлекает капитализацию по каждой компании.

    Формат строки:
        <td>1</td>
        <td><a href="/forum/SBER">Сбербанк</a></td>
        <td>SBER</td>
        <td>...</td>  <!-- chart icon -->
        <td>...</td>  <!-- fundamental icon -->
        <td><strong> 7 151 </strong></td>  <!-- market cap -->

    Возвращает список (ticker, market_cap_bln).
    """
    results = []
    rows = re.findall(r'<tr[^>]*>(.*?)</tr>', html_clean, re.DOTALL)

    for row in rows:
        # Ищем строки с тикерами (ссылка на /q/TICKER, но не на shares_fundamental)
        if '/q/' not in row or 'shares_fundamental' in row:
            continue

        cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL)
        if len(cells) < 6:
            continue

        # cells[2] = тикер (plain text)
        ticker = re.sub(r'<[^>]+>', '', cells[2]).strip()
        if not ticker or not re.match(r'^[A-Za-z0-9]+$', ticker):
            continue

        # cells[5] = капитализация в <strong>, убираем HTML-теги
        cap_text = re.sub(r'<[^>]+>', '', cells[5]).strip()
        cap_match = re.search(r'([\d][\d\s]*)', cap_text)
        if not cap_match:
            continue

        raw_cap = cap_match.group(1).strip().replace(' ', '')
        if not raw_cap:
            continue

        try:
            cap_value = float(raw_cap)
            if cap_value > 0:
                results.append((ticker, cap_value))
        except ValueError:
            continue

    return results


def save_per_company_caps(engine, caps: List[Tuple[str, float]]) -> int:
    """Сохраняет капитализацию по компаниям, только для тикеров из instruments."""
    # Загружаем список нужных тикеров
    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT sec_id FROM instruments WHERE type = 'stock'"
        )).fetchall()
    stock_tickers = {r[0] for r in rows}

    # Маппинг привилегированных акций к SmartLab тикерам (SmartLab показывает одну капитализацию)
    # SBERP → SBER, SNGSP → SNGS, TATNP → TATN, TRNFP → TRNF
    pref_to_smartlab = {"TRNFP": "TRNF"}  # Транснефть: в instruments только TRNFP
    for t in stock_tickers:
        if t.endswith('P') and t[:-1] not in pref_to_smartlab:
            common = t[:-1]
            pref_to_smartlab[t] = common

    # Строим словарь тикер→капитализация
    cap_by_ticker = {ticker: cap for ticker, cap in caps}

    today = date.today()
    saved = 0

    with engine.connect() as conn:
        for ticker in stock_tickers:
            # Для привилегированных берём капитализацию обыкновенных
            lookup = pref_to_smartlab.get(ticker, ticker)
            cap_value = cap_by_ticker.get(lookup)
            if cap_value is None:
                continue
            conn.execute(text("""
                INSERT INTO stock_market_cap (sec_id, period_date, market_cap)
                VALUES (:sec_id, :pd, :cap)
                ON CONFLICT (sec_id, period_date) DO UPDATE SET
                    market_cap = EXCLUDED.market_cap,
                    created_at = CURRENT_TIMESTAMP
            """), {"sec_id": ticker, "pd": today, "cap": cap_value})
            saved += 1
        conn.commit()

    log.info(f"  Капитализация по компаниям: {saved}/{len(stock_tickers)} сохранено (из {len(caps)} на SmartLab)")
    return saved


def subtract_bogus_company_caps(engine, total: float, per_company: List[Tuple[str, float]]) -> float:
    """Вычитает из «Всего» капу бумаг с явно ошибочной капитализацией.

    SmartLab изредка присваивает бумаге абсурдную капу, и она попадает в строку «Всего».
    Инцидент 2026-06-17: новому IPO «Инкаб Холдинг» (INCB) присвоено 8 трлн руб (больше
    Сбербанка) → total раздулся на ~15% (60.8 против 52.9 трлн), индикатор Баффетта дал
    ложный всплеск на 17-18 июня, пока 19-го SmartLab не убрал бумагу.

    Жёсткий потолок по величине не годится (Сбер уже ~7 трлн), поэтому ловим бумагу,
    которой НЕТ в нашем реестре instruments И при этом её капа в топ-эшелоне
    (> UNKNOWN_TICKER_CAP_CEILING): свежее IPO столько стоить не может (реальный Инкаб
    ~30 млрд). Такую капу вычитаем из «Всего». Бумаги, которые мы просто не отслеживаем
    (мелкий неликвид с капой ниже потолка), остаются в total как часть рынка."""
    with engine.connect() as conn:
        known = {r[0] for r in conn.execute(text("SELECT sec_id FROM instruments")).fetchall()}
    # SmartLab показывает обыкновенные тикеры; добавляем их для бумаг, что у нас только префом
    known |= {t[:-1] for t in list(known) if t.endswith('P')}
    known.add('TRNF')  # Транснефть: в instruments только TRNFP, на SmartLab показана как TRNF

    adjusted = total
    for ticker, cap in per_company:
        if cap > UNKNOWN_TICKER_CAP_CEILING and ticker not in known:
            log.error(f"  Аномальная капа неизвестной бумаги {ticker}={cap:,.0f} млрд "
                      f"(нет в instruments, > {UNKNOWN_TICKER_CAP_CEILING}) — вычитаю из «Всего», "
                      f"вероятна ошибка источника")
            adjusted -= cap
    if adjusted != total:
        log.warning(f"  «Всего» скорректировано: {total:,.0f} → {adjusted:,.0f} млрд")
    return adjusted


def fetch_from_smartlab(engine) -> int:
    """Парсит SmartLab и сохраняет капитализацию (общую + по компаниям)."""
    log.info("Загрузка капитализации с SmartLab...")
    log.debug(f"  URL: {SMARTLAB_URL}")

    try:
        req = urllib.request.Request(SMARTLAB_URL, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        })
        resp = urllib.request.urlopen(req, timeout=30)
        status = resp.getcode()
        log.info(f"  HTTP статус: {status}")

        if status != 200:
            log.error(f"  Неожиданный HTTP статус: {status}")
            return 0

        html = resp.read().decode('utf-8', errors='replace')
        log.info(f"  HTML получен: {len(html):,} байт")

        # Нормализуем HTML: заменяем &nbsp; на пробелы
        html_clean = html.replace('&nbsp;', ' ').replace('\xa0', ' ')

        # === Общая капитализация ===
        # Формат SmartLab:
        #   <td ...>Всего:</td>
        #   <td><strong> 54 273 </strong></td>
        # Ищем "Всего:" и далее первое число в следующих тегах
        pattern = r'Всего:</td>\s*<td[^>]*>\s*(?:<[^>]+>\s*)*([\d\s]+)\s*(?:</[^>]+>)*'
        match = re.search(pattern, html_clean, re.DOTALL)

        if not match:
            # Фолбек: "Всего" + число на той же или следующей строке
            pattern2 = r'Всего[:\s]*?(?:<[^>]*>)*\s*([\d\s]{3,})'
            match = re.search(pattern2, html_clean, re.DOTALL)

        if not match:
            log.error("  Не найдена строка 'Всего' с числом в HTML SmartLab")
            return 0

        raw_number = match.group(1).strip()
        # Убираем пробелы из числа: "54 273" -> "54273"
        cap_value = float(raw_number.replace(' ', ''))

        log.info(f"  Найдено: '{match.group(0).strip()}'")
        log.info(f"  Капитализация: {cap_value:,.0f} млрд руб")

        # Капитализацию по компаниям парсим заранее: она нужна, чтобы вычистить из «Всего»
        # ошибочные капы отдельных бумаг (см. subtract_bogus_company_caps) ДО валидации и сейва.
        per_company = parse_per_company_caps(html_clean)
        cap_value = subtract_bogus_company_caps(engine, cap_value, per_company)

        # Валидация диапазона
        ok, msg = validate_value(cap_value)
        if not ok:
            log.error(f"  Валидация не пройдена: {msg}")
            return 0

        today = date.today()

        # Защита от кривого снимка SmartLab: сверяем с последним известным значением.
        # Полная капа за день так не ходит — грубый скачок (>30%) почти всегда ошибка
        # страницы/парса → НЕ сохраняем (иначе кривой день отравляет и серию, и
        # калибровку gap-fill: инцидент 2026-06-17, SmartLab отдал 60.8 трлн против
        # 52.9 накануне при падающем рынке → «забор» в графике кап/ВВП). Умеренный
        # скачок (>15%) сохраняем, но громко логируем для ручной проверки.
        with engine.connect() as conn:
            prev = conn.execute(text("""
                SELECT value FROM macro_data
                WHERE indicator = 'MARKET_CAP_TOTAL' AND period_date < :pd
                ORDER BY period_date DESC LIMIT 1
            """), {"pd": today}).fetchone()
        if prev and float(prev[0]) > 0:
            jump = abs(cap_value - float(prev[0])) / float(prev[0]) * 100
            if jump > 30:
                log.error(f"  Скачок {jump:.1f}% vs {float(prev[0]):,.0f} млрд — похоже на ошибку, НЕ сохраняем")
                return 0
            if jump > 15:
                log.warning(f"  Скачок {jump:.1f}% vs {float(prev[0]):,.0f} млрд — сохраняю, но проверь вручную")

        # Сохраняем общую
        with engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO macro_data (indicator, period_date, value, source)
                VALUES ('MARKET_CAP_TOTAL', :pd, :val, 'SMARTLAB')
                ON CONFLICT (indicator, period_date) DO UPDATE SET
                    value = EXCLUDED.value, source = EXCLUDED.source,
                    created_at = CURRENT_TIMESTAMP
            """), {"pd": today, "val": cap_value})
            conn.commit()

        log.info(f"  Сохранено: {today} = {cap_value:,.0f} млрд руб")

        # === Капитализация по компаниям (уже распарсено выше) ===
        if per_company:
            save_per_company_caps(engine, per_company)
        else:
            log.warning("  Не удалось распарсить капитализацию по компаниям")

        return 1

    except urllib.error.URLError as e:
        log.error(f"  Ошибка сети: {e}")
        return 0
    except Exception as e:
        log.error(f"  Ошибка загрузки: {e}", exc_info=True)
        return 0


# ======================================================================
#     GuruFocus/WebArchive: импорт исторических данных
# ======================================================================

def import_historical(engine, html_path: str) -> int:
    """Импорт исторической капитализации из сохранённого HTML GuruFocus."""
    if not os.path.exists(html_path):
        log.error(f"Файл не найден: {html_path}")
        return 0

    file_size = os.path.getsize(html_path)
    log.info(f"Импорт из {html_path} ({file_size:,} байт)...")

    try:
        with open(html_path, 'r', encoding='utf-8', errors='replace') as f:
            content = f.read()
    except Exception as e:
        log.error(f"  Не удалось прочитать файл: {e}")
        return 0

    # Ищем серию "Russia Total Market Cap (Billion, National Currency)"
    marker = "Russia Total Market Cap (Billion, National Currency)"
    idx = content.find(marker)
    if idx < 0:
        log.error(f"  Не найдена серия '{marker}' в HTML")
        return 0

    # Ищем 'data :' после маркера и извлекаем JSON-массив
    data_prefix = content.find('data :', idx)
    if data_prefix < 0:
        log.error("  Не найден блок 'data :' после маркера")
        return 0

    # Находим начало массива [[
    i = data_prefix + len('data :')
    while i < len(content) and content[i] != '[':
        i += 1
    if i >= len(content):
        log.error("  Не найдено начало массива данных")
        return 0

    # Находим конец массива, считая скобки
    start = i
    bracket_count = 0
    while i < len(content):
        if content[i] == '[':
            bracket_count += 1
        elif content[i] == ']':
            bracket_count -= 1
            if bracket_count == 0:
                break
        i += 1
    end = i + 1

    data_str = content[start:end]

    try:
        raw_data = json.loads(data_str)
    except json.JSONDecodeError as e:
        log.error(f"  Ошибка парсинга JSON: {e}")
        return 0

    log.info(f"  Распарсено точек: {len(raw_data)}")

    # Конвертируем [timestamp_ms, value_bln] -> [(date, value)]
    data = []
    validation_errors = []

    for item in raw_data:
        if not isinstance(item, (list, tuple)) or len(item) != 2:
            validation_errors.append(f"  Некорректный формат: {item}")
            continue

        ts_ms, value = item
        try:
            dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
            period_date = dt.date()
            value = float(value)

            ok, msg = validate_date(period_date)
            if not ok:
                validation_errors.append(f"  Дата: {msg}")
                continue

            ok, msg = validate_value(value)
            if not ok:
                validation_errors.append(f"  Значение {period_date}: {msg}")
                continue

            data.append((period_date, value))
        except (ValueError, TypeError, OSError) as e:
            validation_errors.append(f"  Ошибка парсинга: {item}: {e}")
            continue

    if not data:
        log.error("  Не удалось распарсить ни одной записи!")
        for err in validation_errors[:10]:
            log.error(err)
        return 0

    data.sort(key=lambda x: x[0])
    log.info(f"  Валидных точек: {len(data)} ({data[0][0]} - {data[-1][0]})")

    if validation_errors:
        log.warning(f"  Ошибок валидации: {len(validation_errors)}")
        for err in validation_errors[:5]:
            log.warning(err)

    # Проверка на аномальные скачки
    jumps = validate_series_jumps(data)
    if jumps:
        log.warning(f"  Аномальных скачков: {len(jumps)}")
        for j in jumps[:5]:
            log.warning(f"    {j}")

    # Получаем текущее состояние БД
    with engine.connect() as conn:
        r = conn.execute(text(
            "SELECT COUNT(*) FROM macro_data WHERE indicator = 'MARKET_CAP_TOTAL'"
        )).fetchone()
        db_count_before = r[0] if r else 0

    # Upsert
    inserted = 0
    errors = 0
    with engine.connect() as conn:
        for period_date, value in data:
            try:
                conn.execute(text("""
                    INSERT INTO macro_data (indicator, period_date, value, source)
                    VALUES ('MARKET_CAP_TOTAL', :pd, :val, 'GURUFOCUS')
                    ON CONFLICT (indicator, period_date) DO UPDATE SET
                        value = EXCLUDED.value, source = EXCLUDED.source,
                        created_at = CURRENT_TIMESTAMP
                """), {"pd": period_date, "val": value})
                inserted += 1
            except Exception as e:
                errors += 1
                log.error(f"  Ошибка вставки {period_date}: {e}")
        conn.commit()

    # Верификация
    with engine.connect() as conn:
        r = conn.execute(text(
            "SELECT COUNT(*) FROM macro_data WHERE indicator = 'MARKET_CAP_TOTAL'"
        )).fetchone()
        db_count_after = r[0] if r else 0

    log.info(f"  Итог: {inserted} upserted, {errors} ошибок")
    log.info(f"  В БД: было {db_count_before}, стало {db_count_after}")

    return inserted


# ======================================================================
#                      ПРОВЕРКА ЦЕЛОСТНОСТИ
# ======================================================================

def check_market_cap(engine) -> Dict:
    """Проверить MARKET_CAP_TOTAL на целостность."""
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT period_date, value, source FROM macro_data
            WHERE indicator = 'MARKET_CAP_TOTAL'
            ORDER BY period_date
        """)).fetchall()

    if not rows:
        return {'status': 'empty', 'total': 0, 'first': None, 'last': None}

    dates = [r[0] for r in rows]
    values = [float(r[1]) for r in rows]
    sources = [r[2] for r in rows]

    # Считаем по источникам
    source_counts = {}
    for s in sources:
        source_counts[s] = source_counts.get(s, 0) + 1

    # Проверка на большие дыры (> 60 дней)
    gaps = []
    for i in range(1, len(dates)):
        diff = (dates[i] - dates[i - 1]).days
        if diff > 60:
            gaps.append({
                'from': dates[i - 1],
                'to': dates[i],
                'days': diff
            })

    # Аномальные скачки
    jump_warnings = validate_series_jumps(list(zip(dates, values)))

    return {
        'status': 'ok' if not gaps and not jump_warnings else 'issues',
        'total': len(dates),
        'first': dates[0],
        'last': dates[-1],
        'last_value': values[-1],
        'source_counts': source_counts,
        'gaps': gaps,
        'jump_warnings': jump_warnings,
    }


def print_check_report(engine):
    """Вывести полный отчёт о целостности MARKET_CAP_TOTAL."""
    sep = '=' * 60
    log.info(sep)
    log.info("ПРОВЕРКА ЦЕЛОСТНОСТИ MARKET_CAP_TOTAL")
    log.info(sep)

    result = check_market_cap(engine)

    if result['status'] == 'empty':
        log.warning("  НЕТ ДАННЫХ в БД!")
        log.info(sep)
        return result

    log.info(f"  Записей: {result['total']}")
    log.info(f"  Период: {result['first']} - {result['last']}")
    log.info(f"  Последнее значение: {result['last_value']:,.0f} млрд руб")
    log.info(f"  Источники: {result['source_counts']}")

    if result['gaps']:
        log.warning(f"  ДЫРЫ: {len(result['gaps'])} пропусков > 60 дней:")
        for g in result['gaps']:
            log.warning(f"    {g['from']} -> {g['to']} ({g['days']} дней)")
    else:
        log.info("  Больших дыр нет (< 60 дней)")

    if result.get('jump_warnings'):
        log.warning(f"  АНОМАЛИИ: {len(result['jump_warnings'])} скачков:")
        for w in result['jump_warnings']:
            log.warning(f"    {w}")
    else:
        log.info("  Аномальных скачков нет")

    # Актуальность
    today = date.today()
    age = (today - result['last']).days
    if age > 7:
        log.warning(f"  АКТУАЛЬНОСТЬ: последняя запись {result['last']} ({age} дней назад) — УСТАРЕЛА")
    else:
        log.info(f"  АКТУАЛЬНОСТЬ: последняя запись {result['last']} ({age} дней назад) — OK")

    log.info(sep)
    return result


# ======================================================================
#                              MAIN
# ======================================================================

def fetch_issuesize() -> dict:
    """ISSUESIZE (общее число ВЫПУЩЕННЫХ акций — включая заблокированные, они тоже
    выпущенные) по бумагам TQBR из ISS. База для gap-safe расчёта total."""
    req = urllib.request.Request(ISS_TQBR_URL, headers={"User-Agent": "Mozilla/5.0 (compatible; FrameBot/1.0)"})
    with urllib.request.urlopen(req, timeout=30) as r:
        d = json.load(r)
    c = d["securities"]["columns"]
    si, ii = c.index("SECID"), c.index("ISSUESIZE")
    out = {}
    for row in d["securities"]["data"]:
        if row[ii]:
            out[row[si]] = float(row[ii])
    return out


def compute_total_from_candles(engine, days: int = 45) -> int:
    """Gap-safe fallback для MARKET_CAP_TOTAL: total = Σ(close × ISSUESIZE)/1e9 из
    дневных свечей, пересчёт скользящего окна `days` — пропущенный день авто-залечивается,
    как compute_breadth_history. FILL-ONLY: заполняет только дни без реального якоря;
    строки SMARTLAB/GURUFOCUS/IMOEX_EXTRAPOLATED НЕ перетираем. Уровень калибруется
    множителем к последнему SmartLab-якорю (наше покрытие ~88%, остальное — хвост
    неликвида). ISSUESIZE = выпущенные акции (вкл. заблокированные) → методология как SmartLab."""
    try:
        issize = fetch_issuesize()
    except Exception as e:
        log.warning(f"  compute: ISSUESIZE недоступен ({e}) → пропуск gap-fill")
        return 0
    if not issize:
        return 0
    # дедуп преф→обычка: компанию считаем один раз (как SmartLab)
    universe = {s for s in issize if not (s.endswith('P') and s[:-1] in issize)}
    cutoff = date.today() - timedelta(days=days)  # cutoff в Python — pg8000 не любит date−int

    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT begin_time::date AS d, secid, close FROM candles "
            "WHERE type='stock' AND interval=24 AND close>0 AND begin_time::date >= :cutoff"
        ), {"cutoff": cutoff}).fetchall()

    totals: Dict[date, float] = {}
    for d, secid, close in rows:
        if secid in universe:  # фильтр вселенной в Python (без SQL ANY)
            totals[d] = totals.get(d, 0.0) + float(close) * issize[secid] / 1e9
    if not totals:
        log.info("  compute: нет дневных свечей в окне → пропуск")
        return 0

    # калибровочный множитель: МЕДИАНА по нескольким последним якорям.
    # Раньше брался один самый свежий якорь — и одиночный кривой день SmartLab
    # отравлял всю заливку (инцидент 2026-06-17: завышенный якорь раздул gap-fill
    # на ~18%, в графике кап/ВВП появился «забор»). Медиана устойчива к выбросу.
    with engine.connect() as conn:
        anchors = conn.execute(text(
            "SELECT period_date, value FROM macro_data "
            "WHERE indicator='MARKET_CAP_TOTAL' AND source IN ('SMARTLAB','GURUFOCUS') "
            "AND period_date >= :cutoff ORDER BY period_date DESC"
        ), {"cutoff": cutoff}).fetchall()
    factor_pts = []
    for ad, av in anchors:
        if totals.get(ad, 0) > 0:
            factor_pts.append((ad, float(av) / totals[ad]))
        if len(factor_pts) >= 7:  # достаточно свежих якорей для устойчивой медианы
            break
    if not factor_pts:
        log.warning("  compute: нет SmartLab-якоря в окне для калибровки → пропуск")
        return 0
    factor = statistics.median([f for _, f in factor_pts])
    log.info(f"  compute: множитель {factor:.4f} (медиана по {len(factor_pts)} якорям; "
             f"свежий {factor_pts[0][0]}={factor_pts[0][1]:.4f})")

    filled = 0
    with engine.begin() as conn:
        for d, t in sorted(totals.items()):
            res = conn.execute(text("""
                INSERT INTO macro_data (indicator, period_date, value, source)
                VALUES ('MARKET_CAP_TOTAL', :pd, :val, 'COMPUTED_CANDLES')
                ON CONFLICT (indicator, period_date) DO UPDATE SET
                    value = EXCLUDED.value, source = EXCLUDED.source,
                    created_at = CURRENT_TIMESTAMP
                WHERE macro_data.source = 'COMPUTED_CANDLES'
            """), {"pd": d, "val": round(t * factor)})
            filled += res.rowcount or 0
    log.info(f"  compute: gap-fill {filled} расчётных дней (множитель {factor:.4f}; реальные якоря не тронуты)")
    return filled


async def update_market_cap(force: bool = False) -> dict:
    """Обновить капитализацию: SmartLab (живой якорь) + gap-safe backfill из свечей."""
    engine = get_engine()
    ensure_table(engine)

    count = fetch_from_smartlab(engine)
    # gap-safe: заполнить пропущенные дни из свечей (fill-only, реальные якоря не трогаем).
    # Некритично — если упало, SmartLab-снимок уже сохранён.
    try:
        compute_total_from_candles(engine)
    except Exception as e:
        log.warning(f"  compute_total_from_candles упал (некритично): {e}")

    engine.dispose()
    return {'MARKET_CAP_TOTAL': count}


async def main():
    parser = argparse.ArgumentParser(description="Market cap total updater (SmartLab + GuruFocus)")
    parser.add_argument("--once", action="store_true", help="Single update from SmartLab")
    parser.add_argument("--force", action="store_true", help="Force (ignore trading day check)")
    parser.add_argument("--import-historical", type=str, metavar="FILE",
                        help="Import historical data from GuruFocus HTML")
    parser.add_argument("--check", action="store_true", help="Check data integrity only")
    args = parser.parse_args()

    sep = '=' * 60
    log.info(sep)
    log.info("MARKET CAP TOTAL UPDATER")
    log.info(f"Time: {datetime.now()}")
    log.info(f"Args: once={args.once}, force={args.force}, check={args.check}")
    log.info(sep)

    engine = get_engine()
    ensure_table(engine)

    if args.check:
        print_check_report(engine)
        engine.dispose()
        return

    if args.import_historical:
        count = import_historical(engine, args.import_historical)
        if count > 0:
            log.info("Проверка целостности после импорта...")
            print_check_report(engine)
        engine.dispose()
        return

    if args.once:
        is_trading, reason = is_trading_day()
        if not is_trading and not args.force:
            log.info(f"Skip: {reason} (use --force)")
            engine.dispose()
            return
        results = await update_market_cap(force=args.force)
        for k, v in results.items():
            log.info(f"  {k}: {v}")
        engine.dispose()
    else:
        log.info("Daemon mode: checking daily at 10:00 MSK")
        last_update = None
        while True:
            now = get_moscow_time()
            today = now.date()
            is_trade, _ = is_trading_day(today)

            if is_trade and now.hour >= 10 and last_update != today:
                log.info(f"Daily update ({now:%H:%M})...")
                try:
                    await update_market_cap()
                except Exception as e:
                    log.error(f"Ошибка daily update: {e}", exc_info=True)
                last_update = today

            await asyncio.sleep(300)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Stopped")
    except Exception as e:
        log.critical(f"Error: {e}", exc_info=True)
        sys.exit(1)
