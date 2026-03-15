#!/usr/bin/env python3
"""
Автоматическое обновление макроэкономических данных (M2, GDP).

Источники:
- M2: ЦБ РФ XLSX (https://www.cbr.ru/vfs/statistics/credit_statistics/monetary_agg.xlsx)
- GDP: Росстат XLSX (ручное обновление, файл VVP_kvartal_s1995-2025*.xlsx)

Расписание: ежедневно (M2 обновляется ежемесячно, но проверяем каждый день)

Использование:
  python Macro/fetch_macro_realtime.py --once          # Обновить M2 из ЦБ
  python Macro/fetch_macro_realtime.py --once --force   # Полная перезагрузка
  python Macro/fetch_macro_realtime.py --import-gdp FILE  # Импорт GDP из Excel
  python Macro/fetch_macro_realtime.py --check          # Проверка целостности данных
"""

import asyncio
import argparse
import logging
import sys
import os
import tempfile
import urllib.request
from pathlib import Path
from datetime import datetime, date, timedelta
from calendar import monthrange
from typing import List, Tuple, Optional, Dict

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# === Пути ===
BASE_DIR = Path(__file__).parent
PROJECT_DIR = BASE_DIR.parent

load_dotenv(PROJECT_DIR / ".env")
DB_URL = os.getenv("DB_URL")

LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

# === URL источников ===
CBR_M2_URL = "https://www.cbr.ru/vfs/statistics/credit_statistics/monetary_agg.xlsx"

# === Лимиты валидации ===
M2_MIN_VALUE = 1            # M2 min (млрд руб) — в 1993 M2 было ~6.5 млрд руб
M2_MAX_VALUE = 200_000_000  # M2 max (200 трлн руб) — запас на будущее
M2_MAX_JUMP_PCT = 30        # Максимальный скачок между соседними месяцами (%)
M2_MIN_DATE = date(1993, 1, 1)   # M2 доступна с 1993

GDP_MIN_VALUE = 100         # GDP min (млрд руб)
GDP_MAX_VALUE = 100_000_000 # GDP max (100 трлн руб за квартал)
GDP_MAX_JUMP_PCT = 50       # Максимальный скачок между кварталами (%)
GDP_MIN_DATE = date(1995, 1, 1)  # GDP доступна с 1995

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
    """Настройка логирования: консоль + файл всех логов + файл ошибок."""
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
    if sys.platform == 'win32':
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(console_fmt)
    root.addHandler(ch)

    # Файл всех логов (DEBUG+)
    fh_all = logging.FileHandler(
        LOG_DIR / f"macro_realtime_{datetime.now():%Y%m%d}.log",
        encoding='utf-8'
    )
    fh_all.setLevel(logging.DEBUG)
    fh_all.setFormatter(detailed_fmt)
    root.addHandler(fh_all)

    # Файл ошибок (WARNING+)
    fh_err = logging.FileHandler(
        LOG_DIR / f"macro_errors_{datetime.now():%Y%m%d}.log",
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
    """Создать таблицы macro (metadata) и macro_data (данные)."""
    with engine.connect() as conn:
        # Metadata-таблица (реестр макроиндикаторов)
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

        # Таблица данных
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

        # Заполнить macro реестр (upsert)
        conn.execute(text("""
            INSERT INTO macro (indicator, name, frequency, source)
            VALUES ('M2_MONTHLY', 'Денежная масса M2', 'monthly', 'CBR_XLSX')
            ON CONFLICT (indicator) DO UPDATE SET
                name = EXCLUDED.name,
                frequency = EXCLUDED.frequency,
                source = EXCLUDED.source
        """))
        conn.execute(text("""
            INSERT INTO macro (indicator, name, frequency, source)
            VALUES ('GDP_QUARTERLY', 'ВВП России', 'quarterly', 'ROSSTAT_XLSX')
            ON CONFLICT (indicator) DO UPDATE SET
                name = EXCLUDED.name,
                frequency = EXCLUDED.frequency,
                source = EXCLUDED.source
        """))

        conn.commit()


# ======================================================================
#                         ВАЛИДАЦИЯ ДАННЫХ
# ======================================================================

def validate_value(value: float, indicator: str) -> Tuple[bool, str]:
    """Проверка одного значения на допустимость."""
    if value is None:
        return False, "значение None"
    if value <= 0:
        return False, f"отрицательное или нулевое значение: {value}"

    if indicator == 'M2_MONTHLY':
        if value < M2_MIN_VALUE:
            return False, f"M2 слишком мало: {value} < {M2_MIN_VALUE}"
        if value > M2_MAX_VALUE:
            return False, f"M2 слишком велико: {value} > {M2_MAX_VALUE}"
    elif indicator == 'GDP_QUARTERLY':
        if value < GDP_MIN_VALUE:
            return False, f"GDP слишком мало: {value} < {GDP_MIN_VALUE}"
        if value > GDP_MAX_VALUE:
            return False, f"GDP слишком велико: {value} > {GDP_MAX_VALUE}"

    return True, "OK"


def validate_date(period_date: date, indicator: str) -> Tuple[bool, str]:
    """Проверка даты на допустимость."""
    today = date.today()

    if period_date > today + timedelta(days=90):
        return False, f"дата в далеком будущем: {period_date}"

    if indicator == 'M2_MONTHLY' and period_date < M2_MIN_DATE:
        return False, f"M2 до {M2_MIN_DATE}: {period_date}"
    elif indicator == 'GDP_QUARTERLY' and period_date < GDP_MIN_DATE:
        return False, f"GDP до {GDP_MIN_DATE}: {period_date}"

    return True, "OK"


def validate_series_jumps(data: List[Tuple[date, float]], indicator: str) -> List[str]:
    """
    Проверить серию на аномальные скачки между соседними значениями.
    Возвращает список предупреждений.
    """
    warnings = []
    max_jump = M2_MAX_JUMP_PCT if indicator == 'M2_MONTHLY' else GDP_MAX_JUMP_PCT
    sorted_data = sorted(data, key=lambda x: x[0])

    for i in range(1, len(sorted_data)):
        prev_date, prev_val = sorted_data[i - 1]
        curr_date, curr_val = sorted_data[i]

        if prev_val == 0:
            continue

        change_pct = abs(curr_val - prev_val) / prev_val * 100
        if change_pct > max_jump:
            warnings.append(
                f"Скачок {change_pct:.1f}% между {prev_date} ({prev_val:,.0f}) "
                f"и {curr_date} ({curr_val:,.0f})"
            )

    return warnings


# ======================================================================
#                    ПРОВЕРКА ДЫР В ДАННЫХ
# ======================================================================

def check_m2_gaps(engine) -> Dict:
    """Проверить M2 на пропущенные месяцы."""
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT period_date, value FROM macro_data
            WHERE indicator = 'M2_MONTHLY'
            ORDER BY period_date
        """)).fetchall()

    if not rows:
        return {'status': 'empty', 'total': 0, 'gaps': [], 'first': None, 'last': None}

    dates = [r[0] for r in rows]
    values = [float(r[1]) for r in rows]
    gaps = []

    for i in range(1, len(dates)):
        prev = dates[i - 1]
        curr = dates[i]

        # Ожидаемая следующая дата: конец следующего месяца
        if prev.month == 12:
            expected_year = prev.year + 1
            expected_month = 1
        else:
            expected_year = prev.year
            expected_month = prev.month + 1
        _, last_day = monthrange(expected_year, expected_month)
        expected = date(expected_year, expected_month, last_day)

        if curr != expected:
            # Посчитаем сколько месяцев пропущено
            months_diff = (curr.year - prev.year) * 12 + (curr.month - prev.month)
            if months_diff > 1:
                gaps.append({
                    'from': prev,
                    'to': curr,
                    'missing_months': months_diff - 1
                })

    # Проверка на аномальные скачки
    jump_warnings = validate_series_jumps(list(zip(dates, values)), 'M2_MONTHLY')

    return {
        'status': 'ok' if not gaps and not jump_warnings else 'issues',
        'total': len(dates),
        'first': dates[0],
        'last': dates[-1],
        'gaps': gaps,
        'jump_warnings': jump_warnings,
    }


def check_gdp_gaps(engine) -> Dict:
    """Проверить GDP на пропущенные кварталы."""
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT period_date, value FROM macro_data
            WHERE indicator = 'GDP_QUARTERLY'
            ORDER BY period_date
        """)).fetchall()

    if not rows:
        return {'status': 'empty', 'total': 0, 'gaps': [], 'first': None, 'last': None}

    dates = [r[0] for r in rows]
    values = [float(r[1]) for r in rows]
    gaps = []

    # Квартальные даты: 31.03, 30.06, 30.09, 31.12
    Q_SEQUENCE = [(3, 31), (6, 30), (9, 30), (12, 31)]

    for i in range(1, len(dates)):
        prev = dates[i - 1]
        curr = dates[i]

        # Ожидаемый следующий квартал
        prev_q_idx = None
        for idx, (m, _) in enumerate(Q_SEQUENCE):
            if prev.month == m:
                prev_q_idx = idx
                break

        if prev_q_idx is not None:
            next_q_idx = (prev_q_idx + 1) % 4
            next_year = prev.year + (1 if next_q_idx == 0 else 0)
            next_m, next_d = Q_SEQUENCE[next_q_idx]
            expected = date(next_year, next_m, next_d)

            if curr != expected:
                quarters_diff = (curr.year - prev.year) * 4
                for idx, (m, _) in enumerate(Q_SEQUENCE):
                    if curr.month == m:
                        quarters_diff += idx
                        break
                quarters_diff -= prev_q_idx
                if quarters_diff > 1:
                    gaps.append({
                        'from': prev,
                        'to': curr,
                        'missing_quarters': quarters_diff - 1
                    })

    # Проверка на аномальные скачки
    jump_warnings = validate_series_jumps(list(zip(dates, values)), 'GDP_QUARTERLY')

    return {
        'status': 'ok' if not gaps and not jump_warnings else 'issues',
        'total': len(dates),
        'first': dates[0],
        'last': dates[-1],
        'gaps': gaps,
        'jump_warnings': jump_warnings,
    }


def print_check_report(engine):
    """Вывести полный отчёт о целостности данных."""
    sep = '=' * 60
    log.info(sep)
    log.info("ПРОВЕРКА ЦЕЛОСТНОСТИ МАКРОДАННЫХ")
    log.info(sep)

    # --- M2 ---
    m2 = check_m2_gaps(engine)
    log.info("")
    log.info("--- M2_MONTHLY ---")
    if m2['status'] == 'empty':
        log.warning("  НЕТ ДАННЫХ M2 в БД!")
    else:
        log.info(f"  Записей: {m2['total']}")
        log.info(f"  Период: {m2['first']} - {m2['last']}")
        if m2['gaps']:
            log.warning(f"  ДЫРЫ: {len(m2['gaps'])} пропусков:")
            for g in m2['gaps']:
                log.warning(f"    {g['from']} -> {g['to']} ({g['missing_months']} мес. пропущено)")
        else:
            log.info("  Дыр нет (все месяцы подряд)")
        if m2.get('jump_warnings'):
            log.warning(f"  АНОМАЛИИ: {len(m2['jump_warnings'])} скачков:")
            for w in m2['jump_warnings']:
                log.warning(f"    {w}")
        else:
            log.info("  Аномальных скачков нет")

    # --- GDP ---
    gdp = check_gdp_gaps(engine)
    log.info("")
    log.info("--- GDP_QUARTERLY ---")
    if gdp['status'] == 'empty':
        log.warning("  НЕТ ДАННЫХ GDP в БД!")
    else:
        log.info(f"  Записей: {gdp['total']}")
        log.info(f"  Период: {gdp['first']} - {gdp['last']}")
        if gdp['gaps']:
            log.warning(f"  ДЫРЫ: {len(gdp['gaps'])} пропусков:")
            for g in gdp['gaps']:
                log.warning(f"    {g['from']} -> {g['to']} ({g['missing_quarters']} кварт. пропущено)")
        else:
            log.info("  Дыр нет (все кварталы подряд)")
        if gdp.get('jump_warnings'):
            log.warning(f"  АНОМАЛИИ: {len(gdp['jump_warnings'])} скачков:")
            for w in gdp['jump_warnings']:
                log.warning(f"    {w}")
        else:
            log.info("  Аномальных скачков нет")

    # --- Актуальность ---
    log.info("")
    log.info("--- АКТУАЛЬНОСТЬ ---")
    today = date.today()
    if m2['last']:
        m2_age = (today - m2['last']).days
        if m2_age > 60:
            log.warning(f"  M2: последняя запись {m2['last']} ({m2_age} дней назад) — УСТАРЕЛА")
        else:
            log.info(f"  M2: последняя запись {m2['last']} ({m2_age} дней назад) — OK")
    if gdp['last']:
        gdp_age = (today - gdp['last']).days
        if gdp_age > 120:
            log.warning(f"  GDP: последняя запись {gdp['last']} ({gdp_age} дней назад) — УСТАРЕЛА")
        else:
            log.info(f"  GDP: последняя запись {gdp['last']} ({gdp_age} дней назад) — OK")

    log.info(sep)

    return m2, gdp


# ═══════════════════════════════════════════════════════════════════
# M2: Автозагрузка с ЦБ РФ
# ═══════════════════════════════════════════════════════════════════

def fetch_m2_from_cbr(engine) -> int:
    """Скачать M2 из XLSX ЦБ РФ и обновить в БД."""
    log.info("Загрузка M2 с ЦБ РФ...")

    # Скачиваем XLSX во временный файл
    tmp_path = None
    try:
        log.debug(f"  URL: {CBR_M2_URL}")
        req = urllib.request.Request(CBR_M2_URL, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
        })
        resp = urllib.request.urlopen(req, timeout=30)
        status = resp.getcode()
        log.info(f"  HTTP статус: {status}")

        if status != 200:
            log.error(f"  Неожиданный HTTP статус: {status}")
            return 0

        tmp_fd, tmp_path = tempfile.mkstemp(suffix=".xlsx")
        raw_data = resp.read()
        file_size = len(raw_data)
        with os.fdopen(tmp_fd, 'wb') as f:
            f.write(raw_data)

        log.info(f"  XLSX скачан: {file_size:,} байт")

        if file_size < 1000:
            log.error(f"  Файл слишком маленький ({file_size} байт) — вероятно ошибка загрузки")
            return 0

        # Парсим
        import openpyxl
        wb = openpyxl.load_workbook(tmp_path, read_only=True, data_only=True)

        sheet_name = 'Денежные агрегаты'
        if sheet_name not in wb.sheetnames:
            log.error(f"  Лист '{sheet_name}' не найден! Доступные: {wb.sheetnames}")
            wb.close()
            return 0

        ws = wb[sheet_name]

        # Row 1 = даты, Row 14 = M2
        dates_row = []
        m2_row = []
        for cell in ws[1]:
            dates_row.append(cell.value)
        for cell in ws[14]:
            m2_row.append(cell.value)
        wb.close()

        log.debug(f"  Столбцов с датами: {len(dates_row)}, с M2: {len(m2_row)}")

        # Парсим пары (date, value)
        data = []
        skipped = 0
        validation_errors = []

        for dt, val in zip(dates_row[1:], m2_row[1:]):  # skip col A (label)
            if dt is None or val is None:
                skipped += 1
                continue
            try:
                if isinstance(dt, datetime):
                    d = dt.date()
                elif isinstance(dt, date):
                    d = dt
                else:
                    skipped += 1
                    continue

                _, last_day = monthrange(d.year, d.month)
                period_date = date(d.year, d.month, last_day)
                value = float(val)

                # Валидация даты
                ok, msg = validate_date(period_date, 'M2_MONTHLY')
                if not ok:
                    validation_errors.append(f"  Дата: {msg}")
                    continue

                # Валидация значения
                ok, msg = validate_value(value, 'M2_MONTHLY')
                if not ok:
                    validation_errors.append(f"  Значение {period_date}: {msg}")
                    continue

                data.append((period_date, value))
            except (ValueError, TypeError) as e:
                validation_errors.append(f"  Ошибка парсинга: {dt} / {val}: {e}")
                continue

        if not data:
            log.error("  Не удалось распарсить ни одной записи M2!")
            for err in validation_errors[:10]:
                log.error(err)
            return 0

        log.info(f"  Распарсено: {len(data)} записей M2 ({data[0][0]} - {data[-1][0]})")
        if skipped:
            log.debug(f"  Пропущено пустых: {skipped}")
        if validation_errors:
            log.warning(f"  Ошибок валидации: {len(validation_errors)}")
            for err in validation_errors[:5]:
                log.warning(err)

        # Проверка на аномальные скачки
        jumps = validate_series_jumps(data, 'M2_MONTHLY')
        if jumps:
            log.warning(f"  Обнаружены аномальные скачки в M2 ({len(jumps)}):")
            for j in jumps[:5]:
                log.warning(f"    {j}")

        # Получаем последнюю дату в БД
        with engine.connect() as conn:
            r = conn.execute(text(
                "SELECT MAX(period_date) FROM macro_data WHERE indicator = 'M2_MONTHLY'"
            )).fetchone()
            last_db_date = r[0] if r and r[0] else None

            r2 = conn.execute(text(
                "SELECT COUNT(*) FROM macro_data WHERE indicator = 'M2_MONTHLY'"
            )).fetchone()
            db_count_before = r2[0] if r2 else 0

        log.info(f"  В БД до обновления: {db_count_before} записей, последняя: {last_db_date}")

        # Вставляем/обновляем
        inserted = 0
        errors = 0
        with engine.connect() as conn:
            for period_date, value in data:
                try:
                    conn.execute(text("""
                        INSERT INTO macro_data (indicator, period_date, value, source)
                        VALUES ('M2_MONTHLY', :pd, :val, 'CBR_XLSX')
                        ON CONFLICT (indicator, period_date) DO UPDATE SET
                            value = EXCLUDED.value, source = EXCLUDED.source,
                            created_at = CURRENT_TIMESTAMP
                    """), {"pd": period_date, "val": value})
                    inserted += 1
                except Exception as e:
                    errors += 1
                    log.error(f"  Ошибка вставки M2 {period_date}: {e}")
            conn.commit()

        # Верификация после вставки
        with engine.connect() as conn:
            r = conn.execute(text(
                "SELECT COUNT(*) FROM macro_data WHERE indicator = 'M2_MONTHLY'"
            )).fetchone()
            db_count_after = r[0] if r else 0

        new_count = len([d for d in data if last_db_date is None or d[0] > last_db_date])
        log.info(f"  M2 итог: {inserted} upserted, {new_count} новых, {errors} ошибок")
        log.info(f"  В БД после обновления: {db_count_after} записей (было {db_count_before})")

        if errors > 0:
            log.warning(f"  {errors} записей M2 не удалось вставить!")

        return inserted

    except urllib.error.URLError as e:
        log.error(f"  Ошибка сети при загрузке M2: {e}")
        return 0
    except Exception as e:
        log.error(f"  Ошибка загрузки M2: {e}", exc_info=True)
        return 0
    finally:
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.unlink(tmp_path)
            except OSError:
                pass


# ═══════════════════════════════════════════════════════════════════
# GDP: Импорт из Excel (ручной)
# ═══════════════════════════════════════════════════════════════════

def import_gdp_from_xlsx(engine, xlsx_path: str) -> int:
    """Импортировать GDP из Excel файла Росстат."""
    import openpyxl

    if not os.path.exists(xlsx_path):
        log.error(f"Файл не найден: {xlsx_path}")
        return 0

    file_size = os.path.getsize(xlsx_path)
    log.info(f"Импорт GDP из {xlsx_path} ({file_size:,} байт)...")

    try:
        wb = openpyxl.load_workbook(xlsx_path, read_only=True, data_only=True)
    except Exception as e:
        log.error(f"  Не удалось открыть Excel: {e}")
        return 0

    Q_DATES = {
        'I квартал': (3, 31),
        'II квартал': (6, 30),
        'III квартал': (9, 30),
        'IV квартал': (12, 31),
    }

    all_data = {}
    validation_errors = []
    sheets_processed = 0

    for sheet_name in wb.sheetnames:
        ws = wb[sheet_name]
        rows_list = list(ws.iter_rows(min_row=3, max_row=5, values_only=True))
        if len(rows_list) < 3:
            log.debug(f"  Лист '{sheet_name}': пропущен (мало строк)")
            continue
        row3, row4, row5 = rows_list[0], rows_list[1], rows_list[2]
        sheets_processed += 1

        for col_idx in range(len(row4)):
            quarter_label = row4[col_idx]
            value = row5[col_idx]
            if quarter_label not in Q_DATES or value is None:
                continue

            year = None
            for j in range(col_idx, -1, -1):
                y = row3[j]
                if y is not None:
                    try:
                        year = int(str(y).strip()[:4])
                        break
                    except (ValueError, TypeError):
                        continue
            if year is None:
                continue

            month, day = Q_DATES[quarter_label]
            period_date = date(year, month, day)
            fval = float(value)

            # Валидация даты
            ok, msg = validate_date(period_date, 'GDP_QUARTERLY')
            if not ok:
                validation_errors.append(f"  Дата: {msg}")
                continue

            # Валидация значения
            ok, msg = validate_value(fval, 'GDP_QUARTERLY')
            if not ok:
                validation_errors.append(f"  Значение {period_date}: {msg}")
                continue

            all_data[period_date] = fval

    wb.close()

    if not all_data:
        log.error("  Не удалось распарсить ни одной записи GDP!")
        for err in validation_errors[:10]:
            log.error(err)
        return 0

    log.info(f"  Листов обработано: {sheets_processed}")
    log.info(f"  Распарсено: {len(all_data)} кварталов ({min(all_data)} - {max(all_data)})")

    if validation_errors:
        log.warning(f"  Ошибок валидации: {len(validation_errors)}")
        for err in validation_errors[:5]:
            log.warning(err)

    # Проверка на аномальные скачки
    sorted_data = sorted(all_data.items())
    jumps = validate_series_jumps(sorted_data, 'GDP_QUARTERLY')
    if jumps:
        log.warning(f"  Обнаружены аномальные скачки в GDP ({len(jumps)}):")
        for j in jumps[:5]:
            log.warning(f"    {j}")

    # Получаем текущее состояние БД
    with engine.connect() as conn:
        r = conn.execute(text(
            "SELECT COUNT(*) FROM macro_data WHERE indicator = 'GDP_QUARTERLY'"
        )).fetchone()
        db_count_before = r[0] if r else 0

    # Upsert
    errors = 0
    with engine.connect() as conn:
        conn.execute(text("DELETE FROM macro_data WHERE indicator = 'GDP_QUARTERLY'"))
        for period_date, value in sorted(all_data.items()):
            try:
                conn.execute(text("""
                    INSERT INTO macro_data (indicator, period_date, value, source)
                    VALUES ('GDP_QUARTERLY', :pd, :val, 'ROSSTAT_XLSX')
                    ON CONFLICT (indicator, period_date) DO UPDATE SET
                        value = EXCLUDED.value, source = EXCLUDED.source
                """), {"pd": period_date, "val": value})
            except Exception as e:
                errors += 1
                log.error(f"  Ошибка вставки GDP {period_date}: {e}")
        conn.commit()

    # Верификация
    with engine.connect() as conn:
        r = conn.execute(text(
            "SELECT COUNT(*) FROM macro_data WHERE indicator = 'GDP_QUARTERLY'"
        )).fetchone()
        db_count_after = r[0] if r else 0

    log.info(f"  GDP итог: {len(all_data)} вставлено, {errors} ошибок")
    log.info(f"  В БД: было {db_count_before}, стало {db_count_after}")

    if db_count_after != len(all_data):
        log.warning(f"  Несовпадение: вставлено {len(all_data)}, в БД {db_count_after}")

    if errors > 0:
        log.warning(f"  {errors} записей GDP не удалось вставить!")

    return len(all_data)


# ═══════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════

async def update_macro(force: bool = False) -> dict:
    """Обновить все макроданные."""
    engine = get_engine()
    ensure_table(engine)
    results = {}

    # M2 — всегда обновляем (ЦБ обновляет ежемесячно)
    m2_count = fetch_m2_from_cbr(engine)
    results['M2'] = m2_count

    # Проверка целостности после обновления
    log.info("Проверка целостности данных после обновления...")
    m2_check = check_m2_gaps(engine)
    gdp_check = check_gdp_gaps(engine)

    if m2_check['status'] == 'empty':
        log.warning("  M2: данных нет!")
    elif m2_check['gaps']:
        log.warning(f"  M2: обнаружено {len(m2_check['gaps'])} дыр в данных")
    else:
        log.info(f"  M2: OK ({m2_check['total']} записей, {m2_check['first']} - {m2_check['last']})")

    if gdp_check['status'] == 'empty':
        log.warning("  GDP: данных нет (требуется ручной импорт --import-gdp)")
    elif gdp_check['gaps']:
        log.warning(f"  GDP: обнаружено {len(gdp_check['gaps'])} дыр в данных")
    else:
        log.info(f"  GDP: OK ({gdp_check['total']} записей, {gdp_check['first']} - {gdp_check['last']})")

    results['m2_check'] = m2_check['status']
    results['gdp_check'] = gdp_check['status']

    engine.dispose()
    return results


async def main():
    parser = argparse.ArgumentParser(description="Macro data realtime updater")
    parser.add_argument("--once", action="store_true", help="Single update")
    parser.add_argument("--force", action="store_true", help="Force full reload")
    parser.add_argument("--import-gdp", type=str, metavar="FILE", help="Import GDP from Excel")
    parser.add_argument("--check", action="store_true", help="Check data integrity only")
    args = parser.parse_args()

    sep = '=' * 60
    log.info(sep)
    log.info("MACRO DATA UPDATER")
    log.info(f"Time: {datetime.now()}")
    log.info(f"Args: once={args.once}, force={args.force}, check={args.check}")
    log.info(sep)

    engine = get_engine()
    ensure_table(engine)

    # Режим проверки целостности
    if args.check:
        print_check_report(engine)
        engine.dispose()
        return

    # Импорт GDP
    if args.import_gdp:
        count = import_gdp_from_xlsx(engine, args.import_gdp)
        if count > 0:
            log.info("Запуск проверки целостности после импорта...")
            print_check_report(engine)
        engine.dispose()
        return

    # Однократное обновление
    if args.once:
        is_trading, reason = is_trading_day()
        if not is_trading and not args.force:
            log.info(f"Skip: {reason} (use --force)")
            return
        results = await update_macro(force=args.force)
        for k, v in results.items():
            log.info(f"  {k}: {v}")
    else:
        # Daemon mode
        log.info("Daemon mode: checking daily at 09:30 MSK")
        last_update = None
        while True:
            now = get_moscow_time()
            today = now.date()
            is_trade, _ = is_trading_day(today)

            if is_trade and now.hour >= 9 and last_update != today:
                log.info(f"Daily update ({now:%H:%M})...")
                try:
                    await update_macro()
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
