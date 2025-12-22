#!/usr/bin/env python3
"""
Агрегация Open Interest: 5 минут → 60 минут

Логика:
1. Режим once (по умолчанию) — проверяет ВСЮ историю и создаёт недостающие часовые записи
2. Режим --last-hour — агрегирует только последний завершённый час (быстро, для оркестратора)
3. Режим daemon — ждёт окончания часа (минута 00) и агрегирует последний час

Принцип агрегации:
- Берём последнюю 5-минутку каждого часа (tradetime с минутой 55)
- Записываем как interval=60 с tradetime = HH:00:00

Запуск:
  python aggregate_oi_hourly.py              # Однократно: проверка и агрегация всего
  python aggregate_oi_hourly.py --last-hour  # Только последний час (для оркестратора)
  python aggregate_oi_hourly.py daemon       # Работа в цикле после каждого часа
"""

import sys
import time
import logging
import argparse
from datetime import datetime, timedelta, date, time as dt_time
from pathlib import Path

# === Импорт из корневой папки ===
from dotenv import load_dotenv
import os

sys.path.insert(0, str(Path(__file__).parent.parent))
load_dotenv(Path(__file__).parent.parent / ".env")

DB_URL = os.getenv("DB_URL")

try:
    from moex_calendar import get_moscow_time, is_trading_day
except ImportError as e:
    print(f"❌ Ошибка импорта: {e}")
    print("Убедитесь, что moex_calendar.py находится в корневой папке MOEX/")
    sys.exit(1)

from sqlalchemy import create_engine, text

# === Директория логов ===
LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_DIR / f"aggregate_oi_{datetime.now():%Y%m%d}.log", encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)


# Тикеры с 5-минутными данными (Algopack)
ALGOPACK_TICKERS = [
    "CR", "CNYRUBF", "Si", "Eu", "IB", "VB", "USDRUBF", "GZ", "IMOEXF", "RB",
    "CC", "GL", "GLDRUBF", "NA", "NR", "ED", "GK", "SV", "SS", "X5",
    "MX", "MM", "NG", "GD", "SR", "SF", "GAZPF", "MN", "YD", "BR",
    "SE", "TN", "PT", "AF", "KC", "FF", "AL", "EURRUBF", "SBERF", "CE",
    "HS", "NK", "RI", "RL", "LK", "UC", "PD", "NM", "MC", "RM",
    "RN", "SP", "SN", "ME", "HY", "BM", "TT", "OJ", "MG", "W4",
    "DX", "CH", "MY", "VI", "AU",
]


def get_engine():
    return create_engine(DB_URL)


def get_missing_hours(engine, sectype: str, limit_days: int = None) -> list[tuple[date, int]]:
    """
    Найти часы для которых есть 5-минутки, но нет часовой агрегации.

    Args:
        engine: SQLAlchemy engine
        sectype: Код инструмента
        limit_days: Ограничить поиск последними N днями (None = вся история)

    Возвращает список (tradedate, hour) для которых нужна агрегация.
    """

    # Базовый запрос
    date_filter = ""
    params = {"sectype": sectype}

    if limit_days:
        date_filter = "AND tradedate >= :min_date"
        params["min_date"] = date.today() - timedelta(days=limit_days)

    # Используем EXTRACT без параметров - только константы
    query = text(f"""
        WITH five_min_hours AS (
            SELECT DISTINCT 
                tradedate,
                EXTRACT(HOUR FROM tradetime)::int AS hour
            FROM open_interest
            WHERE sectype = :sectype 
              AND interval = 5
              AND EXTRACT(MINUTE FROM tradetime) = 55
              {date_filter}
        ),
        existing_hourly AS (
            SELECT DISTINCT 
                tradedate,
                EXTRACT(HOUR FROM tradetime)::int AS hour
            FROM open_interest
            WHERE sectype = :sectype 
              AND interval = 60
              {date_filter}
        )
        SELECT f.tradedate, f.hour
        FROM five_min_hours f
        LEFT JOIN existing_hourly e 
            ON f.tradedate = e.tradedate AND f.hour = e.hour
        WHERE e.tradedate IS NULL
        ORDER BY f.tradedate, f.hour
    """)

    with engine.connect() as conn:
        result = conn.execute(query, params)
        return [(row[0], row[1]) for row in result.fetchall()]


def aggregate_hour(engine, sectype: str, tradedate: date, hour: int) -> int:
    """
    Агрегировать один час для тикера.

    Берём запись с tradetime = HH:55:00 и копируем как interval=60 с tradetime = HH:00:00
    """
    # Получаем данные за 55-ю минуту
    # Используем прямое сравнение времени вместо EXTRACT чтобы избежать проблем с pg8000
    time_55 = dt_time(hour, 55, 0)

    query = text("""
        SELECT sectype, tradedate, clgroup, pos, pos_long, pos_short, 
               pos_long_num, pos_short_num, systime
        FROM open_interest
        WHERE sectype = :sectype
          AND tradedate = :tradedate
          AND interval = 5
          AND tradetime = :time_55
    """)

    with engine.connect() as conn:
        result = conn.execute(query, {
            "sectype": sectype,
            "tradedate": tradedate,
            "time_55": time_55
        })
        rows = result.fetchall()

        if not rows:
            return 0

        inserted = 0
        hourly_time = dt_time(hour, 0, 0)  # Объект time вместо строки

        for row in rows:
            try:
                conn.execute(text("""
                    INSERT INTO open_interest
                    (sectype, tradedate, tradetime, clgroup, pos, pos_long, pos_short,
                     pos_long_num, pos_short_num, systime, interval)
                    VALUES (:sectype, :tradedate, :tradetime, :clgroup, :pos, :pos_long, :pos_short,
                            :pos_long_num, :pos_short_num, :systime, 60)
                    ON CONFLICT (sectype, tradedate, tradetime, clgroup, interval) DO NOTHING
                """), {
                    "sectype": row[0],
                    "tradedate": row[1],
                    "tradetime": hourly_time,
                    "clgroup": row[2],
                    "pos": row[3],
                    "pos_long": row[4],
                    "pos_short": row[5],
                    "pos_long_num": row[6],
                                 "pos_short_num": row[7],
                    "systime": row[8],
                })
                inserted += 1
            except Exception as e:
                logger.debug(f"Ошибка вставки: {e}")

        conn.commit()
        return inserted


def aggregate_all_missing(engine, limit_days: int = None) -> int:
    """
    Агрегировать все недостающие часы для всех тикеров.

    Args:
        engine: SQLAlchemy engine
        limit_days: Ограничить поиск последними N днями (None = вся история)
    """

    total_inserted = 0

    for i, sectype in enumerate(ALGOPACK_TICKERS):
        missing = get_missing_hours(engine, sectype, limit_days)

        if not missing:
            continue

        logger.info(f"[{i+1}/{len(ALGOPACK_TICKERS)}] {sectype}: {len(missing)} часов для агрегации")

        sectype_inserted = 0
        for tradedate, hour in missing:
            inserted = aggregate_hour(engine, sectype, tradedate, hour)
            sectype_inserted += inserted

        if sectype_inserted > 0:
            logger.info(f"  ✅ Создано {sectype_inserted} записей")
            total_inserted += sectype_inserted

    return total_inserted


def aggregate_last_hour(engine) -> int:
    """Агрегировать последний завершённый час для всех тикеров."""

    now = get_moscow_time()
    # Последний завершённый час
    last_hour = now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
    tradedate = last_hour.date()
    hour = last_hour.hour

    logger.info(f"Агрегация часа: {tradedate} {hour:02d}:00")

    total_inserted = 0

    for sectype in ALGOPACK_TICKERS:
        inserted = aggregate_hour(engine, sectype, tradedate, hour)
        if inserted > 0:
            total_inserted += inserted

    return total_inserted


def get_stats(engine) -> dict:
    """Получить статистику по interval."""

    query = text("""
        SELECT 
            interval,
            COUNT(*) as records,
            COUNT(DISTINCT sectype) as tickers,
            MIN(tradedate) as date_from,
            MAX(tradedate) as date_to
        FROM open_interest
        WHERE interval IN (5, 60, 24)
        GROUP BY interval
        ORDER BY interval
    """)

    with engine.connect() as conn:
        result = conn.execute(query)
        stats = {}
        for row in result.fetchall():
            stats[row[0]] = {
                'records': row[1],
                'tickers': row[2],
                'date_from': row[3],
                'date_to': row[4]
            }
        return stats


def run_once(engine, force: bool = False):
    """Однократный запуск: проверить и агрегировать всё недостающее."""

    logger.info("=" * 60)
    logger.info("АГРЕГАЦИЯ OI: 5 мин → 60 мин")
    logger.info("Режим: однократная проверка (полная)")
    logger.info("=" * 60)

    # === Проверка торгового дня ===
    if not force:
        is_trading, reason = is_trading_day()
        if not is_trading:
            logger.info(f"⏭️ Пропуск: {reason}")
            logger.info("  (используйте --force для принудительного запуска)")
            return

    # Статистика до
    stats_before = get_stats(engine)
    logger.info(f"\nСтатистика ДО:")
    for interval, s in stats_before.items():
        logger.info(f"  interval={interval}: {s['records']:,} записей, {s['tickers']} тикеров")

    # Агрегация
    logger.info(f"\n📊 Поиск недостающих часов...")
    inserted = aggregate_all_missing(engine)

    # Статистика после
    stats_after = get_stats(engine)
    logger.info(f"\nСтатистика ПОСЛЕ:")
    for interval, s in stats_after.items():
        logger.info(f"  interval={interval}: {s['records']:,} записей, {s['tickers']} тикеров")

    logger.info(f"\n✅ Всего создано: {inserted} часовых записей")


def run_last_hour(engine, force: bool = False):
    """Быстрая агрегация только последнего часа (для оркестратора)."""

    logger.info("=" * 60)
    logger.info("АГРЕГАЦИЯ OI: 5 мин → 60 мин")
    logger.info("Режим: только последний час")
    logger.info("=" * 60)

    # === Проверка торгового дня ===
    if not force:
        is_trading, reason = is_trading_day()
        if not is_trading:
            logger.info(f"⏭️ Пропуск: {reason}")
            logger.info("  (используйте --force для принудительного запуска)")
            return

    inserted = aggregate_last_hour(engine)

    if inserted > 0:
        logger.info(f"✅ Создано {inserted} записей")
    else:
        logger.info("- Нет данных для агрегации")


def run_recent(engine, days: int = 7, force: bool = False):
    """Агрегация только за последние N дней (компромисс между скоростью и полнотой)."""

    logger.info("=" * 60)
    logger.info("АГРЕГАЦИЯ OI: 5 мин → 60 мин")
    logger.info(f"Режим: последние {days} дней")
    logger.info("=" * 60)

    # === Проверка торгового дня ===
    if not force:
        is_trading, reason = is_trading_day()
        if not is_trading:
            logger.info(f"⏭️ Пропуск: {reason}")
            logger.info("  (используйте --force для принудительного запуска)")
            return

    inserted = aggregate_all_missing(engine, limit_days=days)

    logger.info(f"\n✅ Всего создано: {inserted} часовых записей")


def run_daemon(engine):
    """Работа в цикле: ждём окончания часа и агрегируем."""

    logger.info("=" * 60)
    logger.info("АГРЕГАЦИЯ OI: 5 мин → 60 мин")
    logger.info("Режим: daemon (ожидание после каждого часа)")
    logger.info("=" * 60)

    # Сначала агрегируем недостающее за последние 7 дней (быстрее чем вся история)
    logger.info("\n📊 Первичная проверка (последние 7 дней)...")
    inserted = aggregate_all_missing(engine, limit_days=7)
    logger.info(f"✅ Создано {inserted} записей")

    logger.info("\n🔄 Переход в режим ожидания...")

    while True:
        try:
            now = get_moscow_time()

            # === Проверка торгового дня ===
            is_trading, reason = is_trading_day()
            if not is_trading:
                logger.debug(f"Вне торгов: {reason}")
                time.sleep(3600)  # Спим час
                continue

            # Ждём до начала следующего часа + 2 минуты (чтобы Algopack успел записать 55-ю минуту)
            next_hour = now.replace(minute=2, second=0, microsecond=0) + timedelta(hours=1)
            if now.minute >= 2:
                next_hour += timedelta(hours=0)
            else:
                next_hour = now.replace(minute=2, second=0, microsecond=0)

            wait_seconds = (next_hour - now).total_seconds()

            if wait_seconds > 0:
                logger.info(f"⏳ Ожидание до {next_hour.strftime('%H:%M')} ({int(wait_seconds)} сек)...")
                time.sleep(wait_seconds)

            # Агрегируем последний час
            logger.info(f"\n🔄 [{get_moscow_time().strftime('%H:%M:%S')}] Агрегация последнего часа...")
            inserted = aggregate_last_hour(engine)

            if inserted > 0:
                logger.info(f"✅ Создано {inserted} записей")
            else:
                logger.info("- Нет данных для агрегации (возможно Algopack ещё не записал)")

        except KeyboardInterrupt:
            logger.info("\n⏹ Остановка daemon...")
            break
        except Exception as e:
            logger.error(f"Ошибка: {e}")
            time.sleep(60)


def main():
    parser = argparse.ArgumentParser(
        description='Агрегация OI: 5 минут → 60 минут',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры:
  python aggregate_oi_hourly.py              # Полная проверка всей истории
  python aggregate_oi_hourly.py --last-hour  # Только последний час (быстро)
  python aggregate_oi_hourly.py --recent 3   # Последние 3 дня
  python aggregate_oi_hourly.py daemon       # Работа в цикле
  python aggregate_oi_hourly.py --force      # Игнорировать проверку торгового дня
        """
    )

    parser.add_argument('mode', nargs='?', default='once',
                        choices=['once', 'daemon'],
                        help='Режим работы: once (по умолчанию) или daemon')
    parser.add_argument('--last-hour', action='store_true',
                        help='Агрегировать только последний час (для оркестратора)')
    parser.add_argument('--recent', type=int, metavar='DAYS',
                        help='Агрегировать только последние N дней')
    parser.add_argument('--force', action='store_true',
                        help='Игнорировать проверку торгового дня')

    args = parser.parse_args()

    engine = get_engine()

    # Определяем режим работы
    if args.last_hour:
        run_last_hour(engine, force=args.force)
    elif args.recent:
        run_recent(engine, days=args.recent, force=args.force)
    elif args.mode == "daemon":
        run_daemon(engine)
    else:
        run_once(engine, force=args.force)


if __name__ == "__main__":
    main()