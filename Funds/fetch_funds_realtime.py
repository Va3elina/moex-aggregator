#!/usr/bin/env python3
"""
Реалтайм обновление данных фондов с ISS MOEX.

Источник: ISS MOEX API (https://iss.moex.com)
Данные:
- ПАЙ (CLOSE) берётся с торгов (market=shares, board=TQTF)
- СЧА (CAPITALIZATION) берётся с iNAV (market=index)

Расписание (МСК):
- Раз в день: 09:00 (данные за вчера появляются к утру)
- Выходные пропускаются

Использование:
  python fetch_funds_realtime.py --once --force    # Разово
  python fetch_funds_realtime.py                   # Демон
"""

import asyncio
import aiohttp
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta, date
from typing import Optional, Dict, List
import logging
import sys
from pathlib import Path
import argparse

# === Импорт из корневой папки ===
from dotenv import load_dotenv
import os

sys.path.insert(0, str(Path(__file__).parent.parent))
load_dotenv(Path(__file__).parent.parent / ".env")

DB_URL = os.getenv("DB_URL")

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


# ═══════════════════════════════════════════════════════════════════════════════
# КОНФИГУРАЦИЯ
# ═══════════════════════════════════════════════════════════════════════════════

# Время обновления (МСК)
UPDATE_HOUR = 9
UPDATE_MINUTE = 0

# Директория логов
LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

CHUNK_SIZE_DAYS = 100

# URL шаблоны
URL_SHARES = (
    "https://iss.moex.com/iss/history/engines/stock/markets/shares"
    "/boards/TQTF/securities/{secid}.json"
)
URL_INDEX = (
    "https://iss.moex.com/iss/history/engines/stock/markets/index"
    "/securities/{secid}.json"
)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
}

# Конфигурация фондов
# ID соответствует `fund_id` в базе данных (таблица funds)
FUNDS = {
    # Денежный рынок
    8181: {"shares": "AKMM", "inav": "AKMMA", "name": "Альфа-Капитал Денежный рынок"},
    8628: {"shares": "TMON", "inav": "TMONA", "name": "Т-Капитал Денежный Рынок"},
    7373: {"shares": "SBMM", "inav": "SBMMA", "name": "Первая Сберегательный"},
    5973: {"shares": "LQDT", "inav": "LQDTM", "name": "ВИМ Ликвидность"},

    # Акции
    6333: {"shares": "TMOS", "inav": "TMOSA", "name": "Т-Капитал Индекс МосБиржи"},
    5247: {"shares": "SBMX", "inav": "SBMXA", "name": "Первая Топ Российских акций"},
    6073: {"shares": "EQMX", "inav": "EQMXE", "name": "Индекс МосБиржи ВИМ"},
    6575: {"shares": "AKME", "inav": "AKMEI", "name": "Альфа-Капитал Управляемые акции"},

    # Облигации
    6225: {"shares": "AKMB", "inav": "AKMBA", "name": "Альфа Управляемые облигации"},
    10331: {"shares": "SBLB", "inav": "SBLBA", "name": "Первая Долгосрочные гособлигации"},
    11445: {"shares": "TOFZ", "inav": "TOFZA", "name": "Т-Капитал ОФЗ"},
    11705: {"shares": "AMGB", "inav": "AMGBA", "name": "АТОН Длинные ОФЗ"},

    # Золото
    4713: {"shares": "AKGD", "inav": "AKGDA", "name": "Альфа-Капитал Золото"},
    4038: {"shares": "GOLD", "inav": "GOLDO", "name": "Золото Биржевой"},
    5061: {"shares": "SBGD", "inav": "SBGDA", "name": "Первая Доступное золото"},
    4098: {"shares": "TGLD", "inav": "TGLDB", "name": "Т-Капитал Золото"},
}


# ═══════════════════════════════════════════════════════════════════════════════
# ЛОГИРОВАНИЕ
# ═══════════════════════════════════════════════════════════════════════════════

def setup_logging():
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

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(console_fmt)
    root.addHandler(ch)

    fh = logging.FileHandler(
        LOG_DIR / f"funds_{datetime.now():%Y%m%d}.log",
        encoding='utf-8'
    )
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(detailed_fmt)
    root.addHandler(fh)

    fh_err = logging.FileHandler(
        LOG_DIR / f"funds_errors_{datetime.now():%Y%m%d}.log",
        encoding='utf-8'
    )
    fh_err.setLevel(logging.WARNING)
    fh_err.setFormatter(detailed_fmt)
    root.addHandler(fh_err)

    return logging.getLogger(__name__)


log = setup_logging()


# ═══════════════════════════════════════════════════════════════════════════════
# ISS MOEX API
# ═══════════════════════════════════════════════════════════════════════════════

async def fetch_chunk(session, url_template, secid, date_from, date_to):
    """Получить данные за один чанк (до ~100 записей)."""
    params = {
        "from": date_from.isoformat(), "till": date_to.isoformat(),
        "limit": 100, "iss.meta": "off", "iss.json": "extended", "lang": "ru",
    }
    url = url_template.format(secid=secid)
    try:
        async with session.get(url, params=params, headers=HEADERS, timeout=30) as resp:
            if resp.status != 200:
                log.warning(f"  [{secid}] HTTP {resp.status}")
                return []
            raw = await resp.json()
            return raw[1].get("history", []) if len(raw) >= 2 else []
    except Exception as e:
        log.error(f"  [{secid}] Ошибка: {e}")
        return []


async def fetch_full(session, url_template, secid, date_from, date_to):
    """Получить все данные с пагинацией по чанкам."""
    all_records = []
    current = date_from
    while current <= date_to:
        chunk_end = min(current + timedelta(days=CHUNK_SIZE_DAYS), date_to)
        records = await fetch_chunk(session, url_template, secid, current, chunk_end)
        all_records.extend(records)
        log.debug(f"  [{secid}] {current}—{chunk_end}: {len(records)}")
        current = chunk_end + timedelta(days=1)
        await asyncio.sleep(0.2)
    return all_records


# ═══════════════════════════════════════════════════════════════════════════════
# БАЗА ДАННЫХ
# ═══════════════════════════════════════════════════════════════════════════════

def get_engine():
    if not DB_URL:
        raise ValueError("DB_URL не установлен в .env")
    return create_engine(DB_URL, connect_args={"ssl_context": False})


def get_last_date(engine, fund_id: int) -> Optional[date]:
    """Получить последнюю дату для фонда в БД"""
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT MAX(trade_date) FROM fund_data WHERE fund_id = :fund_id
        """), {"fund_id": fund_id}).fetchone()
        return result[0] if result and result[0] else None


def merge_shares_inav(shares_rows, inav_rows):
    """Объединить данные shares и iNAV по дате → {date: {pay, nav}}"""
    merged = {}
    for r in shares_rows:
        td = r.get("TRADEDATE")
        if td:
            merged.setdefault(td, {})["pay"] = r.get("CLOSE")
    for r in inav_rows:
        td = r.get("TRADEDATE")
        if td:
            merged.setdefault(td, {})["nav"] = r.get("CAPITALIZATION")
    return merged


def save_fund_data(engine, fund_id: int, merged: Dict[str, dict]) -> int:
    """Сохранить данные в fund_data."""
    if not merged:
        return 0
    with engine.connect() as conn:
        n = 0
        for td_str, vals in merged.items():
            pay = vals.get("pay")
            nav = vals.get("nav")
            if pay is None and nav is None:
                continue
            conn.execute(text("""
                INSERT INTO fund_data (fund_id, trade_date, pay, nav)
                VALUES (:fid, :td, :pay, :nav)
                ON CONFLICT (fund_id, trade_date) DO UPDATE SET
                    pay = COALESCE(EXCLUDED.pay, fund_data.pay),
                    nav = COALESCE(EXCLUDED.nav, fund_data.nav),
                    created_at = CURRENT_TIMESTAMP
            """), {"fid": fund_id, "td": td_str, "pay": pay, "nav": nav})
            n += 1
        conn.commit()
        return n


# ═══════════════════════════════════════════════════════════════════════════════
# ОСНОВНАЯ ЛОГИКА
# ═══════════════════════════════════════════════════════════════════════════════

async def update_all_funds(force: bool = False) -> Dict[int, int]:
    """Обновить все фонды"""
    engine = get_engine()
    results = {}
    today = date.today()

    log.info("=" * 60)
    log.info("📊 ОБНОВЛЕНИЕ ФОНДОВ (ISS MOEX)")
    log.info("=" * 60)

    async with aiohttp.ClientSession() as session:
        for fund_id, info in FUNDS.items():
            name = info["name"]
            ticker = info["shares"]
            secid_inav = info["inav"]

            log.info(f"\n── {ticker:6s} │ iNAV={secid_inav} │ fund_id={fund_id}")

            # Определяем дату начала
            last_date = get_last_date(engine, fund_id)
            if last_date and not force:
                start = last_date - timedelta(days=3)
                log.info(f"   Докачка с {start} (последняя: {last_date})")
            else:
                start = today - timedelta(days=365)
                log.info(f"   Полная загрузка с {start}")

            # Параллельно грузим shares и iNAV (с пагинацией)
            shares_task = fetch_full(session, URL_SHARES, ticker, start, today)
            inav_task = fetch_full(session, URL_INDEX, secid_inav, start, today)
            shares_rows, inav_rows = await asyncio.gather(shares_task, inav_task)

            merged = merge_shares_inav(shares_rows, inav_rows)
            log.info(f"   shares: {len(shares_rows)}, iNAV: {len(inav_rows)}, дат: {len(merged)}")

            saved = save_fund_data(engine, fund_id, merged)
            results[fund_id] = saved

            if saved > 0:
                log.info(f"   ✅ Сохранено: {saved}")
            else:
                log.debug(f"   — нет новых данных")

            await asyncio.sleep(0.3)

    engine.dispose()

    total = sum(results.values())
    log.info("=" * 60)
    log.info(f"✅ ГОТОВО: {total} записей")
    log.info("=" * 60)

    return results


# ═══════════════════════════════════════════════════════════════════════════════
# ДЕМОН
# ═══════════════════════════════════════════════════════════════════════════════

async def run_daemon():
    """Запуск в режиме демона"""
    log.info("🚀 Запуск демона обновления фондов")
    log.info(f"  Расписание: {UPDATE_HOUR:02d}:{UPDATE_MINUTE:02d} МСК")

    last_update_date = None

    while True:
        now = get_moscow_time()
        today = now.date()

        # Проверяем расписание
        is_trade_day, reason = is_trading_day(today)

        if is_trade_day:
            if now.hour >= UPDATE_HOUR and now.minute >= UPDATE_MINUTE:
                if last_update_date != today:
                    log.info(f"⏰ [{now:%H:%M:%S} МСК] Запуск обновления...")
                    try:
                        await update_all_funds(force=False)
                        last_update_date = today
                    except Exception as e:
                        log.error(f"Ошибка обновления: {e}")

        await asyncio.sleep(60)


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

async def main():
    parser = argparse.ArgumentParser(description="Обновление данных фондов (ISS MOEX)")
    parser.add_argument("--once", action="store_true", help="Однократный запуск")
    parser.add_argument("--force", action="store_true", help="Принудительное обновление")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("ЗАПУСК СКРИПТА ФОНДОВ (БПИФ) - ISS MOEX")
    log.info(f"Время: {datetime.now()}")
    log.info(f"МСК: {get_moscow_time()}")
    log.info(f"Режим: {'однократный' if args.once else 'daemon'}")
    log.info("=" * 60)

    # Проверка торгового дня в режиме --once
    if args.once and not args.force:
        is_trading, reason = is_trading_day()
        if not is_trading:
            log.info(f"⏭️ Пропуск: {reason}")
            log.info("  (используйте --force для принудительного запуска)")
            return

    if args.once:
        await update_all_funds(force=args.force)
    else:
        await run_daemon()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Остановлено")
    except Exception as e:
        log.critical(f"Ошибка: {e}")
        sys.exit(1)
