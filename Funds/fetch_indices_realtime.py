#!/usr/bin/env python3
"""
Реалтайм обновление индексов MOEX (для оркестратора).

Индексы:
- IMOEX — Индекс МосБиржи
- RGBITR — Индекс гособлигаций полной доходности
- RUSFAR3M — Индекс денежного рынка
- GLDRUB_TOM — Золото (spot)

Расписание:
- Ежедневно в 09:00 МСК
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

LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

# Индексы для загрузки
INDICES = {
    "IMOEX": {
        "name": "Индекс МосБиржи",
        "engine": "stock",
        "market": "index",
        "board": "SNDX",
        "start_date": date(1997, 9, 22),  # IMOEX inception date
    },
    "RGBITR": {
        "name": "Индекс гособлигаций полной доходности",
        "engine": "stock",
        "market": "index",
        "board": "SNDX",
    },
    "RUSFAR3M": {
        "name": "Индекс денежного рынка RUSFAR 3M",
        "engine": "stock",
        "market": "index",
        "board": "SNDX",
    },
    "GLDRUB_TOM": {
        "name": "Золото (руб/г, спот TOM)",
        "engine": "currency",
        "market": "selt",
        "board": "CETS",
        "filter_board": True,
    },
    "MCFTR": {
        "name": "Индекс полной доходности МосБиржи",
        "engine": "stock",
        "market": "index",
        "board": "SNDX",
        "start_date": date(2005, 1, 1),  # MCFTR available from 2005
    },
    "RTSI": {
        "name": "Индекс РТС",
        "engine": "stock",
        "market": "index",
        "board": "RTSI",
        "filter_board": True,
        "start_date": date(1995, 9, 1),
    },
    "RGBI": {
        "name": "Индекс гособлигаций ценовой",
        "engine": "stock",
        "market": "index",
        "board": "SNDX",
        "start_date": date(2002, 12, 30),
    },
    "RVI": {
        "name": "Индекс волатильности RVI",
        "engine": "stock",
        "market": "index",
        "board": "RTSI",
        "filter_board": True,
        "start_date": date(2013, 11, 18),
    },
    "USD000UTSTOM": {
        "name": "Доллар/Рубль (спот TOM)",
        "engine": "currency",
        "market": "selt",
        "board": "CETS",
        "filter_board": True,
        "use_candles": True,  # history endpoint отдаёт нули, используем candles
        "start_date": date(2003, 4, 15),
    },
    "EUR_RUB__TOM": {
        "name": "Евро/Рубль (спот TOM)",
        "engine": "currency",
        "market": "selt",
        "board": "CETS",
        "filter_board": True,
        "start_date": date(2013, 1, 1),
    },
    "CNYRUB_TOM": {
        "name": "Юань/Рубль (спот TOM)",
        "engine": "currency",
        "market": "selt",
        "board": "CETS",
        "filter_board": True,
        # ISS отдаёт непрерывную историю CNYRUB_TOM с 15.04.2013 — достаточно
        # для годовой сезонности. До 2022 объёмы малые, но close-и валидны.
        "start_date": date(2013, 4, 15),
    },
}

DEFAULT_START_DATE = date(2020, 1, 1)
UPDATE_HOUR = 9  # МСК


CHUNK_SIZE_DAYS = 100


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
        LOG_DIR / f"indices_{datetime.now():%Y%m%d}.log",
        encoding='utf-8'
    )
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(detailed_fmt)
    root.addHandler(fh)

    fh_err = logging.FileHandler(
        LOG_DIR / f"indices_errors_{datetime.now():%Y%m%d}.log",
        encoding='utf-8'
    )
    fh_err.setLevel(logging.WARNING)
    fh_err.setFormatter(detailed_fmt)
    root.addHandler(fh_err)

    return logging.getLogger(__name__)

log = setup_logging()

# URL шаблоны
BASE_URL_BOARD = "https://iss.moex.com/iss/history/engines/{engine}/markets/{market}/boards/{board}/securities/{secid}.json"
BASE_URL_NOBOARD = "https://iss.moex.com/iss/history/engines/{engine}/markets/{market}/securities/{secid}.json"
BASE_URL_CANDLES = "https://iss.moex.com/iss/engines/{engine}/markets/{market}/boards/{board}/securities/{secid}/candles.json"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
}

# Маппинг полей
FIELD_MAP = {
    "trade_date": ["TRADEDATE"],
    "open":       ["OPEN"],
    "high":       ["HIGH"],
    "low":        ["LOW"],
    "close":      ["CLOSE", "LEGALCLOSEPRICE", "WAPRICE"],
    "value":      ["VALUE", "VOLRUR"],
    "capitalization": ["CAPITALIZATION"],
}

def extract_field(record: dict, field_name: str) -> Optional[float]:
    """Извлечь значение поля, пробуя несколько вариантов имени."""
    candidates = FIELD_MAP.get(field_name, [field_name])
    for key in candidates:
        val = record.get(key)
        if val is not None:
            return val
    return None


# ═══════════════════════════════════════════════════════════════════════════════
# ISS MOEX API
# ═══════════════════════════════════════════════════════════════════════════════

async def fetch_index_chunk(
    session: aiohttp.ClientSession,
    secid: str,
    info: dict,
    date_from: date,
    date_to: date,
) -> List[dict]:
    """Получить данные индекса за период (один чанк)."""
    use_candles = info.get("use_candles", False)

    if use_candles:
        url = BASE_URL_CANDLES.format(
            engine=info["engine"],
            market=info["market"],
            board=info["board"],
            secid=secid,
        )
    elif info.get("filter_board"):
        url = BASE_URL_BOARD.format(
            engine=info["engine"],
            market=info["market"],
            board=info["board"],
            secid=secid,
        )
    else:
        url = BASE_URL_NOBOARD.format(
            engine=info["engine"],
            market=info["market"],
            secid=secid,
        )

    params = {
        "from": date_from.strftime("%Y-%m-%d"),
        "iss.meta": "off",
    }
    if use_candles:
        params["interval"] = "24"
        params["till"] = date_to.strftime("%Y-%m-%d")
    else:
        params["till"] = date_to.strftime("%Y-%m-%d")

    try:
        async with session.get(url, params=params, headers=HEADERS, timeout=30) as response:
            if response.status == 404:
                log.error(f"❌ [{secid}] Индекс не найден (404).")
                return []
            elif response.status in (500, 502, 503):
                log.error(f"❌ [{secid}] Сервер недоступен ({response.status}).")
                return []
            elif response.status != 200:
                log.error(f"❌ [{secid}] HTTP {response.status}. URL: {url}")
                return []

            try:
                data = await response.json()
            except Exception as json_err:
                log.error(f"❌ [{secid}] Ошибка парсинга JSON: {json_err}")
                return []

            if use_candles:
                # Candles endpoint: ключ "candles", колонки: open, close, high, low, value, volume, begin, end
                if "candles" not in data:
                    log.error(f"❌ [{secid}] Отсутствует ключ 'candles' в ответе.")
                    return []
                candles_data = data["candles"]
                columns = candles_data.get("columns", [])
                rows = candles_data.get("data", [])
                if not rows:
                    return []
                # Конвертируем в формат history: TRADEDATE, OPEN, HIGH, LOW, CLOSE, VALUE
                result = []
                for row in rows:
                    rec = dict(zip(columns, row))
                    result.append({
                        "TRADEDATE": rec["begin"][:10],  # "2025-01-03 00:00:00" → "2025-01-03"
                        "OPEN": rec.get("open"),
                        "HIGH": rec.get("high"),
                        "LOW": rec.get("low"),
                        "CLOSE": rec.get("close"),
                        "VALUE": rec.get("value"),
                    })
                return result
            else:
                if not isinstance(data, dict) or "history" not in data:
                    log.error(f"❌ [{secid}] Отсутствует ключ 'history' в ответе.")
                    return []

                history = data["history"]
                columns = history.get("columns", [])
                rows = history.get("data", [])

                if not rows:
                    log.debug(f"  [{secid}] Нет данных за {date_from} - {date_to}")
                    return []

                return [dict(zip(columns, row)) for row in rows]

    except asyncio.TimeoutError:
        log.error(f"❌ [{secid}] Таймаут соединения (30с).")
        return []
    except aiohttp.ClientConnectorError as e:
        log.error(f"❌ [{secid}] Ошибка соединения: {e}")
        return []
    except aiohttp.ServerDisconnectedError:
        log.error(f"❌ [{secid}] Сервер разорвал соединение.")
        return []
    except Exception as e:
        log.error(f"❌ [{secid}] Ошибка: {type(e).__name__}: {e}")
        return []


async def fetch_index_full(
    session: aiohttp.ClientSession,
    secid: str,
    info: dict,
    date_from: date,
    date_to: date,
) -> List[dict]:
    """Получить все данные индекса (с пагинацией по датам)."""
    all_records = []
    current_from = date_from

    while current_from <= date_to:
        current_to = min(current_from + timedelta(days=CHUNK_SIZE_DAYS), date_to)
        records = await fetch_index_chunk(session, secid, info, current_from, current_to)
        all_records.extend(records)
        log.debug(f"  [{secid}] {current_from} - {current_to}: {len(records)} записей")
        current_from = current_to + timedelta(days=1)
        await asyncio.sleep(0.3)

    return all_records


# ═══════════════════════════════════════════════════════════════════════════════
# БАЗА ДАННЫХ
# ═══════════════════════════════════════════════════════════════════════════════

def get_engine():
    if not DB_URL:
        raise ValueError("DB_URL не установлен в .env")
    return create_engine(DB_URL, connect_args={"ssl_context": False})


def create_tables(engine):
    """Создать таблицы indices (metadata) и index_data (данные)."""
    with engine.connect() as conn:
        # Metadata-таблица (реестр индексов)
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS indices (
                id SERIAL PRIMARY KEY,
                secid VARCHAR(20) UNIQUE NOT NULL,
                name VARCHAR(255) NOT NULL,
                engine VARCHAR(20),
                market VARCHAR(20),
                board VARCHAR(20),
                start_date DATE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))

        # Таблица данных
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS index_data (
                id SERIAL PRIMARY KEY,
                secid VARCHAR(20) NOT NULL,
                trade_date DATE NOT NULL,
                open NUMERIC(18, 4),
                high NUMERIC(18, 4),
                low NUMERIC(18, 4),
                close NUMERIC(18, 4),
                value NUMERIC(24, 2),
                capitalization NUMERIC(24, 2),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(secid, trade_date)
            )
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_index_data_secid_date
            ON index_data(secid, trade_date)
        """))

        # Заполнить indices из INDICES dict (upsert)
        for secid, info in INDICES.items():
            conn.execute(text("""
                INSERT INTO indices (secid, name, engine, market, board)
                VALUES (:secid, :name, :engine, :market, :board)
                ON CONFLICT (secid) DO UPDATE SET
                    name = EXCLUDED.name,
                    engine = EXCLUDED.engine,
                    market = EXCLUDED.market,
                    board = EXCLUDED.board
            """), {
                "secid": secid,
                "name": info["name"],
                "engine": info["engine"],
                "market": info["market"],
                "board": info["board"],
            })

        conn.commit()
    log.info("✅ Таблицы indices + index_data готовы")


def get_last_date(engine, secid: str) -> Optional[date]:
    """Последняя дата в БД для индекса."""
    with engine.connect() as conn:
        result = conn.execute(text(
            "SELECT MAX(trade_date) FROM index_data WHERE secid = :secid"
        ), {"secid": secid}).fetchone()
        return result[0] if result and result[0] else None


def save_to_db(engine, secid: str, records: List[dict]) -> int:
    """Сохранить данные индекса в БД."""
    if not records:
        return 0

    with engine.connect() as conn:
        inserted = 0
        for record in records:
            try:
                trade_date_str = extract_field(record, "trade_date")
                if not trade_date_str:
                    continue

                trade_date = datetime.strptime(str(trade_date_str), "%Y-%m-%d").date()

                conn.execute(text("""
                    INSERT INTO index_data (secid, trade_date, open, high, low, close, value, capitalization)
                    VALUES (:secid, :trade_date, :open, :high, :low, :close, :value, :capitalization)
                    ON CONFLICT (secid, trade_date) DO UPDATE SET
                        open = EXCLUDED.open,
                        high = EXCLUDED.high,
                        low = EXCLUDED.low,
                        close = EXCLUDED.close,
                        value = EXCLUDED.value,
                        capitalization = EXCLUDED.capitalization,
                        created_at = CURRENT_TIMESTAMP
                """), {
                    "secid": secid,
                    "trade_date": trade_date,
                    "open": extract_field(record, "open"),
                    "high": extract_field(record, "high"),
                    "low": extract_field(record, "low"),
                    "close": extract_field(record, "close"),
                    "value": extract_field(record, "value"),
                    "capitalization": extract_field(record, "capitalization"),
                })
                inserted += 1
            except Exception as e:
                log.warning(f"Ошибка записи {record.get('TRADEDATE')}: {e}")

        conn.commit()
        return inserted


# ═══════════════════════════════════════════════════════════════════════════════
# ОСНОВНАЯ ЛОГИКА
# ═══════════════════════════════════════════════════════════════════════════════

async def update_indices(force: bool = False) -> Dict[str, int]:
    """Обновить все индексы (докачка недостающих)."""
    engine = get_engine()
    create_tables(engine)
    results = {}
    today = date.today()

    log.info("=" * 60)
    log.info("📊 ЗАГРУЗКА ИНДЕКСОВ MOEX")
    log.info("=" * 60)

    async with aiohttp.ClientSession() as session:
        for secid, info in INDICES.items():
            log.info(f"\n📈 {secid} — {info['name']}")
            log.info(f"   endpoint: {info['engine']}/{info['market']}")

            index_start = info.get("start_date", DEFAULT_START_DATE)
            last_date = get_last_date(engine, secid)
            if last_date and not force:
                start = last_date - timedelta(days=3)
                log.info(f"  Последняя дата: {last_date}, докачка с {start}")
            else:
                start = index_start
                log.info(f"  Нет данных, загрузка с {start}")

            records = await fetch_index_full(session, secid, info, start, today)
            log.info(f"  Получено: {len(records)} записей")

            saved = save_to_db(engine, secid, records)
            results[secid] = saved

            if saved > 0:
                log.info(f"  ✅ Сохранено: {saved} записей")
            else:
                log.debug(f"  — нет новых данных")

            await asyncio.sleep(0.5)

    engine.dispose()

    total = sum(results.values())
    log.info("\n" + "=" * 60)
    log.info(f"✅ ГОТОВО: {total} записей всего")
    for secid, count in results.items():
        log.info(f"  {secid}: {count}")
    log.info("=" * 60)

    return results


async def run_daemon():
    """Режим демона — обновление по расписанию"""
    log.info("=" * 60)
    log.info("🚀 ЗАПУСК РЕАЛТАЙМ ИНДЕКСОВ")
    log.info(f"  Расписание: ежедневно в {UPDATE_HOUR:02d}:00 МСК")
    log.info("=" * 60)

    last_update_date = None

    while True:
        now = get_moscow_time()
        today = now.date()

        # Проверяем расписание
        is_trade, _ = is_trading_day(today)
        should_update = (
            is_trade and
            now.hour >= UPDATE_HOUR and
            last_update_date != today
        )

        if should_update:
            log.info(f"⏰ [{now:%H:%M}] Запуск обновления...")
            try:
                await update_indices()
                last_update_date = today
            except Exception as e:
                log.error(f"Ошибка обновления: {e}")

        # Спим до следующей проверки
        await asyncio.sleep(300)  # 5 минут


async def main():
    parser = argparse.ArgumentParser(description="Реалтайм индексы MOEX")
    parser.add_argument("--once", action="store_true", help="Однократное обновление")
    parser.add_argument("--force", action="store_true", help="Принудительно с START_DATE")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("ЗАПУСК СКРИПТА ИНДЕКСОВ - ISS MOEX")
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
        await update_indices(force=args.force)
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
