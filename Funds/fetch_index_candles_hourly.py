#!/usr/bin/env python3
"""
Часовые свечи индексов и валют MOEX (для intraday-сезонности).

Источник: публичный ISS /candles.json (interval=60) — проверено, совпадает
байт-в-байт с платным Algopack (см. Candles/backfill_futures_intraday.py),
платный источник не нужен. Глубина по /candleborders.json — per-тикер,
см. inception_date в конфиге ниже (проверялось вручную на ISS).

Пишем в таблицу candles с type из конфига ('index' для индексов MOEX,
'currency' для валютных пар TOM). Дневная история остаётся в index_data —
она длиннее (индексы с 1997-09-22) и используется дневными режимами сезонности.

Режимы:
  --once   — докачка от последнего begin_time в БД
  --force  — полный бэкфилл с inception_date конфигурации
"""

import argparse
import asyncio
import logging
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

import aiohttp
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

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
        if check.weekday() in (5, 6):
            return False, "Выходной"
        return True, "Торговый день"


# ═══════════════════════════════════════════════════════════════════════════════
# КОНФИГУРАЦИЯ
# ═══════════════════════════════════════════════════════════════════════════════

# Индексы и валюты MOEX, для которых тянем часовые бары.
# inception_date — самая ранняя дата с interval=60, проверено вручную по
# https://iss.moex.com/iss/engines/{engine}/markets/{market}/securities/{secid}/candleborders.json
# EUR_RUB__TOM сознательно не добавлен — часовые данные обрываются 2024-06-11
# (MOEX перестал публиковать евро после отключения клиринга), живых обновлений
# не будет.
INDICES = {
    "IMOEX":        {"engine": "stock",    "market": "index", "board": "SNDX", "type": "index",    "inception_date": date(2011, 11, 17)},
    "RTSI":         {"engine": "stock",    "market": "index", "board": "RTSI", "type": "index",    "inception_date": date(2011, 12, 19)},
    "RGBI":         {"engine": "stock",    "market": "index", "board": "SNDX", "type": "index",    "inception_date": date(2011, 11, 17)},
    "RGBITR":       {"engine": "stock",    "market": "index", "board": "SNDX", "type": "index",    "inception_date": date(2011, 11, 17)},
    "RVI":          {"engine": "stock",    "market": "index", "board": "RTSI", "type": "index",    "inception_date": date(2013, 12, 16)},
    "USD000UTSTOM": {"engine": "currency", "market": "selt",  "board": "CETS", "type": "currency", "inception_date": date(2011, 11, 17)},
    "CNYRUB_TOM":   {"engine": "currency", "market": "selt",  "board": "CETS", "type": "currency", "inception_date": date(2013, 4, 15)},
    "GLDRUB_TOM":   {"engine": "currency", "market": "selt",  "board": "CETS", "type": "currency", "inception_date": date(2013, 10, 21)},
}

INTERVAL = 60  # 1 час
CHUNK_SIZE_DAYS = 90  # ~9 свечей/день * 90 ≈ 810 — две страницы по 500
ISS_PAGE_SIZE = 500

LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)


# ═══════════════════════════════════════════════════════════════════════════════
# ЛОГИРОВАНИЕ
# ═══════════════════════════════════════════════════════════════════════════════

def setup_logging():
    detailed_fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    console_fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%H:%M:%S",
    )

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    root.handlers.clear()

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(console_fmt)
    root.addHandler(ch)

    fh = logging.FileHandler(
        LOG_DIR / f"index_candles_hourly_{datetime.now():%Y%m%d}.log",
        encoding="utf-8",
    )
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(detailed_fmt)
    root.addHandler(fh)

    fh_err = logging.FileHandler(
        LOG_DIR / f"index_candles_hourly_errors_{datetime.now():%Y%m%d}.log",
        encoding="utf-8",
    )
    fh_err.setLevel(logging.WARNING)
    fh_err.setFormatter(detailed_fmt)
    root.addHandler(fh_err)

    return logging.getLogger(__name__)


log = setup_logging()


# ═══════════════════════════════════════════════════════════════════════════════
# ISS MOEX
# ═══════════════════════════════════════════════════════════════════════════════

ISS_URL = (
    "https://iss.moex.com/iss/engines/{engine}/markets/{market}"
    "/boards/{board}/securities/{secid}/candles.json"
)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
}


async def fetch_candles_chunk(
    session: aiohttp.ClientSession,
    secid: str,
    info: dict,
    date_from: date,
    date_to: date,
) -> list[dict]:
    """Тянет все часовые свечи [date_from, date_to] с пагинацией."""
    url = ISS_URL.format(
        engine=info["engine"],
        market=info["market"],
        board=info["board"],
        secid=secid,
    )

    out: list[dict] = []
    columns: Optional[list[str]] = None
    start = 0

    while True:
        params = {
            "interval": INTERVAL,
            "from": date_from.strftime("%Y-%m-%d"),
            "till": date_to.strftime("%Y-%m-%d"),
            "start": start,
            "iss.meta": "off",
        }
        try:
            async with session.get(url, params=params, headers=HEADERS, timeout=30) as resp:
                if resp.status != 200:
                    log.error(f"❌ [{secid}] HTTP {resp.status} for {date_from}..{date_to}")
                    return out
                data = await resp.json()
        except asyncio.TimeoutError:
            log.error(f"❌ [{secid}] Таймаут (30с) {date_from}..{date_to}")
            return out
        except Exception as e:
            log.error(f"❌ [{secid}] {type(e).__name__}: {e}")
            return out

        candles = data.get("candles", {})
        cols = candles.get("columns") or columns
        rows = candles.get("data", [])
        if not rows:
            break
        columns = cols
        for row in rows:
            out.append(dict(zip(cols, row)))

        if len(rows) < ISS_PAGE_SIZE:
            break
        start += len(rows)
        await asyncio.sleep(0.1)

    return out


async def fetch_full(
    session: aiohttp.ClientSession,
    secid: str,
    info: dict,
    date_from: date,
    date_to: date,
) -> list[dict]:
    """Скачивает диапазон чанками по CHUNK_SIZE_DAYS."""
    all_records: list[dict] = []
    cur = date_from
    while cur <= date_to:
        nxt = min(cur + timedelta(days=CHUNK_SIZE_DAYS), date_to)
        rows = await fetch_candles_chunk(session, secid, info, cur, nxt)
        log.debug(f"  [{secid}] {cur}..{nxt}: {len(rows)} свечей")
        all_records.extend(rows)
        cur = nxt + timedelta(days=1)
        await asyncio.sleep(0.2)
    return all_records


# ═══════════════════════════════════════════════════════════════════════════════
# БАЗА ДАННЫХ
# ═══════════════════════════════════════════════════════════════════════════════

def get_engine():
    if not DB_URL:
        raise ValueError("DB_URL не установлен в .env")
    return create_engine(DB_URL, connect_args={"ssl_context": False})


def get_last_begin_time(engine, secid: str, type_label: str) -> Optional[datetime]:
    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT MAX(begin_time)
            FROM candles
            WHERE secid = :secid AND interval = :interval AND type = :type
        """), {"secid": secid, "interval": INTERVAL, "type": type_label}).fetchone()
    return row[0] if row and row[0] else None


def save_candles(engine, secid: str, records: list[dict], type_label: str) -> int:
    if not records:
        return 0

    inserted = 0
    with engine.connect() as conn:
        for rec in records:
            begin = rec.get("begin")
            end = rec.get("end")
            if not begin:
                continue
            try:
                begin_time = datetime.strptime(begin, "%Y-%m-%d %H:%M:%S")
                end_time = datetime.strptime(end, "%Y-%m-%d %H:%M:%S") if end else None
            except (ValueError, TypeError):
                continue

            try:
                conn.execute(text("""
                    INSERT INTO candles
                        (secid, sec_id, open, close, high, low, value, volume,
                         begin_time, end_time, interval, type)
                    VALUES
                        (:secid, :sec_id, :open, :close, :high, :low, :value, :volume,
                         :begin_time, :end_time, :interval, :type)
                    ON CONFLICT (secid, begin_time, interval, type) DO UPDATE SET
                        open = EXCLUDED.open,
                        close = EXCLUDED.close,
                        high = EXCLUDED.high,
                        low = EXCLUDED.low,
                        value = EXCLUDED.value,
                        volume = EXCLUDED.volume,
                        end_time = EXCLUDED.end_time
                """), {
                    "secid": secid,
                    "sec_id": secid,
                    "open": rec.get("open"),
                    "close": rec.get("close"),
                    "high": rec.get("high"),
                    "low": rec.get("low"),
                    "value": rec.get("value"),
                    "volume": rec.get("volume"),
                    "begin_time": begin_time,
                    "end_time": end_time,
                    "interval": INTERVAL,
                    "type": type_label,
                })
                inserted += 1
            except Exception as e:
                log.warning(f"  [{secid}] Ошибка записи {begin}: {e}")

        conn.commit()
    return inserted


# ═══════════════════════════════════════════════════════════════════════════════
# ОСНОВНАЯ ЛОГИКА
# ═══════════════════════════════════════════════════════════════════════════════

async def update_one(session: aiohttp.ClientSession, engine, secid: str, info: dict, force: bool) -> int:
    today = date.today()
    type_label = info["type"]
    if force:
        start = info["inception_date"]
        log.info(f"📈 {secid}: полный бэкфилл с {start}")
    else:
        last = get_last_begin_time(engine, secid, type_label)
        if last is None:
            start = info["inception_date"]
            log.info(f"📈 {secid}: пустая БД, бэкфилл с {start}")
        else:
            # Перекрываем 1 день, чтобы перезаписать неполную последнюю свечу
            start = max(info["inception_date"], last.date() - timedelta(days=1))
            log.info(f"📈 {secid}: последняя свеча {last}, докачка с {start}")

    records = await fetch_full(session, secid, info, start, today)
    log.info(f"  получено: {len(records)} свечей")
    saved = save_candles(engine, secid, records, type_label)
    log.info(f"  ✅ сохранено/обновлено: {saved}")
    return saved


async def update_all(force: bool = False) -> dict[str, int]:
    engine = get_engine()
    log.info("=" * 60)
    log.info("📊 ЧАСОВЫЕ СВЕЧИ ИНДЕКСОВ И ВАЛЮТ MOEX")
    log.info("=" * 60)

    results: dict[str, int] = {}
    async with aiohttp.ClientSession() as session:
        for secid, info in INDICES.items():
            try:
                results[secid] = await update_one(session, engine, secid, info, force)
            except Exception as e:
                log.error(f"❌ [{secid}] {type(e).__name__}: {e}")
                results[secid] = 0
            await asyncio.sleep(0.3)

    engine.dispose()
    total = sum(results.values())
    log.info("=" * 60)
    log.info(f"✅ ГОТОВО: {total} строк всего")
    for s, c in results.items():
        log.info(f"  {s}: {c}")
    log.info("=" * 60)
    return results


async def main():
    parser = argparse.ArgumentParser(description="Часовые свечи индексов MOEX (ISS)")
    parser.add_argument("--once", action="store_true", help="Однократная докачка")
    parser.add_argument("--force", action="store_true", help="Полный бэкфилл с inception_date")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("ЗАПУСК: часовые свечи индексов")
    log.info(f"Время: {datetime.now()} (МСК: {get_moscow_time()})")
    log.info("=" * 60)

    if args.once and not args.force:
        is_trading, reason = is_trading_day()
        if not is_trading:
            log.info(f"⏭️ Пропуск: {reason} (используйте --force для бэкфилла)")
            return

    await update_all(force=args.force)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Остановлено")
    except Exception as e:
        log.critical(f"Ошибка: {e}", exc_info=True)
        sys.exit(1)
