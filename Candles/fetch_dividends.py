#!/usr/bin/env python3
"""
Загрузка дивидендов акций с ISS MOEX и определение экс-дивидендных дат.

Для каждого тикера:
1. Загружает дивиденды с https://iss.moex.com/iss/securities/{SECID}/dividends.json
2. Определяет экс-дату эмпирически (ищет гэп prev_close - open ≈ dividend)
3. Сохраняет в таблицу dividends

Таблица: dividends(secid, registry_close_date, ex_date, value)

Запуск:
    python fetch_dividends.py           # обновить все акции
    python fetch_dividends.py --once --force  # для оркестратора
"""

import sys
import logging
import argparse
import httpx
import time
from datetime import date, timedelta
from pathlib import Path
from collections import defaultdict

from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

sys.path.insert(0, str(Path(__file__).parent.parent))
load_dotenv(Path(__file__).parent.parent / ".env")

DB_URL = os.getenv("DB_URL", "postgresql+pg8000://postgres:1803@localhost:5432/moex_db")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger(__name__)

ISS_DIVIDENDS_URL = "https://iss.moex.com/iss/securities/{secid}/dividends.json"

# Тикеры для исключения
EXCLUDED = {
    'IMOEX', 'IMOEXF', 'RGBI', 'USDRUBF', 'CNYRUBF', 'EURRUBF',
    'GLDRUBF', 'GAZPF', 'SBERF',
}


def create_schema(engine) -> None:
    """Создаёт таблицу dividends."""
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS dividends (
                secid             VARCHAR(20) NOT NULL,
                registry_close_date DATE NOT NULL,
                ex_date           DATE,
                value             REAL NOT NULL,
                PRIMARY KEY (secid, registry_close_date, value)
            )
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_dividends_secid
            ON dividends(secid)
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_dividends_ex_date
            ON dividends(ex_date)
        """))
        conn.commit()
    log.info("Схема dividends готова")


def get_stock_tickers(engine) -> list[str]:
    """Список акций из таблицы instruments (тип stock)."""
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT sec_id
            FROM instruments
            WHERE type = 'stock'
            ORDER BY sec_id
        """)).fetchall()
    tickers = [r[0] for r in rows if r[0] not in EXCLUDED]
    log.info(f"Тикеры из instruments: {len(tickers)} акций")
    return tickers


def fetch_dividends_from_iss(secid: str) -> list[dict]:
    """Загружает дивиденды с ISS MOEX для одного тикера."""
    url = ISS_DIVIDENDS_URL.format(secid=secid)
    try:
        resp = httpx.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        cols = data.get("dividends", {}).get("columns", [])
        rows = data.get("dividends", {}).get("data", [])

        if not rows:
            return []

        date_idx = cols.index("registryclosedate") if "registryclosedate" in cols else None
        val_idx = cols.index("value") if "value" in cols else None

        if date_idx is None or val_idx is None:
            return []

        result = []
        for row in rows:
            d = row[date_idx]
            v = row[val_idx]
            if d and v and float(v) > 0:
                result.append({
                    "registry_close_date": d,
                    "value": float(v),
                })
        return result
    except Exception as e:
        log.debug(f"  Ошибка загрузки дивидендов {secid}: {e}")
        return []


def find_ex_date(engine, secid: str, registry_date_str: str, dividend_value: float) -> date | None:
    """
    Эмпирически определяет экс-дивидендную дату.
    Ищет день с гэпом prev_close - open ≈ dividend_value
    в окне [registry_date - 5, registry_date + 1].
    """
    reg_date = date.fromisoformat(registry_date_str)
    window_start = reg_date - timedelta(days=5)
    window_end = reg_date + timedelta(days=1)

    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT
                begin_time::date as trade_date,
                open,
                LAG(close) OVER (ORDER BY begin_time) as prev_close
            FROM candles
            WHERE secid = :secid
              AND interval = 24
              AND type = 'stock'
              AND open > 0
              AND begin_time::date BETWEEN :start AND :end_ext
            ORDER BY begin_time
        """), {
            "secid": secid,
            "start": (window_start - timedelta(days=3)).isoformat(),
            "end_ext": window_end.isoformat(),
        }).fetchall()

    best_date = None
    best_diff = float("inf")

    for row in rows:
        td = row[0]
        open_price = float(row[1])
        prev_close = float(row[2]) if row[2] is not None else None

        if prev_close is None or td < window_start or td > window_end:
            continue

        gap = prev_close - open_price
        if gap <= 0 or gap < dividend_value * 0.3:
            continue

        diff = abs(gap - dividend_value)
        if diff < best_diff:
            best_diff = diff
            best_date = td

    if best_date:
        return best_date

    # Fallback: T-2 от даты реестра
    with engine.connect() as conn:
        trade_days = conn.execute(text("""
            SELECT DISTINCT begin_time::date as td
            FROM candles
            WHERE secid = :secid AND interval = 24 AND type = 'stock'
              AND begin_time::date <= :reg AND begin_time::date >= :start
            ORDER BY td DESC
            LIMIT 3
        """), {
            "secid": secid,
            "reg": reg_date.isoformat(),
            "start": (reg_date - timedelta(days=10)).isoformat(),
        }).fetchall()

    if len(trade_days) >= 3:
        return trade_days[2][0]

    return None


def main() -> None:
    parser = argparse.ArgumentParser(description="Загрузка дивидендов акций")
    parser.add_argument("--once", action="store_true", help="Однократный запуск")
    parser.add_argument("--force", action="store_true", help="Принудительный запуск")
    args = parser.parse_args()

    t0 = time.time()

    engine = create_engine(DB_URL, connect_args={"ssl_context": False})
    create_schema(engine)

    tickers = get_stock_tickers(engine)
    if not tickers:
        log.error("Нет тикеров — выход")
        return

    total_new = 0
    total_updated = 0

    for i, secid in enumerate(tickers):
        divs = fetch_dividends_from_iss(secid)
        if not divs:
            continue

        log.info(f"  [{i+1}/{len(tickers)}] {secid}: {len(divs)} дивидендов")

        for div in divs:
            reg_date = div["registry_close_date"]
            value = div["value"]

            # Проверяем, есть ли уже в БД
            with engine.connect() as conn:
                existing = conn.execute(text("""
                    SELECT ex_date FROM dividends
                    WHERE secid = :secid AND registry_close_date = :reg AND value = :val
                """), {"secid": secid, "reg": reg_date, "val": value}).fetchone()

            if existing and existing[0] is not None:
                continue  # Уже есть с экс-датой

            # Определяем экс-дату
            ex = find_ex_date(engine, secid, reg_date, value)

            with engine.connect() as conn:
                conn.execute(text("""
                    INSERT INTO dividends (secid, registry_close_date, ex_date, value)
                    VALUES (:secid, :reg, :ex, :val)
                    ON CONFLICT (secid, registry_close_date, value) DO UPDATE SET
                        ex_date = COALESCE(EXCLUDED.ex_date, dividends.ex_date)
                """), {"secid": secid, "reg": reg_date, "ex": ex, "val": value})
                conn.commit()

            if existing:
                total_updated += 1
            else:
                total_new += 1

        # Пауза между тикерами чтобы не нагружать ISS
        time.sleep(0.2)

    engine.dispose()
    log.info(f"Готово за {time.time() - t0:.1f}с. Новых: {total_new}, обновлённых: {total_updated}")


if __name__ == "__main__":
    main()
