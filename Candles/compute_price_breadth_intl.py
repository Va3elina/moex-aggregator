#!/usr/bin/env python3
"""
Предвычисление «силы рынка» ПО ЦЕНЕ для NSE/TAIFEX голубых фишек — точный
аналог Candles/compute_breadth_history.py (IMOEX): % компаний-компонентов
рынка, у которых текущая цена (candles_intl.close) выше своей EMA(period).

В отличие от compute_oi_intl_strength.py (та же идея, но по ОИ) — здесь
EMA_PERIODS = [20, 50, 100, 200], СОВПАДАЕТ с RF-версией один в один (там
считается по ОИ periods=[50,100,200], это была отдельная методология).

Бенчмарки (NSE_NIFTY, TAIFEX_TX) — индексные фьючерсы, не компании,
исключены из breadth ровно как IMOEX исключён из своих 100 акций и как
NSE_ALL исключён в compute_oi_intl_strength.py.

РФ-плечо сюда не пишется — комбинированный "3 рынка" эндпоинт на API-слое
читает его напрямую из существующего breadth_history (universe='imoex').

Таблица: price_breadth_intl_history (db/migrations/044_price_breadth_intl.sql)

Запуск:
    python3 compute_price_breadth_intl.py            # пересчитать всё
    python3 compute_price_breadth_intl.py --once      # для оркестратора (алиас)
"""
import argparse
import logging
import sys
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import os

sys.path.insert(0, str(Path(__file__).parent.parent))
load_dotenv(Path(__file__).parent.parent / ".env")

DB_URL = os.getenv("DB_URL")

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

EMA_PERIODS = [20, 50, 100, 200]
EXCHANGES = ["NSE", "TAIFEX"]

# Индексные бенчмарки — физически лежат в той же candles_intl, что и голубые
# фишки, но сами компанией не являются (см. IntlOI/fetch_nse_stocks.py
# BENCHMARKS / IntlOI/fetch_taifex.py BENCHMARKS) — исключаются из breadth
# ровно как IMOEX исключён из своих 100 акций в compute_breadth_history.py.
EXCLUDE_FROM_BREADTH = {("NSE", "NSE_NIFTY"), ("TAIFEX", "TAIFEX_TX")}


def calculate_ema(values: list[float], period: int) -> list[float]:
    return pd.Series(values).ewm(span=period, adjust=False).mean().tolist()


def load_assets(engine, exchange: str) -> list[tuple[str, str]]:
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT DISTINCT exchange, asset_code FROM candles_intl WHERE exchange = :exchange
        """), {"exchange": exchange}).fetchall()
    return [(r[0], r[1]) for r in rows if (r[0], r[1]) not in EXCLUDE_FROM_BREADTH]


def load_series(engine, exchange: str, asset_code: str) -> list[tuple[str, float]]:
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT trade_date, close FROM candles_intl
            WHERE exchange = :exchange AND asset_code = :asset_code AND close IS NOT NULL
            ORDER BY trade_date
        """), {"exchange": exchange, "asset_code": asset_code}).fetchall()
    return [(str(r[0]), float(r[1])) for r in rows]


def compute_breadth_for_exchange(engine, exchange: str, assets: list[tuple[str, str]]) -> None:
    for ema_period in EMA_PERIODS:
        daily_above: dict[str, dict[str, bool]] = {}
        for exch, asset_code in assets:
            series = load_series(engine, exch, asset_code)
            if len(series) < ema_period:
                continue
            dates = [d for d, _ in series]
            values = [v for _, v in series]
            ema_values = calculate_ema(values, ema_period)
            key = f"{exch}:{asset_code}"
            for i in range(ema_period - 1, len(values)):
                daily_above.setdefault(dates[i], {})[key] = values[i] > ema_values[i]

        rows_to_upsert = []
        for d in sorted(daily_above.keys()):
            statuses = daily_above[d]
            total = len(statuses)
            above = sum(1 for v in statuses.values() if v)
            rows_to_upsert.append({
                "exchange": exchange, "ema_period": ema_period, "trade_date": d,
                "percent_above": round((above / total) * 100, 1) if total else 0.0,
                "count_above": above, "count_total": total,
            })

        if not rows_to_upsert:
            log.info(f"{exchange} EMA{ema_period}: нет данных (мало истории)")
            continue

        with engine.begin() as conn:
            for r in rows_to_upsert:
                conn.execute(text("""
                    INSERT INTO price_breadth_intl_history (exchange, ema_period, trade_date, percent_above, count_above, count_total)
                    VALUES (:exchange, :ema_period, :trade_date, :percent_above, :count_above, :count_total)
                    ON CONFLICT (exchange, ema_period, trade_date) DO UPDATE SET
                        percent_above = EXCLUDED.percent_above,
                        count_above = EXCLUDED.count_above,
                        count_total = EXCLUDED.count_total
                """), r)
        log.info(f"{exchange} EMA{ema_period}: {len(rows_to_upsert)} точек, последняя {rows_to_upsert[-1]['percent_above']}%")


def main():
    parser = argparse.ArgumentParser(description="Пересчёт price_breadth_intl_history")
    parser.add_argument("--once", action="store_true", help="Алиас полного пересчёта (для оркестратора)")
    parser.parse_args()

    if not DB_URL:
        log.critical("DB_URL не задан (.env)")
        sys.exit(1)

    engine = create_engine(DB_URL, pool_pre_ping=True)

    for exchange in EXCHANGES:
        assets = load_assets(engine, exchange)
        if not assets:
            log.info(f"{exchange}: пока нет данных, пропуск")
            continue
        compute_breadth_for_exchange(engine, exchange, assets)

    log.info("✓ Готово")


if __name__ == "__main__":
    main()
