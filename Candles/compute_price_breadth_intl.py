#!/usr/bin/env python3
"""
Предвычисление «силы рынка» ПО ЦЕНЕ для NSE/TAIFEX — точный аналог
Candles/compute_breadth_history.py (IMOEX): % компаний-компонентов рынка,
у которых текущая цена (candles_intl.close) выше своей EMA(period).

EMA_PERIODS = [20, 50, 100, 200] — совпадает с RF-версией один в один
(в отличие от снесённой OI-версии, где было [50,100,200] — другая,
признанная нежизнеспособной методология, см. memory/international_oi.md).

Два режима на биржу (Вадим, 2026-07-25 — "повторяем нашу силу рынка"):
  'index' — голубые фишки (BLUE_CHIPS в фетчерах; для NSE это прокси
            Nifty50-констант, для TAIFEX — просто вручную отобранные
            ликвидные имена, официального индекс-фида нет)
  'all'   — все тикеры биржи, что реально есть в candles_intl. Для NSE это
            честная широкая вселенная (~210 FUTSTK тикеров — bhavcopy даёт
            чистое поле instrument-type, расширили в fetch_nse_stocks.py).
            Для TAIFEX 'all' СОВПАДАЕТ с 'index' — в сыром CSV нет поля
            instrument-type, единственный способ узнать, что "CDF"=TSMC —
            вручную найденная страница-справочник на taifex.com.tw;
            мини-разведка её дропдауна 2026-07-25 дала короткий (6 кодов,
            смешаны акции+ETF) и явно неполный список — расширять TAIFEX
            "all" за пределы BLUE_CHIPS отложено, не сделано наугад.

Бенчмарки (NSE_NIFTY, TAIFEX_TX) исключены из breadth (не компании) ровно
как IMOEX исключён из своих 100 акций в compute_breadth_history.py.

РФ-плечо сюда не пишется — комбинированный эндпоинт на API-слое читает его
напрямую из существующего breadth_history (universe='imoex').

Таблица: price_breadth_intl_history (db/migrations/044, 046)

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
sys.path.insert(0, str(Path(__file__).parent.parent / "IntlOI"))
load_dotenv(Path(__file__).parent.parent / ".env")

from fetch_nse_stocks import BLUE_CHIPS as NSE_BLUE_CHIPS  # noqa: E402
from fetch_taifex import BLUE_CHIPS as TAIFEX_BLUE_CHIPS  # noqa: E402

DB_URL = os.getenv("DB_URL")

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

EMA_PERIODS = [20, 50, 100, 200]

# Индексные бенчмарки — физически лежат в той же candles_intl, что и акции,
# но сами компанией не являются — исключаются из ОБОИХ режимов breadth.
EXCLUDE_FROM_BREADTH = {("NSE", "NSE_NIFTY"), ("TAIFEX", "TAIFEX_TX")}

# 'index' — asset_code для голубых фишек биржи (из фетчеров, не дублируем руками).
INDEX_CODES = {
    "NSE": {f"NSE_{sym}" for sym in NSE_BLUE_CHIPS},
    "TAIFEX": {f"TAIFEX_{sym}" for sym in TAIFEX_BLUE_CHIPS},
}


def calculate_ema(values: list[float], period: int) -> list[float]:
    return pd.Series(values).ewm(span=period, adjust=False).mean().tolist()


def load_all_codes(engine, exchange: str) -> set[str]:
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT DISTINCT asset_code FROM candles_intl WHERE exchange = :exchange
        """), {"exchange": exchange}).fetchall()
    return {r[0] for r in rows} - {code for exch, code in EXCLUDE_FROM_BREADTH if exch == exchange}


def load_series(engine, exchange: str, asset_code: str) -> list[tuple[str, float]]:
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT trade_date, close FROM candles_intl
            WHERE exchange = :exchange AND asset_code = :asset_code AND close IS NOT NULL
            ORDER BY trade_date
        """), {"exchange": exchange, "asset_code": asset_code}).fetchall()
    return [(str(r[0]), float(r[1])) for r in rows]


def compute_breadth(engine, exchange: str, universe: str, asset_codes: set[str]) -> None:
    for ema_period in EMA_PERIODS:
        daily_above: dict[str, dict[str, bool]] = {}
        for asset_code in asset_codes:
            series = load_series(engine, exchange, asset_code)
            if len(series) < ema_period:
                continue
            dates = [d for d, _ in series]
            values = [v for _, v in series]
            ema_values = calculate_ema(values, ema_period)
            for i in range(ema_period - 1, len(values)):
                daily_above.setdefault(dates[i], {})[asset_code] = values[i] > ema_values[i]

        rows_to_upsert = []
        for d in sorted(daily_above.keys()):
            statuses = daily_above[d]
            total = len(statuses)
            above = sum(1 for v in statuses.values() if v)
            rows_to_upsert.append({
                "exchange": exchange, "universe": universe, "ema_period": ema_period, "trade_date": d,
                "percent_above": round((above / total) * 100, 1) if total else 0.0,
                "count_above": above, "count_total": total,
            })

        if not rows_to_upsert:
            log.info(f"{exchange}/{universe} EMA{ema_period}: нет данных (мало истории)")
            continue

        with engine.begin() as conn:
            for r in rows_to_upsert:
                conn.execute(text("""
                    INSERT INTO price_breadth_intl_history (exchange, universe, ema_period, trade_date, percent_above, count_above, count_total)
                    VALUES (:exchange, :universe, :ema_period, :trade_date, :percent_above, :count_above, :count_total)
                    ON CONFLICT (exchange, universe, ema_period, trade_date) DO UPDATE SET
                        percent_above = EXCLUDED.percent_above,
                        count_above = EXCLUDED.count_above,
                        count_total = EXCLUDED.count_total
                """), r)
        log.info(f"{exchange}/{universe} EMA{ema_period}: {len(rows_to_upsert)} точек ({len(asset_codes)} активов), последняя {rows_to_upsert[-1]['percent_above']}%")


def main():
    parser = argparse.ArgumentParser(description="Пересчёт price_breadth_intl_history")
    parser.add_argument("--once", action="store_true", help="Алиас полного пересчёта (для оркестратора)")
    parser.parse_args()

    if not DB_URL:
        log.critical("DB_URL не задан (.env)")
        sys.exit(1)

    engine = create_engine(DB_URL, pool_pre_ping=True)

    for exchange in ("NSE", "TAIFEX"):
        all_codes = load_all_codes(engine, exchange)
        if not all_codes:
            log.info(f"{exchange}: пока нет данных, пропуск")
            continue

        index_codes = INDEX_CODES[exchange] & all_codes
        compute_breadth(engine, exchange, "index", index_codes)

        # TAIFEX: нет надёжного полного списка тикеров — 'all' пока равен
        # 'index' (см. докстринг). NSE: честная широкая вселенная.
        all_universe_codes = all_codes if exchange == "NSE" else index_codes
        compute_breadth(engine, exchange, "all", all_universe_codes)

    log.info("✓ Готово")


if __name__ == "__main__":
    main()
