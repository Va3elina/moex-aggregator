#!/usr/bin/env python3
"""
Предвычисление «силы рынка по ОИ» — % КОМПАНИЙ-КОМПОНЕНТОВ индекса/рынка, у
которых текущий OI (category='TOTAL' в open_interest_intl) выше своей
EMA(period). По аналогии с Candles/compute_breadth_history.py (там — % акций
выше EMA цены).

ВАЖНО (пересмотрено 2026-07-24 по замечанию Вадима): breadth осмысленна
ТОЛЬКО когда это реальные компании-компоненты одного рынка — как в
breadth.py с IMOEX. Смешивать в одну "% активов расширяется" метрику разные
классы инструментов (валюты+товары+ставки+индексы у CFTC; индексы+облигации
у Eurex) — методологически неверно, это не breadth ничего. Поэтому:

  - CFTC (индексы/товары/валюты/ставки) и EUREX-индексы/облигации (FDAX,
    FGBL, FGBM, FGBS, FGBX, FESX) в strength НЕ участвуют — это не компании,
    а разнородный набор. Их сырые OI-ряды остаются доступны через
    /api/admin/oi-intl/history для чартов, просто не в breadth.
  - NSE: агрегат NSE_ALL (весь рынок) ИСКЛЮЧЁН из подсчёта — как IMOEX не
    входит в список 42 тикеров в breadth.py, а идёт отдельной reference-
    линией. Breadth считается только по 20 отдельным голубым фишкам.
  - TAIFEX: все активы — реальные отдельные компании (TSMC, UMC, Foxconn...),
    участвуют целиком.

exchange='ALL' — объединение NSE (без агрегата) + TAIFEX — единая "мировая"
breadth по реальным компаниям, без примеси commodities/индексов/облигаций.

Таблица: oi_intl_strength_history (см. db/migrations/041_open_interest_intl.sql)

Запуск:
    python3 compute_oi_intl_strength.py            # пересчитать всё
    python3 compute_oi_intl_strength.py --once      # для оркестратора (алиас --full)
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

EMA_PERIODS = [50, 100, 200]
# Только рынки, где активы — реальные отдельные компании (не индексы/товары/
# облигации). CFTC и "чистый" EUREX (DAX/Bund/Bobl/Schatz/Buxl/STOXX50)
# сознательно исключены — см. докстринг выше.
EXCHANGES = ["NSE", "TAIFEX"]

# (exchange, asset_code) — агрегаты/индексы-прокси, которые физически лежат
# в той же таблице что и отдельные компании, но сами компанией не являются —
# исключаются из breadth ровно как IMOEX исключён из списка тикеров в
# breadth.py (используется как overlay, не как член выборки).
EXCLUDE_FROM_BREADTH = {("NSE", "NSE_ALL")}


def calculate_ema(values: list[float], period: int) -> list[float]:
    return pd.Series(values).ewm(span=period, adjust=False).mean().tolist()


def load_assets(engine, exchange: str) -> list[tuple[str, str]]:
    """Возвращает [(exchange, asset_code), ...] с TOTAL-рядом для одной из EXCHANGES,
    за вычетом EXCLUDE_FROM_BREADTH (агрегаты/индексы-прокси, не компании)."""
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT DISTINCT exchange, asset_code FROM open_interest_intl
            WHERE category = 'TOTAL' AND exchange = :exchange
        """), {"exchange": exchange}).fetchall()
    return [(r[0], r[1]) for r in rows if (r[0], r[1]) not in EXCLUDE_FROM_BREADTH]


def load_series(engine, exchange: str, asset_code: str) -> list[tuple[str, int]]:
    """
    Один OI на дату. Некоторые активы (Eurex с 2022-08) имеют ОБА источника
    (daily + monthly) на одну дату — разные числа, не дубликаты (см.
    db/migrations/042). DISTINCT ON + приоритет daily>weekly>monthly берёт
    самую мелкую доступную гранулярность, иначе EMA получит 2 точки на одну
    дату и разъедется.
    """
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT DISTINCT ON (trade_date) trade_date, oi_total FROM open_interest_intl
            WHERE exchange = :exchange AND asset_code = :asset_code AND category = 'TOTAL'
            ORDER BY trade_date,
              CASE granularity WHEN 'daily' THEN 0 WHEN 'weekly' THEN 1 WHEN 'monthly' THEN 2 ELSE 3 END
        """), {"exchange": exchange, "asset_code": asset_code}).fetchall()
    return [(str(r[0]), int(r[1])) for r in rows]


def compute_strength_for_scope(engine, scope_label: str, assets: list[tuple[str, str]]) -> None:
    """scope_label — 'ALL' либо конкретная биржа. assets — активы, входящие в эту вселенную."""
    for ema_period in EMA_PERIODS:
        daily_above: dict[str, dict[str, bool]] = {}
        for exchange, asset_code in assets:
            series = load_series(engine, exchange, asset_code)
            if len(series) < ema_period:
                continue
            dates = [d for d, _ in series]
            values = [v for _, v in series]
            ema_values = calculate_ema(values, ema_period)
            key = f"{exchange}:{asset_code}"
            for i in range(ema_period - 1, len(values)):
                daily_above.setdefault(dates[i], {})[key] = values[i] > ema_values[i]

        rows_to_upsert = []
        for d in sorted(daily_above.keys()):
            statuses = daily_above[d]
            total = len(statuses)
            above = sum(1 for v in statuses.values() if v)
            rows_to_upsert.append({
                "exchange": scope_label, "ema_period": ema_period, "trade_date": d,
                "percent_above": round((above / total) * 100, 1) if total else 0.0,
                "count_above": above, "count_total": total,
            })

        if not rows_to_upsert:
            log.info(f"{scope_label} EMA{ema_period}: нет данных (мало истории)")
            continue

        with engine.begin() as conn:
            for r in rows_to_upsert:
                conn.execute(text("""
                    INSERT INTO oi_intl_strength_history (exchange, ema_period, trade_date, percent_above, count_above, count_total)
                    VALUES (:exchange, :ema_period, :trade_date, :percent_above, :count_above, :count_total)
                    ON CONFLICT (exchange, ema_period, trade_date) DO UPDATE SET
                        percent_above = EXCLUDED.percent_above,
                        count_above = EXCLUDED.count_above,
                        count_total = EXCLUDED.count_total
                """), r)
        log.info(f"{scope_label} EMA{ema_period}: {len(rows_to_upsert)} точек, последняя {rows_to_upsert[-1]['percent_above']}%")


def main():
    parser = argparse.ArgumentParser(description="Пересчёт oi_intl_strength_history")
    parser.add_argument("--once", action="store_true", help="Алиас полного пересчёта (для оркестратора)")
    parser.parse_args()

    if not DB_URL:
        log.critical("DB_URL не задан (.env)")
        sys.exit(1)

    engine = create_engine(DB_URL, pool_pre_ping=True)

    all_assets: list[tuple[str, str]] = []
    for exchange in EXCHANGES:
        assets = load_assets(engine, exchange)
        if not assets:
            log.info(f"{exchange}: пока нет данных, пропуск")
            continue
        all_assets.extend(assets)
        compute_strength_for_scope(engine, exchange, assets)

    log.info(f"ALL: {len(all_assets)} компаний (NSE без агрегата + TAIFEX)")
    compute_strength_for_scope(engine, "ALL", all_assets)

    log.info("✓ Готово")


if __name__ == "__main__":
    main()
