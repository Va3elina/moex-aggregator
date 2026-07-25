"""
Общие хелперы для международных OI-фетчеров (CFTC/NSE/Eurex/B3).

Таблица open_interest_intl — см. db/migrations/041_open_interest_intl.sql +
042_open_interest_intl_granularity.sql. Каждый фетчер обязан прислать хотя бы
одну строку с category='TOTAL' на (asset_code, trade_date, granularity) — по
ней считается "сила рынка". granularity ('daily'/'weekly'/'monthly')
обязателен: источники разной частоты (например Eurex daily+monthly) могут
давать РАЗНОЕ число на одну и ту же дату — это не дубликат, а два разных
измерения, они не должны делить одну ячейку и тихо перезаписывать друг друга
(было — см. историю PR/чата 2026-07-24).
"""
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

sys.path.insert(0, str(Path(__file__).parent.parent))
load_dotenv(Path(__file__).parent.parent / ".env")

DB_URL = os.getenv("DB_URL")

VALID_GRANULARITIES = {"daily", "weekly", "monthly"}


def get_engine():
    return create_engine(DB_URL, pool_pre_ping=True)


UPSERT_SQL = text("""
    INSERT INTO open_interest_intl
        (exchange, country, asset_code, asset_name, category, trade_date, granularity, oi_long, oi_short, oi_total, systime)
    VALUES
        (:exchange, :country, :asset_code, :asset_name, :category, :trade_date, :granularity, :oi_long, :oi_short, :oi_total, NOW())
    ON CONFLICT (exchange, asset_code, category, trade_date, granularity) DO UPDATE SET
        country = EXCLUDED.country,
        asset_name = EXCLUDED.asset_name,
        oi_long = EXCLUDED.oi_long,
        oi_short = EXCLUDED.oi_short,
        oi_total = EXCLUDED.oi_total,
        systime = NOW()
""")


def upsert_oi_intl_rows(engine, rows: list[dict]) -> int:
    """
    rows: [{exchange, country, asset_code, asset_name, category, trade_date,
            granularity, oi_long, oi_short, oi_total}, ...]
    granularity обязателен и должен быть одним из VALID_GRANULARITIES —
    без дефолта, чтобы не замаскировать забытое поле у нового фетчера.
    Возвращает число обработанных строк.
    """
    if not rows:
        return 0
    with engine.begin() as conn:
        for row in rows:
            granularity = row["granularity"]
            if granularity not in VALID_GRANULARITIES:
                raise ValueError(f"Неизвестная granularity={granularity!r}, ожидалось одно из {VALID_GRANULARITIES}")
            conn.execute(UPSERT_SQL, {
                "exchange": row["exchange"],
                "country": row["country"],
                "asset_code": row["asset_code"],
                "asset_name": row["asset_name"],
                "category": row["category"],
                "trade_date": row["trade_date"],
                "granularity": granularity,
                "oi_long": row.get("oi_long"),
                "oi_short": row.get("oi_short"),
                "oi_total": row["oi_total"],
            })
    return len(rows)


UPSERT_CANDLE_SQL = text("""
    INSERT INTO candles_intl
        (exchange, country, asset_code, asset_name, trade_date, granularity, open, high, low, close, settlement_price, systime)
    VALUES
        (:exchange, :country, :asset_code, :asset_name, :trade_date, :granularity, :open, :high, :low, :close, :settlement_price, NOW())
    ON CONFLICT (exchange, asset_code, trade_date, granularity) DO UPDATE SET
        country = EXCLUDED.country,
        asset_name = EXCLUDED.asset_name,
        open = EXCLUDED.open,
        high = EXCLUDED.high,
        low = EXCLUDED.low,
        close = EXCLUDED.close,
        settlement_price = EXCLUDED.settlement_price,
        systime = NOW()
""")


def upsert_candle_intl_rows(engine, rows: list[dict]) -> int:
    """
    rows: [{exchange, country, asset_code, asset_name, trade_date, granularity,
            open, high, low, close, settlement_price}, ...]
    Цена ПЕРЕДНЕГО/самого ликвидного контрактного месяца — не сумма и не
    среднее по всем месяцам (в отличие от OI, цены разных месяцев складывать
    нельзя). granularity обязателен, как и в upsert_oi_intl_rows.
    """
    if not rows:
        return 0
    with engine.begin() as conn:
        for row in rows:
            granularity = row["granularity"]
            if granularity not in VALID_GRANULARITIES:
                raise ValueError(f"Неизвестная granularity={granularity!r}, ожидалось одно из {VALID_GRANULARITIES}")
            conn.execute(UPSERT_CANDLE_SQL, {
                "exchange": row["exchange"],
                "country": row["country"],
                "asset_code": row["asset_code"],
                "asset_name": row["asset_name"],
                "trade_date": row["trade_date"],
                "granularity": granularity,
                "open": row.get("open"),
                "high": row.get("high"),
                "low": row.get("low"),
                "close": row.get("close"),
                "settlement_price": row.get("settlement_price"),
            })
    return len(rows)
