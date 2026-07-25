"""
Международный Open Interest (CFTC/NSE/Eurex/B3) + «Сила рынка по ОИ» —
admin-only, см. .claude/plans (Вадим + Саша Тория).

Таблицы: open_interest_intl, oi_intl_strength_history
(db/migrations/041_open_interest_intl.sql)
"""
from datetime import date, timedelta

from fastapi import APIRouter, Query, Depends, HTTPException
from sqlalchemy import text

from api.database import get_engine
from api.cache import get_or_set
from api.routers.auth import require_admin

router = APIRouter(prefix="/api/admin/oi-intl", tags=["oi-intl-admin"])


@router.get("/assets")
async def list_assets(user=Depends(require_admin)):
    """Список активов с TOTAL-рядом — для пикера на фронте."""
    engine = get_engine()
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT exchange, country, asset_code, asset_name, MAX(trade_date) as last_date
            FROM open_interest_intl
            WHERE category = 'TOTAL'
            GROUP BY exchange, country, asset_code, asset_name
            ORDER BY exchange, asset_name
        """)).fetchall()
    return {
        "assets": [
            {
                "exchange": r[0], "country": r[1], "asset_code": r[2],
                "asset_name": r[3], "last_date": str(r[4]),
            }
            for r in rows
        ]
    }


@router.get("/history")
async def get_history(
    exchange: str = Query(...),
    asset_code: str = Query(...),
    category: str = Query("TOTAL"),
    granularity: str | None = Query(None, description="daily|weekly|monthly — если не задано, на каждую дату берётся самая мелкая доступная (daily > weekly > monthly)"),
    days: int = Query(730, ge=1, le=9000),
    user=Depends(require_admin),
):
    """
    Временной ряд OI для чарта: одна биржа + один актив + одна категория.

    Некоторые активы (сейчас — Eurex с 2022-08) имеют ОБА источника на одну и
    ту же дату: daily-снимок и агрегат из monthly-отчёта — это разные числа,
    не дубликаты (см. db/migrations/042). Без явного granularity отдаём по
    одной точке на дату, предпочитая более мелкую грануляцию.
    """
    engine = get_engine()
    date_from = date.today() - timedelta(days=days)
    params = {
        "exchange": exchange, "asset_code": asset_code, "category": category, "date_from": date_from,
    }
    with engine.connect() as conn:
        if granularity:
            params["granularity"] = granularity
            rows = conn.execute(text("""
                SELECT trade_date, oi_long, oi_short, oi_total, granularity
                FROM open_interest_intl
                WHERE exchange = :exchange AND asset_code = :asset_code AND category = :category
                  AND granularity = :granularity AND trade_date >= :date_from
                ORDER BY trade_date
            """), params).fetchall()
        else:
            rows = conn.execute(text("""
                SELECT DISTINCT ON (trade_date) trade_date, oi_long, oi_short, oi_total, granularity
                FROM open_interest_intl
                WHERE exchange = :exchange AND asset_code = :asset_code AND category = :category
                  AND trade_date >= :date_from
                ORDER BY trade_date,
                  CASE granularity WHEN 'daily' THEN 0 WHEN 'weekly' THEN 1 WHEN 'monthly' THEN 2 ELSE 3 END
            """), params).fetchall()
    return {
        "exchange": exchange, "asset_code": asset_code, "category": category,
        "data": [
            {"date": str(r[0]), "oi_long": r[1], "oi_short": r[2], "oi_total": r[3], "granularity": r[4]}
            for r in rows
        ],
    }


@router.get("/candles")
async def get_candles(
    exchange: str = Query(...),
    asset_code: str = Query(...),
    days: int = Query(730, ge=1, le=9000),
    user=Depends(require_admin),
):
    """
    Свечи (цена переднего/самого ликвидного контрактного месяца) — для
    наложения на график OI того же актива. Сейчас есть только для NSE и
    TAIFEX (см. candles_intl, db/migrations/043) — для CFTC/Eurex-индексов
    цены не собираются, вернётся пустой data.
    """
    engine = get_engine()
    date_from = date.today() - timedelta(days=days)
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT trade_date, open, high, low, close, settlement_price
            FROM candles_intl
            WHERE exchange = :exchange AND asset_code = :asset_code AND trade_date >= :date_from
            ORDER BY trade_date
        """), {"exchange": exchange, "asset_code": asset_code, "date_from": date_from}).fetchall()
    return {
        "exchange": exchange, "asset_code": asset_code,
        "data": [
            {
                "date": str(r[0]),
                "open": float(r[1]) if r[1] is not None else None,
                "high": float(r[2]) if r[2] is not None else None,
                "low": float(r[3]) if r[3] is not None else None,
                "close": float(r[4]) if r[4] is not None else None,
                "settlement_price": float(r[5]) if r[5] is not None else None,
            }
            for r in rows
        ],
    }


PRICE_BREADTH_MARKETS = {"RF", "NSE", "TAIFEX"}
BENCHMARK_SOURCES = {
    "IMOEX": ("index_data", "IMOEX"),
    "NIFTY": ("candles_intl:NSE", "NSE_NIFTY"),
    "TX": ("candles_intl:TAIFEX", "TAIFEX_TX"),
}


@router.get("/price-breadth")
async def get_price_breadth(
    markets: str = Query("RF,NSE,TAIFEX", description="Через запятую: RF,NSE,TAIFEX"),
    ema_period: int = Query(50),
    benchmark: str = Query("IMOEX", description="IMOEX|NIFTY|TX"),
    days: int = Query(730, ge=1, le=9000),
    user=Depends(require_admin),
):
    """
    «Сила рынка» по ЦЕНЕ (аналог RF-индикатора Strength, см.
    Candles/compute_breadth_history.py) сразу по нескольким рынкам — РФ
    (готовое breadth_history, universe='imoex', читается напрямую, без
    дублирования) + NSE/TAIFEX (price_breadth_intl_history, см.
    Candles/compute_price_breadth_intl.py). Бенчмарк (индекс для наложения
    на график) выбирается отдельно от набора рынков — IMOEX/NIFTY/TX сами
    по себе не входят в свою же breadth, ровно как IMOEX исключён из
    RF-версии.
    """
    requested = [m.strip().upper() for m in markets.split(",") if m.strip()]
    invalid = [m for m in requested if m not in PRICE_BREADTH_MARKETS]
    if invalid:
        raise HTTPException(400, f"неизвестные рынки: {invalid}, доступны: {sorted(PRICE_BREADTH_MARKETS)}")
    if benchmark not in BENCHMARK_SOURCES:
        raise HTTPException(400, f"неизвестный бенчмарк: {benchmark}, доступны: {sorted(BENCHMARK_SOURCES)}")

    engine = get_engine()
    date_from = date.today() - timedelta(days=days)
    series: dict[str, list[dict]] = {}

    with engine.connect() as conn:
        if "RF" in requested:
            rows = conn.execute(text("""
                SELECT trade_date, percent_above, count_above, count_total
                FROM breadth_history WHERE universe = 'imoex' AND ema_period = :ema_period
                  AND trade_date >= :date_from ORDER BY trade_date
            """), {"ema_period": ema_period, "date_from": date_from}).fetchall()
            series["RF"] = [
                {"date": str(r[0]), "percent_above": float(r[1]), "count_above": int(r[2]), "count_total": int(r[3])}
                for r in rows
            ]

        for exch in ("NSE", "TAIFEX"):
            if exch not in requested:
                continue
            rows = conn.execute(text("""
                SELECT trade_date, percent_above, count_above, count_total
                FROM price_breadth_intl_history WHERE exchange = :exchange AND ema_period = :ema_period
                  AND trade_date >= :date_from ORDER BY trade_date
            """), {"exchange": exch, "ema_period": ema_period, "date_from": date_from}).fetchall()
            series[exch] = [
                {"date": str(r[0]), "percent_above": float(r[1]), "count_above": int(r[2]), "count_total": int(r[3])}
                for r in rows
            ]

        src, code = BENCHMARK_SOURCES[benchmark]
        if src == "index_data":
            rows = conn.execute(text("""
                SELECT trade_date, close FROM index_data WHERE secid = :code AND trade_date >= :date_from ORDER BY trade_date
            """), {"code": code, "date_from": date_from}).fetchall()
        else:
            exch = src.split(":")[1]
            rows = conn.execute(text("""
                SELECT trade_date, close FROM candles_intl
                WHERE exchange = :exchange AND asset_code = :code AND trade_date >= :date_from ORDER BY trade_date
            """), {"exchange": exch, "code": code, "date_from": date_from}).fetchall()
        benchmark_data = [{"date": str(r[0]), "close": float(r[1])} for r in rows if r[1] is not None]

    return {
        "ema_period": ema_period, "benchmark": benchmark,
        "series": series, "benchmark_data": benchmark_data,
    }


@router.get("/categories")
async def list_categories(
    exchange: str = Query(...),
    asset_code: str = Query(...),
    user=Depends(require_admin),
):
    """Какие holder-категории есть у данного актива (для переключателя на фронте)."""
    engine = get_engine()
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT DISTINCT category FROM open_interest_intl
            WHERE exchange = :exchange AND asset_code = :asset_code
        """), {"exchange": exchange, "asset_code": asset_code}).fetchall()
    return {"categories": sorted(r[0] for r in rows)}


@router.get("/strength/current")
async def get_strength_current(
    exchange: str = Query("ALL"),
    ema_period: int = Query(200),
    user=Depends(require_admin),
):
    cache_key = f"oi_intl:strength:current:{exchange}:{ema_period}"
    cached = get_or_set(cache_key)
    if cached is not None:
        return cached

    engine = get_engine()
    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT trade_date, percent_above, count_above, count_total
            FROM oi_intl_strength_history
            WHERE exchange = :exchange AND ema_period = :ema_period
            ORDER BY trade_date DESC
            LIMIT 1
        """), {"exchange": exchange, "ema_period": ema_period}).fetchone()

    result = (
        {
            "exchange": exchange, "ema_period": ema_period, "date": str(row[0]),
            "percent_above": float(row[1]), "count_above": int(row[2]), "count_total": int(row[3]),
        }
        if row else
        {"exchange": exchange, "ema_period": ema_period, "date": None, "percent_above": 0, "count_above": 0, "count_total": 0}
    )
    get_or_set(cache_key, result, ttl=900)
    return result


@router.get("/strength/history")
async def get_strength_history(
    exchange: str = Query("ALL"),
    ema_period: int = Query(200),
    days: int = Query(730, ge=1, le=9000),
    user=Depends(require_admin),
):
    engine = get_engine()
    date_from = date.today() - timedelta(days=days)
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT trade_date, percent_above, count_above, count_total
            FROM oi_intl_strength_history
            WHERE exchange = :exchange AND ema_period = :ema_period AND trade_date >= :date_from
            ORDER BY trade_date
        """), {"exchange": exchange, "ema_period": ema_period, "date_from": date_from}).fetchall()
    return {
        "exchange": exchange, "ema_period": ema_period,
        "data": [
            {"date": str(r[0]), "percent_above": float(r[1]), "count_above": int(r[2]), "count_total": int(r[3])}
            for r in rows
        ],
    }
