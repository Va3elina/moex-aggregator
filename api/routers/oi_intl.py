"""
Международный Open Interest (CFTC/NSE/Eurex/B3) + «сила рынка» по цене
(NSE/TAIFEX) — сырые данные для admin-only расширения реальных страниц
OpenInterestPage/StrengthPage (Вадим + Саша Тория). НЕ отдельная страница —
см. фронт: условный рендер под role=admin внутри тех же компонентов.

Таблицы: open_interest_intl, candles_intl, price_breadth_intl_history
(db/migrations/041, 043, 044)

Примечание: OI-based «сила рынка» (oi_intl_strength_history,
Candles/compute_oi_intl_strength.py) была признана методологически
нежизнеспособной и удалена 2026-07-25 — смешивать в одну breadth-метрику
компании и commodities/currencies/индексы бессмысленно (CFTC/Eurex).
Заменена price-based версией, повторяющей методологию
Candles/compute_breadth_history.py.
"""
from datetime import date, timedelta

from fastapi import APIRouter, Query, Depends, HTTPException
from sqlalchemy import text

from api.database import get_engine
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


# Бенчмарк — НЕ выбирается клиентом, привязан к бирже ровно как для RF он
# привязан к валюте (не отдельный параметр). NSE→NIFTY, TAIFEX→TX.
STRENGTH_BENCHMARK = {
    "NSE": "NSE_NIFTY",
    "TAIFEX": "TAIFEX_TX",
}
STRENGTH_BENCHMARK_LABEL = {"NSE": "NIFTY", "TAIFEX": "TX"}


@router.get("/strength")
async def get_intl_strength(
    exchange: str = Query(..., description="NSE|TAIFEX"),
    universe: str = Query("index", description="index|all — см. Candles/compute_price_breadth_intl.py"),
    ema_period: int = Query(50),
    days: int = Query(730, ge=1, le=9000),
    user=Depends(require_admin),
):
    """
    «Сила рынка» по ЦЕНЕ для одной международной биржи — аналог RF-индикатора
    Strength (Candles/compute_breadth_history.py), из price_breadth_intl_history
    (Candles/compute_price_breadth_intl.py). РФ сюда не входит — фронт для
    РФ продолжает использовать существующий /api/breadth/*, без изменений.
    """
    if exchange not in STRENGTH_BENCHMARK:
        raise HTTPException(400, f"неизвестная биржа: {exchange}, доступны: {sorted(STRENGTH_BENCHMARK)}")
    if universe not in ("index", "all"):
        raise HTTPException(400, f"неизвестная вселенная: {universe}, доступны: index, all")

    engine = get_engine()
    date_from = date.today() - timedelta(days=days)
    benchmark_code = STRENGTH_BENCHMARK[exchange]

    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT trade_date, percent_above, count_above, count_total
            FROM price_breadth_intl_history
            WHERE exchange = :exchange AND universe = :universe AND ema_period = :ema_period
              AND trade_date >= :date_from ORDER BY trade_date
        """), {"exchange": exchange, "universe": universe, "ema_period": ema_period, "date_from": date_from}).fetchall()
        data = [
            {"date": str(r[0]), "percent_above": float(r[1]), "count_above": int(r[2]), "count_total": int(r[3])}
            for r in rows
        ]

        bench_rows = conn.execute(text("""
            SELECT trade_date, close FROM candles_intl
            WHERE exchange = :exchange AND asset_code = :code AND trade_date >= :date_from ORDER BY trade_date
        """), {"exchange": exchange, "code": benchmark_code, "date_from": date_from}).fetchall()
        benchmark_data = [{"date": str(r[0]), "close": float(r[1])} for r in bench_rows if r[1] is not None]

    return {
        "exchange": exchange, "universe": universe, "ema_period": ema_period,
        "benchmark": STRENGTH_BENCHMARK_LABEL[exchange],
        "data": data, "benchmark_data": benchmark_data,
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
