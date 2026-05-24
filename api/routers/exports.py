"""
CSV export endpoints для всех индикаторов — Pro tier only.

Архитектура: каждый endpoint reuse-ит логику существующих data-loader'ов
(SQL queries inline для простоты — у нас не миллиарды строк, streaming
оверхед был бы overkill).

Auth: require_pro dependency (api/routers/auth.py) → 403 для гостей/Free/Basic.
Frontend на 403 показывает UpgradeModal.

Endpoints:
  GET /api/export/heatmap.csv
  GET /api/export/breadth.csv      ?ema=200&universe=imoex&days=180
  GET /api/export/buffett.csv      ?period=10y&mode=cap-gdp&timeframe=1m
  GET /api/export/seasonality.csv  ?ticker=SBER&mode=monthly
  GET /api/export/funds-money.csv  ?category=stocks&period=1y
  GET /api/export/cbr-flows.csv    ?instrument=stocks
  GET /api/export/oi.csv           ?instrument=SR&period=6m&interval=24
"""
from datetime import datetime, date as _date_cls

from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session

from api.database import get_db
from api.models import User
from api.routers.auth import require_pro
from api.utils.csv_export import csv_streaming_response

router = APIRouter(prefix="/api/export", tags=["export"])


def _ts() -> str:
    """Timestamp suffix для filename'ов: 20260524_0934."""
    return datetime.now().strftime("%Y%m%d_%H%M")


# ════════════════════════════════════════════════════════════════════
# Heatmap — все акции
# ════════════════════════════════════════════════════════════════════
@router.get("/heatmap.csv")
def export_heatmap(
    user: User = Depends(require_pro),
    db: Session = Depends(get_db),
):
    """Все акции с current price + change % за разные периоды + market cap."""
    rows = db.execute(text("""
        SELECT sec_id, name, sector, price, prev_close,
               change_1d, change_1w, change_1m, change_1y,
               volume_1d, value_1d, market_cap
        FROM mv_heatmap_stocks
        ORDER BY market_cap DESC NULLS LAST
    """)).mappings().all()
    return csv_streaming_response(
        rows=[dict(r) for r in rows],
        fieldnames=[
            "sec_id", "name", "sector",
            "price", "prev_close",
            "change_1d", "change_1w", "change_1m", "change_1y",
            "volume_1d", "value_1d", "market_cap",
        ],
        filename=f"heatmap_{_ts()}.csv",
    )


# ════════════════════════════════════════════════════════════════════
# Strength — breadth history (% акций выше EMA)
# ════════════════════════════════════════════════════════════════════
@router.get("/breadth.csv")
def export_breadth(
    ema: int = Query(200, ge=10, le=500),
    universe: str = Query("imoex"),
    days: int = Query(365, ge=1, le=3650),
    user: User = Depends(require_pro),
    db: Session = Depends(get_db),
):
    """Историческая Сила рынка: дата + % выше EMA + count above/total."""
    if universe not in ("all", "imoex", "all_usd", "imoex_usd"):
        raise HTTPException(status_code=400, detail="universe invalid")

    rows = db.execute(text("""
        SELECT trade_date, percent_above, count_above, count_total
        FROM breadth_history
        WHERE ema_period = :ema AND universe = :universe
          AND trade_date >= CURRENT_DATE - :days
        ORDER BY trade_date ASC
    """), {"ema": ema, "universe": universe, "days": days}).mappings().all()

    return csv_streaming_response(
        rows=[dict(r) for r in rows],
        fieldnames=["trade_date", "percent_above", "count_above", "count_total"],
        filename=f"strength_ema{ema}_{universe}_{_ts()}.csv",
    )


# ════════════════════════════════════════════════════════════════════
# Seasonality — per-ticker history
# ════════════════════════════════════════════════════════════════════
@router.get("/seasonality.csv")
def export_seasonality(
    ticker: str = Query(..., min_length=1, max_length=20),
    user: User = Depends(require_pro),
    db: Session = Depends(get_db),
):
    """Daily history тикера для self-расчёта сезонности (10+ лет)."""
    ticker = ticker.strip().upper()
    rows = db.execute(text("""
        SELECT begin_time::date AS trade_date, open, high, low, close, volume
        FROM candles
        WHERE secid = :ticker AND interval = 24 AND type = 'stock'
          AND close > 0
        ORDER BY begin_time ASC
    """), {"ticker": ticker}).mappings().all()
    if not rows:
        raise HTTPException(status_code=404, detail=f"Нет данных по {ticker}")

    return csv_streaming_response(
        rows=[dict(r) for r in rows],
        fieldnames=["trade_date", "open", "high", "low", "close", "volume"],
        filename=f"seasonality_{ticker}_{_ts()}.csv",
    )


# ════════════════════════════════════════════════════════════════════
# Buffett — capitalization / GDP / ratio
# ════════════════════════════════════════════════════════════════════
@router.get("/buffett.csv")
def export_buffett(
    user: User = Depends(require_pro),
    db: Session = Depends(get_db),
):
    """Историческая капитализация + GDP TTM + ratio Кап/ВВП."""
    rows = db.execute(text("""
        SELECT period_date AS trade_date, indicator, value
        FROM macro_data
        WHERE indicator IN ('MARKET_CAP_TOTAL', 'GDP_QUARTERLY')
        ORDER BY period_date ASC, indicator ASC
    """)).mappings().all()

    return csv_streaming_response(
        rows=[dict(r) for r in rows],
        fieldnames=["trade_date", "indicator", "value"],
        filename=f"buffett_{_ts()}.csv",
    )


# ════════════════════════════════════════════════════════════════════
# FundsMoney — NAV history per fund в категории
# ════════════════════════════════════════════════════════════════════
@router.get("/funds-money.csv")
def export_funds_money(
    category: str = Query("money_market"),
    user: User = Depends(require_pro),
    db: Session = Depends(get_db),
):
    """NAV history по всем фондам категории + ticker для join'ов."""
    if category not in ("money_market", "stocks", "bonds", "gold"):
        raise HTTPException(status_code=400, detail="category invalid")

    rows = db.execute(text("""
        SELECT fd.trade_date, f.ticker, f.name, fd.nav
        FROM fund_data fd
        JOIN funds f ON f.fund_id = fd.fund_id
        WHERE f.category = :cat
        ORDER BY fd.trade_date ASC, f.ticker ASC
    """), {"cat": category}).mappings().all()

    return csv_streaming_response(
        rows=[dict(r) for r in rows],
        fieldnames=["trade_date", "ticker", "name", "nav"],
        filename=f"funds_{category}_{_ts()}.csv",
    )


# ════════════════════════════════════════════════════════════════════
# CBR Flows — ОРФР
# ════════════════════════════════════════════════════════════════════
@router.get("/cbr-flows.csv")
def export_cbr_flows(
    instrument: str = Query("stocks"),
    user: User = Depends(require_pro),
    db: Session = Depends(get_db),
):
    """Полная история ОРФР по типу инструмента (stocks/ofz/fx)."""
    if instrument not in ("stocks", "ofz", "fx"):
        raise HTTPException(status_code=400, detail="instrument invalid")
    # ORFR хранит instrument_type как 'stocks'/'ofz'/'fx' — совпадает с param'ом.
    rows = db.execute(text("""
        SELECT period_year, period_label, period_kind, period_end_date,
               category, value
        FROM cbr_flows
        WHERE instrument_type = :it
        ORDER BY period_end_date ASC, category ASC
    """), {"it": instrument}).mappings().all()

    return csv_streaming_response(
        rows=[dict(r) for r in rows],
        fieldnames=["period_year", "period_label", "period_kind",
                    "period_end_date", "category", "value"],
        filename=f"cbr_flows_{instrument}_{_ts()}.csv",
    )


# ════════════════════════════════════════════════════════════════════
# Open Interest — позиции участников + open interest
# ════════════════════════════════════════════════════════════════════
@router.get("/oi.csv")
def export_oi(
    instrument: str = Query(..., min_length=1, max_length=20),
    clgroup: str = Query("YUR"),
    days: int = Query(365, ge=1, le=3650),
    user: User = Depends(require_pro),
    db: Session = Depends(get_db),
):
    """OI history: дата + время + позиции long/short + open_interest по cl_group."""
    if clgroup not in ("YUR", "FIZ"):
        raise HTTPException(status_code=400, detail="clgroup invalid")

    instrument = instrument.strip().upper()
    # NB: open_interest table колонки — tradedate, tradetime, sectype, pos
    # (не trade_date/secid/open_interest как было бы интуитивно). Кэстуем
    # под привычные имена в CSV-output для читаемости.
    rows = db.execute(text("""
        SELECT tradedate AS trade_date,
               tradetime AS trade_time,
               pos       AS open_interest,
               pos_long, pos_short, pos_long_num, pos_short_num,
               interval
        FROM open_interest
        WHERE sectype = :inst AND clgroup = :cl AND interval = 24
          AND tradedate >= CURRENT_DATE - :days
        ORDER BY tradedate ASC, tradetime ASC
    """), {"inst": instrument, "cl": clgroup, "days": days}).mappings().all()
    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"Нет OI данных по {instrument} {clgroup}",
        )

    return csv_streaming_response(
        rows=[dict(r) for r in rows],
        fieldnames=["trade_date", "trade_time", "open_interest",
                    "pos_long", "pos_short", "pos_long_num", "pos_short_num", "interval"],
        filename=f"oi_{instrument}_{clgroup}_{_ts()}.csv",
    )
