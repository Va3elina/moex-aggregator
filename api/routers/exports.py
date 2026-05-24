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
    """
    Daily history тикера + готовые аггрегации сезонности:
      - daily: trade_date, open, high, low, close, volume, change_pct,
               weekday, month, day_of_month, year (для всех видов pivot'ов)

    Юзер может в Excel сам построить pivot по weekday/month/monthday если
    нужны другие срезы — все исходные колонки даны. Это полнее чем UI
    (UI показывает только один срез).
    """
    ticker = ticker.strip().upper()
    rows = db.execute(text("""
        SELECT begin_time::date AS trade_date,
               open, high, low, close, volume,
               -- Change % vs previous trading day
               ROUND(((close - LAG(close) OVER (ORDER BY begin_time)) /
                      NULLIF(LAG(close) OVER (ORDER BY begin_time), 0) * 100)::numeric, 4)
                 AS change_pct,
               -- Time decompositions для self-pivot
               EXTRACT(ISODOW FROM begin_time)::int AS weekday,
               EXTRACT(MONTH  FROM begin_time)::int AS month,
               EXTRACT(DAY    FROM begin_time)::int AS day_of_month,
               EXTRACT(YEAR   FROM begin_time)::int AS year
        FROM candles
        WHERE secid = :ticker AND interval = 24 AND type = 'stock'
          AND close > 0
        ORDER BY begin_time ASC
    """), {"ticker": ticker}).mappings().all()
    if not rows:
        raise HTTPException(status_code=404, detail=f"Нет данных по {ticker}")

    return csv_streaming_response(
        rows=[dict(r) for r in rows],
        fieldnames=[
            "trade_date", "year", "month", "day_of_month", "weekday",
            "open", "high", "low", "close", "volume", "change_pct",
        ],
        filename=f"seasonality_{ticker}_{_ts()}.csv",
    )


# ════════════════════════════════════════════════════════════════════
# Buffett — capitalization / GDP / ratio
# ════════════════════════════════════════════════════════════════════
@router.get("/buffett.csv")
def export_buffett(
    mode: str = Query("cap-gdp"),
    user: User = Depends(require_pro),
    db: Session = Depends(get_db),
):
    """
    Кап / ВВП или Кап / M2 — wide format (date | cap | gdp_ttm | ratio).
    Mode выбирается на UI:
      - cap-gdp: Кап / ВВП (МСК Биржа cap / Росстат GDP TTM × 100)
      - cap-m2:  Кап / M2 (M2 берётся из macro_data)
    """
    if mode not in ("cap-gdp", "cap-m2"):
        raise HTTPException(status_code=400, detail="mode invalid")

    # Используем pivot через JOIN на одной MARKET_CAP_TOTAL дате.
    # GDP_QUARTERLY → TTM через LAG×4 (4 квартала).
    # M2_MONTHLY → монотонно растёт, JOIN по ближайшей дате.
    if mode == "cap-gdp":
        rows = db.execute(text("""
            WITH cap AS (
              SELECT period_date AS dt, value AS cap
              FROM macro_data WHERE indicator='MARKET_CAP_TOTAL'
            ),
            gdp AS (
              SELECT period_date AS dt, value AS gdp_q,
                     -- TTM = сумма последних 4 кварталов
                     value + LAG(value,1) OVER (ORDER BY period_date)
                           + LAG(value,2) OVER (ORDER BY period_date)
                           + LAG(value,3) OVER (ORDER BY period_date)
                       AS gdp_ttm
              FROM macro_data WHERE indicator='GDP_QUARTERLY'
            ),
            joined AS (
              SELECT c.dt AS trade_date,
                     c.cap AS market_cap,
                     (SELECT gdp_ttm FROM gdp g
                      WHERE g.dt <= c.dt AND g.gdp_ttm IS NOT NULL
                      ORDER BY g.dt DESC LIMIT 1) AS gdp_ttm
              FROM cap c
            )
            SELECT trade_date, market_cap, gdp_ttm,
                   CASE WHEN gdp_ttm > 0
                        THEN ROUND((market_cap / gdp_ttm * 100)::numeric, 4)
                        ELSE NULL END AS buffett_ratio_pct
            FROM joined
            ORDER BY trade_date ASC
        """)).mappings().all()
        fields = ["trade_date", "market_cap", "gdp_ttm", "buffett_ratio_pct"]
    else:  # cap-m2
        rows = db.execute(text("""
            WITH cap AS (
              SELECT period_date AS dt, value AS cap
              FROM macro_data WHERE indicator='MARKET_CAP_TOTAL'
            ),
            m2 AS (
              SELECT period_date AS dt, value AS m2
              FROM macro_data WHERE indicator='M2_MONTHLY'
            ),
            joined AS (
              SELECT c.dt AS trade_date,
                     c.cap AS market_cap,
                     (SELECT m2 FROM m2 mm
                      WHERE mm.dt <= c.dt AND mm.m2 IS NOT NULL
                      ORDER BY mm.dt DESC LIMIT 1) AS m2
              FROM cap c
            )
            SELECT trade_date, market_cap, m2,
                   CASE WHEN m2 > 0
                        THEN ROUND((market_cap / m2)::numeric, 6)
                        ELSE NULL END AS cap_m2_ratio
            FROM joined
            ORDER BY trade_date ASC
        """)).mappings().all()
        fields = ["trade_date", "market_cap", "m2", "cap_m2_ratio"]

    return csv_streaming_response(
        rows=[dict(r) for r in rows],
        fieldnames=fields,
        filename=f"buffett_{mode}_{_ts()}.csv",
    )


# ════════════════════════════════════════════════════════════════════
# FundsMoney — NAV history per fund в категории
# ════════════════════════════════════════════════════════════════════
@router.get("/funds-money.csv")
def export_funds_money(
    category: str = Query("money_market"),
    days: int = Query(365, ge=1, le=3650),
    funds: str | None = Query(None, description="Comma-separated fund tickers, для фильтрации (e.g. SBMM,LQDT)"),
    user: User = Depends(require_pro),
    db: Session = Depends(get_db),
):
    """
    NAV history фондов категории, с учётом UI-выбора:
      - category: money_market / stocks / bonds / gold
      - days: глубина истории (отражает UI period)
      - funds: comma-sep tickers (отражает hiddenFunds toggle — если задано,
        только эти фонды)
    """
    if category not in ("money_market", "stocks", "bonds", "gold"):
        raise HTTPException(status_code=400, detail="category invalid")

    params: dict = {"cat": category, "days": days}
    funds_filter = ""
    if funds:
        # Список тикеров приходит как "SBMM,LQDT,...". Разбиваем + bind как
        # отдельные параметры (:t0, :t1, ...) — SQLAlchemy text() с named-
        # params надёжнее чем PostgreSQL ANY(array) для строк.
        ticker_list = [t.strip().upper() for t in funds.split(",") if t.strip()]
        if ticker_list:
            placeholders = ",".join(f":t{i}" for i in range(len(ticker_list)))
            funds_filter = f"AND f.ticker IN ({placeholders})"
            for i, t in enumerate(ticker_list):
                params[f"t{i}"] = t

    rows = db.execute(text(f"""
        SELECT fd.trade_date, f.ticker, f.name, f.subcategory, fd.nav
        FROM fund_data fd
        JOIN funds f ON f.fund_id = fd.fund_id
        WHERE f.category = :cat
          AND fd.trade_date >= CURRENT_DATE - :days
          {funds_filter}
        ORDER BY fd.trade_date ASC, f.ticker ASC
    """), params).mappings().all()

    return csv_streaming_response(
        rows=[dict(r) for r in rows],
        fieldnames=["trade_date", "ticker", "name", "subcategory", "nav"],
        filename=f"funds_{category}_{_ts()}.csv",
    )


# ════════════════════════════════════════════════════════════════════
# CBR Flows — ОРФР
# ════════════════════════════════════════════════════════════════════
@router.get("/cbr-flows.csv")
def export_cbr_flows(
    instrument: str = Query("stocks"),
    years: int = Query(10, ge=1, le=30,
                       description="Глубина истории в годах (отражает UI period)"),
    categories: str | None = Query(None,
                                   description="Comma-separated фильтр категорий"),
    user: User = Depends(require_pro),
    db: Session = Depends(get_db),
):
    """
    ОРФР данные с фильтрами:
      - instrument: stocks/ofz/fx (тип инструмента)
      - years: глубина истории (UI period: 1Y → 1, 3Y → 3, All → 30)
      - categories: comma-sep фильтр (отражает скрытые категории в UI)
    """
    if instrument not in ("stocks", "ofz", "fx"):
        raise HTTPException(status_code=400, detail="instrument invalid")

    params: dict = {"it": instrument, "years": years}
    cat_filter = ""
    if categories:
        cat_list = [c.strip() for c in categories.split(",") if c.strip()]
        if cat_list:
            placeholders = ",".join(f":c{i}" for i in range(len(cat_list)))
            cat_filter = f"AND category IN ({placeholders})"
            for i, c in enumerate(cat_list):
                params[f"c{i}"] = c

    rows = db.execute(text(f"""
        SELECT period_year, period_label, period_kind, period_end_date,
               category, value
        FROM cbr_flows
        WHERE instrument_type = :it
          AND period_year >= EXTRACT(YEAR FROM CURRENT_DATE)::int - :years
          {cat_filter}
        ORDER BY period_end_date ASC, category ASC
    """), params).mappings().all()

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
    interval: int = Query(24, description="5/60/24 — 5min/1h/1d"),
    days: int = Query(365, ge=1, le=3650),
    user: User = Depends(require_pro),
    db: Session = Depends(get_db),
):
    """
    OI с учётом UI-выбора:
      - instrument: тикер фьючерса (SR, GZ, MX, ...)
      - clgroup: YUR (юрлица) / FIZ (физлица)
      - interval: 5 / 60 / 24 (5min / 1h / 1d, как в UI)
      - days: глубина истории (отражает UI period)

    Колонки: дата + время + open_interest + pos_long/short + участники.
    """
    if clgroup not in ("YUR", "FIZ"):
        raise HTTPException(status_code=400, detail="clgroup invalid")
    if interval not in (5, 60, 24):
        raise HTTPException(status_code=400, detail="interval invalid")

    instrument = instrument.strip().upper()
    rows = db.execute(text("""
        SELECT tradedate AS trade_date,
               tradetime AS trade_time,
               pos       AS open_interest,
               pos_long, pos_short, pos_long_num, pos_short_num,
               interval
        FROM open_interest
        WHERE sectype = :inst AND clgroup = :cl AND interval = :iv
          AND tradedate >= CURRENT_DATE - :days
        ORDER BY tradedate ASC, tradetime ASC
    """), {"inst": instrument, "cl": clgroup, "iv": interval, "days": days}).mappings().all()
    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"Нет OI данных по {instrument} {clgroup} interval={interval}",
        )

    return csv_streaming_response(
        rows=[dict(r) for r in rows],
        fieldnames=["trade_date", "trade_time", "open_interest",
                    "pos_long", "pos_short", "pos_long_num", "pos_short_num", "interval"],
        filename=f"oi_{instrument}_{clgroup}_{interval}_{_ts()}.csv",
    )
