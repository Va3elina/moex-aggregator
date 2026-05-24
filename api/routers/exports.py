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
from api.utils.csv_export import csv_streaming_response, zip_response

router = APIRouter(prefix="/api/export", tags=["export"])


def _ts() -> str:
    """Timestamp suffix для filename'ов: 20260524_0934."""
    return datetime.now().strftime("%Y%m%d_%H%M")


def _parse_layers(layers: str | None, default: list[str], allowed: set[str]) -> list[str]:
    """Парсит query-param ?layers=A,B,C → list. Невалидные drop. Пустой → default."""
    if not layers:
        return default
    parsed = [l.strip() for l in layers.split(",") if l.strip() in allowed]
    return parsed or default


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
    layers: str | None = Query(None, description="history,stocks (comma-sep)"),
    user: User = Depends(require_pro),
    db: Session = Depends(get_db),
):
    """
    Сила рынка с выбором слоёв:
      - history: timeseries % акций выше EMA (по дням)
      - stocks: snapshot всех акций с current price, EMA, is_above, diff_pct
    """
    if universe not in ("all", "imoex", "all_usd", "imoex_usd"):
        raise HTTPException(status_code=400, detail="universe invalid")

    selected = _parse_layers(layers, ["history"], {"history", "stocks"})

    def history_data():
        rows = db.execute(text("""
            SELECT trade_date, percent_above, count_above, count_total
            FROM breadth_history
            WHERE ema_period = :ema AND universe = :universe
              AND trade_date >= CURRENT_DATE - :days
            ORDER BY trade_date ASC
        """), {"ema": ema, "universe": universe, "days": days}).mappings().all()
        return [dict(r) for r in rows], ["trade_date", "percent_above",
                                          "count_above", "count_total"]

    def stocks_data():
        # Текущий снапшот через mv_heatmap_stocks (current price) +
        # вычислить EMA per stock было бы дорого тут — отдаём цены, юзер
        # сам считает EMA из seasonality CSV-загрузки если хочет.
        rows = db.execute(text("""
            SELECT m.sec_id AS ticker, m.name, m.sector,
                   m.price AS current_price,
                   m.change_1d, m.change_1w, m.change_1m
            FROM mv_heatmap_stocks m
            JOIN instruments i ON i.sectype = m.sec_id
            WHERE i.type = 'stock' AND i."group" = 'Акции'
            ORDER BY m.sec_id ASC
        """)).mappings().all()
        return [dict(r) for r in rows], [
            "ticker", "name", "sector", "current_price",
            "change_1d", "change_1w", "change_1m",
        ]

    layer_map = {
        "history": (f"breadth_history_ema{ema}_{universe}.csv", history_data),
        "stocks": (f"breadth_stocks_snapshot.csv", stocks_data),
    }

    if len(selected) == 1:
        name, fn = layer_map[selected[0]]
        rows, fields = fn()
        return csv_streaming_response(rows=rows, fieldnames=fields, filename=name)

    # Multi-layer → ZIP.
    files = {}
    for layer in selected:
        name, fn = layer_map[layer]
        rows, fields = fn()
        files[name] = (rows, fields)
    return zip_response(files, filename=f"strength_{ema}_{universe}_{_ts()}.zip")


# ════════════════════════════════════════════════════════════════════
# Seasonality — per-ticker history
# ════════════════════════════════════════════════════════════════════
@router.get("/seasonality.csv")
def export_seasonality(
    ticker: str = Query(..., min_length=1, max_length=20),
    layers: str | None = Query(None, description="daily,weekday_avg,monthly_avg,monthday_avg"),
    user: User = Depends(require_pro),
    db: Session = Depends(get_db),
):
    """
    Сезонность с выбором слоёв:
      - daily: дневные свечи + декомпозиция (year/month/weekday) + change_pct
      - weekday_avg: средний дневной change_pct по дню недели (Пн-Вс)
      - monthly_avg: средний месячный change_pct по месяцу (Янв-Дек)
      - monthday_avg: средний дневной change_pct по дню месяца (1-31)
    """
    ticker = ticker.strip().upper()
    allowed = {"daily", "weekday_avg", "monthly_avg", "monthday_avg"}
    selected = _parse_layers(layers, ["daily"], allowed)

    # Базовая выборка свечей — используется во всех слоях.
    candle_rows = db.execute(text("""
        SELECT begin_time::date AS trade_date,
               open, high, low, close, volume,
               ROUND(((close - LAG(close) OVER (ORDER BY begin_time)) /
                      NULLIF(LAG(close) OVER (ORDER BY begin_time), 0) * 100)::numeric, 4)
                 AS change_pct,
               EXTRACT(ISODOW FROM begin_time)::int AS weekday,
               EXTRACT(MONTH  FROM begin_time)::int AS month,
               EXTRACT(DAY    FROM begin_time)::int AS day_of_month,
               EXTRACT(YEAR   FROM begin_time)::int AS year
        FROM candles
        WHERE secid = :ticker AND interval = 24 AND type = 'stock'
          AND close > 0
        ORDER BY begin_time ASC
    """), {"ticker": ticker}).mappings().all()
    if not candle_rows:
        raise HTTPException(status_code=404, detail=f"Нет данных по {ticker}")

    candles = [dict(r) for r in candle_rows]

    def daily_data():
        return candles, [
            "trade_date", "year", "month", "day_of_month", "weekday",
            "open", "high", "low", "close", "volume", "change_pct",
        ]

    def weekday_avg_data():
        # Aggregation in Python (БД query + Python — проще чем complex SQL).
        from statistics import mean, stdev
        buckets: dict[int, list[float]] = {}
        for c in candles:
            if c["change_pct"] is None:
                continue
            buckets.setdefault(c["weekday"], []).append(float(c["change_pct"]))
        labels = {1: "Понедельник", 2: "Вторник", 3: "Среда",
                  4: "Четверг", 5: "Пятница", 6: "Суббота", 7: "Воскресенье"}
        rows = []
        for wd in sorted(buckets):
            vals = buckets[wd]
            rows.append({
                "weekday": wd,
                "weekday_label": labels.get(wd, str(wd)),
                "avg_change_pct": round(mean(vals), 4),
                "stdev_change_pct": round(stdev(vals), 4) if len(vals) > 1 else 0,
                "sample_size": len(vals),
            })
        return rows, ["weekday", "weekday_label", "avg_change_pct",
                       "stdev_change_pct", "sample_size"]

    def monthly_avg_data():
        from statistics import mean, stdev
        buckets: dict[int, list[float]] = {}
        for c in candles:
            if c["change_pct"] is None:
                continue
            buckets.setdefault(c["month"], []).append(float(c["change_pct"]))
        labels = {1: "Январь", 2: "Февраль", 3: "Март", 4: "Апрель",
                  5: "Май", 6: "Июнь", 7: "Июль", 8: "Август",
                  9: "Сентябрь", 10: "Октябрь", 11: "Ноябрь", 12: "Декабрь"}
        rows = []
        for m in sorted(buckets):
            vals = buckets[m]
            rows.append({
                "month": m,
                "month_label": labels.get(m, str(m)),
                "avg_change_pct": round(mean(vals), 4),
                "stdev_change_pct": round(stdev(vals), 4) if len(vals) > 1 else 0,
                "sample_size": len(vals),
            })
        return rows, ["month", "month_label", "avg_change_pct",
                       "stdev_change_pct", "sample_size"]

    def monthday_avg_data():
        from statistics import mean, stdev
        buckets: dict[int, list[float]] = {}
        for c in candles:
            if c["change_pct"] is None:
                continue
            buckets.setdefault(c["day_of_month"], []).append(float(c["change_pct"]))
        rows = []
        for d in sorted(buckets):
            vals = buckets[d]
            rows.append({
                "day_of_month": d,
                "avg_change_pct": round(mean(vals), 4),
                "stdev_change_pct": round(stdev(vals), 4) if len(vals) > 1 else 0,
                "sample_size": len(vals),
            })
        return rows, ["day_of_month", "avg_change_pct",
                       "stdev_change_pct", "sample_size"]

    layer_map = {
        "daily": (f"seasonality_{ticker}_daily.csv", daily_data),
        "weekday_avg": (f"seasonality_{ticker}_weekday_avg.csv", weekday_avg_data),
        "monthly_avg": (f"seasonality_{ticker}_monthly_avg.csv", monthly_avg_data),
        "monthday_avg": (f"seasonality_{ticker}_monthday_avg.csv", monthday_avg_data),
    }

    if len(selected) == 1:
        name, fn = layer_map[selected[0]]
        rows, fields = fn()
        return csv_streaming_response(rows=rows, fieldnames=fields, filename=name)

    files = {}
    for layer in selected:
        name, fn = layer_map[layer]
        rows, fields = fn()
        files[name] = (rows, fields)
    return zip_response(files, filename=f"seasonality_{ticker}_{_ts()}.zip")


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
