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
from api.utils.csv_export import csv_streaming_response, zip_response, xlsx_response

router = APIRouter(prefix="/api/export", tags=["export"])


def _bundle_response(
    files: dict,
    base_name: str,
    fmt: str,
):
    """
    Унифицированный выбор формата output'а.

    fmt:
      'csv'  → если 1 файл — CSV, если >1 — ZIP с CSV.
      'xlsx' → всегда XLSX с sheet per file.

    Args:
        files: {name: (rows, fieldnames)}.
        base_name: без extension (e.g. 'seasonality_20260524_0934').
        fmt: 'csv' или 'xlsx'.
    """
    if fmt == "xlsx":
        return xlsx_response(files, filename=f"{base_name}.xlsx")

    # csv mode
    if len(files) == 1:
        name, (rows, fieldnames) = next(iter(files.items()))
        return csv_streaming_response(rows=rows, fieldnames=fieldnames, filename=name)
    return zip_response(files, filename=f"{base_name}.zip")


def _ts() -> str:
    """Timestamp suffix для filename'ов: 20260524_0934."""
    return datetime.now().strftime("%Y%m%d_%H%M")


def _parse_layers(layers: str | None, default: list[str], allowed: set[str]) -> list[str]:
    """Парсит query-param ?layers=A,B,C → list. Невалидные drop. Пустой → default."""
    if not layers:
        return default
    parsed = [l.strip() for l in layers.split(",") if l.strip() in allowed]
    return parsed or default


# Hard cap на multi-value пикеры — backend дублирует UI-лимит чтоб защитить
# от monster-ZIP'ов даже если кто-то обойдёт UI (curl напрямую).
MAX_MULTI_VALUES = 6


def _cap_list(items: list, name: str) -> list:
    """Применяет жёсткий cap. При превышении — HTTP 400."""
    if len(items) > MAX_MULTI_VALUES:
        raise HTTPException(
            status_code=400,
            detail=f"Слишком много значений в {name} (макс. {MAX_MULTI_VALUES})",
        )
    return items


def _date_range_clause(
    days: int | None,
    period_from: str | None,
    period_to: str | None,
    date_column: str = "trade_date",
) -> tuple[str, dict]:
    """
    Возвращает SQL фрагмент + params dict для фильтрации по диапазону дат.

    Приоритет:
      1. period_from + period_to → BETWEEN (custom range mode)
      2. period_from → >= (open-ended)
      3. days → >= today - days (preset mode, threshold вычисляется в Python
         потому что pg8000 не умеет `CURRENT_DATE - :integer` без явных
         type casts — ловили 'operator does not exist: date >= integer').
      4. ничего → '' пустой clause (вся история)
    """
    from datetime import date, timedelta

    if period_from and period_to:
        return f"AND {date_column} BETWEEN :_pfrom AND :_pto", {
            "_pfrom": period_from,
            "_pto": period_to,
        }
    if period_from:
        return f"AND {date_column} >= :_pfrom", {"_pfrom": period_from}
    if days:
        threshold = date.today() - timedelta(days=days)
        return f"AND {date_column} >= :_pthreshold", {"_pthreshold": threshold}
    return "", {}


# ════════════════════════════════════════════════════════════════════
# Heatmap — все акции
# ════════════════════════════════════════════════════════════════════
@router.get("/heatmap.csv")
def export_heatmap(
    fmt: str = Query("csv", description="csv|xlsx — формат output'а"),
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
    fieldnames = [
        "sec_id", "name", "sector",
        "price", "prev_close",
        "change_1d", "change_1w", "change_1m", "change_1y",
        "volume_1d", "value_1d", "market_cap",
    ]
    files = {f"heatmap_{_ts()}.csv": ([dict(r) for r in rows], fieldnames)}
    return _bundle_response(files, f"heatmap_{_ts()}", fmt)


# ════════════════════════════════════════════════════════════════════
# Strength — breadth history (% акций выше EMA)
# ════════════════════════════════════════════════════════════════════
@router.get("/breadth.csv")
def export_breadth(
    ema: str = Query("200", description="EMA-период(ы), comma-sep: 50,100,200"),
    universe: str = Query("imoex", description="all|imoex|all_usd|imoex_usd, comma-sep"),
    days: int | None = Query(None, ge=1, le=3650),
    period_from: str | None = Query(None),
    period_to: str | None = Query(None),
    layers: str | None = Query(None, description="history,stocks"),
    fmt: str = Query("csv", description="csv|xlsx — формат output'а"),
    user: User = Depends(require_pro),
    db: Session = Depends(get_db),
):
    """
    Сила рынка с выбором слоёв:
      - history: timeseries % акций выше EMA (по дням)
      - stocks: snapshot всех акций с current price, EMA, is_above, diff_pct
    """
    # Parse multi-values + cap.
    ema_list = []
    for e in ema.split(","):
        try:
            v = int(e.strip())
            if 10 <= v <= 500:
                ema_list.append(v)
        except ValueError:
            pass
    ema_list = _cap_list(ema_list, "ema")
    if not ema_list:
        raise HTTPException(status_code=400, detail="ema invalid")

    universe_list = _cap_list(
        [u.strip() for u in universe.split(",")
         if u.strip() in ("all", "imoex", "all_usd", "imoex_usd")],
        "universe",
    )
    if not universe_list:
        raise HTTPException(status_code=400, detail="universe invalid")

    selected = _parse_layers(layers, ["history"], {"history", "stocks"})

    effective_days = days if (days is not None or period_from) else 365
    date_clause, date_params = _date_range_clause(effective_days, period_from, period_to)

    def history_data(e: int, u: str):
        rows = db.execute(text(f"""
            SELECT trade_date, percent_above, count_above, count_total
            FROM breadth_history
            WHERE ema_period = :ema AND universe = :universe
              {date_clause}
            ORDER BY trade_date ASC
        """), {"ema": e, "universe": u, **date_params}).mappings().all()
        return [dict(r) for r in rows], ["trade_date", "percent_above",
                                          "count_above", "count_total"]

    def stocks_data():
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

    # Декартово (ema × universe × layer).
    files: dict = {}
    if "history" in selected:
        for e in ema_list:
            for u in universe_list:
                rows, fields = history_data(e, u)
                files[f"strength_history_ema{e}_{u}.csv"] = (rows, fields)
    if "stocks" in selected:
        # Snapshot не зависит от EMA/universe — один на всю выборку.
        rows, fields = stocks_data()
        files["strength_stocks_snapshot.csv"] = (rows, fields)

    return _bundle_response(files, f"strength_{_ts()}", fmt)


# ════════════════════════════════════════════════════════════════════
# Seasonality — per-ticker history
# ════════════════════════════════════════════════════════════════════
@router.get("/seasonality.csv")
def export_seasonality(
    ticker: str = Query(..., description="Тикер ИЛИ comma-sep список: SBER или SBER,GAZP,LKOH"),
    layers: str | None = Query(None, description="daily,weekday_avg,monthly_avg,monthday_avg"),
    fmt: str = Query("csv", description="csv|xlsx — формат output'а"),
    user: User = Depends(require_pro),
    db: Session = Depends(get_db),
):
    """
    Сезонность с multi-ticker × multi-layer поддержкой.

    Слои:
      - daily: дневные свечи + декомпозиция + change_pct
      - weekday_avg: средний по дню недели
      - monthly_avg: средний по месяцу
      - monthday_avg: средний по дню месяца

    Multi-ticker: ?ticker=SBER,GAZP,LKOH → перебираются все combinations
    (ticker × layer) → ZIP с CSV-файлами `seasonality_{ticker}_{layer}.csv`.

    Если 1 ticker × 1 layer → single CSV.
    """
    tickers = [t.strip().upper() for t in ticker.split(",") if t.strip()]
    if not tickers:
        raise HTTPException(status_code=400, detail="ticker required")

    allowed = {"daily", "weekday_avg", "monthly_avg", "monthday_avg"}
    selected_layers = _parse_layers(layers, ["daily"], allowed)

    def fetch_candles(tk: str) -> list[dict]:
        rows = db.execute(text("""
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
        """), {"ticker": tk}).mappings().all()
        return [dict(r) for r in rows]

    def layer_daily(candles: list[dict]):
        return candles, [
            "trade_date", "year", "month", "day_of_month", "weekday",
            "open", "high", "low", "close", "volume", "change_pct",
        ]

    def layer_weekday_avg(candles: list[dict]):
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

    def layer_monthly_avg(candles: list[dict]):
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

    def layer_monthday_avg(candles: list[dict]):
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

    layer_fns = {
        "daily": layer_daily,
        "weekday_avg": layer_weekday_avg,
        "monthly_avg": layer_monthly_avg,
        "monthday_avg": layer_monthday_avg,
    }

    # Декартово произведение (ticker × layer).
    files: dict = {}
    for tk in tickers:
        candles = fetch_candles(tk)
        if not candles:
            # Один из тикеров не найден — пропускаем (graceful), а не 404.
            # Если ВСЕ не найдены — 404 в конце.
            continue
        for layer in selected_layers:
            rows, fields = layer_fns[layer](candles)
            files[f"seasonality_{tk}_{layer}.csv"] = (rows, fields)

    if not files:
        raise HTTPException(status_code=404, detail=f"Нет данных по {','.join(tickers)}")

    return _bundle_response(files, f"seasonality_{_ts()}", fmt)


# ════════════════════════════════════════════════════════════════════
# Buffett — capitalization / GDP / ratio
# ════════════════════════════════════════════════════════════════════
@router.get("/buffett.csv")
def export_buffett(
    mode: str = Query("cap-gdp", description="cap-gdp|cap-m2, comma-sep для обоих"),
    days: int | None = Query(None, ge=1, le=15000),
    period_from: str | None = Query(None),
    period_to: str | None = Query(None),
    timeframe: str = Query("1d", description="1d|1w|1m — sampling step"),
    fmt: str = Query("csv", description="csv|xlsx — формат output'а"),
    user: User = Depends(require_pro),
    db: Session = Depends(get_db),
):
    """
    Кап / ВВП или Кап / M2 — wide format (date | cap | gdp_ttm | ratio).
    Mode выбирается на UI:
      - cap-gdp: Кап / ВВП (МСК Биржа cap / Росстат GDP TTM × 100)
      - cap-m2:  Кап / M2 (M2 берётся из macro_data)
    """
    modes = [m.strip() for m in mode.split(",") if m.strip() in ("cap-gdp", "cap-m2")]
    if not modes:
        raise HTTPException(status_code=400, detail="mode invalid")

    if timeframe not in ("1d", "1w", "1m"):
        raise HTTPException(status_code=400, detail="timeframe invalid")

    # Date range — opt-in. Default = вся история.
    date_clause, date_params = _date_range_clause(
        days, period_from, period_to, date_column="trade_date",
    )

    # Timeframe sampling — после fetch'а rows.
    def sample(rows: list[dict]) -> list[dict]:
        """Sample каждую N-ю строку для weekly/monthly. Для 1d — нет sampling."""
        if timeframe == "1d" or not rows:
            return rows
        # Простой sampling по дате: для 1w — Friday only, для 1m — last
        # day of month. Это даёт регулярный шаг + соответствует тому что
        # юзер видит на странице (UI timeframe тоже sampling).
        seen_buckets: set = set()
        sampled = []
        for r in rows:
            d = r["trade_date"]
            if timeframe == "1w":
                # ISO week year-week (одна сэмпл per неделю — берём последнюю).
                bucket = (d.isocalendar()[0], d.isocalendar()[1]) if hasattr(d, 'isocalendar') else str(d)[:7]
            else:  # 1m
                bucket = (d.year, d.month) if hasattr(d, 'year') else str(d)[:7]
            seen_buckets.add(bucket)
        # Для каждого bucket — берём last row.
        last_per_bucket: dict = {}
        for r in rows:
            d = r["trade_date"]
            if timeframe == "1w":
                bucket = (d.isocalendar()[0], d.isocalendar()[1]) if hasattr(d, 'isocalendar') else str(d)[:7]
            else:
                bucket = (d.year, d.month) if hasattr(d, 'year') else str(d)[:7]
            last_per_bucket[bucket] = r
        sampled = list(last_per_bucket.values())
        sampled.sort(key=lambda x: x["trade_date"])
        return sampled

    def cap_gdp_data():
        rows = db.execute(text(f"""
            WITH cap AS (
              SELECT period_date AS dt, value AS cap
              FROM macro_data WHERE indicator='MARKET_CAP_TOTAL'
            ),
            gdp AS (
              SELECT period_date AS dt, value AS gdp_q,
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
            WHERE 1=1 {date_clause}
            ORDER BY trade_date ASC
        """), date_params).mappings().all()
        return sample([dict(r) for r in rows]), ["trade_date", "market_cap", "gdp_ttm", "buffett_ratio_pct"]

    def cap_m2_data():
        rows = db.execute(text(f"""
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
            WHERE 1=1 {date_clause}
            ORDER BY trade_date ASC
        """), date_params).mappings().all()
        return sample([dict(r) for r in rows]), ["trade_date", "market_cap", "m2", "cap_m2_ratio"]

    mode_fns = {"cap-gdp": cap_gdp_data, "cap-m2": cap_m2_data}
    files: dict = {}
    for m in modes:
        rows, fields = mode_fns[m]()
        files[f"buffett_{m}.csv"] = (rows, fields)

    return _bundle_response(files, f"buffett_{_ts()}", fmt)


# ════════════════════════════════════════════════════════════════════
# FundsMoney — NAV history per fund в категории
# ════════════════════════════════════════════════════════════════════
@router.get("/funds-money.csv")
def export_funds_money(
    category: str = Query("money_market", description="comma-sep для нескольких"),
    days: int | None = Query(None, ge=1, le=3650),
    period_from: str | None = Query(None),
    period_to: str | None = Query(None),
    funds: str | None = Query(None, description="Comma-sep tickers фондов"),
    fmt: str = Query("csv", description="csv|xlsx — формат output'а"),
    user: User = Depends(require_pro),
    db: Session = Depends(get_db),
):
    """NAV history фондов с multi-category support."""
    cat_list = _cap_list(
        [c.strip() for c in category.split(",")
         if c.strip() in ("money_market", "stocks", "bonds", "gold")],
        "category",
    )
    if not cat_list:
        raise HTTPException(status_code=400, detail="category invalid")

    effective_days = days if (days is not None or period_from) else 365
    date_clause, date_params = _date_range_clause(
        effective_days, period_from, period_to, date_column="fd.trade_date",
    )

    funds_filter = ""
    base_params: dict = {**date_params}
    if funds:
        ticker_list = [t.strip().upper() for t in funds.split(",") if t.strip()]
        if ticker_list:
            placeholders = ",".join(f":t{i}" for i in range(len(ticker_list)))
            funds_filter = f"AND f.ticker IN ({placeholders})"
            for i, t in enumerate(ticker_list):
                base_params[f"t{i}"] = t

    def fetch(cat: str):
        rows = db.execute(text(f"""
            SELECT fd.trade_date, f.ticker, f.name, f.subcategory, fd.nav
            FROM fund_data fd
            JOIN funds f ON f.fund_id = fd.fund_id
            WHERE f.category = :cat
              {date_clause}
              {funds_filter}
            ORDER BY fd.trade_date ASC, f.ticker ASC
        """), {**base_params, "cat": cat}).mappings().all()
        return [dict(r) for r in rows], ["trade_date", "ticker", "name", "subcategory", "nav"]

    files: dict = {}
    for cat in cat_list:
        rows, fields = fetch(cat)
        files[f"funds_{cat}.csv"] = (rows, fields)

    return _bundle_response(files, f"funds_{_ts()}", fmt)


# ════════════════════════════════════════════════════════════════════
# CBR Flows — ОРФР
# ════════════════════════════════════════════════════════════════════
@router.get("/cbr-flows.csv")
def export_cbr_flows(
    instrument: str = Query("stocks", description="comma-sep: stocks,ofz,fx"),
    years: int = Query(10, ge=1, le=30),
    categories: str | None = Query(None),
    fmt: str = Query("csv", description="csv|xlsx — формат output'а"),
    user: User = Depends(require_pro),
    db: Session = Depends(get_db),
):
    """ОРФР данные с multi-instrument."""
    inst_list = [
        i.strip() for i in instrument.split(",")
        if i.strip() in ("stocks", "ofz", "fx")
    ]
    if not inst_list:
        raise HTTPException(status_code=400, detail="instrument invalid")

    cat_filter = ""
    base_params: dict = {"years": years}
    if categories:
        cat_list = [c.strip() for c in categories.split(",") if c.strip()]
        if cat_list:
            placeholders = ",".join(f":c{i}" for i in range(len(cat_list)))
            cat_filter = f"AND category IN ({placeholders})"
            for i, c in enumerate(cat_list):
                base_params[f"c{i}"] = c

    def fetch(inst: str):
        rows = db.execute(text(f"""
            SELECT period_year, period_label, period_kind, period_end_date,
                   category, value
            FROM cbr_flows
            WHERE instrument_type = :it
              AND period_year >= EXTRACT(YEAR FROM CURRENT_DATE)::int - :years
              {cat_filter}
            ORDER BY period_end_date ASC, category ASC
        """), {**base_params, "it": inst}).mappings().all()
        return [dict(r) for r in rows], ["period_year", "period_label",
                                          "period_kind", "period_end_date",
                                          "category", "value"]

    files: dict = {}
    for inst in inst_list:
        rows, fields = fetch(inst)
        files[f"cbr_flows_{inst}.csv"] = (rows, fields)

    return _bundle_response(files, f"cbr_flows_{_ts()}", fmt)


# ════════════════════════════════════════════════════════════════════
# Open Interest — позиции участников + open interest
# ════════════════════════════════════════════════════════════════════
@router.get("/oi.csv")
def export_oi(
    instrument: str = Query(..., description="Тикер ИЛИ comma-sep: SR,GZ,MX"),
    clgroup: str = Query("YUR", description="YUR|FIZ, comma-sep для обоих"),
    interval: str = Query("24", description="5/60/24, comma-sep"),
    days: int | None = Query(None, ge=1, le=3650),
    period_from: str | None = Query(None, description="ISO date YYYY-MM-DD (range mode)"),
    period_to: str | None = Query(None, description="ISO date YYYY-MM-DD (range mode)"),
    fmt: str = Query("csv", description="csv|xlsx — формат output'а"),
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
    # Parse multi-values + cap.
    inst_list = _cap_list(
        [i.strip().upper() for i in instrument.split(",") if i.strip()],
        "instrument",
    )
    if not inst_list:
        raise HTTPException(status_code=400, detail="instrument required")

    cl_list = _cap_list(
        [c.strip().upper() for c in clgroup.split(",") if c.strip() in ("YUR", "FIZ")],
        "clgroup",
    )
    if not cl_list:
        raise HTTPException(status_code=400, detail="clgroup invalid")

    int_list = []
    for s in interval.split(","):
        try:
            v = int(s.strip())
            if v in (5, 60, 24):
                int_list.append(v)
        except ValueError:
            pass
    int_list = _cap_list(int_list, "interval")
    if not int_list:
        raise HTTPException(status_code=400, detail="interval invalid")

    # Effective days fallback если ни days, ни range не заданы.
    effective_days = days if (days is not None or period_from) else 365
    date_clause, date_params = _date_range_clause(
        effective_days, period_from, period_to, date_column="tradedate",
    )

    def fetch(inst: str, cl: str, iv: int):
        rows = db.execute(text(f"""
            SELECT tradedate AS trade_date,
                   tradetime AS trade_time,
                   pos       AS open_interest,
                   pos_long, pos_short, pos_long_num, pos_short_num,
                   interval
            FROM open_interest
            WHERE sectype = :inst AND clgroup = :cl AND interval = :iv
              {date_clause}
            ORDER BY tradedate ASC, tradetime ASC
        """), {"inst": inst, "cl": cl, "iv": iv, **date_params}).mappings().all()
        return [dict(r) for r in rows], [
            "trade_date", "trade_time", "open_interest",
            "pos_long", "pos_short", "pos_long_num", "pos_short_num", "interval",
        ]

    # Декартово (instrument × clgroup × interval).
    files: dict = {}
    for inst in inst_list:
        for cl in cl_list:
            for iv in int_list:
                rows, fields = fetch(inst, cl, iv)
                if rows:
                    files[f"oi_{inst}_{cl}_{iv}.csv"] = (rows, fields)

    if not files:
        raise HTTPException(
            status_code=404,
            detail=f"Нет OI данных для выбранных параметров",
        )

    return _bundle_response(files, f"oi_{_ts()}", fmt)
