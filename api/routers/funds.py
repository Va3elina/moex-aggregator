"""
API роутер для данных фондов и индексов
Раздел: Деньги в Фондах
"""

from fastapi import APIRouter, Query, HTTPException, Depends
from sqlalchemy import text
from typing import Literal, Optional, List
from datetime import date, timedelta
from collections import defaultdict
import time

from api.billing.features import get_indicator_limits
from api.billing.tiers import user_tier
from api.database import get_engine
from api.logger import get_logger
from api.routers.auth import get_current_user_optional
from api.security.access_control import enforce_guest_limits, enforce_tier_limits

router = APIRouter(prefix="/api/funds", tags=["funds"])
log = get_logger()

# Маппинг category → отображаемое имя и индекс-бенчмарк
# min_date — нижняя граница истории. У money_market и gold реально доступная
# история до 2022 содержит шум (мало эмитентов / низкие объёмы / неточные NAV),
# поэтому отрезаем до 2022-01-09 чтобы не вводить пользователей в заблуждение.
CATEGORY_INDEX_MAP = {
    "money_market": {"name": "Денежный рынок", "index": "RUSFAR3M", "min_date": "2022-01-09"},
    "stocks": {"name": "Акции", "index": "IMOEX", "min_date": "1997-05-26"},
    "bonds": {"name": "Облигации", "index": "RGBITR", "min_date": "1997-03-24"},
    "gold": {"name": "Золото", "index": "GLDRUB_TOM", "min_date": "2022-01-09"},
}


def load_fund_categories() -> dict:
    """Загрузка категорий фондов из БД. Возвращает структуру аналогичную старой FUND_CATEGORIES."""
    engine = get_engine()
    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT fund_id, ticker, name, category, subcategory, uk_id FROM funds ORDER BY category, subcategory_order, ticker"
        )).fetchall()

    categories = {}
    for row in rows:
        fund_id, ticker, name, cat, subcat, uk_id = row[0], row[1], row[2], row[3], row[4], row[5]
        if cat not in CATEGORY_INDEX_MAP:
            continue
        if cat not in categories:
            meta = CATEGORY_INDEX_MAP[cat]
            categories[cat] = {
                "name": meta["name"],
                "index": meta["index"],
                "funds": {}
            }
        categories[cat]["funds"][fund_id] = {"ticker": ticker, "name": name, "subcategory": subcat, "uk_id": uk_id}

    return categories

# Периоды
PERIODS = {
    "1w": 7,
    "1m": 30,
    "3m": 90,
    "6m": 180,
    "1y": 365,
    "2y": 730,
    "3y": 1095,
    "all": None
}

CategoryType = Literal["money_market", "stocks", "bonds", "gold"]
PeriodType = Literal["1w", "1m", "3m", "6m", "1y", "2y", "3y", "all"]


@router.get("/chart")
async def get_funds_chart(
    category: CategoryType = Query(..., description="Категория фондов"),
    period: PeriodType = Query("6m", description="Период"),
    user = Depends(get_current_user_optional)
):
    """
    Данные для графика фондов + индекс

    Возвращает:
    - funds: список фондов с данными СЧА
    - index: данные индекса
    - total_nav: суммарная СЧА по дням
    """
    # Tier: period limit для funds_money
    enforce_tier_limits(user, "funds_money", period=period)

    # Tier whitelist — Free показывает только 8 крупных фондов (tickers_whitelist),
    # Basic/Pro — все. Получаем limits и фильтруем fund_ids ниже.
    tier = user_tier(user)
    limits = get_indicator_limits(tier, "funds_money")
    tickers_whitelist: Optional[List[str]] = limits.get("tickers_whitelist")

    start_time = time.time()
    log.info(f"REQUEST: /funds/chart category={category}, period={period}, tier={tier}")

    from api.cache import get_or_set
    # Cache key учитывает tier-фильтр — без этого Free мог бы получить cache
    # Basic'а со всеми фондами или наоборот.
    cache_suffix = "wl_" + ("_".join(tickers_whitelist) if tickers_whitelist else "all")
    cache_key = f"funds_chart:{category}:{period}:{cache_suffix}"
    cached = get_or_set(cache_key)
    if cached is not None:
        return cached

    fund_categories = load_fund_categories()
    if category not in fund_categories:
        raise HTTPException(status_code=400, detail=f"Неизвестная категория: {category}")

    cat_config = fund_categories[category]
    fund_ids = list(cat_config["funds"].keys())

    # Применяем whitelist для Free tier — оставляем только fund_id с ticker
    # в whitelist'е. cat_config["funds"] это dict {fund_id: {ticker, name, ...}}.
    if tickers_whitelist:
        wl_set = set(tickers_whitelist)
        fund_ids = [
            fid for fid in fund_ids
            if cat_config["funds"].get(fid, {}).get("ticker") in wl_set
        ]

    index_secid = cat_config["index"]

    # Расчёт периода (с учётом min_date категории)
    days = PERIODS.get(period)
    if days:
        date_from = date.today() - timedelta(days=days)
    else:
        date_from = date(2020, 1, 1)
    min_date_str = CATEGORY_INDEX_MAP.get(category, {}).get("min_date")
    if min_date_str:
        min_date = date.fromisoformat(min_date_str)
        date_from = max(date_from, min_date)

    engine = get_engine()

    try:
        with engine.connect() as conn:
            # === Данные фондов (СЧА) ===
            funds_query = text("""
                SELECT fd.fund_id, fd.trade_date, fd.nav
                FROM fund_data fd
                WHERE fd.fund_id = ANY(:fund_ids)
                  AND fd.trade_date >= :date_from
                  AND fd.nav IS NOT NULL
                  AND fd.nav > 0
                ORDER BY fd.fund_id, fd.trade_date
            """)
            
            funds_result = conn.execute(funds_query, {
                "fund_ids": fund_ids,
                "date_from": date_from
            }).fetchall()
            
            # Группируем по фондам
            funds_data = {}
            for row in funds_result:
                fid = row[0]
                if fid not in funds_data:
                    fund_info = cat_config["funds"].get(fid, {})
                    funds_data[fid] = {
                        "fund_id": fid,
                        "ticker": fund_info.get("ticker", str(fid)),
                        "name": fund_info.get("name", f"Фонд {fid}"),
                        "subcategory": fund_info.get("subcategory"),
                        "uk_id": fund_info.get("uk_id"),
                        "data": []
                    }
                funds_data[fid]["data"].append({
                    "date": row[1].isoformat(),
                    "nav": float(row[2]) if row[2] else None
                })
            
            # === Суммарная СЧА по датам ===
            total_query = text("""
                SELECT trade_date, SUM(nav) as total_nav
                FROM fund_data
                WHERE fund_id = ANY(:fund_ids)
                  AND trade_date >= :date_from
                  AND nav IS NOT NULL
                GROUP BY trade_date
                ORDER BY trade_date
            """)
            
            total_result = conn.execute(total_query, {
                "fund_ids": fund_ids,
                "date_from": date_from
            }).fetchall()
            
            total_nav = [
                {"date": row[0].isoformat(), "nav": float(row[1])}
                for row in total_result
                if float(row[1]) > 0
            ]
            
            # === Данные индекса ===
            index_query = text("""
                SELECT trade_date, close
                FROM index_data
                WHERE secid = :secid
                  AND trade_date >= :date_from
                ORDER BY trade_date
            """)
            
            index_result = conn.execute(index_query, {
                "secid": index_secid,
                "date_from": date_from
            }).fetchall()
            
            index_data = [
                {"date": row[0].isoformat(), "close": float(row[1]) if row[1] else None}
                for row in index_result
            ]
            
            duration = time.time() - start_time
            log.info(f"DONE: /funds/chart {len(funds_data)} funds, {duration:.2f}s")
            
            response = {
                "category": category,
                "category_name": cat_config["name"],
                "period": period,
                "funds": [funds_data[fid] for fid in cat_config["funds"] if fid in funds_data],
                "total_nav": total_nav,
                "index": {
                    "secid": index_secid,
                    "data": index_data
                }
            }
            get_or_set(cache_key, response, ttl=600)  # 10 мин
            return response
            
    except Exception as e:
        log.error(f"Ошибка получения данных фондов: {e}")
        raise HTTPException(status_code=500, detail="Ошибка получения данных")


@router.get("/summary")
async def get_funds_summary():
    """
    Сводка по всем категориям фондов
    """
    engine = get_engine()
    
    try:
        fund_categories = load_fund_categories()
        with engine.connect() as conn:
            result = []

            for cat_key, cat_config in fund_categories.items():
                fund_ids = list(cat_config["funds"].keys())
                
                # Последняя суммарная СЧА
                query = text("""
                    WITH latest AS (
                        SELECT fund_id, nav, trade_date,
                               ROW_NUMBER() OVER (PARTITION BY fund_id ORDER BY trade_date DESC) as rn
                        FROM fund_data
                        WHERE fund_id = ANY(:fund_ids) AND nav IS NOT NULL
                    ),
                    prev AS (
                        SELECT fund_id, nav, trade_date,
                               ROW_NUMBER() OVER (PARTITION BY fund_id ORDER BY trade_date DESC) as rn
                        FROM fund_data
                        WHERE fund_id = ANY(:fund_ids) 
                          AND nav IS NOT NULL
                          AND trade_date < (SELECT MAX(trade_date) FROM fund_data WHERE fund_id = ANY(:fund_ids))
                    )
                    SELECT 
                        COALESCE(SUM(l.nav), 0) as current_nav,
                        COALESCE(SUM(p.nav), 0) as prev_nav,
                        MAX(l.trade_date) as last_date
                    FROM latest l
                    LEFT JOIN prev p ON l.fund_id = p.fund_id AND p.rn = 1
                    WHERE l.rn = 1
                """)
                
                row = conn.execute(query, {"fund_ids": fund_ids}).fetchone()
                
                current_nav = float(row[0]) if row[0] else 0
                prev_nav = float(row[1]) if row[1] else current_nav
                last_date = row[2].isoformat() if row[2] else None
                
                change_pct = ((current_nav - prev_nav) / prev_nav * 100) if prev_nav > 0 else 0
                
                result.append({
                    "category": cat_key,
                    "name": cat_config["name"],
                    "index": cat_config["index"],
                    "funds_count": len(cat_config["funds"]),
                    "total_nav": current_nav,
                    "total_nav_formatted": f"{current_nav / 1e9:.2f} млрд ₽",
                    "change_pct": round(change_pct, 2),
                    "last_date": last_date
                })
            
            return {"categories": result}
            
    except Exception as e:
        log.error(f"Ошибка получения сводки: {e}")
        raise HTTPException(status_code=500, detail="Ошибка получения данных")


@router.get("/catalog")
async def get_funds_catalog():
    """Каталог всех фондов с метриками и составом."""
    from api.cache import get_or_set
    cache_key = "funds_catalog"
    cached = get_or_set(cache_key)
    if cached is not None:
        return cached

    engine = get_engine()
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT f.fund_id, f.ticker, f.name, f.category, f.subcategory, f.uk_id,
                fd_last.nav AS last_nav, fd_last.pay AS last_pay, fd_last.trade_date AS last_date,
                fd_1m.pay AS pay_1m, fd_3m.pay AS pay_3m, fd_6m.pay AS pay_6m, fd_1y.pay AS pay_1y,
                (SELECT COUNT(*) FROM fund_holdings fh WHERE fh.fund_id = f.fund_id) AS holdings_count
            FROM funds f
            LEFT JOIN LATERAL (
                SELECT nav, pay, trade_date FROM fund_data WHERE fund_id = f.fund_id AND nav IS NOT NULL
                ORDER BY trade_date DESC LIMIT 1
            ) fd_last ON true
            LEFT JOIN LATERAL (
                SELECT pay FROM fund_data WHERE fund_id = f.fund_id AND pay IS NOT NULL
                AND trade_date <= CURRENT_DATE - 30 ORDER BY trade_date DESC LIMIT 1
            ) fd_1m ON true
            LEFT JOIN LATERAL (
                SELECT pay FROM fund_data WHERE fund_id = f.fund_id AND pay IS NOT NULL
                AND trade_date <= CURRENT_DATE - 90 ORDER BY trade_date DESC LIMIT 1
            ) fd_3m ON true
            LEFT JOIN LATERAL (
                SELECT pay FROM fund_data WHERE fund_id = f.fund_id AND pay IS NOT NULL
                AND trade_date <= CURRENT_DATE - 180 ORDER BY trade_date DESC LIMIT 1
            ) fd_6m ON true
            LEFT JOIN LATERAL (
                SELECT pay FROM fund_data WHERE fund_id = f.fund_id AND pay IS NOT NULL
                AND trade_date <= CURRENT_DATE - 365 ORDER BY trade_date DESC LIMIT 1
            ) fd_1y ON true
            ORDER BY f.category, f.subcategory_order, f.ticker
        """)).fetchall()

        # Загрузим топ-5 holdings для каждого фонда
        holdings_rows = conn.execute(text("""
            SELECT fund_id, asset_name, weight FROM fund_holdings
            WHERE fund_id IN (SELECT fund_id FROM funds)
            ORDER BY fund_id, weight DESC
        """)).fetchall()

    engine.dispose()

    # Группируем holdings по fund_id (топ-5)
    from collections import defaultdict
    holdings_map = defaultdict(list)
    for r in holdings_rows:
        if len(holdings_map[r[0]]) < 5:
            holdings_map[r[0]].append({"name": r[1], "weight": float(r[2])})

    def calc_return(last, prev):
        if last and prev and float(prev) > 0:
            return round((float(last) - float(prev)) / float(prev) * 100, 2)
        return None

    funds = []
    for r in rows:
        fid = r[0]
        last_nav = float(r[6]) if r[6] else None
        last_pay = float(r[7]) if r[7] else None
        funds.append({
            "fund_id": fid,
            "ticker": r[1],
            "name": r[2],
            "category": r[3],
            "subcategory": r[4],
            "uk_id": r[5],
            "last_nav": last_nav,
            "last_pay": last_pay,
            "last_date": r[8].isoformat() if r[8] else None,
            "return_1m": calc_return(last_pay, float(r[9]) if r[9] else None),
            "return_3m": calc_return(last_pay, float(r[10]) if r[10] else None),
            "return_6m": calc_return(last_pay, float(r[11]) if r[11] else None),
            "return_1y": calc_return(last_pay, float(r[12]) if r[12] else None),
            "holdings_count": int(r[13]),
            "top_holdings": holdings_map.get(fid, []),
        })

    result = {"funds": funds, "total": len(funds)}
    get_or_set(cache_key, result, ttl=600)
    return result


@router.get("/holdings/{fund_id}")
async def get_fund_holdings(fund_id: int):
    """Состав фонда (топ позиции с долями)"""
    engine = get_engine()
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT asset_name, weight FROM fund_holdings
            WHERE fund_id = :fid ORDER BY weight DESC
        """), {"fid": fund_id}).fetchall()
    engine.dispose()
    return {
        "fund_id": fund_id,
        "holdings": [{"name": r[0], "weight": float(r[1])} for r in rows]
    }


@router.get("/categories")
async def get_categories():
    """Список категорий с фондами"""
    fund_categories = load_fund_categories()
    return {
        "categories": [
            {
                "key": key,
                "name": config["name"],
                "index": config["index"],
                "funds": [
                    {"id": fid, **finfo}
                    for fid, finfo in config["funds"].items()
                ]
            }
            for key, config in fund_categories.items()
        ]
    }


FlowTimeframeType = Literal["1d", "1w", "1m", "3m", "1y"]


@router.get("/flows")
async def get_funds_flows(
    category: CategoryType = Query(..., description="Категория фондов"),
    timeframe: FlowTimeframeType = Query("1w", description="Таймфрейм агрегации"),
    period: PeriodType = Query("1y", description="Общий период данных"),
    fund_ids_filter: Optional[str] = Query(None, alias="fund_ids", description="ID фондов через запятую (если не указаны — все)"),
    user = Depends(get_current_user_optional)
):
    """
    Притоки и оттоки в фондах (изменение AUM по периодам)

    Возвращает гистограмму:
    - flow > 0 → приток (зелёный)
    - flow < 0 → отток (красный)
    """
    # Tier-gating: timeframe (Free → только 1w/1m) + period + tickers_whitelist
    enforce_tier_limits(user, "funds_money", period=period, timeframe=timeframe)

    tier = user_tier(user)
    limits = get_indicator_limits(tier, "funds_money")
    tickers_whitelist: Optional[List[str]] = limits.get("tickers_whitelist")

    fund_categories = load_fund_categories()
    if category not in fund_categories:
        raise HTTPException(status_code=400, detail=f"Неизвестная категория: {category}")

    cat_config = fund_categories[category]
    all_fund_ids = list(cat_config["funds"].keys())

    # Применяем tier whitelist (Free → 8 фондов). Делаем до пользовательского
    # filter — иначе можно обойти tier-rule передав fund_ids=non-whitelisted.
    if tickers_whitelist:
        wl_set = set(tickers_whitelist)
        all_fund_ids = [
            fid for fid in all_fund_ids
            if cat_config["funds"].get(fid, {}).get("ticker") in wl_set
        ]

    # Фильтр по выбранным фондам (UI-уровень — фронтенд может убрать какие-то
    # из видимых; пересекаем с уже-tier-filtered all_fund_ids).
    if fund_ids_filter:
        try:
            requested_ids = [int(x) for x in fund_ids_filter.split(",") if x.strip()]
            fund_ids = [fid for fid in requested_ids if fid in all_fund_ids]
        except ValueError:
            fund_ids = all_fund_ids
    else:
        fund_ids = all_fund_ids

    if not fund_ids:
        return {"category": category, "timeframe": timeframe, "period": period, "flows": []}

    # Период (с учётом min_date категории)
    days = PERIODS.get(period)
    if days:
        date_from = date.today() - timedelta(days=days)
    else:
        date_from = date(2020, 1, 1)
    min_date_str = CATEGORY_INDEX_MAP.get(category, {}).get("min_date")
    if min_date_str:
        date_from = max(date_from, date.fromisoformat(min_date_str))

    engine = get_engine()

    def get_period_key(d):
        if timeframe == "1w":
            return d.isocalendar()[:2]
        elif timeframe == "1m":
            return (d.year, d.month)
        elif timeframe == "3m":
            return (d.year, (d.month - 1) // 3)
        elif timeframe == "1y":
            return (d.year,)
        else:
            return d.isocalendar()[:2]

    try:
        with engine.connect() as conn:
            # Получаем данные per-fund с forward-fill (не агрегируем в SQL —
            # агрегация по сумме pay некорректна при появлении новых фондов)
            query = text("""
                WITH all_dates AS (
                    SELECT DISTINCT trade_date FROM fund_data
                    WHERE fund_id = ANY(:fund_ids) AND trade_date >= :date_from
                ),
                fund_filled AS (
                    SELECT d.trade_date, f.fund_id,
                        COALESCE(
                            fd.nav,
                            (SELECT fd2.nav FROM fund_data fd2
                             WHERE fd2.fund_id = f.fund_id AND fd2.trade_date < d.trade_date
                               AND fd2.nav IS NOT NULL
                             ORDER BY fd2.trade_date DESC LIMIT 1)
                        ) as nav,
                        COALESCE(
                            fd.pay,
                            (SELECT fd2.pay FROM fund_data fd2
                             WHERE fd2.fund_id = f.fund_id AND fd2.trade_date < d.trade_date
                               AND fd2.pay IS NOT NULL
                             ORDER BY fd2.trade_date DESC LIMIT 1)
                        ) as pay
                    FROM all_dates d
                    CROSS JOIN (SELECT DISTINCT fund_id FROM fund_data WHERE fund_id = ANY(:fund_ids)) f
                    LEFT JOIN fund_data fd ON fd.fund_id = f.fund_id AND fd.trade_date = d.trade_date
                )
                SELECT fund_id, trade_date, nav, pay
                FROM fund_filled
                WHERE nav IS NOT NULL
                ORDER BY fund_id, trade_date
            """)

            result = conn.execute(query, {
                "fund_ids": fund_ids,
                "date_from": date_from
            }).fetchall()

            if len(result) < 2:
                return {"flows": [], "timeframe": timeframe}

            # Группируем данные по фондам
            fund_series: dict = defaultdict(list)
            for row in result:
                fund_series[row[0]].append((row[1], float(row[2]), float(row[3] or 0)))

            flows = []

            if timeframe == "1d":
                # Строим date -> (nav, pay) per fund
                fund_date_map: dict = {
                    fid: {d: (nav, pay) for d, nav, pay in rows}
                    for fid, rows in fund_series.items()
                }
                all_dates = sorted(set(d for rows in fund_series.values() for d, _, _ in rows))

                for i in range(1, len(all_dates)):
                    prev_d = all_dates[i - 1]
                    curr_d = all_dates[i]
                    total_net = 0.0
                    gross_in = 0.0
                    gross_out = 0.0
                    prev_total = 0.0

                    for fid, date_map in fund_date_map.items():
                        if prev_d not in date_map or curr_d not in date_map:
                            continue  # фонд отсутствует в одном из дней — пропускаем
                        prev_nav, prev_pay = date_map[prev_d]
                        curr_nav, curr_pay = date_map[curr_d]
                        prev_total += prev_nav
                        total_flow = curr_nav - prev_nav
                        if prev_pay > 0 and curr_pay > 0:
                            market_change = prev_nav * (curr_pay - prev_pay) / prev_pay
                            net_flow = total_flow - market_change
                        else:
                            net_flow = total_flow
                        total_net += net_flow
                        if net_flow >= 0:
                            gross_in += net_flow
                        else:
                            gross_out += net_flow

                    flows.append({
                        "period_start": prev_d.isoformat(),
                        "period_end": curr_d.isoformat(),
                        "flow": round(total_net / 1e9, 4),
                        "gross_in": round(gross_in / 1e9, 4),
                        "gross_out": round(gross_out / 1e9, 4),
                        "flow_pct": round((total_net / prev_total) * 100, 2) if prev_total > 0 else 0
                    })
            else:
                # Группируем каждый фонд по периодам отдельно
                fund_period: dict = {}
                for fid, rows in fund_series.items():
                    periods: dict = defaultdict(list)
                    for d, nav, pay in rows:
                        periods[get_period_key(d)].append((d, nav, pay))
                    fund_period[fid] = dict(periods)

                all_period_keys = sorted(
                    set(pk for fp in fund_period.values() for pk in fp.keys())
                )

                for i in range(1, len(all_period_keys)):
                    pk = all_period_keys[i]
                    ppk = all_period_keys[i - 1]
                    total_net = 0.0
                    gross_in = 0.0
                    gross_out = 0.0
                    prev_total = 0.0
                    period_start = None
                    period_end = None

                    for fid, fp in fund_period.items():
                        curr_pts = fp.get(pk)
                        if not curr_pts:
                            continue
                        curr_pts_s = sorted(curr_pts, key=lambda x: x[0])
                        start_d = curr_pts_s[0][0]
                        end_d, end_nav, end_pay = curr_pts_s[-1]

                        if period_start is None or start_d < period_start:
                            period_start = start_d
                        if period_end is None or end_d > period_end:
                            period_end = end_d

                        prev_pts = fp.get(ppk)
                        if not prev_pts:
                            # Фонд появился впервые — пропускаем (структурное изменение, не поток)
                            continue

                        prev_nav = sorted(prev_pts, key=lambda x: x[0])[-1][1]
                        prev_pay = sorted(prev_pts, key=lambda x: x[0])[-1][2]
                        prev_total += prev_nav
                        total_flow = end_nav - prev_nav
                        if prev_pay > 0 and end_pay > 0:
                            market_change = prev_nav * (end_pay - prev_pay) / prev_pay
                            net_flow = total_flow - market_change
                        else:
                            net_flow = total_flow
                        total_net += net_flow
                        if net_flow >= 0:
                            gross_in += net_flow
                        else:
                            gross_out += net_flow

                    if period_start is not None:
                        flows.append({
                            "period_start": period_start.isoformat(),
                            "period_end": period_end.isoformat(),
                            "flow": round(total_net / 1e9, 4),
                            "gross_in": round(gross_in / 1e9, 4),
                            "gross_out": round(gross_out / 1e9, 4),
                            "flow_pct": round((total_net / prev_total) * 100, 2) if prev_total > 0 else 0
                        })

            return {
                "category": category,
                "timeframe": timeframe,
                "period": period,
                "flows": flows
            }

    except Exception as e:
        log.error(f"Ошибка получения flows: {e}")
        raise HTTPException(status_code=500, detail="Ошибка получения данных")

