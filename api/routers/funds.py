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

from api.database import get_engine
from api.logger import get_logger
from api.routers.auth import get_current_user_optional
from api.security.access_control import enforce_guest_limits

router = APIRouter(prefix="/api/funds", tags=["funds"])
log = get_logger()

# Маппинг category → отображаемое имя и индекс-бенчмарк
CATEGORY_INDEX_MAP = {
    "money_market": {"name": "Денежный рынок", "index": "RUSFAR3M"},
    "stocks": {"name": "Акции", "index": "IMOEX"},
    "bonds": {"name": "Облигации", "index": "RGBITR"},
    "gold": {"name": "Золото", "index": "GLDRUB_TOM"},
}


def load_fund_categories() -> dict:
    """Загрузка категорий фондов из БД. Возвращает структуру аналогичную старой FUND_CATEGORIES."""
    engine = get_engine()
    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT fund_id, ticker, name, category FROM funds ORDER BY category, ticker"
        )).fetchall()

    categories = {}
    for row in rows:
        fund_id, ticker, name, cat = row[0], row[1], row[2], row[3]
        if cat not in CATEGORY_INDEX_MAP:
            continue
        if cat not in categories:
            meta = CATEGORY_INDEX_MAP[cat]
            categories[cat] = {
                "name": meta["name"],
                "index": meta["index"],
                "funds": {}
            }
        categories[cat]["funds"][fund_id] = {"ticker": ticker, "name": name}

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
    # Ограничения для гостей
    enforce_guest_limits(user, period=period)

    start_time = time.time()
    log.info(f"REQUEST: /funds/chart category={category}, period={period}")

    from api.cache import get_or_set
    cache_key = f"funds_chart:{category}:{period}"
    cached = get_or_set(cache_key)
    if cached is not None:
        return cached

    fund_categories = load_fund_categories()
    if category not in fund_categories:
        raise HTTPException(status_code=400, detail=f"Неизвестная категория: {category}")

    cat_config = fund_categories[category]
    fund_ids = list(cat_config["funds"].keys())
    index_secid = cat_config["index"]
    
    # Расчёт периода
    days = PERIODS.get(period)
    if days:
        date_from = date.today() - timedelta(days=days)
    else:
        date_from = date(2020, 1, 1)
    
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
                "funds": list(funds_data.values()),
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
    # Ограничения для гостей
    enforce_guest_limits(user, period=period)

    fund_categories = load_fund_categories()
    if category not in fund_categories:
        raise HTTPException(status_code=400, detail=f"Неизвестная категория: {category}")

    cat_config = fund_categories[category]
    all_fund_ids = list(cat_config["funds"].keys())

    # Фильтр по выбранным фондам
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

    # Период
    days = PERIODS.get(period)
    if days:
        date_from = date.today() - timedelta(days=days)
    else:
        date_from = date(2020, 1, 1)
    
    engine = get_engine()
    
    try:
        with engine.connect() as conn:
            # Получаем суммарную СЧА по датам
            query = text("""
                SELECT trade_date, SUM(nav) as total_nav
                FROM fund_data
                WHERE fund_id = ANY(:fund_ids)
                  AND trade_date >= :date_from
                  AND nav IS NOT NULL
                GROUP BY trade_date
                ORDER BY trade_date
            """)
            
            result = conn.execute(query, {
                "fund_ids": fund_ids,
                "date_from": date_from
            }).fetchall()
            
            if len(result) < 2:
                return {"flows": [], "timeframe": timeframe}
            
            # Группируем по периодам и считаем дельту
            flows = []
            
            if timeframe == "1d":
                # Дневное изменение
                for i in range(1, len(result)):
                    prev_date, prev_nav = result[i-1]
                    curr_date, curr_nav = result[i]
                    flow = float(curr_nav) - float(prev_nav)
                    flows.append({
                        "period_start": prev_date.isoformat(),
                        "period_end": curr_date.isoformat(),
                        "flow": round(flow / 1e9, 2),  # В млрд
                        "flow_pct": round((flow / float(prev_nav)) * 100, 2) if prev_nav else 0
                    })
            else:
                # Группировка по неделям/месяцам/кварталам/годам
                periods_data = defaultdict(list)
                
                for row in result:
                    d = row[0]
                    nav = float(row[1])
                    
                    if timeframe == "1w":
                        # Неделя (ISO week)
                        period_key = d.isocalendar()[:2]  # (year, week)
                    elif timeframe == "1m":
                        period_key = (d.year, d.month)
                    elif timeframe == "3m":
                        quarter = (d.month - 1) // 3
                        period_key = (d.year, quarter)
                    elif timeframe == "1y":
                        period_key = (d.year,)
                    else:
                        period_key = d.isocalendar()[:2]
                    
                    periods_data[period_key].append((d, nav))
                
                # Сортируем периоды и считаем дельту
                sorted_periods = sorted(periods_data.keys())
                
                for period_key in sorted_periods:
                    data_points = periods_data[period_key]
                    if len(data_points) < 1:
                        continue
                    
                    # Начало и конец периода
                    data_points.sort(key=lambda x: x[0])
                    start_date, start_nav = data_points[0]
                    end_date, end_nav = data_points[-1]
                    
                    flow = end_nav - start_nav
                    flows.append({
                        "period_start": start_date.isoformat(),
                        "period_end": end_date.isoformat(),
                        "flow": round(flow / 1e9, 2),  # В млрд
                        "flow_pct": round((flow / start_nav) * 100, 2) if start_nav else 0
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


# ═══════════════════════════════════════════════════════════════════════════════
# FUND FEAR INDEX
# ═══════════════════════════════════════════════════════════════════════════════

def get_fear_index_fund_ids() -> dict:
    """Получить fund_id для расчёта Fear Index из БД."""
    cats = load_fund_categories()
    return {
        "money_market": list(cats.get("money_market", {}).get("funds", {}).keys()),
        "stocks": list(cats.get("stocks", {}).get("funds", {}).keys()),
    }


@router.get("/fear-index")
async def get_fear_index():
    """
    Текущее значение Fund Fear Index
    
    Шкала 0-100:
    - 0-25: Extreme Greed (экстремальная жадность)
    - 25-45: Greed (жадность)
    - 45-55: Neutral (нейтрально)
    - 55-75: Fear (страх)
    - 75-100: Extreme Fear (экстремальный страх)
    """
    from api.cache import get_or_set
    cache_key = "fear_index"
    cached = get_or_set(cache_key)
    if cached is not None:
        return cached

    engine = get_engine()
    fear_funds = get_fear_index_fund_ids()

    try:
        with engine.connect() as conn:
            # Загружаем СЧА за последние 30 дней для расчёта
            date_from = date.today() - timedelta(days=30)

            # СЧА денежного рынка
            mm_query = text("""
                SELECT trade_date, SUM(nav) as total_nav
                FROM fund_data
                WHERE fund_id = ANY(:fund_ids)
                  AND trade_date >= :date_from
                  AND nav IS NOT NULL
                GROUP BY trade_date
                ORDER BY trade_date
            """)

            mm_result = conn.execute(mm_query, {
                "fund_ids": fear_funds["money_market"],
                "date_from": date_from
            }).fetchall()

            # СЧА акций
            stocks_result = conn.execute(mm_query, {
                "fund_ids": fear_funds["stocks"],
                "date_from": date_from
            }).fetchall()
            
            if not mm_result or not stocks_result:
                return {
                    "error": "Недостаточно данных",
                    "fear_index": None,
                    "classification": None
                }
            
            # Преобразуем в словари по датам
            mm_data = {row[0]: float(row[1]) for row in mm_result}
            stocks_data = {row[0]: float(row[1]) for row in stocks_result}
            
            # Находим общие даты
            common_dates = sorted(set(mm_data.keys()) & set(stocks_data.keys()))
            
            if len(common_dates) < 5:
                return {
                    "error": "Недостаточно данных",
                    "fear_index": None,
                    "classification": None
                }
            
            # Рассчитываем Rotation Ratio за последние дни
            rotation_ratios = []
            for d in common_dates[-10:]:
                ratio = mm_data[d] / stocks_data[d] if stocks_data[d] > 0 else 1
                rotation_ratios.append(ratio)
            
            current_ratio = rotation_ratios[-1]
            avg_ratio = sum(rotation_ratios) / len(rotation_ratios)
            
            # Рассчитываем потоки (изменения СЧА)
            mm_values = [mm_data[d] for d in common_dates[-6:]]
            stocks_values = [stocks_data[d] for d in common_dates[-6:]]
            
            mm_flow = (mm_values[-1] - mm_values[0]) / mm_values[0] if mm_values[0] > 0 else 0
            stocks_flow = (stocks_values[-1] - stocks_values[0]) / stocks_values[0] if stocks_values[0] > 0 else 0
            
            # Velocity (скорость изменений)
            velocity = abs(mm_flow) + abs(stocks_flow)
            
            # Нормализация компонентов к шкале 0-100
            # Rotation ratio: выше среднего = страх
            rotation_score = min(100, max(0, 50 + (current_ratio - avg_ratio) / avg_ratio * 100))
            
            # MM flow: приток в MM = страх
            mm_flow_score = min(100, max(0, 50 + mm_flow * 500))
            
            # Stocks flow: отток из акций = страх
            stocks_flow_score = min(100, max(0, 50 - stocks_flow * 500))
            
            # Velocity: высокая = страх
            velocity_score = min(100, max(0, 50 + velocity * 200))
            
            # Итоговый индекс
            fear_index = (
                rotation_score * 0.35 +
                mm_flow_score * 0.25 +
                stocks_flow_score * 0.25 +
                velocity_score * 0.15
            )
            
            # Классификация
            if fear_index < 25:
                classification = "Extreme Greed"
                classification_ru = "Экстремальная жадность"
            elif fear_index < 45:
                classification = "Greed"
                classification_ru = "Жадность"
            elif fear_index < 55:
                classification = "Neutral"
                classification_ru = "Нейтрально"
            elif fear_index < 75:
                classification = "Fear"
                classification_ru = "Страх"
            else:
                classification = "Extreme Fear"
                classification_ru = "Экстремальный страх"
            
            response = {
                "date": common_dates[-1].isoformat(),
                "fear_index": round(fear_index, 1),
                "classification": classification,
                "classification_ru": classification_ru,
                "components": {
                    "rotation_ratio": round(rotation_score, 1),
                    "money_market_flow": round(mm_flow_score, 1),
                    "stocks_flow": round(stocks_flow_score, 1),
                    "velocity": round(velocity_score, 1),
                },
                "raw_values": {
                    "rotation_ratio": round(current_ratio, 4),
                    "mm_flow_pct": round(mm_flow * 100, 2),
                    "stocks_flow_pct": round(stocks_flow * 100, 2),
                    "velocity": round(velocity * 100, 2),
                },
                "totals": {
                    "money_market_nav": round(mm_data[common_dates[-1]] / 1e9, 2),
                    "stocks_nav": round(stocks_data[common_dates[-1]] / 1e9, 2),
                }
            }
            get_or_set(cache_key, response, ttl=300)  # 5 мин
            return response
            
    except Exception as e:
        log.error(f"Ошибка расчёта Fear Index: {e}")
        raise HTTPException(status_code=500, detail="Ошибка расчёта индекса")


@router.get("/fear-index/history")
async def get_fear_index_history(
    period: PeriodType = Query("3m", description="Период"),
    user = Depends(get_current_user_optional)
):
    """
    История Fund Fear Index
    """
    # Ограничения для гостей
    enforce_guest_limits(user, period=period)

    from api.cache import get_or_set
    cache_key = f"fear_history:{period}"
    cached = get_or_set(cache_key)
    if cached is not None:
        return cached

    engine = get_engine()
    fear_funds = get_fear_index_fund_ids()

    days = PERIODS.get(period)
    if days:
        date_from = date.today() - timedelta(days=days)
    else:
        date_from = date(2020, 1, 1)

    try:
        with engine.connect() as conn:
            # Загружаем СЧА за период
            query = text("""
                WITH mm AS (
                    SELECT trade_date, SUM(nav) as nav
                    FROM fund_data
                    WHERE fund_id = ANY(:mm_funds)
                      AND trade_date >= :date_from
                      AND nav IS NOT NULL
                    GROUP BY trade_date
                ),
                stocks AS (
                    SELECT trade_date, SUM(nav) as nav
                    FROM fund_data
                    WHERE fund_id = ANY(:stocks_funds)
                      AND trade_date >= :date_from
                      AND nav IS NOT NULL
                    GROUP BY trade_date
                )
                SELECT
                    mm.trade_date,
                    mm.nav as mm_nav,
                    stocks.nav as stocks_nav,
                    mm.nav / NULLIF(stocks.nav, 0) as rotation_ratio
                FROM mm
                JOIN stocks ON mm.trade_date = stocks.trade_date
                ORDER BY mm.trade_date
            """)

            result = conn.execute(query, {
                "mm_funds": fear_funds["money_market"],
                "stocks_funds": fear_funds["stocks"],
                "date_from": date_from
            }).fetchall()
            
            if not result:
                return {"error": "Нет данных", "history": []}
            
            # Рассчитываем fear index для каждого дня
            history = []
            ratios = [float(row[3]) for row in result if row[3]]
            avg_ratio = sum(ratios) / len(ratios) if ratios else 1
            
            for i, row in enumerate(result):
                if i < 5:  # Нужно минимум 5 дней для расчёта
                    continue
                
                current_ratio = float(row[3]) if row[3] else 1
                
                # Потоки за последние 5 дней
                mm_start = float(result[i-5][1])
                mm_end = float(row[1])
                stocks_start = float(result[i-5][2])
                stocks_end = float(row[2])
                
                mm_flow = (mm_end - mm_start) / mm_start if mm_start > 0 else 0
                stocks_flow = (stocks_end - stocks_start) / stocks_start if stocks_start > 0 else 0
                velocity = abs(mm_flow) + abs(stocks_flow)
                
                # Scores
                rotation_score = min(100, max(0, 50 + (current_ratio - avg_ratio) / avg_ratio * 100))
                mm_flow_score = min(100, max(0, 50 + mm_flow * 500))
                stocks_flow_score = min(100, max(0, 50 - stocks_flow * 500))
                velocity_score = min(100, max(0, 50 + velocity * 200))
                
                fear_index = (
                    rotation_score * 0.35 +
                    mm_flow_score * 0.25 +
                    stocks_flow_score * 0.25 +
                    velocity_score * 0.15
                )
                
                history.append({
                    "date": row[0].isoformat(),
                    "fear_index": round(fear_index, 1),
                    "rotation_ratio": round(current_ratio, 4),
                    "mm_nav": round(float(row[1]) / 1e9, 2),
                    "stocks_nav": round(float(row[2]) / 1e9, 2),
                })
            
            response = {
                "period": period,
                "count": len(history),
                "history": history
            }
            get_or_set(cache_key, response, ttl=600)  # 10 мин
            return response
            
    except Exception as e:
        log.error(f"Ошибка получения истории Fear Index: {e}")
        raise HTTPException(status_code=500, detail="Ошибка получения данных")

