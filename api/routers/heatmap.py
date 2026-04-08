"""
API для карты рынка (Heatmap) — стиль TradingView
С валидацией входных данных
"""
import httpx
from fastapi import APIRouter, Query, HTTPException, Depends
from sqlalchemy import text

from api.database import get_engine
from api.routers.auth import require_admin
from api.schemas.validators import HeatmapSizeByType, HeatmapColorByType, HeatmapGroupByType

IMOEX_ISS_URL = "https://iss.moex.com/iss/statistics/engines/stock/markets/index/analytics/IMOEX.json?limit=100"

router = APIRouter(prefix="/api/heatmap", tags=["heatmap"])


@router.get("/stocks")
async def get_stocks_heatmap(
    size_by: HeatmapSizeByType = Query("value_1d", description="Размер блока"),
    color_by: HeatmapColorByType = Query("change_1d", description="Цвет блока"),
    group_by: HeatmapGroupByType = Query("sector", description="Группировка"),
):
    """
    Возвращает данные для карты рынка из материализованного представления.
    Параметры валидируются автоматически через Literal типы.
    """
    from api.cache import get_or_set

    cache_key = f"heatmap:{size_by}:{color_by}:{group_by}"
    cached = get_or_set(cache_key)
    if cached is not None:
        return cached

    engine = get_engine()

    # Безопасный запрос — без пользовательских данных в SQL
    query = text("""
        SELECT
            sec_id, name, sector, price,
            change_1d, change_1w, change_1m, change_1y,
            volume_1d, volume_1w, volume_1m,
            value_1d, value_1w, value_1m,
            market_cap
        FROM mv_heatmap_stocks
        ORDER BY value_1d DESC NULLS LAST
    """)

    try:
        with engine.connect() as conn:
            result = conn.execute(query)
            rows = result.fetchall()
    except Exception as e:
        raise HTTPException(status_code=500, detail="Ошибка получения данных heatmap")

    stocks = []
    for row in rows:
        stocks.append({
            "secId": row[0],
            "name": row[1],
            "sector": row[2],
            "price": float(row[3]) if row[3] else 0,
            "change_1d": float(row[4]) if row[4] else 0,
            "change_1w": float(row[5]) if row[5] else 0,
            "change_1m": float(row[6]) if row[6] else 0,
            "change_1y": float(row[7]) if row[7] else 0,
            "volume_1d": float(row[8]) if row[8] else 0,
            "volume_1w": float(row[9]) if row[9] else 0,
            "volume_1m": float(row[10]) if row[10] else 0,
            "value_1d": float(row[11]) if row[11] else 0,
            "value_1w": float(row[12]) if row[12] else 0,
            "value_1m": float(row[13]) if row[13] else 0,
            "market_cap": float(row[14]) if row[14] else 0,
        })

    # Группировка
    if group_by == "sector":
        sectors = {}
        for stock in stocks:
            sector = stock["sector"]
            if sector not in sectors:
                sectors[sector] = {"name": sector, "stocks": [], "totalValue": 0}
            sectors[sector]["stocks"].append(stock)
            sectors[sector]["totalValue"] += stock["value_1d"]

        sectors_list = sorted(sectors.values(), key=lambda x: x["totalValue"], reverse=True)
    else:
        sectors_list = [{"name": "Все акции", "stocks": stocks, "totalValue": sum(s["value_1d"] for s in stocks)}]

    from datetime import datetime
    response = {
        "stocks": stocks,
        "sectors": sectors_list,
        "params": {"size_by": size_by, "color_by": color_by, "group_by": group_by},
        "updated_at": datetime.now().strftime("%H:%M"),
    }
    get_or_set(cache_key, response, ttl=300)  # 5 мин
    return response


@router.post("/refresh")
async def refresh_heatmap(user=Depends(require_admin)):
    """Обновляет материализованное представление"""
    from api.cache import invalidate

    engine = get_engine()
    try:
        with engine.connect() as conn:
            conn.execute(text("REFRESH MATERIALIZED VIEW mv_heatmap_stocks"))
            conn.commit()
    except Exception as e:
        raise HTTPException(status_code=500, detail="Ошибка обновления heatmap")

    invalidate("heatmap")
    return {"status": "ok", "message": "Heatmap обновлён"}


@router.get("/imoex")
async def get_imoex_heatmap(
    color_by: HeatmapColorByType = Query("change_1w", description="Цвет блока"),
    group_by: HeatmapGroupByType = Query("sector", description="Группировка"),
):
    """Карта индекса IMOEX — размер по весу в индексе."""
    from api.cache import get_or_set

    cache_key = f"heatmap_imoex:{color_by}:{group_by}"
    cached = get_or_set(cache_key)
    if cached is not None:
        return cached

    # Получаем веса из ISS (кеш 1 час)
    weights_cache_key = "imoex_weights"
    weights = get_or_set(weights_cache_key)
    if weights is None:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(IMOEX_ISS_URL)
            resp.raise_for_status()
            data = resp.json()
        cols = data["analytics"]["columns"]
        rows_data = data["analytics"]["data"]
        idx_ticker = cols.index("ticker")
        idx_weight = cols.index("weight")
        weights = {row[idx_ticker]: float(row[idx_weight]) for row in rows_data}
        get_or_set(weights_cache_key, weights, ttl=3600)

    # Берём данные акций из mv_heatmap_stocks
    engine = get_engine()
    query = text("""
        SELECT sec_id, name, sector, price,
               change_1d, change_1w, change_1m, change_1y,
               volume_1d, volume_1w, volume_1m,
               value_1d, value_1w, value_1m,
               market_cap
        FROM mv_heatmap_stocks
        ORDER BY value_1d DESC NULLS LAST
    """)

    try:
        with engine.connect() as conn:
            result = conn.execute(query)
            all_rows = {row[0]: row for row in result.fetchall()}
    except Exception as e:
        raise HTTPException(status_code=500, detail="Ошибка получения данных heatmap")

    stocks = []
    for ticker, weight in weights.items():
        row = all_rows.get(ticker)
        if not row:
            continue
        stocks.append({
            "secId": row[0],
            "name": row[1],
            "sector": row[2],
            "price": float(row[3]) if row[3] else 0,
            "change_1d": float(row[4]) if row[4] else 0,
            "change_1w": float(row[5]) if row[5] else 0,
            "change_1m": float(row[6]) if row[6] else 0,
            "change_1y": float(row[7]) if row[7] else 0,
            "volume_1d": float(row[8]) if row[8] else 0,
            "volume_1w": float(row[9]) if row[9] else 0,
            "volume_1m": float(row[10]) if row[10] else 0,
            "value_1d": float(row[11]) if row[11] else 0,
            "value_1w": float(row[12]) if row[12] else 0,
            "value_1m": float(row[13]) if row[13] else 0,
            "market_cap": float(row[14]) if row[14] else 0,
            "weight": weight,
        })

    if group_by == "sector":
        sectors = {}
        for stock in stocks:
            sector = stock["sector"]
            if sector not in sectors:
                sectors[sector] = {"name": sector, "stocks": [], "totalValue": 0}
            sectors[sector]["stocks"].append(stock)
            sectors[sector]["totalValue"] += stock["weight"]
        sectors_list = sorted(sectors.values(), key=lambda x: x["totalValue"], reverse=True)
    else:
        sectors_list = [{"name": "Индекс IMOEX", "stocks": stocks, "totalValue": sum(s["weight"] for s in stocks)}]

    from datetime import datetime
    response = {
        "stocks": stocks,
        "sectors": sectors_list,
        "params": {"size_by": "weight", "color_by": color_by, "group_by": group_by},
        "updated_at": datetime.now().strftime("%H:%M"),
    }
    get_or_set(cache_key, response, ttl=300)
    return response