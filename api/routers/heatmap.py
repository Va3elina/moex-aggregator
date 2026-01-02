"""
API для карты рынка (Heatmap) — стиль TradingView
"""
from fastapi import APIRouter, Query
from sqlalchemy import text

from api.database import get_engine

router = APIRouter(prefix="/api/heatmap", tags=["heatmap"])


@router.get("/stocks")
async def get_stocks_heatmap(
    size_by: str = Query("value_1d", description="Размер блока"),
    color_by: str = Query("change_1d", description="Цвет блока"),
    group_by: str = Query("sector", description="Группировка"),
):
    """
    Возвращает данные для карты рынка из материализованного представления.
    """
    engine = get_engine()

    query = text("""
        SELECT 
            sec_id, name, sector, price,
            change_1d, change_1w, change_1m,
            volume_1d, volume_1w, volume_1m,
            value_1d, value_1w, value_1m
        FROM mv_heatmap_stocks
        ORDER BY value_1d DESC NULLS LAST
    """)

    with engine.connect() as conn:
        result = conn.execute(query)
        rows = result.fetchall()

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
            "volume_1d": float(row[7]) if row[7] else 0,
            "volume_1w": float(row[8]) if row[8] else 0,
            "volume_1m": float(row[9]) if row[9] else 0,
            "value_1d": float(row[10]) if row[10] else 0,
            "value_1w": float(row[11]) if row[11] else 0,
            "value_1m": float(row[12]) if row[12] else 0,
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

    return {
        "stocks": stocks,
        "sectors": sectors_list,
        "params": {"size_by": size_by, "color_by": color_by, "group_by": group_by}
    }


@router.post("/refresh")
async def refresh_heatmap():
    """Обновляет материализованное представление"""
    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(text("REFRESH MATERIALIZED VIEW mv_heatmap_stocks"))
        conn.commit()
    return {"status": "ok", "message": "Heatmap обновлён"}