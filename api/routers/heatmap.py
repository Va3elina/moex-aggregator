"""
API для карты рынка (Heatmap) — стиль TradingView
С валидацией входных данных
"""
import httpx
import logging
from fastapi import APIRouter, Query, HTTPException, Depends
from sqlalchemy import text

from api.database import get_engine
from api.routers.auth import require_admin, get_current_user_optional
from api.schemas.validators import HeatmapSizeByType, HeatmapColorByType, HeatmapGroupByType

IMOEX_ISS_URL = "https://iss.moex.com/iss/statistics/engines/stock/markets/index/analytics/IMOEX.json?limit=100"

logger = logging.getLogger("moex_api")

router = APIRouter(prefix="/api/heatmap", tags=["heatmap"])


def _persist_imoex_weights(weights: dict) -> None:
    """Сохранить веса IMOEX в БД (imoex_weights) — durable fallback на случай
    недоступности ISS (блок сети MOEX при просрочке Algopack-подписки, тех.работы
    ISS). Веса индекса меняются раз в квартал → stale-копия полностью допустима."""
    if not weights:
        return
    try:
        with get_engine().begin() as conn:
            conn.execute(text(
                "CREATE TABLE IF NOT EXISTS imoex_weights ("
                "ticker text PRIMARY KEY, weight double precision NOT NULL, "
                "updated_at timestamptz DEFAULT now())"
            ))
            conn.execute(text("DELETE FROM imoex_weights"))
            conn.execute(
                text("INSERT INTO imoex_weights(ticker, weight) VALUES (:t, :w)"),
                [{"t": str(t), "w": float(w)} for t, w in weights.items()],
            )
    except Exception as e:
        logger.warning(f"persist IMOEX weights failed: {type(e).__name__}: {e}")


def _load_imoex_weights_fallback() -> dict:
    """Последние сохранённые веса IMOEX из БД (stale-on-error для карты рынка)."""
    try:
        with get_engine().connect() as conn:
            rows = conn.execute(text("SELECT ticker, weight FROM imoex_weights")).fetchall()
        return {r[0]: float(r[1]) for r in rows}
    except Exception:
        return {}


def build_stocks_heatmap(size_by: str, color_by: str, group_by: str):
    """Собирает (или достаёт из кеша) данные карты «все акции».

    Вынесено из роута, чтобы прогрев кеша (_warmup_cache) мог наполнить кеш
    напрямую, минуя tier-проверку (guest-клиент warmup'а получил бы 403).
    Сам tier-gating остаётся на роуте get_stocks_heatmap.
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
            sec_id, name, sector, price, prev_close,
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
            "prev_close": float(row[4]) if row[4] else 0,
            "change_1d": float(row[5]) if row[5] else 0,
            "change_1w": float(row[6]) if row[6] else 0,
            "change_1m": float(row[7]) if row[7] else 0,
            "change_1y": float(row[8]) if row[8] else 0,
            "volume_1d": float(row[9]) if row[9] else 0,
            "volume_1w": float(row[10]) if row[10] else 0,
            "volume_1m": float(row[11]) if row[11] else 0,
            "value_1d": float(row[12]) if row[12] else 0,
            "value_1w": float(row[13]) if row[13] else 0,
            "value_1m": float(row[14]) if row[14] else 0,
            "market_cap": float(row[15]) if row[15] else 0,
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

    from datetime import datetime, timezone, timedelta
    msk = timezone(timedelta(hours=3))
    response = {
        "stocks": stocks,
        "sectors": sectors_list,
        "params": {"size_by": size_by, "color_by": color_by, "group_by": group_by},
        "updated_at": datetime.now(msk).strftime("%H:%M"),
    }
    get_or_set(cache_key, response, ttl=300)  # 5 мин
    return response


@router.get("/stocks")
async def get_stocks_heatmap(
    size_by: HeatmapSizeByType = Query("value_1d", description="Размер блока"),
    color_by: HeatmapColorByType = Query("change_1d", description="Цвет блока"),
    group_by: HeatmapGroupByType = Query("sector", description="Группировка"),
    user = Depends(get_current_user_optional),
):
    """
    Возвращает данные для карты рынка из материализованного представления.
    Параметры валидируются автоматически через Literal типы.
    """
    # Free: только режим IMOEX (см. /imoex endpoint); /stocks — для Basic+
    from api.security.access_control import enforce_tier_limits
    enforce_tier_limits(user, "heatmap", mode="all")
    return build_stocks_heatmap(size_by, color_by, group_by)


@router.get("/prices")
async def get_heatmap_prices():
    """Только текущие цены акций — lightweight endpoint для real-time обновления."""
    from api.cache import get_or_set

    cache_key = "heatmap:prices"
    cached = get_or_set(cache_key)
    if cached is not None:
        return cached

    # LATERAL по списку акций из mv_heatmap_stocks (96 строк) вместо
    # DISTINCT ON по всей таблице 5-мин свечей. Прежний вариант с
    # `begin_time::date = CURRENT_DATE` (и даже range-предикат) читал ВСЕ
    # сегодняшние бары всех акций (~10k строк) с heap-fetch по холодным
    # страницам → 45с и регулярные 504. Здесь — 96 индексных seek'ов по
    # idx_candles_stock_daily_secid_time, по одному на акцию: ~50мс.
    # Семантика та же: акция без свечи за сегодня просто отсутствует в ответе.
    engine = get_engine()
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT m.sec_id, c.close
            FROM mv_heatmap_stocks m
            CROSS JOIN LATERAL (
                SELECT close FROM candles
                WHERE secid = m.sec_id AND type = 'stock' AND interval = 5
                  AND begin_time >= date_trunc('day', now())
                  AND close > 0
                ORDER BY begin_time DESC
                LIMIT 1
            ) c
        """)).fetchall()

    result = {row[0]: round(float(row[1]), 2) for row in rows}
    get_or_set(cache_key, result, ttl=30)  # кэш 30 сек (не 5 мин)
    return result


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
        # ISS может быть недоступен (блок сети MOEX при просрочке Algopack-подписки,
        # тех.работы ISS) → карта НЕ должна падать 500. Пробуем ISS; при ошибке —
        # последние веса из БД (imoex_weights). 503 только если и БД-фоллбэк пуст.
        try:
            async with httpx.AsyncClient(timeout=5) as client:
                resp = await client.get(IMOEX_ISS_URL)
                resp.raise_for_status()
                data = resp.json()
            cols = data["analytics"]["columns"]
            rows_data = data["analytics"]["data"]
            idx_ticker = cols.index("ticker")
            idx_weight = cols.index("weight")
            weights = {row[idx_ticker]: float(row[idx_weight]) for row in rows_data}
            get_or_set(weights_cache_key, weights, ttl=3600)
            _persist_imoex_weights(weights)
        except Exception as e:
            weights = _load_imoex_weights_fallback()
            if not weights:
                raise HTTPException(status_code=503, detail="IMOEX веса временно недоступны")
            # Кэшируем фоллбэк-веса на 10 мин: пока сеть до MOEX закрыта, не дёргаем
            # ISS (и не ждём таймаут) на каждом запросе. Через 10 мин — повторная
            # проба ISS, подхватит свежие веса как только MOEX вернёт доступ.
            get_or_set(weights_cache_key, weights, ttl=600)
            logger.warning(
                f"IMOEX weights via ISS failed ({type(e).__name__}) — fallback на БД "
                f"({len(weights)} бумаг)"
            )

    # Берём данные акций из mv_heatmap_stocks
    engine = get_engine()
    query = text("""
        SELECT sec_id, name, sector, price, prev_close,
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
            "prev_close": float(row[4]) if row[4] else 0,
            "change_1d": float(row[5]) if row[5] else 0,
            "change_1w": float(row[6]) if row[6] else 0,
            "change_1m": float(row[7]) if row[7] else 0,
            "change_1y": float(row[8]) if row[8] else 0,
            "volume_1d": float(row[9]) if row[9] else 0,
            "volume_1w": float(row[10]) if row[10] else 0,
            "volume_1m": float(row[11]) if row[11] else 0,
            "value_1d": float(row[12]) if row[12] else 0,
            "value_1w": float(row[13]) if row[13] else 0,
            "value_1m": float(row[14]) if row[14] else 0,
            "market_cap": float(row[15]) if row[15] else 0,
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

    from datetime import datetime, timezone, timedelta
    msk = timezone(timedelta(hours=3))
    response = {
        "stocks": stocks,
        "sectors": sectors_list,
        "params": {"size_by": "weight", "color_by": color_by, "group_by": group_by},
        "updated_at": datetime.now(msk).strftime("%H:%M"),
    }
    get_or_set(cache_key, response, ttl=300)
    return response