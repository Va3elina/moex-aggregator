"""
Public API v1 — programmatic access для Pro юзеров через API key.

Все endpoints под /api/v1/public/* и требуют X-API-Key header.
Возвращают JSON (не CSV — это для programmatic use).

Endpoints:
  GET /api/v1/public/heatmap
  GET /api/v1/public/breadth/current?ema=200&universe=imoex
  GET /api/v1/public/instruments
  GET /api/v1/public/oi/current?instrument=SR&clgroup=YUR
  GET /api/v1/public/funds/categories
  GET /api/v1/public/seasonality?ticker=SBER

Rate-limit: 60 req/min per API key (через Redis-counter).
Поверх — общий nginx rate-limit (30 r/s per IP) и FastAPI middleware.
"""
import time
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from api.cache import _get_redis as get_redis  # internal but stable alias
from api.database import get_db
from api.models import User
from api.security.api_key import get_user_by_api_key

router = APIRouter(prefix="/api/v1/public", tags=["public-api"])


# Rate-limit: 60 запросов/мин per API key. Защита поверх nginx (30 r/s per IP)
# чтобы один компрометированный ключ не пожрал бюджет всего IP.
RATE_LIMIT_PER_MIN = 60


def check_api_rate_limit(user: User):
    """
    Проверяет rate-limit для API key. Используется как inline-call в каждом
    public endpoint после auth. Бросает 429 при превышении.

    Реализация: Redis counter `apikey:rate:{user_id}:{minute}` с TTL 60s.
    Технически лимит per-USER, но юзеры обычно держат один активный ключ —
    в практике это per-key. Если надо строго per-key — нужно прокидывать
    api_key.id из dependency.
    """
    redis = get_redis()
    if not redis:
        return  # Redis недоступен — skip (graceful)
    minute_bucket = int(time.time() // 60)
    key = f"apikey:rate:{user.id}:{minute_bucket}"
    try:
        # INCR atomic — returns post-increment value.
        count = redis.incr(key)
        # Set TTL только на первый increment (после `expire` no-op).
        if count == 1:
            redis.expire(key, 70)  # 70s — небольшой запас на boundary
        if count > RATE_LIMIT_PER_MIN:
            raise HTTPException(
                status_code=429,
                detail=f"Rate limit exceeded ({RATE_LIMIT_PER_MIN} req/min per API key)",
                headers={"Retry-After": "60"},
            )
    except HTTPException:
        raise
    except Exception:
        # Redis ошибка — не блокируем юзера. Лучше пропустить чем 500.
        pass


# ════════════════════════════════════════════════════════════════════
# Heatmap — snapshot всех акций
# ════════════════════════════════════════════════════════════════════
@router.get("/heatmap")
def public_heatmap(
    user: User = Depends(get_user_by_api_key),
    db: Session = Depends(get_db),
):
    """
    Snapshot всех акций с current price + change % + market cap.
    Source: mv_heatmap_stocks (refreshed daily orchestrator'ом).
    """
    check_api_rate_limit(user)
    rows = db.execute(text("""
        SELECT sec_id, name, sector, price, prev_close,
               change_1d, change_1w, change_1m, change_1y,
               volume_1d, value_1d, market_cap
        FROM mv_heatmap_stocks
        ORDER BY market_cap DESC NULLS LAST
    """)).mappings().all()
    return {
        "data": [dict(r) for r in rows],
        "meta": {
            "count": len(rows),
            "source": "MOEX (via Frame)",
            "as_of": datetime.utcnow().isoformat() + "Z",
        },
    }


# ════════════════════════════════════════════════════════════════════
# Strength — текущий снапшот % акций выше EMA
# ════════════════════════════════════════════════════════════════════
@router.get("/breadth/current")
def public_breadth_current(
    ema: int = Query(200, ge=10, le=500),
    universe: str = Query("imoex"),
    user: User = Depends(get_user_by_api_key),
    db: Session = Depends(get_db),
):
    check_api_rate_limit(user)
    if universe not in ("all", "imoex", "all_usd", "imoex_usd"):
        raise HTTPException(status_code=400, detail="universe invalid")

    row = db.execute(text("""
        SELECT trade_date, percent_above, count_above, count_total
        FROM breadth_history
        WHERE ema_period = :ema AND universe = :universe
        ORDER BY trade_date DESC LIMIT 1
    """), {"ema": ema, "universe": universe}).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="data not found")
    return {"data": dict(row), "ema_period": ema, "universe": universe}


# ════════════════════════════════════════════════════════════════════
# Instruments — список всех инструментов в БД
# ════════════════════════════════════════════════════════════════════
@router.get("/instruments")
def public_instruments(
    type: str | None = Query(None, description="stock|futures"),
    user: User = Depends(get_user_by_api_key),
    db: Session = Depends(get_db),
):
    check_api_rate_limit(user)
    if type and type not in ("stock", "futures"):
        raise HTTPException(status_code=400, detail="type invalid")
    where = "WHERE type = :type" if type else ""
    params = {"type": type} if type else {}
    rows = db.execute(text(f"""
        SELECT sec_id, sectype, name, type, "group"
        FROM instruments
        {where}
        ORDER BY sectype
    """), params).mappings().all()
    return {"data": [dict(r) for r in rows], "count": len(rows)}


# ════════════════════════════════════════════════════════════════════
# OI — last open interest snapshot
# ════════════════════════════════════════════════════════════════════
@router.get("/oi/current")
def public_oi_current(
    instrument: str = Query(...),
    clgroup: str = Query("YUR"),
    user: User = Depends(get_user_by_api_key),
    db: Session = Depends(get_db),
):
    check_api_rate_limit(user)
    if clgroup not in ("YUR", "FIZ"):
        raise HTTPException(status_code=400, detail="clgroup invalid")
    row = db.execute(text("""
        SELECT tradedate, tradetime, pos AS open_interest,
               pos_long, pos_short, pos_long_num, pos_short_num
        FROM open_interest
        WHERE sectype = :inst AND clgroup = :cl AND interval = 24
        ORDER BY tradedate DESC, tradetime DESC
        LIMIT 1
    """), {"inst": instrument.upper(), "cl": clgroup}).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="data not found")
    return {"data": dict(row), "instrument": instrument.upper(), "clgroup": clgroup}


# ════════════════════════════════════════════════════════════════════
# Funds — категории + фонды
# ════════════════════════════════════════════════════════════════════
@router.get("/funds/categories")
def public_funds_categories(
    user: User = Depends(get_user_by_api_key),
    db: Session = Depends(get_db),
):
    check_api_rate_limit(user)
    rows = db.execute(text("""
        SELECT ticker, name, category, subcategory
        FROM funds
        ORDER BY category, ticker
    """)).mappings().all()
    return {"data": [dict(r) for r in rows], "count": len(rows)}


# ════════════════════════════════════════════════════════════════════
# Seasonality — daily candles для ticker (для self-calc)
# ════════════════════════════════════════════════════════════════════
@router.get("/seasonality")
def public_seasonality(
    ticker: str = Query(..., min_length=1, max_length=20),
    days: int = Query(3650, ge=30, le=15000, description="Глубина истории"),
    user: User = Depends(get_user_by_api_key),
    db: Session = Depends(get_db),
):
    check_api_rate_limit(user)
    from datetime import date, timedelta
    threshold = date.today() - timedelta(days=days)

    rows = db.execute(text("""
        SELECT begin_time::date AS trade_date,
               open, high, low, close, volume
        FROM candles
        WHERE secid = :ticker AND interval = 24 AND type = 'stock'
          AND close > 0 AND begin_time >= :threshold
        ORDER BY begin_time ASC
    """), {"ticker": ticker.upper(), "threshold": threshold}).mappings().all()
    if not rows:
        raise HTTPException(status_code=404, detail=f"data not found for {ticker}")
    return {"data": [dict(r) for r in rows], "ticker": ticker.upper(), "count": len(rows)}
