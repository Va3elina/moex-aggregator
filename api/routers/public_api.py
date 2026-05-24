"""
Public API v1 — programmatic access для Pro юзеров через API key.

Все endpoints под /api/v1/public/* и требуют X-API-Key header.
Возвращают JSON (не CSV — это для programmatic use).

Endpoints (catalog):
  ── Снимки текущего состояния ──
  GET /api/v1/public/heatmap                      — карта рынка (все акции)
  GET /api/v1/public/stocks/{ticker}/quote        — одна акция (price+change)
  GET /api/v1/public/instruments                  — справочник тикеров
  GET /api/v1/public/breadth/current              — Сила рынка (current)
  GET /api/v1/public/oi/current                   — Открытый интерес (current)

  ── Историчные данные ──
  GET /api/v1/public/breadth/history              — Сила рынка (history)
  GET /api/v1/public/oi/history                   — Открытый интерес (history)
  GET /api/v1/public/seasonality                  — Свечи акций (OHLCV daily)

  ── Индикаторы Баффетта ──
  GET /api/v1/public/buffett/cap-gdp              — Кап / ВВП
  GET /api/v1/public/buffett/cap-m2               — Кап / M2

  ── Фонды (БПИФ) ──
  GET /api/v1/public/funds/categories             — список фондов с категориями
  GET /api/v1/public/funds/{ticker}               — детали фонда (NAV+pay+returns+holdings)
  GET /api/v1/public/funds/{ticker}/history       — NAV/pay daily history

  ── Потоки ЦБ ──
  GET /api/v1/public/cbr-flows                    — ОРФР (stocks/ofz/fx)

Rate-limit: 60 req/min per API key (через Redis-counter).
Поверх — общий nginx rate-limit (30 r/s per IP) и FastAPI middleware.
"""
import time
from datetime import datetime, date, timedelta

from fastapi import APIRouter, Depends, HTTPException, Path, Query, Request, Response
from sqlalchemy import text
from sqlalchemy.orm import Session

from api.cache import _get_redis as get_redis  # internal but stable alias
from api.database import get_db
from api.models import User
from api.security.api_key import get_user_by_api_key
from api.utils.csv_export import csv_streaming_response

router = APIRouter(prefix="/api/v1/public", tags=["public-api"])


# Rate-limit: 60 запросов/мин per API key. Защита поверх nginx (30 r/s per IP)
# чтобы один компрометированный ключ не пожрал бюджет всего IP.
RATE_LIMIT_PER_MIN = 60


def check_rate_limit(
    response: Response,
    user: User = Depends(get_user_by_api_key),
) -> User:
    """
    Combined dependency: auth via API key + rate-limit check + headers.

    Возвращает `User` чтобы endpoint мог использовать это как drop-in
    замену `get_user_by_api_key`. Дополнительно устанавливает три
    стандартных rate-limit заголовка:
      - X-RateLimit-Limit:    суммарный лимит за окно (60).
      - X-RateLimit-Remaining: сколько запросов осталось в текущей минуте.
      - X-RateLimit-Reset:    unix timestamp когда окно сбросится.

    Реализация: Redis counter `apikey:rate:{user_id}:{minute}` с TTL 70s
    (минута + небольшой запас на boundary effect). INCR atomic, поэтому
    параллельные запросы не теряются. Если Redis недоступен — graceful:
    не блокируем юзера и не выставляем headers (вместо 500).
    """
    now = int(time.time())
    minute_bucket = now // 60
    reset_at = (minute_bucket + 1) * 60  # начало следующей минуты

    # Дефолтные headers — даже если Redis упадёт, клиент увидит лимит.
    response.headers["X-RateLimit-Limit"] = str(RATE_LIMIT_PER_MIN)
    response.headers["X-RateLimit-Reset"] = str(reset_at)

    redis = get_redis()
    if not redis:
        # Redis выключен — лимит неизвестен, но не блокируем.
        response.headers["X-RateLimit-Remaining"] = str(RATE_LIMIT_PER_MIN)
        return user

    key = f"apikey:rate:{user.id}:{minute_bucket}"
    try:
        # INCR atomic — returns post-increment value.
        count = redis.incr(key)
        if count == 1:
            redis.expire(key, 70)
        remaining = max(0, RATE_LIMIT_PER_MIN - count)
        response.headers["X-RateLimit-Remaining"] = str(remaining)
        if count > RATE_LIMIT_PER_MIN:
            raise HTTPException(
                status_code=429,
                detail=f"Rate limit exceeded ({RATE_LIMIT_PER_MIN} req/min per API key)",
                headers={
                    "Retry-After": str(reset_at - now),
                    "X-RateLimit-Limit": str(RATE_LIMIT_PER_MIN),
                    "X-RateLimit-Remaining": "0",
                    "X-RateLimit-Reset": str(reset_at),
                },
            )
    except HTTPException:
        raise
    except Exception:
        # Redis ошибка — graceful: пропускаем без блокировки.
        response.headers["X-RateLimit-Remaining"] = str(RATE_LIMIT_PER_MIN)
    return user


# ════════════════════════════════════════════════════════════════════
# Health — проверка работоспособности (без auth)
# ════════════════════════════════════════════════════════════════════
@router.get("/health")
def public_health():
    """
    Health-check для UptimeRobot / Pingdom / других мониторингов.
    Не требует API-ключа — это публичная проверка что v1 endpoint'ы
    отвечают. Возвращает версию API и timestamp.
    """
    return {
        "status": "ok",
        "version": "v1",
        "as_of": datetime.utcnow().isoformat() + "Z",
    }


# ════════════════════════════════════════════════════════════════════
# OpenAPI export — машиночитаемая спека для Postman / openapi-codegen
# ════════════════════════════════════════════════════════════════════
@router.get("/openapi.json")
def public_openapi(request: Request):
    """
    OpenAPI 3.0 спека только для /api/v1/public/* endpoint'ов.
    Импортируйте URL в Postman / Insomnia или скормите openapi-codegen
    для авто-генерации клиента на любом языке.

    Зачем фильтруем: главный openapi содержит ВСЕ роуты включая
    internal-only (/api/admin/*, /api/auth/*) — не хотим утечки
    приватной структуры наружу.
    """
    full = request.app.openapi()
    public_paths = {
        path: spec for path, spec in full.get("paths", {}).items()
        if path.startswith("/api/v1/public")
    }
    return {
        "openapi": full.get("openapi", "3.1.0"),
        "info": {
            "title": "Frame Public API",
            "description": "REST API для Pro юзеров. Аутентификация через X-API-Key.",
            "version": "1.0.0",
            "contact": {"email": "frameinfo@mail.ru"},
        },
        "servers": [{"url": "https://xn--80aklbnczmv.xn--p1ai", "description": "Production"}],
        "paths": public_paths,
        "components": {
            **full.get("components", {}),
            "securitySchemes": {
                "ApiKeyAuth": {
                    "type": "apiKey",
                    "in": "header",
                    "name": "X-API-Key",
                },
            },
        },
        "security": [{"ApiKeyAuth": []}],
    }


# ════════════════════════════════════════════════════════════════════
# Heatmap — snapshot всех акций
# ════════════════════════════════════════════════════════════════════
@router.get("/heatmap")
def public_heatmap(
    user: User = Depends(check_rate_limit),
    db: Session = Depends(get_db),
):
    """
    Snapshot всех акций с current price + change % + market cap.
    Source: mv_heatmap_stocks (refreshed daily orchestrator'ом).
    """
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
    user: User = Depends(check_rate_limit),
    db: Session = Depends(get_db),
):
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
    user: User = Depends(check_rate_limit),
    db: Session = Depends(get_db),
):
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
    user: User = Depends(check_rate_limit),
    db: Session = Depends(get_db),
):
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
    user: User = Depends(check_rate_limit),
    db: Session = Depends(get_db),
):
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
    format: str = Query("json", description="json | csv"),
    user: User = Depends(check_rate_limit),
    db: Session = Depends(get_db),
):
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
    if format == "csv":
        return csv_streaming_response(
            [dict(r) for r in rows],
            ["trade_date", "open", "high", "low", "close", "volume"],
            f"seasonality_{ticker.upper()}.csv",
        )
    return {"data": [dict(r) for r in rows], "ticker": ticker.upper(), "count": len(rows)}


# ════════════════════════════════════════════════════════════════════
# Stock quote — одна акция: current price + change
# ════════════════════════════════════════════════════════════════════
@router.get("/stocks/{ticker}/quote")
def public_stock_quote(
    ticker: str = Path(..., min_length=1, max_length=20),
    user: User = Depends(check_rate_limit),
    db: Session = Depends(get_db),
):
    """
    Текущая котировка одной акции. Это subset /heatmap по конкретному
    тикеру — удобно для бота который тащит цену по одной бумаге без
    overhead'а всей карты рынка.
    """
    row = db.execute(text("""
        SELECT sec_id, name, sector, price, prev_close,
               change_1d, change_1w, change_1m, change_1y,
               volume_1d, value_1d, market_cap
        FROM mv_heatmap_stocks
        WHERE sec_id = :ticker
    """), {"ticker": ticker.upper()}).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail=f"stock {ticker} not found")
    return {
        "data": dict(row),
        "meta": {
            "ticker": ticker.upper(),
            "source": "MOEX (via Frame)",
            "as_of": datetime.utcnow().isoformat() + "Z",
        },
    }


# ════════════════════════════════════════════════════════════════════
# Breadth — история Силы рынка
# ════════════════════════════════════════════════════════════════════
@router.get("/breadth/history")
def public_breadth_history(
    ema: int = Query(200, ge=10, le=500),
    universe: str = Query("imoex"),
    days: int = Query(365, ge=1, le=15000, description="Глубина истории в днях"),
    format: str = Query("json", description="json | csv"),
    user: User = Depends(check_rate_limit),
    db: Session = Depends(get_db),
):
    """
    История Силы рынка за `days` дней назад. Каждая точка — % акций
    торгующихся выше EMA на эту дату.
    """
    if universe not in ("all", "imoex", "all_usd", "imoex_usd"):
        raise HTTPException(status_code=400, detail="universe invalid")

    threshold = date.today() - timedelta(days=days)
    rows = db.execute(text("""
        SELECT trade_date, percent_above, count_above, count_total
        FROM breadth_history
        WHERE ema_period = :ema AND universe = :universe
          AND trade_date >= :threshold
        ORDER BY trade_date ASC
    """), {"ema": ema, "universe": universe, "threshold": threshold}).mappings().all()
    if not rows:
        raise HTTPException(status_code=404, detail="data not found")
    if format == "csv":
        return csv_streaming_response(
            [dict(r) for r in rows],
            ["trade_date", "percent_above", "count_above", "count_total"],
            f"breadth_history_{universe}_ema{ema}.csv",
        )
    return {
        "data": [dict(r) for r in rows],
        "ema_period": ema,
        "universe": universe,
        "count": len(rows),
    }


# ════════════════════════════════════════════════════════════════════
# OI — история открытого интереса
# ════════════════════════════════════════════════════════════════════
@router.get("/oi/history")
def public_oi_history(
    instrument: str = Query(..., description="Sectype фьючерса (SR, GZ, MX, BR, ...)"),
    clgroup: str = Query("YUR", description="YUR (юрлица) или FIZ (физлица)"),
    days: int = Query(365, ge=1, le=3650),
    format: str = Query("json", description="json | csv"),
    user: User = Depends(check_rate_limit),
    db: Session = Depends(get_db),
):
    """
    История открытого интереса по фьючерсу за `days` дней назад.
    Только daily (interval=24) — internal intraday данные слишком тяжёлые
    для programmatic access.
    """
    if clgroup not in ("YUR", "FIZ"):
        raise HTTPException(status_code=400, detail="clgroup invalid")
    threshold = date.today() - timedelta(days=days)
    rows = db.execute(text("""
        SELECT tradedate AS trade_date,
               tradetime AS trade_time,
               pos AS open_interest,
               pos_long, pos_short, pos_long_num, pos_short_num
        FROM open_interest
        WHERE sectype = :inst AND clgroup = :cl AND interval = 24
          AND tradedate >= :threshold
        ORDER BY tradedate ASC, tradetime ASC
    """), {
        "inst": instrument.upper(),
        "cl": clgroup,
        "threshold": threshold,
    }).mappings().all()
    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"OI data not found for {instrument}/{clgroup}",
        )
    if format == "csv":
        return csv_streaming_response(
            [dict(r) for r in rows],
            ["trade_date", "trade_time", "open_interest", "pos_long",
             "pos_short", "pos_long_num", "pos_short_num"],
            f"oi_{instrument.upper()}_{clgroup}.csv",
        )
    return {
        "data": [dict(r) for r in rows],
        "instrument": instrument.upper(),
        "clgroup": clgroup,
        "count": len(rows),
    }


# ════════════════════════════════════════════════════════════════════
# Buffett — Капитализация / ВВП
# ════════════════════════════════════════════════════════════════════
@router.get("/buffett/cap-gdp")
def public_buffett_cap_gdp(
    days: int = Query(3650, ge=30, le=15000, description="Глубина истории, дни"),
    format: str = Query("json", description="json | csv"),
    user: User = Depends(check_rate_limit),
    db: Session = Depends(get_db),
):
    """
    Индикатор Баффетта: рыночная капитализация / ВВП TTM × 100.
    Источники: capitalization из MOEX, GDP quarterly из Росстата
    (TTM = сумма последних 4 кварталов forward-fill'нутые на каждую
    дату capitalization).
    """
    threshold = date.today() - timedelta(days=days)
    rows = db.execute(text("""
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
          WHERE c.dt >= :threshold
        )
        SELECT trade_date, market_cap, gdp_ttm,
               CASE WHEN gdp_ttm > 0
                    THEN ROUND((market_cap / gdp_ttm * 100)::numeric, 4)
                    ELSE NULL END AS buffett_ratio_pct
        FROM joined
        ORDER BY trade_date ASC
    """), {"threshold": threshold}).mappings().all()
    if not rows:
        raise HTTPException(status_code=404, detail="buffett data not found")
    if format == "csv":
        return csv_streaming_response(
            [dict(r) for r in rows],
            ["trade_date", "market_cap", "gdp_ttm", "buffett_ratio_pct"],
            "buffett_cap_gdp.csv",
        )
    return {
        "data": [dict(r) for r in rows],
        "meta": {
            "indicator": "cap-gdp",
            "unit": "ratio_pct",
            "source_cap": "MOEX",
            "source_gdp": "Росстат (TTM forward-fill)",
            "count": len(rows),
        },
    }


# ════════════════════════════════════════════════════════════════════
# Buffett — Капитализация / M2
# ════════════════════════════════════════════════════════════════════
@router.get("/buffett/cap-m2")
def public_buffett_cap_m2(
    days: int = Query(3650, ge=30, le=15000),
    format: str = Query("json", description="json | csv"),
    user: User = Depends(check_rate_limit),
    db: Session = Depends(get_db),
):
    """
    Кап / M2 — отношение рыночной капитализации к денежной массе M2.
    M2 — данные ЦБ (monthly), forward-fill'нутые до каждой даты cap.
    """
    threshold = date.today() - timedelta(days=days)
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
          WHERE c.dt >= :threshold
        )
        SELECT trade_date, market_cap, m2,
               CASE WHEN m2 > 0
                    THEN ROUND((market_cap / m2)::numeric, 6)
                    ELSE NULL END AS cap_m2_ratio
        FROM joined
        ORDER BY trade_date ASC
    """), {"threshold": threshold}).mappings().all()
    if not rows:
        raise HTTPException(status_code=404, detail="buffett m2 data not found")
    if format == "csv":
        return csv_streaming_response(
            [dict(r) for r in rows],
            ["trade_date", "market_cap", "m2", "cap_m2_ratio"],
            "buffett_cap_m2.csv",
        )
    return {
        "data": [dict(r) for r in rows],
        "meta": {
            "indicator": "cap-m2",
            "unit": "ratio",
            "source_cap": "MOEX",
            "source_m2": "ЦБ РФ (monthly forward-fill)",
            "count": len(rows),
        },
    }


# ════════════════════════════════════════════════════════════════════
# Fund detail — конкретный фонд с метриками и составом
# ════════════════════════════════════════════════════════════════════
@router.get("/funds/{ticker}")
def public_fund_detail(
    ticker: str = Path(..., min_length=1, max_length=20),
    user: User = Depends(check_rate_limit),
    db: Session = Depends(get_db),
):
    """
    Детали одного фонда:
      - Метаданные: ticker, name, category, subcategory, uk_id
      - Текущие NAV и pay (last available)
      - Доходность за 1m / 3m / 6m / 1y (по pay)
      - Топ-5 holdings (по weight)

    Возвращает 404 если ticker не найден.
    """
    row = db.execute(text("""
        SELECT f.fund_id, f.ticker, f.name, f.category, f.subcategory, f.uk_id,
            fd_last.nav AS last_nav, fd_last.pay AS last_pay,
            fd_last.trade_date AS last_date,
            fd_1m.pay AS pay_1m, fd_3m.pay AS pay_3m,
            fd_6m.pay AS pay_6m, fd_1y.pay AS pay_1y
        FROM funds f
        LEFT JOIN LATERAL (
            SELECT nav, pay, trade_date FROM fund_data
            WHERE fund_id = f.fund_id AND nav IS NOT NULL
            ORDER BY trade_date DESC LIMIT 1
        ) fd_last ON true
        LEFT JOIN LATERAL (
            SELECT pay FROM fund_data
            WHERE fund_id = f.fund_id AND pay IS NOT NULL
              AND trade_date <= :d_1m
            ORDER BY trade_date DESC LIMIT 1
        ) fd_1m ON true
        LEFT JOIN LATERAL (
            SELECT pay FROM fund_data
            WHERE fund_id = f.fund_id AND pay IS NOT NULL
              AND trade_date <= :d_3m
            ORDER BY trade_date DESC LIMIT 1
        ) fd_3m ON true
        LEFT JOIN LATERAL (
            SELECT pay FROM fund_data
            WHERE fund_id = f.fund_id AND pay IS NOT NULL
              AND trade_date <= :d_6m
            ORDER BY trade_date DESC LIMIT 1
        ) fd_6m ON true
        LEFT JOIN LATERAL (
            SELECT pay FROM fund_data
            WHERE fund_id = f.fund_id AND pay IS NOT NULL
              AND trade_date <= :d_1y
            ORDER BY trade_date DESC LIMIT 1
        ) fd_1y ON true
        WHERE UPPER(f.ticker) = UPPER(:ticker)
    """), {
        "ticker": ticker,
        "d_1m": date.today() - timedelta(days=30),
        "d_3m": date.today() - timedelta(days=90),
        "d_6m": date.today() - timedelta(days=180),
        "d_1y": date.today() - timedelta(days=365),
    }).mappings().first()

    if not row:
        raise HTTPException(status_code=404, detail=f"fund {ticker} not found")

    def calc_return(last, prev):
        if last and prev and float(prev) > 0:
            return round((float(last) - float(prev)) / float(prev) * 100, 2)
        return None

    last_pay = float(row["last_pay"]) if row["last_pay"] is not None else None
    last_nav = float(row["last_nav"]) if row["last_nav"] is not None else None

    # Top-5 holdings
    holdings_rows = db.execute(text("""
        SELECT asset_name, weight FROM fund_holdings
        WHERE fund_id = :fid
        ORDER BY weight DESC
        LIMIT 5
    """), {"fid": row["fund_id"]}).fetchall()
    top_holdings = [
        {"name": h[0], "weight": float(h[1])}
        for h in holdings_rows
    ]

    return {
        "data": {
            "fund_id": row["fund_id"],
            "ticker": row["ticker"],
            "name": row["name"],
            "category": row["category"],
            "subcategory": row["subcategory"],
            "uk_id": row["uk_id"],
            "last_nav": last_nav,
            "last_pay": last_pay,
            "last_date": row["last_date"].isoformat() if row["last_date"] else None,
            "return_1m": calc_return(last_pay, row["pay_1m"]),
            "return_3m": calc_return(last_pay, row["pay_3m"]),
            "return_6m": calc_return(last_pay, row["pay_6m"]),
            "return_1y": calc_return(last_pay, row["pay_1y"]),
            "top_holdings": top_holdings,
        }
    }


# ════════════════════════════════════════════════════════════════════
# Fund history — daily NAV/pay history
# ════════════════════════════════════════════════════════════════════
@router.get("/funds/{ticker}/history")
def public_fund_history(
    ticker: str = Path(..., min_length=1, max_length=20),
    days: int = Query(365, ge=1, le=3650),
    format: str = Query("json", description="json | csv"),
    user: User = Depends(check_rate_limit),
    db: Session = Depends(get_db),
):
    """
    Daily NAV + pay history фонда за `days` дней назад.
    На основе этого можно посчитать доходность, sharpe, любые
    кастомные метрики и притоки/оттоки.
    """
    threshold = date.today() - timedelta(days=days)
    rows = db.execute(text("""
        SELECT fd.trade_date, fd.nav, fd.pay
        FROM fund_data fd
        JOIN funds f ON f.fund_id = fd.fund_id
        WHERE UPPER(f.ticker) = UPPER(:ticker)
          AND fd.trade_date >= :threshold
          AND (fd.nav IS NOT NULL OR fd.pay IS NOT NULL)
        ORDER BY fd.trade_date ASC
    """), {"ticker": ticker, "threshold": threshold}).mappings().all()
    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"fund history not found for {ticker}",
        )
    # nav/pay могут быть Decimal — приведём к float для JSON-сериализации.
    data = []
    for r in rows:
        data.append({
            "trade_date": r["trade_date"],
            "nav": float(r["nav"]) if r["nav"] is not None else None,
            "pay": float(r["pay"]) if r["pay"] is not None else None,
        })
    if format == "csv":
        return csv_streaming_response(
            data,
            ["trade_date", "nav", "pay"],
            f"fund_{ticker.upper()}_history.csv",
        )
    return {
        "data": data,
        "ticker": ticker.upper(),
        "count": len(data),
    }


# ════════════════════════════════════════════════════════════════════
# CBR Flows — ОРФР данные
# ════════════════════════════════════════════════════════════════════
@router.get("/cbr-flows")
def public_cbr_flows(
    type: str = Query("stocks", description="stocks | ofz | fx"),
    years: int = Query(10, ge=1, le=30, description="Глубина истории в годах"),
    format: str = Query("json", description="json | csv"),
    user: User = Depends(check_rate_limit),
    db: Session = Depends(get_db),
):
    """
    Данные ОРФР ЦБ по потокам участников биржевых торгов.
    Типы:
      - `stocks` — акции
      - `ofz`    — ОФЗ
      - `fx`     — валюты

    Категории участников (`category` в ответе): retail / banks / non_residents /
    investment_funds / corporates / brokers / others — конкретный набор
    зависит от `type` и периода (ЦБ может менять состав отчётности).
    """
    if type not in ("stocks", "ofz", "fx"):
        raise HTTPException(status_code=400, detail="type invalid")

    rows = db.execute(text("""
        SELECT period_year, period_label, period_kind, period_end_date,
               category, value
        FROM cbr_flows
        WHERE instrument_type = :it
          AND period_year >= EXTRACT(YEAR FROM CURRENT_DATE)::int - :years
        ORDER BY period_end_date ASC, category ASC
    """), {"it": type, "years": years}).mappings().all()
    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"CBR flows not found for {type}",
        )
    if format == "csv":
        return csv_streaming_response(
            [dict(r) for r in rows],
            ["period_year", "period_label", "period_kind",
             "period_end_date", "category", "value"],
            f"cbr_flows_{type}.csv",
        )
    return {
        "data": [dict(r) for r in rows],
        "instrument_type": type,
        "years": years,
        "unit": "млрд руб.",
        "count": len(rows),
    }
