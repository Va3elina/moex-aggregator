"""
API для карты рынка (Heatmap) — стиль TradingView
С валидацией входных данных

Две версии цены (см. services/session_close):
  • публичная — цена только на закрытие 19:00: плитки красятся изменением
    «закрытие к закрытию», карта меняется раз в день после 19:10;
  • админская — прежняя, из mv_heatmap_stocks (5-минутки с задержкой 15 минут).
"""
import httpx
import logging
from datetime import date

from fastapi import APIRouter, Query, HTTPException, Depends, Request
from sqlalchemy import text

from api.database import get_engine
from api.routers.auth import require_admin, get_current_user_optional
from api.services.market_delay import cutoff_for_interval
from api.services.session_close import (is_live_viewer, view_tag, published_end,
                                        published_next_day)
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
            # ⚠️ Идемпотентный upsert, НЕ DELETE+INSERT: писать сюда могут два
            # запроса разом (прогрев кеша и живой /heatmap/imoex — ровно так
            # поймали 30.08: «duplicate key ... (ticker)=(AFKS)»). Обе
            # транзакции удаляли каждая в своём снимке и вставляли один и тот
            # же набор тикеров, второй падал на первичном ключе.
            conn.execute(
                text("INSERT INTO imoex_weights(ticker, weight, updated_at) "
                     "VALUES (:t, :w, now()) "
                     "ON CONFLICT (ticker) DO UPDATE "
                     "SET weight = EXCLUDED.weight, updated_at = now()"),
                [{"t": str(t), "w": float(w)} for t, w in weights.items()],
            )
            # Выбывшие из индекса подчищаем тем же заходом — иначе fallback
            # накапливал бы тикеры, которых в базе расчёта давно нет.
            conn.execute(
                text("DELETE FROM imoex_weights WHERE ticker <> ALL(:keep)"),
                {"keep": [str(t) for t in weights.keys()]},
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


def _heatmap_data_date(engine):
    """(data_date_iso, is_live) — дата данных карты и признак «идёт сессия».

    is_live=True, если за сегодня уже есть 5-мин свеча → data_date = сегодня.
    Иначе (выходной/праздник/до открытия) data_date = дата последней дневной
    свечи = последний торговый день. Нужно, чтобы фронт подписывал карту датой
    данных, а не текущим временем: на выходных «Обновлено в HH:MM» вводило в
    заблуждение (см. snap_date в db/mv_heatmap_stocks.sql — там та же логика).

    ⚠️ Индекс-дружественные предикаты обязательны: это sync-запрос внутри
    async-роутов, он блокирует event-loop на всё время выполнения. Прежний
    вариант `MAX(begin_time::date)` + `begin_time::date = CURRENT_DATE` не-sargable
    (functional predicate глушит индекс) → seq-scan по ~3.7М 5-мин баров с 1.6М
    heap-fetch'ей = до 75с на холодных страницах. Воркер зависал > gunicorn
    timeout(60s) → kill → 502/504-штормы (тот же класс бага, что чинили в /prices).
    Сейчас: LATERAL по 96 секциям mv_heatmap_stocks (точечные index-seek'и) для
    intraday + `MAX(begin_time)::date` (cast ПОСЛЕ агрегата) для daily = ~10мс."""
    try:
        with engine.connect() as conn:
            row = conn.execute(text("""
                SELECT
                  (SELECT MAX(c.begin_time)::date
                     FROM mv_heatmap_stocks m
                     CROSS JOIN LATERAL (
                       SELECT begin_time FROM candles
                       WHERE secid = m.sec_id AND type='stock' AND interval=5
                         AND begin_time >= date_trunc('day', now())
                       ORDER BY begin_time DESC LIMIT 1
                     ) c),
                  (SELECT MAX(begin_time)::date
                     FROM candles WHERE type='stock' AND interval=24)
            """)).fetchone()
        today_intraday, last_daily = row[0], row[1]
        if today_intraday is not None:
            return today_intraday.isoformat(), True
        return (last_daily.isoformat() if last_daily else None), False
    except Exception:
        return None, False


def _num(v) -> float:
    return float(v) if v else 0


def _mv_rows() -> list[dict]:
    """Акции карты из mv_heatmap_stocks — админская (незамедленная) версия."""
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
        with get_engine().connect() as conn:
            rows = conn.execute(query).fetchall()
    except Exception:
        raise HTTPException(status_code=500, detail="Ошибка получения данных heatmap")

    return [{
        "secId": row[0],
        "name": row[1],
        "sector": row[2],
        "price": _num(row[3]),
        "prev_close": _num(row[4]),
        "change_1d": _num(row[5]),
        "change_1w": _num(row[6]),
        "change_1m": _num(row[7]),
        "change_1y": _num(row[8]),
        "volume_1d": _num(row[9]),
        "volume_1w": _num(row[10]),
        "volume_1m": _num(row[11]),
        "value_1d": _num(row[12]),
        "value_1w": _num(row[13]),
        "value_1m": _num(row[14]),
        "market_cap": _num(row[15]),
    } for row in rows]


# Сплиты, по которым дневные свечи в БД остались сырыми (зеркало known_splits в
# db/mv_heatmap_stocks.sql): опорные цены 1н/1м/1г до даты сплита делим на ratio.
_KNOWN_SPLITS: dict[str, tuple[date, float]] = {
    "SFIN": (date(2025, 12, 25), 1.93),
}

# Публичная версия карты: цена — закрытие последней опубликованной сессии
# (последняя 5-минутка до 19:00, services/session_close), изменение за день —
# к закрытию предыдущей сессии. Опорные цены 1н/1м/1г — дневные свечи, как в
# mv_heatmap_stocks (5-минутная история у акций короткая и не пересчитана при
# сплитах). Нецелевые поля (оборот, капитализация, сектор) берём из той же
# матвьюхи: это не цена.
# Если 5-минуток у бумаги нет или они отстали от дневных свечей — цена
# последней опубликованной дневной свечи.
# Индексы: idx_candles_stock_daily_secid_time (type, interval, secid, begin_time DESC)
# — каждый LATERAL это спуск по индексу с конца, ~140 бумаг × 7 спусков.
_PUBLIC_ROWS_SQL = text("""
    WITH st AS (
        SELECT sec_id, name, sector,
               volume_1d, volume_1w, volume_1m,
               value_1d, value_1w, value_1m, market_cap
        FROM mv_heatmap_stocks
    ),
    s AS (
        SELECT st.*,
               CASE WHEN c5.d IS NOT NULL AND (dd.d IS NULL OR c5.d >= dd.d)
                    THEN c5.d ELSE dd.d END AS pd,
               CASE WHEN c5.d IS NOT NULL AND (dd.d IS NULL OR c5.d >= dd.d)
                    THEN c5.close ELSE dd.close END AS price
        FROM st
        LEFT JOIN LATERAL (
            SELECT begin_time::date AS d, close FROM candles
            WHERE type = 'stock' AND interval = 5 AND secid = st.sec_id
              AND begin_time < :pub_end AND begin_time::time < time '19:00'
              AND close > 0 AND volume > 0
            ORDER BY begin_time DESC LIMIT 1
        ) c5 ON true
        LEFT JOIN LATERAL (
            SELECT begin_time::date AS d, close FROM candles
            WHERE type = 'stock' AND interval = 24 AND secid = st.sec_id
              AND begin_time < :pub_next AND close > 0
            ORDER BY begin_time DESC LIMIT 1
        ) dd ON true
    )
    SELECT s.sec_id, s.name, s.sector, s.price, s.pd,
           COALESCE(p5.close, pdd.close) AS prev_close,
           w.close AS p1w, w.d AS d1w,
           m.close AS p1m, m.d AS d1m,
           y.close AS p1y, y.d AS d1y,
           s.volume_1d, s.volume_1w, s.volume_1m,
           s.value_1d, s.value_1w, s.value_1m, s.market_cap
    FROM s
    LEFT JOIN LATERAL (
        SELECT close FROM candles
        WHERE type = 'stock' AND interval = 5 AND secid = s.sec_id
          AND begin_time < s.pd AND begin_time::time < time '19:00'
          AND close > 0 AND volume > 0
        ORDER BY begin_time DESC LIMIT 1
    ) p5 ON true
    LEFT JOIN LATERAL (
        SELECT close FROM candles
        WHERE type = 'stock' AND interval = 24 AND secid = s.sec_id
          AND begin_time < s.pd AND close > 0
        ORDER BY begin_time DESC LIMIT 1
    ) pdd ON true
    LEFT JOIN LATERAL (
        SELECT begin_time::date AS d, close FROM candles
        WHERE type = 'stock' AND interval = 24 AND secid = s.sec_id
          AND begin_time <= s.pd - 7 AND begin_time >= s.pd - 10 AND close > 0
        ORDER BY begin_time DESC LIMIT 1
    ) w ON true
    LEFT JOIN LATERAL (
        SELECT begin_time::date AS d, close FROM candles
        WHERE type = 'stock' AND interval = 24 AND secid = s.sec_id
          AND begin_time <= s.pd - 30 AND begin_time >= s.pd - 35 AND close > 0
        ORDER BY begin_time DESC LIMIT 1
    ) m ON true
    LEFT JOIN LATERAL (
        SELECT begin_time::date AS d, close FROM candles
        WHERE type = 'stock' AND interval = 24 AND secid = s.sec_id
          AND begin_time <= s.pd - 365 AND begin_time >= s.pd - 379 AND close > 0
        ORDER BY begin_time DESC LIMIT 1
    ) y ON true
    WHERE s.price IS NOT NULL
""")


def _pct(price: float, ref: float | None) -> float:
    if not price or not ref or ref <= 0:
        return 0
    return round((price - ref) / ref * 100, 2)


def _public_rows() -> list[dict]:
    """Акции карты с ценой только на закрытие 19:00 — версия для всех, кроме админов."""
    try:
        with get_engine().connect() as conn:
            rows = conn.execute(_PUBLIC_ROWS_SQL, {
                "pub_end": published_end(),
                "pub_next": published_next_day(),
            }).mappings().all()
    except Exception as e:
        logger.error(f"public heatmap rows failed: {type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="Ошибка получения данных heatmap")

    out = []
    for r in rows:
        sec_id = r["sec_id"]
        split = _KNOWN_SPLITS.get(sec_id)

        def ref(value, d):
            if value is None:
                return None
            v = float(value)
            if split and d is not None and d < split[0]:
                v = v / split[1]
            return v

        price = float(r["price"])
        prev = float(r["prev_close"]) if r["prev_close"] else None
        out.append({
            "secId": sec_id,
            "name": r["name"],
            "sector": r["sector"],
            "price": price,
            "prev_close": prev or 0,
            "change_1d": _pct(price, prev),
            "change_1w": _pct(price, ref(r["p1w"], r["d1w"])),
            "change_1m": _pct(price, ref(r["p1m"], r["d1m"])),
            "change_1y": _pct(price, ref(r["p1y"], r["d1y"])),
            "volume_1d": _num(r["volume_1d"]),
            "volume_1w": _num(r["volume_1w"]),
            "volume_1m": _num(r["volume_1m"]),
            "value_1d": _num(r["value_1d"]),
            "value_1w": _num(r["value_1w"]),
            "value_1m": _num(r["value_1m"]),
            "market_cap": _num(r["market_cap"]),
            "price_date": r["pd"].isoformat() if r["pd"] else None,
        })
    out.sort(key=lambda s: s["value_1d"], reverse=True)
    return out


def heatmap_rows(live: bool) -> list[dict]:
    """Акции карты в версии цены зрителя (для карты, экспорта и публичного API)."""
    return _mv_rows() if live else _public_rows()


def _data_meta(engine, stocks: list[dict], live: bool) -> tuple[str | None, bool, str]:
    """(data_date, is_live, updated_at) для подписи карты на фронте.

    Публичная версия всегда is_live=False: карта подписывается датой закрытия
    («Данные за …»), а не временем обновления — внутри дня она не меняется.
    """
    from datetime import datetime, timezone, timedelta
    if live:
        data_date, is_live = _heatmap_data_date(engine)
        msk = timezone(timedelta(hours=3))
        return data_date, is_live, datetime.now(msk).strftime("%H:%M")
    dates = [s["price_date"] for s in stocks if s.get("price_date")]
    return (max(dates) if dates else None), False, "19:00"


def build_stocks_heatmap(size_by: str, color_by: str, group_by: str, live: bool = False):
    """Собирает (или достаёт из кеша) данные карты «все акции».

    Вынесено из роута, чтобы прогрев кеша (_warmup_cache) мог наполнить кеш
    напрямую, минуя tier-проверку (guest-клиент warmup'а получил бы 403).
    Сам tier-gating остаётся на роуте get_stocks_heatmap.
    """
    from api.cache import get_or_compute

    cache_key = f"heatmap:{view_tag(live)}:{size_by}:{color_by}:{group_by}"
    # single-flight: при истечении ключа считает только один воркер, остальные
    # ждут результат (защита от cache-stampede на homepage-карте).
    return get_or_compute(
        cache_key,
        lambda: _compute_stocks_heatmap(size_by, color_by, group_by, live),
        ttl=300,  # 5 мин
    )


def _compute_stocks_heatmap(size_by: str, color_by: str, group_by: str, live: bool = False):
    """Тяжёлый расчёт карты «все акции» (вызывается через single-flight выше)."""
    engine = get_engine()
    stocks = heatmap_rows(live)

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

    data_date, is_live, updated_at = _data_meta(engine, stocks, live)
    response = {
        "stocks": stocks,
        "sectors": sectors_list,
        "params": {"size_by": size_by, "color_by": color_by, "group_by": group_by},
        "updated_at": updated_at,
        "data_date": data_date,
        "is_live": is_live,
    }
    return response


@router.get("/stocks")
async def get_stocks_heatmap(
    request: Request,
    size_by: HeatmapSizeByType = Query("value_1d", description="Размер блока"),
    color_by: HeatmapColorByType = Query("change_1d", description="Цвет блока"),
    group_by: HeatmapGroupByType = Query("sector", description="Группировка"),
    user = Depends(get_current_user_optional),
):
    """
    Возвращает данные для карты рынка.
    Параметры валидируются автоматически через Literal типы.
    """
    # Free: только режим IMOEX (см. /imoex endpoint); /stocks — для Basic+
    from api.security.access_control import enforce_tier_limits
    enforce_tier_limits(user, "heatmap", mode="all")
    return build_stocks_heatmap(size_by, color_by, group_by, is_live_viewer(user, request))


@router.get("/prices")
async def get_heatmap_prices(
    request: Request,
    user = Depends(get_current_user_optional),
):
    """Только текущие цены акций — lightweight endpoint для real-time обновления.

    Только админская (незамедленная) версия: в публичной цена одна на день —
    закрытие 19:00, освежать внутри дня нечего. Остальным — пустой ответ.
    """
    if not is_live_viewer(user, request):
        return {}

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
                  AND begin_time <= :cutoff
                  AND close > 0
                ORDER BY begin_time DESC
                LIMIT 1
            ) c
        """), {"cutoff": cutoff_for_interval(5)}).fetchall()

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
    request: Request,
    color_by: HeatmapColorByType = Query("change_1w", description="Цвет блока"),
    group_by: HeatmapGroupByType = Query("sector", description="Группировка"),
    user = Depends(get_current_user_optional),
):
    """Карта индекса IMOEX — размер по весу в индексе."""
    from api.cache import get_or_set, get_or_compute

    live = is_live_viewer(user, request)
    cache_key = f"heatmap_imoex:{view_tag(live)}:{color_by}:{group_by}"
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

    # single-flight тяжёлого build'а (веса уже разрешены выше — отдельный кэш).
    return get_or_compute(
        cache_key,
        lambda: _compute_imoex_heatmap(color_by, group_by, weights, live),
        ttl=300,
    )


def _compute_imoex_heatmap(color_by: str, group_by: str, weights: dict, live: bool = False):
    """Тяжёлый расчёт карты IMOEX (вызывается через single-flight выше)."""
    engine = get_engine()
    all_rows = {s["secId"]: s for s in heatmap_rows(live)}

    stocks = []
    for ticker, weight in weights.items():
        row = all_rows.get(ticker)
        if not row:
            continue
        stocks.append({**row, "weight": weight})

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

    data_date, is_live, updated_at = _data_meta(engine, stocks, live)
    response = {
        "stocks": stocks,
        "sectors": sectors_list,
        "params": {"size_by": "weight", "color_by": color_by, "group_by": group_by},
        "updated_at": updated_at,
        "data_date": data_date,
        "is_live": is_live,
    }
    return response
