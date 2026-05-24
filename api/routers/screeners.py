"""
Quick screeners — preset-фильтры для главной страницы.

UX: горизонтальная карусель чипсов на overview ("Перепроданные / Объём /
Силач недели / Слабак месяца / Притоки в фонды"). Тап → таблица top-10
тикеров со ссылкой на /heatmap?focus=X или соответствующий индикатор.

Все presets открыты гостям (главная бесплатная). Click-through на актив
вызывает gating только на странице индикатора через useTierAccess.

Endpoints:
  GET /api/screeners              — список presets с метаданными.
  GET /api/screeners/{preset_id}  — top-10 тикеров для preset'а.

Кэш: каждый preset кэшируется в Redis на 5 минут — расчёт включает
агрегирующие SQL queries поверх миллионов candles.
"""
from typing import List

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from sqlalchemy import text

from api.database import get_engine
from api.cache import get_or_set

router = APIRouter(prefix="/api/screeners", tags=["screeners"])

# ─────────────────────────────────────────────────────────────────────
# Presets — каждый описывает SQL запрос и метаданные для UI.
# label — что юзер видит на чипе; description — tooltip; emoji — иконка.
# ─────────────────────────────────────────────────────────────────────


class PresetMeta(BaseModel):
    id: str
    label: str
    description: str
    emoji: str


class ScreenerItem(BaseModel):
    ticker: str
    name: str | None = None
    value: float | None = None  # Главная метрика для preset (e.g. % change, ratio объёма)
    value_label: str | None = None  # Форматированная строка (e.g. "+12.3%")
    extra: dict = {}


PRESETS: dict[str, PresetMeta] = {
    "oversold_ema200": PresetMeta(
        id="oversold_ema200",
        label="Перепроданные",
        description="Акции IMOEX с ценой более чем на 10% ниже EMA200",
        emoji="📉",
    ),
    "overbought_ema200": PresetMeta(
        id="overbought_ema200",
        label="Перекупленные",
        description="Акции IMOEX с ценой более чем на 15% выше EMA200",
        emoji="📈",
    ),
    "volume_spike": PresetMeta(
        id="volume_spike",
        label="Рост объёма",
        description="Объём 1d более 3× от 30-дневного медианного",
        emoji="🔥",
    ),
    "top_week": PresetMeta(
        id="top_week",
        label="Лидеры недели",
        description="Топ-10 акций по росту за последние 7 торговых дней",
        emoji="🚀",
    ),
    "bottom_month": PresetMeta(
        id="bottom_month",
        label="Аутсайдеры месяца",
        description="Топ-10 акций по падению за последние 30 дней",
        emoji="🩸",
    ),
}


def _imoex_universe() -> str:
    """SQL фрагмент: ограничение по акциям IMOEX. Хардкод — список акций
    IMOEX редко меняется, проще обновлять при ребалансировке. Для общего
    universe (все акции MOEX) — убираем фильтр."""
    return "i.type = 'stock' AND i.\"group\" = 'Акции'"


def _run_oversold(threshold_pct: float, sign: str) -> List[ScreenerItem]:
    """
    Акции где цена отклонилась от EMA200 на threshold_pct% в указанном знаке.
    sign = '-' для перепроданных (price ниже EMA), '+' для перекупленных.
    Использует mv_heatmap_stocks как кэш (refreshed orchestrator'ом ежедневно).
    """
    engine = get_engine()
    operator = "<" if sign == "-" else ">"
    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT m.sec_id AS ticker, m.name, m.price, m.change_1m AS value
            FROM mv_heatmap_stocks m
            JOIN instruments i ON i.sectype = m.sec_id
            WHERE {_imoex_universe()}
              AND m.change_1m {operator} :threshold
            ORDER BY m.change_1m {'ASC' if sign == '-' else 'DESC'}
            LIMIT 10
        """), {"threshold": -threshold_pct if sign == "-" else threshold_pct}).fetchall()
    return [
        ScreenerItem(
            ticker=r.ticker,
            name=r.name,
            value=float(r.value or 0),
            value_label=f"{r.value:+.1f}%" if r.value is not None else None,
            extra={"price": float(r.price or 0)},
        )
        for r in rows
    ]


def _run_volume_spike() -> List[ScreenerItem]:
    """
    Акции где объём последнего торгового дня значительно превышает
    30-дневную медиану. Threshold = 3× — отсеивает шум, ловит реальные
    события (news, отчётность). Сортировка — по ratio (макс. сначала).
    """
    engine = get_engine()
    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            WITH last_day AS (
                SELECT secid, volume, begin_time::date AS dt
                FROM candles
                WHERE interval=24 AND type='stock'
                  AND begin_time >= CURRENT_DATE - 3
            ),
            latest AS (
                SELECT DISTINCT ON (secid) secid, volume, dt
                FROM last_day
                ORDER BY secid, dt DESC
            ),
            median_30d AS (
                SELECT secid,
                       PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY volume) AS med
                FROM candles
                WHERE interval=24 AND type='stock'
                  AND begin_time >= CURRENT_DATE - 35
                  AND begin_time < CURRENT_DATE - 1
                  AND volume > 0
                GROUP BY secid
            )
            SELECT l.secid AS ticker, i.name,
                   (l.volume::float / NULLIF(m.med, 0)) AS value
            FROM latest l
            JOIN median_30d m ON m.secid = l.secid
            JOIN instruments i ON i.sectype = l.secid
            WHERE {_imoex_universe()}
              AND m.med > 0
              AND l.volume > m.med * 3
            ORDER BY value DESC NULLS LAST
            LIMIT 10
        """)).fetchall()
    return [
        ScreenerItem(
            ticker=r.ticker,
            name=r.name,
            value=float(r.value or 0),
            value_label=f"×{r.value:.1f}" if r.value else None,
        )
        for r in rows
    ]


def _run_top_change(window: str, direction: str) -> List[ScreenerItem]:
    """
    window: 'change_1w' или 'change_1m'.
    direction: 'top' (gainers) или 'bottom' (losers).
    Берёт из mv_heatmap_stocks — там precomputed change_1w/1m.
    """
    engine = get_engine()
    column = "change_1w" if window == "1w" else "change_1m"
    order = "DESC" if direction == "top" else "ASC"
    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT m.sec_id AS ticker, m.name, m.{column} AS value, m.price
            FROM mv_heatmap_stocks m
            JOIN instruments i ON i.sectype = m.sec_id
            WHERE {_imoex_universe()}
              AND m.{column} IS NOT NULL
            ORDER BY m.{column} {order}
            LIMIT 10
        """)).fetchall()
    return [
        ScreenerItem(
            ticker=r.ticker,
            name=r.name,
            value=float(r.value or 0),
            value_label=f"{r.value:+.1f}%" if r.value is not None else None,
            extra={"price": float(r.price or 0)},
        )
        for r in rows
    ]


# ─────────────────────────────────────────────────────────────────────
# Endpoints
# ─────────────────────────────────────────────────────────────────────


@router.get("", response_model=List[PresetMeta])
def list_presets():
    """Список всех available screeners (метаданные для UI)."""
    return list(PRESETS.values())


@router.get("/{preset_id}", response_model=List[ScreenerItem])
def run_screener(preset_id: str):
    """
    Запускает screener-preset, возвращает top-10 тикеров.
    Кэшируется в Redis на 5 минут (data меняется максимум раз в день
    после orchestrator-refresh mv_heatmap_stocks).
    """
    if preset_id not in PRESETS:
        raise HTTPException(status_code=404, detail=f"Preset {preset_id} не существует")

    cache_key = f"screener:{preset_id}"
    cached = get_or_set(cache_key)
    if cached is not None:
        return cached

    if preset_id == "oversold_ema200":
        result = _run_oversold(10.0, "-")
    elif preset_id == "overbought_ema200":
        result = _run_oversold(15.0, "+")
    elif preset_id == "volume_spike":
        result = _run_volume_spike()
    elif preset_id == "top_week":
        result = _run_top_change("1w", "top")
    elif preset_id == "bottom_month":
        result = _run_top_change("1m", "bottom")
    else:
        result = []

    # Pydantic → dict для Redis cache.
    payload = [item.model_dump() for item in result]
    get_or_set(cache_key, payload, ttl=300)  # 5 минут
    return result
