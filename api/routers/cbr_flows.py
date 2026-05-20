"""
ОРФР Банка России — потоки участников биржевых торгов.

Endpoint:
  GET /api/cbr-flows?type=stocks|ofz|fx

Возвращает:
  {
    "instrument_type": "stocks",
    "categories": [...список категорий в порядке для legend/stack...],
    "periods": [
      {"year": 2024, "label": "I квартал", "kind": "quarter",
       "end_date": "2024-03-31",
       "values": {"Нерезиденты": -3.9, "НФО": 2.5, ...}}
    ],
    "source": "ORFR_2026-4",
    "updated_at": "2026-05-14T12:34:56"
  }

Данные приходят из таблицы cbr_flows, которая обновляется ежедневно
скриптом CBR/fetch_orfr_flows.py из orchestrator'а.
"""
from datetime import datetime, timedelta, timezone
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text

from api.billing.features import get_indicator_limits
from api.billing.tiers import user_tier
from api.cache import get_or_set
from api.database import get_engine
from api.logger import get_logger
from api.routers.auth import get_current_user_optional

log = get_logger()

router = APIRouter(prefix="/api/cbr-flows", tags=["cbr_flows"])

InstrumentType = Literal["stocks", "ofz", "fx"]

# Порядок категорий для stacking (от тёмных к светлым визуально).
# Сохраняет логику ЦБ: «крупные институционалы внизу, физлица сверху».
# Если категория не из списка — добавляется в конец (для forward-compat).
CATEGORY_ORDER = {
    "stocks": [
        "Нерезиденты",
        "НФО",
        "Прочие Банки",
        "СЗКО",
        "Физические лица",
        "Доверительное управление",
        "Нефинансовые организации",
    ],
    "ofz": [
        "Нерезиденты",
        "НФО",
        "Прочие Банки",
        "СЗКО",
        "Физические лица",
        "Доверительное управление",
        "Нефинансовые организации",
    ],
    "fx": [
        "Клиенты российских кредитных организаций",
        "НФО",
        "Российские кредитные организации",
        "Банк России",
        "Физические лица",
    ],
}

INSTRUMENT_LABELS = {
    "stocks": "Акции",
    "ofz": "ОФЗ",
    "fx": "Валюты",
}


@router.get("")
def get_cbr_flows(
    type: InstrumentType = Query("stocks", description="stocks | ofz | fx"),
    user = Depends(get_current_user_optional),
):
    """Потоки участников биржи по выбранному типу инструмента.

    Tier-gating:
      - Free: данные с задержкой 24ч + только последние 365 дней.
      - Basic/Pro: real-time + полная история.
    """
    # Определяем tier limits заранее — нужны для cache_key (разные tier'ы
    # получают разные слайсы, нельзя шарить кэш).
    tier = user_tier(user)
    limits = get_indicator_limits(tier, "cbr_flows")
    delay_hours: int = int(limits.get("data_delay_hours") or 0)
    max_history_days = limits.get("max_history_days")

    cache_key = f"cbr_flows:{type}:delay={delay_hours}:hist={max_history_days}"
    cached = get_or_set(cache_key)
    if cached is not None:
        return cached

    # Граничные даты для tier-фильтра
    now = datetime.now(timezone.utc).date()
    effective_end = now - timedelta(hours=delay_hours) if delay_hours else None
    effective_start = (
        now - timedelta(days=int(max_history_days))
        if max_history_days is not None
        else None
    )

    engine = get_engine()
    sql = """
        SELECT period_year, period_label, period_kind, period_end_date,
               category, value, source_file, updated_at
        FROM cbr_flows
        WHERE instrument_type = :itype
    """
    params: dict = {"itype": type}
    if effective_start is not None:
        sql += " AND period_end_date >= :start_date"
        params["start_date"] = effective_start
    if effective_end is not None:
        sql += " AND period_end_date <= :end_date"
        params["end_date"] = effective_end
    sql += " ORDER BY period_end_date, category"

    with engine.connect() as conn:
        rows = conn.execute(text(sql), params).fetchall()

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"Нет данных для {type}. Запустите CBR/fetch_orfr_flows.py.",
        )

    # Группируем по периоду
    periods_dict: dict[str, dict] = {}
    categories_seen: set[str] = set()
    latest_source: str | None = None
    latest_updated = None

    for r in rows:
        year, label, kind, end_date, cat, val, src, upd = r
        key = end_date.isoformat()
        if key not in periods_dict:
            periods_dict[key] = {
                "year": year,
                "label": label,
                "kind": kind,
                "end_date": key,
                "values": {},
            }
        periods_dict[key]["values"][cat] = float(val)
        categories_seen.add(cat)
        if src:
            latest_source = src
        if upd and (latest_updated is None or upd > latest_updated):
            latest_updated = upd

    # Сортируем категории согласно CATEGORY_ORDER; unknown — в конец
    order_template = CATEGORY_ORDER.get(type, [])
    known_in_data = [c for c in order_template if c in categories_seen]
    unknown_in_data = sorted(categories_seen - set(order_template))
    categories = known_in_data + unknown_in_data

    periods_list = sorted(periods_dict.values(), key=lambda p: p["end_date"])

    response = {
        "instrument_type": type,
        "instrument_label": INSTRUMENT_LABELS.get(type, type),
        "categories": categories,
        "periods": periods_list,
        "source": latest_source,
        "updated_at": latest_updated.isoformat() if latest_updated else None,
        "unit": "млрд руб.",
        "note": (
            "Нетто-покупка (+) / нетто-продажа (−) на вторичных биржевых торгах. "
            "Источник: Банк России, обзор рисков финансовых рынков (ОРФР)."
        ),
    }
    # Кеш 1 час — данные обновляются раз в день, лишние пересчёты не нужны
    get_or_set(cache_key, response, ttl=3600)
    return response
