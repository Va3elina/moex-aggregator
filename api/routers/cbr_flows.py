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
from typing import Literal

from fastapi import APIRouter, HTTPException, Query
from sqlalchemy import text

from api.cache import get_or_set
from api.database import get_engine
from api.logger import get_logger

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
):
    """Потоки участников биржи по выбранному типу инструмента."""

    cache_key = f"cbr_flows:{type}"
    cached = get_or_set(cache_key)
    if cached is not None:
        return cached

    engine = get_engine()
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT period_year, period_label, period_kind, period_end_date,
                   category, value, source_file, updated_at
            FROM cbr_flows
            WHERE instrument_type = :itype
            ORDER BY period_end_date, category
        """), {"itype": type}).fetchall()

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
