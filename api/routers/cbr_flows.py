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

# Порядок категорий для legend/stack/пикера.
# Первыми идут три «розничные» категории (Физлица, Доверительное управление,
# Нерезиденты) — они открыты на free-тарифе и стоят в самом верху списка.
# Остальные институционалы — ниже (доступны с basic). Если категория не из
# списка — добавляется в конец (для forward-compat).
# Для FX из трёх открытых реально существует только «Физические лица».
CATEGORY_ORDER = {
    "stocks": [
        "Физические лица",
        "Доверительное управление",
        "Нерезиденты",
        "НФО",
        "Прочие Банки",
        "СЗКО",
        "Нефинансовые организации",
    ],
    "ofz": [
        "Физические лица",
        "Доверительное управление",
        "Нерезиденты",
        "НФО",
        "Прочие Банки",
        "СЗКО",
        "Нефинансовые организации",
    ],
    "fx": [
        "Физические лица",
        "Клиенты российских кредитных организаций",
        "НФО",
        "Банк России",
        "Российские кредитные организации",
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
    # Whitelist категорий per-type: набор участников у акций/ОФЗ и валюты разный,
    # поэтому лимит хранится как {type: [...]} (или плоский список — legacy, или
    # None → все категории). None для типа → ничего не режем. Иначе видимы только
    # перечисленные, остальные вырезаются из значений и отдаются как
    # locked_categories (апселл на basic).
    raw_whitelist = limits.get("categories_whitelist")
    if isinstance(raw_whitelist, dict):
        type_whitelist = raw_whitelist.get(type)
    else:
        type_whitelist = raw_whitelist  # плоский список или None

    wl_sig = "all" if type_whitelist is None else ",".join(sorted(type_whitelist))
    cache_key = f"cbr_flows:{type}:delay={delay_hours}:hist={max_history_days}:cats={wl_sig}"
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
    # period_kind = 'month' — индикатор работает только с месячными данными.
    # Квартальные строки ОРФР имеют тот же period_end_date что последний
    # месяц квартала и несопоставимы по методологии — отфильтровываем.
    sql = """
        SELECT period_year, period_label, period_kind, period_end_date,
               category, value, source_file, updated_at
        FROM cbr_flows
        WHERE instrument_type = :itype
          AND period_kind = 'month'
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

    # Tier-gating категорий: locked-категории остаются в списке (для пикера с
    # апселлом), но их значения вырезаются из данных — free-тариф видит только
    # whitelist. basic/pro → whitelist=None → ничего не режется.
    locked_categories: list[str] = []
    if type_whitelist is not None:
        allowed = set(type_whitelist)
        locked_categories = [c for c in categories if c not in allowed]
        if locked_categories:
            locked_set = set(locked_categories)
            for p in periods_dict.values():
                for cat in locked_set:
                    p["values"].pop(cat, None)

    periods_list = sorted(periods_dict.values(), key=lambda p: p["end_date"])

    response = {
        "instrument_type": type,
        "instrument_label": INSTRUMENT_LABELS.get(type, type),
        "categories": categories,
        "locked_categories": locked_categories,
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
