"""
Реестр сплитов акций — таблица stock_splits (db/migrations/104_stock_splits.sql).

Единственное место, откуда индикаторы узнают о сплитах; захардкоженные списки
в breadth/heatmap/repaint убраны. Семантика:
  ratio          — новых акций на 1 старую; применяется к строкам ДО split_date.
  price_adjusted — биржа уже пересчитала цены до сплита (ISS отдаёт в новых
                   акциях): цену не трогаем. Иначе цену делим на ratio.
  объём          — биржа не пересчитывает никогда: до split_date volume в
                   старых акциях, умножаем на ratio всегда.

Таблица крошечная, кэшируется в процессе на 10 минут. Нет таблицы (dev-база
без миграции) — пустой реестр с предупреждением в лог, а не 500.
"""
import logging
import time
from datetime import date

from sqlalchemy import text

from api.database import get_engine

log = logging.getLogger(__name__)

_TTL_SEC = 600
_cache: tuple[float, dict[str, tuple[date, float, bool]]] | None = None


def load_splits() -> dict[str, tuple[date, float, bool]]:
    """{secid: (split_date, ratio, price_adjusted)}."""
    global _cache
    if _cache and time.time() - _cache[0] < _TTL_SEC:
        return _cache[1]
    try:
        with get_engine().connect() as conn:
            rows = conn.execute(text(
                "SELECT secid, split_date, ratio, price_adjusted FROM stock_splits"
            )).fetchall()
        data = {r[0]: (r[1], float(r[2]), bool(r[3])) for r in rows}
    except Exception as e:  # noqa: BLE001 — реестр не должен ронять индикатор
        log.warning("stock_splits недоступна (%s: %s) — сплиты не применяются", type(e).__name__, e)
        data = {}
    _cache = (time.time(), data)
    return data


def _as_date(d) -> date:
    return d if isinstance(d, date) else date.fromisoformat(str(d)[:10])


def price_splits() -> dict[str, tuple[date, float]]:
    """Только сплиты с СЫРЫМИ ценами в БД: {secid: (split_date, ratio)} — делить цену до даты."""
    return {s: (d, r) for s, (d, r, adj) in load_splits().items() if not adj}


def price_divisor(secid: str, day) -> float:
    """На что делить цену строки за день day (1.0 — не трогать)."""
    s = load_splits().get(secid)
    if s and not s[2] and _as_date(day) < s[0]:
        return s[1]
    return 1.0


def volume_multiplier(secid: str, day) -> float:
    """На что умножать объём в штуках за день day (1.0 — не трогать)."""
    s = load_splits().get(secid)
    if s and _as_date(day) < s[0]:
        return s[1]
    return 1.0


def adjust_prices(secid: str, dated_prices: list[tuple]) -> list[tuple]:
    """[(date, price), ...] → цены до сплита с сырыми свечами поделены на ratio."""
    s = load_splits().get(secid)
    if not s or s[2]:
        return dated_prices
    split_date, ratio = s[0], s[1]
    return [(d, p / ratio) if _as_date(d) < split_date else (d, p) for d, p in dated_prices]
