"""Календарный ролловер фьючерсов MOEX — единая точка истины.

Заменяет ОБЪЁМНЫЙ выбор фронт-контракта (раньше 3 несогласованные реализации:
api/routers/chart.py, signals/db.get_candles_continuous, api/services/chart_live,
Candles/fetch_candles_futures_realtime) на КАЛЕНДАРНЫЙ — по реальным датам
экспирации (futures_contracts.lsttrade из ISS).

Гарантии (это и есть смысл задачи):
- НЕТ преждевременных роллов: контракт остаётся фронтом ВКЛЮЧАЯ свой день
  экспирации (lsttrade); смена — на следующий календарный день.
- НЕТ пропусков: фронт-окна строятся по ВСЕМ контрактам sectype в порядке
  lsttrade — каждый контракт становится фронтом ровно один раз, по очереди
  (даже неликвидные летние BRQ/BRU/BRV).

Модуль НАМЕРЕННО лёгкий (только sqlalchemy.text + stdlib), без api.database/
api.models — чтобы дёшево импортироваться и из api, и из отдельного процесса
фетчера свечей. Чистые функции (compute_windows / front_sec_id_for_day /
resolve_day) не трогают БД и покрыты юнит-тестами.

Связь кодов:
  secid   'BRN6'  — полный контракт (candles.secid, futures_contracts.secid)
  sec_id  'BRN'   — prefix+месяц    (candles.sec_id, instruments.sec_id)
  sectype 'BR'    — базовый тикер   (instruments.sectype, open_interest.sectype)
"""
from __future__ import annotations

from datetime import date, timedelta
from typing import Dict, Iterable, List, NamedTuple, Optional

from sqlalchemy import text


class Contract(NamedTuple):
    secid: str                    # 'BRN6'
    sec_id: str                   # 'BRN'
    lsttrade: Optional[date]
    is_perpetual: bool
    is_traded: bool


class FrontWindow(NamedTuple):
    """Окно, в течение которого контракт является фронтом.

    start/end — включительные границы по календарным датам. Контракт фронт со
    дня ПОСЛЕ экспирации предыдущего (start) и ВКЛЮЧАЯ свою экспирацию (end).
    Для перпетуала start=end=None (фронт всегда).
    """
    start: Optional[date]
    end: Optional[date]
    secid: str
    sec_id: str
    is_perpetual: bool


# ---------------------------------------------------------------------------
# Чистая логика (без БД) — покрыта юнит-тестами tests/test_contract_calendar.py
# ---------------------------------------------------------------------------

def compute_windows(contracts: Iterable[Contract]) -> List[FrontWindow]:
    """Строит хронологический список фронт-окон для одного sectype.

    - Перпетуал (если есть среди контрактов) → единственное окно (None, None).
    - Иначе: контракты с валидной lsttrade сортируются по ней; окно контракта
      C_i = (lsttrade(C_{i-1})+1д .. lsttrade(C_i)]. Первый контракт — фронт с
      -∞ (start=None) до своей экспирации включительно.

    НЕ фильтруем по is_traded: истёкшие контракты были фронтом в прошлом и нужны
    для исторических окон (resolve_day всё равно откатится на объём, если их
    свечей нет в БД).
    """
    conts = list(contracts)
    perp = next((c for c in conts if c.is_perpetual), None)
    if perp is not None:
        return [FrontWindow(None, None, perp.secid, perp.sec_id, True)]

    regs = sorted(
        (c for c in conts if not c.is_perpetual and c.lsttrade is not None),
        key=lambda c: c.lsttrade,
    )
    windows: List[FrontWindow] = []
    prev_lst: Optional[date] = None
    for c in regs:
        start = None if prev_lst is None else prev_lst + timedelta(days=1)
        windows.append(FrontWindow(start, c.lsttrade, c.secid, c.sec_id, False))
        prev_lst = c.lsttrade
    return windows


def front_sec_id_for_day(windows: List[FrontWindow], day: date) -> Optional[str]:
    """sec_id ('BRN') фронт-контракта на дату (None если вне всех окон)."""
    for w in windows:
        if w.is_perpetual:
            return w.sec_id
        if (w.start is None or day >= w.start) and (w.end is None or day <= w.end):
            return w.sec_id
    return None


def front_secid_for_day(windows: List[FrontWindow], day: date) -> Optional[str]:
    """Полный secid ('BRN6') фронт-контракта на дату (None если вне окон)."""
    for w in windows:
        if w.is_perpetual:
            return w.secid
        if (w.start is None or day >= w.start) and (w.end is None or day <= w.end):
            return w.secid
    return None


def resolve_day(
    windows: List[FrontWindow],
    day: date,
    available: Dict[str, float],
) -> Optional[str]:
    """Какой sec_id показывать за день: КАЛЕНДАРНЫЙ фронт, если его свечи есть в
    данных за этот день; иначе fallback на контракт с макс. объёмом из имеющихся.

    `available` — {sec_id: суммарный объём за день} (что реально есть в БД за день).
    Возвращает None, если данных за день нет вовсе.

    Зачем fallback: на исторических днях старый объёмный фетчер мог не сохранить
    календарный фронт → берём что есть (поведение как раньше, без регрессии).
    Вперёд новый фетчер сохраняет именно фронт → всегда срабатывает календарь.
    Если календаря нет совсем (windows пуст) → чистый объём (старое поведение).
    """
    if not available:
        return None
    if windows:
        cal = front_sec_id_for_day(windows, day)
        if cal is not None and cal in available:
            return cal
    return max(available, key=available.get)


# ---------------------------------------------------------------------------
# DB-обёртки. conn = SQLAlchemy Session или Connection (у обоих есть .execute).
# Любая ошибка БД → пустой результат / None, чтобы вызывающий откатился на объём.
# ---------------------------------------------------------------------------

def _fetch_contracts(conn, sectype: str) -> List[Contract]:
    rows = conn.execute(
        text("""
            SELECT secid, sec_id, lsttrade, is_perpetual, is_traded
            FROM futures_contracts
            WHERE sectype = :sectype
        """),
        {"sectype": sectype},
    ).fetchall()
    return [
        Contract(
            secid=r[0], sec_id=r[1], lsttrade=r[2],
            is_perpetual=bool(r[3]), is_traded=bool(r[4]),
        )
        for r in rows
    ]


def front_windows(conn, sectype: str) -> List[FrontWindow]:
    """Фронт-окна sectype из futures_contracts (пустой список → календаря нет)."""
    try:
        return compute_windows(_fetch_contracts(conn, sectype))
    except Exception:
        return []


def pick_front_contract(conn, sectype: str, as_of_date: Optional[date] = None) -> Optional[str]:
    """Полный secid ('BRN6') фронт-контракта на дату. None — календаря нет/ошибка.

    На день экспирации возвращает сам истекающий контракт (lsttrade >= день) —
    без преждевременного ролла. Назавтра вернёт следующий по lsttrade.
    """
    as_of_date = as_of_date or date.today()
    try:
        row = conn.execute(
            text("""
                SELECT secid FROM futures_contracts
                WHERE sectype = :sectype AND is_traded
                  AND (is_perpetual OR lsttrade >= :d)
                ORDER BY is_perpetual DESC, lsttrade ASC
                LIMIT 1
            """),
            {"sectype": sectype, "d": as_of_date},
        ).fetchone()
        return row[0] if row else None
    except Exception:
        return None


def front_sec_id(conn, sectype: str, as_of_date: Optional[date] = None) -> Optional[str]:
    """sec_id ('BRN') фронт-контракта на дату (через окна; None если нет)."""
    as_of_date = as_of_date or date.today()
    return front_sec_id_for_day(front_windows(conn, sectype), as_of_date)
