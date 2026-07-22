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

Публичный API:
  compute_windows / front_sec_id_for_day / resolve_day — ЧИСТЫЕ функции (без БД),
    покрыты юнит-тестами tests/test_contract_calendar.py.
  front_windows / front_sec_id / front_sec_ids — DB-обёртки (conn = Session/Connection).

Модуль НАМЕРЕННО лёгкий (только sqlalchemy.text + stdlib), без api.database/
api.models — чтобы дёшево импортироваться и из api, и из отдельного процесса.

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

# Максимальная реалистичная длина фронт-окна. Квартальная серия ~91 день;
# запас на форс-мажорный сдвиг экспирации (SRH2: остановка торгов 02.2022 →
# окно 103 дня). Окно длиннее — признак ДЫРЫ в календаре: futures_contracts
# не покрывает историю до ~2019 (ISS не различает контракты с шагом 10 лет:
# SRH8 = март 2018 И март 2028), и без капа первое окно (start=None) утянуло
# бы всю глубокую историю на sec_id первого известного контракта ('SRH' =
# март ЛЮБОГО года) — сотни ложных смен контракта. Дни вне окон уходят в
# объёмный fallback (resolve_day), как до появления календаря.
MAX_FRONT_WINDOW_DAYS = 110

# Гистерезис объёмного fallback: не переключаемся с текущего контракта, пока
# конкурент не превысит его дневной объём в N раз. В зонах без календаря в БД
# местами лежат ДВА дальних контракта с дуэльными объёмами (реального фронта
# нет) — по-дневный argmax объёма флипал между ними почти ежедневно.
STICKY_VOLUME_RATIO = 3.0


def compute_windows(contracts: Iterable[Contract]) -> List[FrontWindow]:
    """Строит хронологический список фронт-окон для одного sectype.

    - Перпетуал (если есть среди контрактов) → единственное окно (None, None).
    - Иначе: контракты с валидной lsttrade сортируются по ней; окно контракта
      C_i = (lsttrade(C_{i-1})+1д .. lsttrade(C_i)], но не длиннее
      MAX_FRONT_WINDOW_DAYS — дни до покрытия календарём (и в его дырах)
      остаются без календарного фронта и решаются объёмом в resolve_day.

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
        # Защита от грязных данных: дубль/немонотонная lsttrade дала бы вырожденное
        # окно (start > end) → контракт «пропадал» бы тихо. Пропускаем такой
        # контракт (тот же день экспирации уже покрыт предыдущим; resolve_day
        # подстрахует объёмом, если у него окажутся уникальные свечи дня).
        if prev_lst is not None and c.lsttrade <= prev_lst:
            continue
        floor = c.lsttrade - timedelta(days=MAX_FRONT_WINDOW_DAYS)
        if prev_lst is None:
            start = floor
        else:
            start = max(prev_lst + timedelta(days=1), floor)
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
    """ПОЛНЫЙ датированный secid ('BRN6') фронт-контракта на дату.

    Отличается от front_sec_id_for_day только тем, что возвращает реально
    существующий тикер контракта (candles.secid), а не префикс серии — для
    показа в UI «актуальный контракт» (обрезанный sectype 'BR' тикером не является).
    """
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
    prev: Optional[str] = None,
) -> Optional[str]:
    """Какой sec_id показывать за день: КАЛЕНДАРНЫЙ фронт, если его свечи есть в
    данных за этот день; иначе fallback на контракт с макс. объёмом из имеющихся.

    `available` — {sec_id: суммарный объём за день} (что реально есть в БД за день).
    Возвращает None, если данных за день нет вовсе. Замечание: календарный фронт
    предпочитается даже при объёме 0 — у дневной свечи фронта валиден close, а
    «без преждевременного ролла» важнее ликвидности соседа.

    `prev` — вчерашний выбор (гистерезис объёмного fallback): остаёмся на prev,
    пока конкурент не превысит его объём в STICKY_VOLUME_RATIO раз. Настоящий
    ролловер это переживает (объём мигрирует на новый фронт решительно), а
    ежедневный флип между двумя дальними контрактами с дуэльными объёмами —
    нет. На календарный выбор prev не влияет.

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
    best = max(available, key=available.get)
    if prev is not None and prev != best and prev in available \
            and available[best] < STICKY_VOLUME_RATIO * available[prev]:
        return prev
    return best


def stable_runs(
    days: List[date],
    choices: Dict[date, Optional[str]],
    min_run_days: int = 5,
) -> List[tuple]:
    """Сжимает по-дневные выборы контракта в устойчивые раны: [(первый_день, sec_id)].

    Для МЕТОК смены контракта (contract_switches): в зонах без календаря
    объёмный fallback может коротко переметнуться на другой контракт и
    вернуться — каждый такой флип давал бы ложную метку экспирации. Раны короче
    min_run_days торговых дней вливаются в предыдущий ран (их дни остаются за
    прежним контрактом), смежные одноимённые раны склеиваются. Выбор свечей
    это НЕ меняет — только список меток.

    `days` — отсортированные торговые дни; `choices` — {день: sec_id | None}
    (None-дни пропускаются). Настоящий ролл (ран длиной в квартал/месяц)
    проходит без изменений — метка встаёт на первый день рана, как раньше.
    """
    runs: List[List] = []  # [sec_id, [дни]]
    for d in days:
        c = choices.get(d)
        if c is None:
            continue
        if runs and runs[-1][0] == c:
            runs[-1][1].append(d)
        else:
            runs.append([c, [d]])

    changed = True
    while changed:
        changed = False
        for i in range(len(runs)):
            if len(runs[i][1]) < min_run_days and len(runs) > 1:
                if i > 0:
                    runs[i - 1][1].extend(runs[i][1])
                else:
                    runs[i + 1][1] = runs[i][1] + runs[i + 1][1]
                runs = runs[:i] + runs[i + 1:]
                changed = True
                break

    merged: List[List] = []
    for c, ds in runs:
        if merged and merged[-1][0] == c:
            merged[-1][1].extend(ds)
        else:
            merged.append([c, ds])
    return [(ds[0], c) for c, ds in merged]


# ---------------------------------------------------------------------------
# DB-обёртки. conn = SQLAlchemy Session или Connection (у обоих есть .execute).
# При ошибке (напр. таблицы futures_contracts ещё нет) — ROLLBACK, чтобы снять
# aborted-состояние транзакции (PostgreSQL 25P02), и пустой результат → вызывающий
# откатится на объём, а общий Session не упадёт на последующих запросах.
# ---------------------------------------------------------------------------

def _safe_rollback(conn) -> None:
    try:
        conn.rollback()
    except Exception:
        pass


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
    """Фронт-окна sectype из futures_contracts (пустой список → календаря нет/ошибка)."""
    try:
        return compute_windows(_fetch_contracts(conn, sectype))
    except Exception:
        _safe_rollback(conn)
        return []


def front_sec_id(conn, sectype: str, as_of_date: Optional[date] = None) -> Optional[str]:
    """sec_id ('BRN') фронт-контракта на дату (через окна; None если нет)."""
    as_of_date = as_of_date or date.today()
    return front_sec_id_for_day(front_windows(conn, sectype), as_of_date)


def front_sec_ids(conn, sectype: str) -> List[str]:
    """Все sec_id ('BRN') контрактов sectype из календаря.

    Для расширения списка контрактов в chart.py — чтобы тянуть свечи контрактов,
    которых ещё нет в instruments (напр. летние BRQ/BRU/BRV до их регистрации).
    [] при ошибке/отсутствии таблицы (+ rollback, чтобы не отравить Session).
    """
    try:
        rows = conn.execute(
            text("SELECT DISTINCT sec_id FROM futures_contracts WHERE sectype = :st"),
            {"st": sectype},
        ).fetchall()
        return [r[0] for r in rows]
    except Exception:
        _safe_rollback(conn)
        return []


def front_secid(conn, sectype: str, as_of_date: Optional[date] = None) -> Optional[str]:
    """Датированный secid ('BRN6') фронт-контракта sectype на дату (None если нет)."""
    as_of_date = as_of_date or date.today()
    return front_secid_for_day(front_windows(conn, sectype), as_of_date)


def front_secids_all(conn, as_of_date: Optional[date] = None) -> Dict[str, str]:
    """{sectype: датированный secid фронт-контракта на дату} для ВСЕХ sectype.

    Один запрос ко всей futures_contracts + чистая логика compute_windows на
    sectype — для пикера инструментов, где надо показать актуальный контракт
    у каждого фьючерса разом, без N запросов. {} при ошибке/отсутствии таблицы.
    """
    as_of_date = as_of_date or date.today()
    try:
        rows = conn.execute(text(
            "SELECT sectype, secid, sec_id, lsttrade, is_perpetual, is_traded "
            "FROM futures_contracts"
        )).fetchall()
    except Exception:
        _safe_rollback(conn)
        return {}
    by_sectype: Dict[str, List[Contract]] = {}
    for r in rows:
        by_sectype.setdefault(r[0], []).append(
            Contract(secid=r[1], sec_id=r[2], lsttrade=r[3],
                     is_perpetual=bool(r[4]), is_traded=bool(r[5]))
        )
    out: Dict[str, str] = {}
    for st, conts in by_sectype.items():
        secid = front_secid_for_day(compute_windows(conts), as_of_date)
        if secid:
            out[st] = secid
    return out
