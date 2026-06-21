#!/usr/bin/env python3
"""Юнит-тесты КАЛЕНДАРНОГО ролловера (api/services/contract_calendar).

Чистая логика без БД. Доказывает главное требование:
  • НЕТ преждевременных роллов (контракт фронт ВКЛЮЧАЯ свою экспирацию),
  • НЕТ пропусков (каждый контракт — фронт ровно один раз, по очереди lsttrade).

Запуск:
    python tests/test_contract_calendar.py     # печатает PASS/FAIL
    pytest tests/test_contract_calendar.py      # тоже работает
"""
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from api.services.contract_calendar import (  # noqa: E402
    Contract, compute_windows, front_sec_id_for_day, resolve_day,
)


def _c(secid, lsttrade, perp=False, traded=True):
    sectype, sec_id, is_perp = (secid, secid, True) if perp else (secid[:2], secid[:3], False)
    return Contract(secid=secid, sec_id=sec_id,
                    lsttrade=None if perp else date.fromisoformat(lsttrade),
                    is_perpetual=perp, is_traded=traded)


# Реальный календарь Brent (из ISS, проверено 2026-06-21): помесячно, включая
# «летние» BRQ/BRU/BRV, которые объёмный ролл пропускал.
BR = [
    _c("BRN6", "2026-07-01"), _c("BRQ6", "2026-08-03"), _c("BRU6", "2026-08-31"),
    _c("BRV6", "2026-10-01"), _c("BRX6", "2026-11-02"), _c("BRZ6", "2026-12-01"),
    _c("BRF7", "2027-01-04"),
]
# Si — квартальный (H/M/U/Z).
SI = [_c("SiM6", "2026-06-18"), _c("SiU6", "2026-09-17"),
      _c("SiZ6", "2026-12-17"), _c("SiH7", "2027-03-19")]


def test_no_skip_every_contract_is_front_once():
    """Каждый контракт становится фронтом ровно один раз, в порядке lsttrade."""
    w = compute_windows(BR)
    seq = [x.sec_id for x in w]
    assert seq == ["BRN", "BRQ", "BRU", "BRV", "BRX", "BRZ", "BRF"], seq
    # ровно по разу
    assert len(seq) == len(set(seq)) == 7
    # окна смежные и монотонные: следующее начинается ровно назавтра после
    # экспирации предыдущего (нет дыр и нет нахлёста = нет пропусков)
    for a, b in zip(w, w[1:]):
        assert (b.start - a.end).days == 1, (a.end, b.start)
        assert b.end > a.end, (a.end, b.end)


def test_no_premature_roll_on_expiry_day():
    """В день экспирации фронт — ВСЁ ЕЩЁ истекающий контракт; смена назавтра."""
    w = compute_windows(BR)
    # 2026-07-01 = lsttrade BRN6 → фронт ещё BRN
    assert front_sec_id_for_day(w, date(2026, 7, 1)) == "BRN"
    # 2026-07-02 → ролл на BRQ (следующий по lsttrade), без пропуска
    assert front_sec_id_for_day(w, date(2026, 7, 2)) == "BRQ"
    # середина августа → BRU (после BRQ 08-03)
    assert front_sec_id_for_day(w, date(2026, 8, 15)) == "BRU"


def test_br_summer_contracts_not_skipped():
    """BRQ/BRU/BRV (летние) ДОЛЖНЫ быть фронтом по очереди — объёмный их пропускал."""
    w = compute_windows(BR)
    assert front_sec_id_for_day(w, date(2026, 7, 15)) == "BRQ"   # авг-контракт
    assert front_sec_id_for_day(w, date(2026, 8, 20)) == "BRU"   # сен-контракт
    assert front_sec_id_for_day(w, date(2026, 9, 15)) == "BRV"   # окт-контракт
    assert front_sec_id_for_day(w, date(2026, 10, 20)) == "BRX"  # ноя-контракт


def test_si_quarterly():
    w = compute_windows(SI)
    assert [x.sec_id for x in w] == ["SiM", "SiU", "SiZ", "SiH"]
    assert front_sec_id_for_day(w, date(2026, 6, 18)) == "SiM"   # день экспирации
    assert front_sec_id_for_day(w, date(2026, 6, 19)) == "SiU"   # назавтра — ролл
    assert front_sec_id_for_day(w, date(2026, 7, 1)) == "SiU"


def test_perpetual_always_self():
    w = compute_windows([_c("USDRUBF", None, perp=True)])
    assert len(w) == 1 and w[0].is_perpetual
    for d in (date(2020, 1, 1), date(2026, 6, 21), date(2030, 12, 31)):
        assert front_sec_id_for_day(w, d) == "USDRUBF"


def test_first_contract_covers_early_days():
    """Дни ДО первой экспирации → первый контракт (start=None)."""
    w = compute_windows(BR)
    assert front_sec_id_for_day(w, date(2026, 1, 1)) == "BRN"
    assert front_sec_id_for_day(w, date(2020, 1, 1)) == "BRN"


def test_resolve_day_prefers_calendar_when_available():
    w = compute_windows(BR)
    day = date(2026, 7, 15)  # календарь говорит BRQ
    # BRQ есть в данных → берём календарь, даже если у BRX больше объёма
    assert resolve_day(w, day, {"BRQ": 100.0, "BRX": 999.0}) == "BRQ"


def test_resolve_day_fallback_to_volume_when_calendar_missing_data():
    w = compute_windows(BR)
    day = date(2026, 7, 15)  # календарь BRQ, но его свечей за день НЕТ
    assert resolve_day(w, day, {"BRN": 50.0, "BRX": 200.0}) == "BRX"   # макс объём


def test_resolve_day_pure_volume_without_calendar():
    assert resolve_day([], date(2026, 7, 15), {"A": 10.0, "B": 30.0, "C": 20.0}) == "B"


def test_resolve_day_empty_returns_none():
    w = compute_windows(BR)
    assert resolve_day(w, date(2026, 7, 15), {}) is None


def _run():
    tests = [v for k, v in sorted(globals().items())
             if k.startswith("test_") and callable(v)]
    failed = 0
    for t in tests:
        try:
            t()
            print(f"  PASS  {t.__name__}")
        except AssertionError as e:
            failed += 1
            print(f"  FAIL  {t.__name__}: {e}")
        except Exception as e:
            failed += 1
            print(f"  ERROR {t.__name__}: {type(e).__name__}: {e}")
    print(f"\n{len(tests) - failed}/{len(tests)} passed")
    return failed


if __name__ == "__main__":
    sys.exit(1 if _run() else 0)
