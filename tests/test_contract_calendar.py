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
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from api.services.contract_calendar import (  # noqa: E402
    MAX_FRONT_WINDOW_DAYS, Contract, compute_windows, front_sec_id_for_day,
    resolve_day, stable_runs,
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


def test_first_window_capped_not_infinite():
    """Окно первого контракта ограничено MAX_FRONT_WINDOW_DAYS, а НЕ -∞.

    Календарь futures_contracts не покрывает историю до ~2019 (ISS не различает
    контракты с шагом 10 лет). Без капа вся глубокая история липла к sec_id
    первого известного контракта ('SRH' = март ЛЮБОГО года) → сотни ложных
    смен контракта на графике ОИ (Сбербанк 2007-2019).
    """
    w = compute_windows(BR)
    # Внутри капа до первой экспирации (2026-07-01) → первый контракт
    assert front_sec_id_for_day(w, date(2026, 5, 1)) == "BRN"
    assert w[0].start == date(2026, 7, 1) - timedelta(days=MAX_FRONT_WINDOW_DAYS)
    # Глубокая история — вне календаря → None (решается объёмом в resolve_day)
    assert front_sec_id_for_day(w, date(2026, 1, 1)) is None
    assert front_sec_id_for_day(w, date(2020, 1, 1)) is None


def test_calendar_hole_days_not_claimed_by_next_contract():
    """Дыра в календаре (пропущенные контракты) → дни дыры без календарного
    фронта, а не приписаны следующему известному контракту."""
    holey = [_c("SRH9", "2019-03-21"), _c("SRH6", "2026-03-19")]
    w = compute_windows(holey)
    assert front_sec_id_for_day(w, date(2022, 6, 1)) is None
    # А у своей экспирации каждый — фронт
    assert front_sec_id_for_day(w, date(2019, 3, 1)) == "SRH"
    assert front_sec_id_for_day(w, date(2026, 3, 1)) == "SRH"
    # День дыры решается объёмом
    assert resolve_day(w, date(2022, 6, 1), {"SRM": 100.0, "SRU": 40.0}) == "SRM"


def test_resolve_day_sticky_fallback():
    """Гистерезис объёмного fallback: prev держится, пока конкурент не превысит
    его объём в STICKY_VOLUME_RATIO раз; календарный выбор prev игнорирует."""
    day = date(2020, 1, 15)  # вне окон BR-календаря 2026 → чистый fallback
    w = compute_windows(BR)
    # Конкурент лишь вдвое больше → остаёмся на prev
    assert resolve_day(w, day, {"BRF": 200.0, "BRG": 100.0}, prev="BRG") == "BRG"
    # Конкурент в ≥3 раза больше → переключаемся
    assert resolve_day(w, day, {"BRF": 300.0, "BRG": 100.0}, prev="BRG") == "BRF"
    # prev вне данных дня → argmax объёма
    assert resolve_day(w, day, {"BRF": 200.0, "BRG": 100.0}, prev="BRX") == "BRF"
    # Календарь главнее гистерезиса
    cal_day = date(2026, 7, 15)  # окно BRQ
    assert resolve_day(w, cal_day, {"BRQ": 1.0, "BRX": 999.0}, prev="BRX") == "BRQ"


def _mk_days(n, start="2020-01-01"):
    d0 = date.fromisoformat(start)
    return [d0 + timedelta(days=i) for i in range(n)]


def test_stable_runs_suppresses_flip_flop():
    """Короткие вылазки (< min_run_days) не дают ложных меток экспирации."""
    days = _mk_days(30)
    choices = {d: "SRH" for d in days}
    for d in days[10:12]:   # 2-дневный флип на SRM и обратно
        choices[d] = "SRM"
    runs = stable_runs(days, choices, min_run_days=5)
    assert runs == [(days[0], "SRH")]


def test_stable_runs_keeps_real_roll():
    """Настоящий ролл (длинные раны) сохраняется, метка на первом дне рана."""
    days = _mk_days(40)
    choices = {d: ("SRH" if i < 20 else "SRM") for i, d in enumerate(days)}
    runs = stable_runs(days, choices, min_run_days=5)
    assert runs == [(days[0], "SRH"), (days[20], "SRM")]


def test_stable_runs_short_head_merges_forward():
    """Короткий ран в САМОМ начале вливается в следующий (метка — с первого дня)."""
    days = _mk_days(20)
    choices = {d: ("SRZ" if i < 2 else "SRH") for i, d in enumerate(days)}
    runs = stable_runs(days, choices, min_run_days=5)
    assert runs == [(days[0], "SRH")]


def test_stable_runs_none_days_skipped():
    days = _mk_days(10)
    choices = {d: "SRH" for d in days}
    choices[days[3]] = None
    runs = stable_runs(days, choices, min_run_days=5)
    assert runs == [(days[0], "SRH")]


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


def test_compute_windows_no_inverted_windows():
    """Ни одно окно не вырождено (start <= end) — иначе контракт «пропадал» бы."""
    for cl in (BR, SI):
        for w in compute_windows(cl):
            if w.start and w.end:
                assert w.start <= w.end, w


def test_duplicate_lsttrade_no_inverted_window():
    """Дубль lsttrade в одном sectype (грязные данные) → нет вырожденного окна,
    покрытие дат непрерывно."""
    dup = [_c("BRN6", "2026-07-01"), _c("BRQ6", "2026-07-01"), _c("BRU6", "2026-08-31")]
    w = compute_windows(dup)
    for x in w:
        if x.start and x.end:
            assert x.start <= x.end, x
    assert front_sec_id_for_day(w, date(2026, 7, 1)) is not None
    assert front_sec_id_for_day(w, date(2026, 8, 15)) == "BRU"


def test_none_lsttrade_among_regulars_skipped():
    """Регулярный контракт без lsttrade отбрасывается, окна не ломаются."""
    mixed = [_c("BRN6", "2026-07-01"),
             Contract("BRQ6", "BRQ", None, False, True),
             _c("BRU6", "2026-08-31")]
    w = compute_windows(mixed)
    assert [x.sec_id for x in w] == ["BRN", "BRU"]
    for x in w:
        if x.start and x.end:
            assert x.start <= x.end


def test_mixed_perpetual_and_regular_returns_only_perpetual():
    """Грязные данные: вечный + регулярные у одного sectype → только вечное окно."""
    mixed = [_c("BRN6", "2026-07-01"), _c("USDRUBF", None, perp=True), _c("BRQ6", "2026-08-03")]
    w = compute_windows(mixed)
    assert len(w) == 1 and w[0].is_perpetual and w[0].sec_id == "USDRUBF"


def test_resolve_day_zero_volume_calendar_candle_still_picked():
    """Календарный фронт берётся даже при объёме 0 (валиден close) — без преждеврем. ролла."""
    w = compute_windows(BR)
    day = date(2026, 7, 15)  # календарь говорит BRQ
    assert resolve_day(w, day, {"BRQ": 0.0, "BRX": 500.0}) == "BRQ"


def test_cross_year_same_sec_id_distinct_windows():
    """BRN6 и BRN7 (один sec_id 'BRN', разные годы) → два разных окна по датам."""
    cross = [_c("BRN6", "2026-07-01"), _c("BRZ6", "2026-12-01"), _c("BRN7", "2027-07-01")]
    w = compute_windows(cross)
    assert [x.secid for x in w] == ["BRN6", "BRZ6", "BRN7"]
    assert front_sec_id_for_day(w, date(2026, 6, 1)) == "BRN"   # 2026 июль-контракт
    assert front_sec_id_for_day(w, date(2027, 6, 1)) == "BRN"   # 2027 июль-контракт
    assert w[0].secid == "BRN6" and w[2].secid == "BRN7"


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
