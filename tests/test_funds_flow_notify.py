#!/usr/bin/env python3
"""Юнит-тесты рекордных потоков «Денег в фондах» (signals/funds_flow_notify.py) и общего расчёта графика
(api/services/fund_flows.py). Чистая логика без БД.

Запуск:
    pytest tests/test_funds_flow_notify.py
"""
import os
import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
os.environ.setdefault("DB_URL", "postgresql+pg8000://ci:ci@127.0.0.1:1/ci")

from api.services import fund_flows as ff  # noqa: E402
from signals import funds_flow_notify as fn  # noqa: E402

CATS = {
    "bonds": {"name": "Облигации", "index": "RGBITR", "funds": {
        1: {"ticker": "A", "name": "A", "subcategory": "Длинные", "uk_id": "5"},
        2: {"ticker": "B", "name": "B", "subcategory": "Длинные", "uk_id": "5"},
        3: {"ticker": "C", "name": "C", "subcategory": "Короткие", "uk_id": "7"},
    }},
    "stocks": {"name": "Акции", "index": "IMOEX", "funds": {
        4: {"ticker": "D", "name": "D", "subcategory": "Управляемые фонды акций", "uk_id": "5"},
        5: {"ticker": "E", "name": "E", "subcategory": None, "uk_id": "5"},
    }},
}


def _bars(flows, start=date(2024, 1, 1), step=1):
    return [{"period_end": (start + timedelta(days=i * step)).isoformat(), "flow": f} for i, f in enumerate(flows)]


def test_fill_series_mirrors_router_sql():
    raw = [
        (1, date(2023, 12, 29), 100.0, 10.0),      # до date_from: протягивается на первую дату сетки
        (1, date(2024, 1, 3), None, None),
        (1, date(2024, 1, 5), 110.0, 10.5),
        (2, date(2024, 1, 2), 50.0, 1.0),
        (2, date(2024, 1, 4), 55.0, None),
    ]
    s = ff.fill_series(raw, [1, 2], date(2024, 1, 1))
    assert [d.day for d, _, _ in s[1]] == [2, 3, 4, 5]
    assert s[1][0] == (date(2024, 1, 2), 100.0, 10.0)
    assert s[1][1] == (date(2024, 1, 3), 100.0, 10.0)        # пустая строка → последнее непустое
    assert s[2][-1] == (date(2024, 1, 5), 55.0, 1.0)         # pay протянут отдельно от nav
    only2 = ff.fill_series(raw, [2], date(2024, 1, 1))
    assert [d.day for d, _, _ in only2[2]] == [2, 4]          # сетка — только даты выбранных фондов


def test_daily_flow_removes_revaluation():
    series = {1: [(date(2024, 1, 1), 100.0, 10.0), (date(2024, 1, 2), 120.0, 11.0)]}
    rows = ff.daily_rows(series, {})
    assert len(rows) == 1
    assert abs(rows[0][2] - 10.0) < 1e-9                       # +20 СЧА, из них +10 — рост пая


def test_weekly_slots_and_partial_last_week():
    d0 = date(2024, 1, 1)                                     # понедельник
    series = {1: [(d0 + timedelta(days=i), 1e9 * (1 + i), 1.0) for i in range(10)]}
    bars = ff.slot_flows(series, {}, "1w")
    assert len(bars) == 1                                     # первый слот без предыдущего не рисуется
    assert bars[0]["period_end"] == "2024-01-10" and bars[0]["flow"] == 3.0


def test_corrections_subtract_reorg_jump():
    cat = {7: {"ticker": "SBGD"}}
    series = {7: [(date(2024, 9, 2), 10e9, 1.0), (date(2024, 9, 3), 8.5e9, 1.0), (date(2024, 9, 4), 8.5e9, 1.0)]}
    corr = ff.corrections("gold", cat, series)
    assert corr == {(7, date(2024, 9, 3)): -1.5e9}
    assert abs(ff.daily_rows(series, corr)[0][2]) < 1e-6      # ликвидация части фонда — не отток


def test_slices_as_on_page():
    names = [s["name"] for s in fn.slices(CATS)]
    assert names == ["Облигации · все фонды", "Облигации · Длинные", "Облигации · Короткие",
                     "Акции · все фонды", "Акции · Управляемые фонды"]
    assert fn.slices(CATS)[1]["ids"] == [1, 2]


def test_bar_records_windows_and_sides():
    hist = [0.5] * 400
    assert fn.bar_records(_bars(hist + [1.0])) == [("in", "за всё время")]
    assert fn.bar_records(_bars(hist + [-1.0])) == [("out", "за всё время")]
    # приток больше, чем 200 дней назад, но не больше, чем 300 дней назад → рекорд за полгода
    flows = [0.1] * 400
    flows[100] = 2.0
    flows[200] = 1.5
    assert fn.bar_records(_bars(flows + [1.8])) == [("in", "за полгода")]
    assert fn.bar_records(_bars([0.1] * 100 + [5.0])) == []    # меньше года истории
    assert fn.bar_records(_bars(hist + [0.5])) == []           # повтор максимума — не рекорд


def test_mark_matches_record_notify():
    assert fn.mark("за 3 месяца") == ""
    assert fn.mark("за полгода") == "🔴 "
    assert fn.mark("за 5 лет") == "🔴🔴🔴🔴🔴 "
    assert fn.mark("за всё время") == "🔥 "


def test_message_and_link():
    x = {**fn.slices(CATS)[1], "tf": "1w", "side": "out", "window": "за 2 года", "day": date(2026, 10, 24)}
    msg = fn.message(x)
    assert msg.startswith("🔴🔴🔴 <b>Облигации · Длинные</b>\nОтток за неделю\nРекорд за 2 года\n")
    assert "/funds-money?category=bonds&amp;funds=1,2&amp;tf=1w&amp;period=3y" in msg
    assert "млрд" not in msg and "₽" not in msg                # цифр потока в сообщении нет


def test_cooldown_key_per_slice_side_horizon():
    x = {"cat": "bonds", "sub": "Длинные", "side": "in", "tf": "1d", "window": "за полгода"}
    assert fn._key(x) == "bonds|Длинные|in|1d"
    prev = {"day": "2026-10-20", "rank": fn.RANK["за полгода"]}
    assert not fn.due(x, prev, date(2026, 10, 24))
    assert fn.due({**x, "window": "за год"}, prev, date(2026, 10, 24))
    assert fn.due(x, prev, date(2026, 10, 20) + timedelta(days=fn.COOLDOWN_DAYS))
