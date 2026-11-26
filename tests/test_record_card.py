"""Юнит-тесты картинки к рекорду позиции (signals/record_card.py): чистая логика без сети и докера.

    pytest tests/test_record_card.py
"""
import datetime as dt
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from signals import record_card as rc  # noqa: E402


def _series(vals, start=dt.date(2024, 1, 1)):
    line = [{"time": f"{start + dt.timedelta(days=i)}T00:00:00", "value": v} for i, v in enumerate(vals)]
    price = [{"time": p["time"], "value": 100.0} for p in line]
    return price, line


def test_front_ticker_like_site_header():
    sw = [{"date": "2026-09-18", "from": "DRU", "to": "DRZ"}]
    assert rc.front_ticker("DR", sw, dt.date(2026, 11, 18)) == "DRZ6"
    assert rc.front_ticker("DR", [{"to": "DRH"}], dt.date(2026, 12, 20)) == "DRH7"   # март — уже следующего года
    assert rc.front_ticker("DR", [], dt.date(2026, 11, 18)) == "DR"


def test_period_follows_prior_higher_point(monkeypatch):
    # было выше ~300 дней назад → период «1г», главный прежний пик — та самая точка
    vals = [10.0] * 700
    vals[400] = 50.0
    vals[-1] = 45.0
    monkeypatch.setattr(rc, "_series", lambda sec, mode: (*_series(vals), []))
    day = dt.date(2024, 1, 1) + dt.timedelta(days=699)
    card = rc.build({"sec": "XX", "name": "Тест", "leg": "netl", "window": "за год"}, 45.0, day)
    assert card["meta"]["details"][1] == "1г"
    assert card["signal"]["headline"] == "рекордный лонг за год"
    assert card["signal"]["peaks"][0]["value"] == 50.0


def test_all_time_takes_whole_history(monkeypatch):
    vals = [10.0] * 700
    vals[200] = 30.0
    vals[-1] = 60.0
    monkeypatch.setattr(rc, "_series", lambda sec, mode: (*_series(vals), []))
    day = dt.date(2024, 1, 1) + dt.timedelta(days=699)
    card = rc.build({"sec": "XX", "name": "Тест", "leg": "nets", "window": "за всё время"}, -60.0, day)
    assert card["meta"]["details"][1] == "Всё" and card["signal"]["dir"] == "low"


def test_record_point_not_duplicated(monkeypatch):
    # последняя точка сайта уже равна рекорду (другая дата) — вторую не дописываем, прошлый пик не теряется
    vals = [10.0] * 700
    vals[400] = 50.0
    vals[-1] = 45.0
    monkeypatch.setattr(rc, "_series", lambda sec, mode: (*_series(vals), []))
    day = dt.date(2024, 1, 1) + dt.timedelta(days=700)
    card = rc.build({"sec": "XX", "name": "Тест", "leg": "netl", "window": "за год"}, 45.0, day)
    assert len(card["line"]) == len(card["price"]) and card["line"][-1]["value"] == 45.0
    assert card["meta"]["details"][1] == "1г"


def test_cases_widen_period_and_skip_out_of_range(monkeypatch):
    # случаи из таблицы истории — кружками на графике; самый старый за год → период 5л; случай раньше начала
    # графика не рисуется в первую точку
    vals = [10.0] * 1500
    vals[1300] = 50.0                                      # прошлый раз выше — внутри года
    vals[-1] = 45.0
    monkeypatch.setattr(rc, "_series", lambda sec, mode: (*_series(vals), []))
    day = dt.date(2024, 1, 1) + dt.timedelta(days=1499)
    base = dt.date(2024, 1, 1)
    cases = [base - dt.timedelta(days=400), base + dt.timedelta(days=900), base + dt.timedelta(days=1300)]
    card = rc.build({"sec": "XX", "name": "Тест", "leg": "netl", "window": "за год"}, 45.0, day, cases)
    assert card["meta"]["details"][1] == "5л"
    got = [p["date"] for p in card["signal"]["peaks"]]
    assert got == [str(base + dt.timedelta(days=1300)), str(base + dt.timedelta(days=900))]
    assert card["signal"]["cases"] is True


def test_short_window_marks_only_current_point(monkeypatch):
    # рекорд за 3 месяца / полгода — без прежних кружков и пунктира, даже если есть случаи истории (Вадим 25.11)
    vals = [10.0] * 700
    vals[500] = 50.0
    vals[-1] = 45.0
    monkeypatch.setattr(rc, "_series", lambda sec, mode: (*_series(vals), []))
    day = dt.date(2024, 1, 1) + dt.timedelta(days=699)
    cases = [dt.date(2024, 1, 1) + dt.timedelta(days=300)]
    for win, period in (("за 3 месяца", "6м"), ("за полгода", "1г")):
        card = rc.build({"sec": "XX", "name": "Тест", "leg": "netl", "window": win}, 45.0, day, cases)
        assert card["signal"]["peaks"] == [] and card["signal"]["cases"] is False
        assert card["meta"]["details"][1] == period
        assert card["signal"]["headline"] == f"рекордный лонг {win}"
