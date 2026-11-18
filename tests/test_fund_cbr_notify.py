"""Юнит-тесты рубрик «Сделки фондов» и «Потоки капитала» в боте (signals/fund_trades_notify.py,
signals/cbr_flows_notify.py): форматы, карточки, выводы — без сети, БД и докера.

    pytest tests/test_fund_cbr_notify.py
"""
import datetime as dt
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from signals import cbr_flows_notify as cb  # noqa: E402
from signals import fund_trades_notify as ft  # noqa: E402


def test_money_like_post():
    assert ft.money(684e6) == ("+684", "млн ₽")
    assert ft.money(-1.66e9) == ("−1,7", "млрд ₽")


def test_month_cards_stocks_and_bonds():
    d = {"funds": 18, "buy": [{"asset": "ГАЗПРОМ ао", "isin": "RU0007661625", "amount": 9e8}],
         "sell": [{"asset": "ЯНДЕКС", "isin": "RU000A107T19", "amount": -1.7e9}],
         "portfolio": [{"asset": "ЛУКОЙЛ", "isin": "RU0009024277", "weight": 11.8}]}
    c = ft.month_cards("stocks", "2026-10", d)
    assert [x["kind"] for x in c] == ["cover", "list", "list", "tiles"]
    assert c[0]["pill"] == "октябрь 2026"
    assert c[1]["sub"] == "Больше всего денег фонды акций вложили в октябре"
    assert c[2]["rows"][0]["color"] == "down"
    b = ft.month_cards("bonds", "2026-10", {**d, "portfolio": None})
    assert len(b) == 3 and b[0]["pill"].startswith("облигации")
    assert "по всем 18 фондам" in ft.month_caption("stocks", "2026-10", d)


def test_fresh_months_only():
    assert ft.fresh("2026-10", dt.date(2026, 11, 18))
    assert not ft.fresh("2025-03", dt.date(2026, 11, 18))


def test_cbr_fmt_and_span():
    assert cb.fmt(-0.04) == "0,0"
    assert cb.fmt(6.94) == "+6,9"
    assert cb.span(5) == "за последние полгода"
    assert cb.span(11) == "за последний год"
    assert cb.span(3) is None
    assert cb.span(None) == "за всю историю данных"


def test_cbr_bullets():
    months = [f"2026-{m:02d}-28" for m in range(1, 11)]
    F = {"stocks": {"Физические лица": dict(zip(months, [5] * 8 + [29.0, 6.9])),
                    "НФО": dict(zip(months, [1] * 10))},
         "ofz": {"СЗКО": dict(zip(months, [-50] * 9 + [-160.0])), "НФО": dict(zip(months, [10] * 9 + [90.0]))}}
    pts = cb.bullets(F, months[-1], months[-2])
    assert pts[0] == "Физлица сократили покупки акций более чем в 4 раза — с 29,0 до 6,9 млрд ₽."
    assert any(p.startswith("Системные банки продали ОФЗ на 160,0 млрд ₽ — больше, чем за любой месяц") for p in pts)
    assert len(pts) <= 4


def test_cbr_rows_sorted_with_prev_note():
    F = {"stocks": {"Физические лица": {"2026-09-30": 29.0, "2026-10-31": -9.2}, "НФО": {"2026-09-30": -5.1, "2026-10-31": 12.0}}}
    r = cb.rows(F, "stocks", "2026-10-31", "2026-09-30", "сентябре")
    assert [x["name"] for x in r] == ["НФО", "Физлица"]
    assert r[1]["note"] == "в сентябре +29,0" and r[1]["color"] == "down"
