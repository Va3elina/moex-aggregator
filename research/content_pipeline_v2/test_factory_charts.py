"""Картинки завода (08.10.2026): каждый тип графика рисуется (фонды, сделки фондов, макро — простые технические 1200×675,
позиции и сезонность — прежний вид); склейка ближнего фьючерса для цены без базового актива (BR, NG, металлы); в
подписи графика для писателя нет «None».
Без сети и базы: данные подставные.

python3 -m pytest research/content_pipeline_v2/test_factory_charts.py -q
"""
import datetime as dt
import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DB_URL", "postgresql+pg8000://x:y@127.0.0.1:1/x")
from signals.insights import cards, charts, data as dbdata  # noqa: E402


def _png_size(path):
    from PIL import Image
    with Image.open(path) as im:
        return im.size


def _line2():
    d = pd.bdate_range("2023-10-02", "2026-10-07")
    y = 50_000 + 30_000 * np.sin(np.arange(len(d)) / 60)
    p = 70 + 8 * np.cos(np.arange(len(d)) / 45)
    return {"type": "line2", "title": "Шорт физлиц, фьючерс на нефть Brent", "x": d, "y": y, "y_label": "контрактов",
            "marks": [(d[-1], y[-1])], "x2": d, "y2": p, "y2_label": "фьючерс на нефть Brent"}


def _charts():
    d = pd.bdate_range("2025-06-02", "2026-10-06")
    months = pd.date_range("2024-05-01", "2026-10-01", freq="MS")
    xs = pd.date_range("2026-01-01", periods=366, freq="D")
    ytd = pd.bdate_range("2026-01-05", "2026-10-07")
    x5 = pd.date_range("2026-10-08 06:00", periods=120, freq="5min")
    return {
        "line2": _line2(),
        "bars": {"type": "bars", "title": "Все биржевые фонды: приток и отток по месяцам, млрд ₽ — октябрь "
                                          "(неполный: к 6 октября) +23", "x": months,
                 "y": np.r_[np.linspace(-20, 120, len(months) - 1), 23.0], "current": True},
        "flow": {"type": "flow", "title": "Все биржевые фонды: приток за 5 торговых дней, млрд ₽ — сейчас +25, больше "
                                          "всего с 15 июня", "x": d, "y": 10 * np.sin(np.arange(len(d)) / 20) + 5},
        "hbars": {"type": "hbars", "title": "Фонды акций за август: крупнейшие покупки и продажи, млрд ₽",
                  "labels": ["Лукойл", "Татнефть", "X5", "Полюс"], "values": [1.56, 0.5, -0.93, -0.46]},
        "season": {"type": "season", "title": "Сезонность: индекс Мосбиржи, средний путь 2016-2025 и 2026 год",
                   "x": xs, "y": 3 * np.sin(np.arange(366) / 58), "x2": ytd, "y2": np.linspace(0, -8, len(ytd)),
                   "now": ytd[-1], "y_label": "средний путь, %", "y2_label": "2026, % с начала года",
                   "marks": [(xs[100], 2.0)]},
        "intraday": {"type": "intraday", "title": "Вечный фьючерс на индекс Мосбиржи по 5 минутам, 8 октября",
                     "x": x5.values, "y": np.linspace(2300, 2280, 120), "event": x5[30]},
    }


@pytest.mark.parametrize("typ", ["line2", "bars", "flow", "hbars", "season", "intraday"])
def test_every_chart_type_draws(tmp_path, typ):
    kind = {"bars": "funds", "flow": "funds", "hbars": "fund_trades", "season": "seasonality",
            "intraday": "macro"}.get(typ, "positions")
    out = tmp_path / f"{typ}.png"
    cards.draw_chart({"kind": kind, "chart": _charts()[typ]}, str(out))
    assert out.stat().st_size > 10_000
    if typ in ("bars", "flow", "hbars", "intraday"):
        assert _png_size(out) == (1200, 675), "простые графики фондов и макро — 1200×675"


def test_line2_without_price_draws(tmp_path):
    ch = {k: v for k, v in _line2().items() if k not in ("x2", "y2", "y2_label")}
    out = tmp_path / "bare.png"
    cards.draw_chart({"kind": "positions", "chart": ch}, str(out))
    assert out.stat().st_size > 10_000


def test_month_ticks_are_russian():
    ticks = charts.date_ticks(pd.Timestamp("2026-07-01"), pd.Timestamp("2026-10-08"))
    assert [t[1] for t in ticks] == ["июл 26", "авг 26", "сен 26", "окт 26"]
    assert charts.date_ticks(pd.Timestamp("2026-10-08 07:00"), pd.Timestamp("2026-10-08 13:00"),
                             intraday=True)[0][1] == "07:00"


def test_front_month_splice_takes_nearest_live_contract():
    d1, d2 = dt.date(2026, 10, 1), dt.date(2026, 11, 3)
    rows = [(d1, 66.0, dt.date(2026, 11, 2), False), (d1, 65.0, dt.date(2026, 12, 1), False),
            (d2, 64.0, dt.date(2026, 11, 2), False),            # контракт истёк — не берём
            (d2, 63.5, dt.date(2026, 12, 1), False), (d2, 70.0, None, True)]
    s = dbdata.splice_front(rows)
    assert list(s.values) == [66.0, 63.5] and list(s.index) == [pd.Timestamp(d1), pd.Timestamp(d2)]


def test_price_for_falls_back_to_front_month(monkeypatch):
    """Brent: базового ряда нет — цена из склейки фьючерса, в долларах (не «₽»)."""
    d = pd.bdate_range("2026-01-05", periods=60)
    monkeypatch.setattr(cards, "data", lambda: ({}, {}, {}, {}, {}, pd.DataFrame(), {}, None))
    monkeypatch.setattr(dbdata, "front_month", lambda s: pd.Series(np.linspace(60, 66, 60), index=d))
    label, ser = cards.price_for("BR")
    assert label == "фьючерс на нефть Brent" and len(ser) == 60
    assert cards.px_ru(65.4, label) == "$65,40"


def test_chart_note_has_no_none(monkeypatch):
    d = pd.bdate_range("2023-01-02", periods=700)
    pos = pd.Series(np.r_[np.linspace(10, 100, 350), np.linspace(100, 60, 350)] * 1000, index=d)
    monkeypatch.setattr(cards, "data", lambda: ({"short": {"BR": pos}}, {}, {}, {}, {}, {}, {}, None))
    monkeypatch.setattr(cards, "human_name", lambda sec: ("фьючерс на нефть Brent", "фьючерсу на нефть Brent"))
    monkeypatch.setattr(cards, "price_for", lambda sec: (None, None))
    monkeypatch.setattr(cards, "intraday_now", lambda *a: None)
    monkeypatch.setattr(cards, "other_legs", lambda *a: [])
    monkeypatch.setattr(cards, "position_angles", lambda *a: [])
    c = cards.positions_card("BR", "short", d[-1])
    text = cards.brief_text(c)
    assert "None" not in text, "без цены в подписи графика стояло «и None (серая)»"
    note = " ".join(c["chart_note"])
    assert "оранжевая линия" in note and "серая" not in note, "цвета — как на прежнем графике; цены нет — нет и слова"


def test_funds_month_bars_sit_on_their_month():
    """resample("ME") давал конец месяца — столбец уезжал на следующий месяц. Теперь x — первое число месяца."""
    idx = pd.bdate_range("2024-01-02", "2026-10-06")
    daily = pd.DataFrame({"all": np.where(np.arange(len(idx)) % 7 == 0, -1e9, 2e9)}, index=idx)
    nav = pd.DataFrame({"all": np.linspace(1e12, 3.5e12, len(idx))}, index=idx)
    import signals.insights.cards as c_mod
    orig = (c_mod.data, c_mod.funds_data)
    try:
        c_mod.data = lambda: ({}, {}, {}, {}, {}, {}, {}, None)
        c_mod.funds_data = lambda: (daily, nav)
        c = cards.funds_card("all", "2026-10-06")
        x = pd.DatetimeIndex(c["chart"]["x"])
        assert (x.day == 1).all() and x[-1] == pd.Timestamp("2026-10-01")
        assert c["chart"]["type"] == "bars" and "неполный: к 6 октября" in c["chart"]["title"]
        c5 = cards.funds_card("all", "2026-10-06", leg="5d")
        assert c5["chart"]["type"] in ("flow", "bars")
        if c5["chart"]["type"] == "flow":
            assert "за 5 торговых дней" in c5["chart"]["title"] and "сейчас" in c5["chart"]["title"]
    finally:
        c_mod.data, c_mod.funds_data = orig


def test_funds_n_day_finding_drives_brief_and_picture(monkeypatch):
    """#3927: находка «приток за 5 дней — рекорд с 15.06», а бриф начинался с «приток с начала октября». Теперь
    заголовок, первый факт и картинка — о пятидневной сумме; месяц — фоном."""
    idx = pd.bdate_range("2024-01-02", "2026-10-06")
    v = np.full(len(idx), 1e9)
    spike = idx.get_loc(pd.Timestamp("2026-06-15"))
    v[spike - 4:spike + 1] = 6e9           # прошлый уровень выше нынешнего — «больше всего с 15 июня»
    v[spike + 1:spike + 5] = 0.0           # 16 июня пятидневка уже ниже нынешней
    v[-5:] = 5e9
    daily = pd.DataFrame({"all": v}, index=idx)
    nav = pd.DataFrame({"all": np.linspace(1e12, 3.5e12, len(idx))}, index=idx)
    monkeypatch.setattr(cards, "data", lambda: ({}, {}, {}, {}, {}, {}, {}, None))
    monkeypatch.setattr(cards, "funds_data", lambda: (daily, nav))
    c = cards.build_card({"kind": "funds", "cat": "all", "leg": "5d"}, "2026-10-06")
    assert c["headline"] == "Все биржевые фонды: приток за 5 торговых дней - 25 млрд ₽, больше всего с 15 июня"
    assert c["facts"][0].startswith("приток за 5 торговых дней - 25 млрд ₽")
    assert any(f.startswith("приток с начала октября") for f in c["facts"]), "месяц остаётся фоном"
    assert c["chart"]["type"] == "flow" and "за 5 торговых дней" in c["chart"]["title"]
    assert "за 5 торговых дней" in c["chart_note"][0]
    assert cards.focus_lines(c)[0] == f"находка: {c['headline']}"
    plain = cards.build_card({"kind": "funds", "cat": "all"}, "2026-10-06")
    assert plain["chart"]["type"] == "bars", "без окна находки — столбцы по месяцам, как раньше"


def test_surge_chart_is_three_months_old_look():
    d = pd.bdate_range("2023-10-02", "2026-10-07")
    card = {"headline": "Шорт физлиц по фьючерсу на нефть Brent - 101 тыс. контрактов", "as_of": d[-1],
            "spec": {"kind": "positions", "sec": "BR", "leg": "short"},
            "angles": [{"type": "рывок", "strength": 8.3, "live": True,
                        "line": "рывок по ходу дня: шорт физлиц по фьючерсу на нефть Brent к 12:55 8 октября +66%",
                        "point": (pd.Timestamp("2026-10-08"), 168_000.0, "12:55")}],
            "facts": [], "price": [], "limits": [],
            "chart": {"type": "line2", "x": d, "y": np.linspace(50e3, 101e3, len(d)), "x2": d,
                      "y2": np.linspace(70, 65, len(d)), "y2_label": "фьючерс на нефть Brent",
                      "marks": [(d[10], 52e3), (d[-1], 101e3)]},
            "chart_note": ["на графике - шорт физлиц по фьючерсу на нефть Brent за три года (оранжевая линия)"]}
    c = cards.angle_card(card, "рывок")
    ch = c["chart"]
    assert (ch["x"][-1] - ch["x"][0]).days <= cards.SURGE_WINDOW_DAYS
    assert ch["y"][-1] == 168_000.0 and (pd.DatetimeIndex(ch["x2"]) >= ch["x"][0]).all()
    assert ch["marks"] == [(d[-1], 101e3), (pd.Timestamp("2026-10-08"), 168_000.0)], "старые пики за окном не рисуем"
    assert "за три месяца" in c["chart_note"][0]
