"""Юнит-тесты картинок к сигналам фондов и сезонности (signals/funds_card.py): чистая логика без сети и докера.

    pytest tests/test_funds_card.py
"""
import datetime as dt
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from signals import funds_card as fc  # noqa: E402


def _bars(flows, start=dt.date(2020, 1, 3), step=7):
    out = []
    for i, f in enumerate(flows):
        d = start + dt.timedelta(days=i * step)
        out.append({"period_start": (d - dt.timedelta(days=step - 1)).isoformat(), "period_end": d.isoformat(),
                    "flow": f, "gross_in": max(f, 0), "gross_out": min(f, 0), "flow_pct": 0})
    return out


def _x(**kw):
    return {"cat": "bonds", "sub": "Короткие", "name": "Облигации · Короткие", "tf": "1w", **kw}


def test_money_and_plural():
    assert fc.money(-5.3332) == "−5,3 млрд"
    assert fc.money(285.4) == "+285 млрд"
    assert fc.plural(21, fc.UNIT["1w"]) == "неделя"
    assert fc.plural(22, fc.UNIT["1w"]) == "недели"
    assert fc.plural(27, fc.UNIT_GEN["1w"]) == "недель"
    assert fc.plural(11, fc.UNIT["1m"]) == "месяцев"


def test_tiny_record_next_to_big_inflows_zooms_in(monkeypatch):
    # Вадим 18.11: отток −5 рядом с притоками в +24 на «Всё» по неделям — палочка, надо вглядываться.
    # Прежний рекорд оттока — в 2020-м, на «Всё» сигнал не читается (столбиков > 160) → 1 год, без прежнего рекорда.
    monkeypatch.setattr(fc, "_index", lambda *a: [])
    flows = [0.1] * 300
    flows[40] = -6.0                                  # прежний рекорд оттока
    for i in range(250, 299):
        flows[i] = 20.0 + (i % 5)
    flows[-1] = -6.5
    card = fc.flow_record(_x(side="out", window="за 5 лет"), _bars(flows))
    assert card["meta"]["details"][2] == "1Г"
    assert "prev" not in card["signal"]
    assert card["signal"]["headline"] == "рекордный отток за 5 лет"
    n = len(card["flows"]["flows"])
    assert card["signal"]["hl"] == [n - 1, n - 1]
    assert card["signal"]["amount"] == "−6,5 млрд"


def test_prev_record_kept_when_readable(monkeypatch):
    # прежний рекорд 2 года назад, столбики соизмеримы → 3 года, кружок на прежнем рекорде с месяцем и годом
    monkeypatch.setattr(fc, "_index", lambda *a: [])
    flows = [1.0] * 200
    flows[100] = 11.0
    flows[-1] = 10.0
    b = _bars(flows)
    card = fc.flow_record(_x(side="in", window="за 2 года"), b)
    assert card["meta"]["details"][2] == "3Г"
    view = card["flows"]["flows"]
    assert view[card["signal"]["prev"]]["flow"] == 11.0
    assert card["signal"]["prevLabel"] == fc.month(b[100]["period_end"])


def test_streak_frames_whole_run_and_break_frames_new_bar(monkeypatch):
    monkeypatch.setattr(fc, "_index", lambda *a: [])
    flows = [3.0] * 60 + [-2.0] * 21
    b = _bars(flows)
    card = fc.flow_streak(_x(side="out", kind="streak", n=21, i=80, run_sum=-42.0), b)
    assert card["signal"]["headline"] == "21 неделя оттока подряд"
    lo, hi = card["signal"]["hl"]
    assert hi - lo == 20 and card["flows"]["flows"][lo]["flow"] == -2.0
    assert card["signal"]["amount"] == "−42 млрд"
    b2 = _bars(flows + [1.5])
    card = fc.flow_streak(_x(side="in", kind="break", n=21, i=81), b2)
    assert card["signal"]["headline"] == "приток после 21 недели оттока"
    assert card["signal"]["hl"][0] == card["signal"]["hl"][1]
    assert card["signal"]["amount"] == "+1,5 млрд"


def test_period_must_contain_whole_streak(monkeypatch):
    # серия длиннее года → период 3 года, хотя 1 год короче
    monkeypatch.setattr(fc, "_index", lambda *a: [])
    flows = [3.0] * 100 + [-1.0] * 70
    card = fc.flow_streak(_x(side="out", kind="streak", n=70, i=169, run_sum=-70.0), _bars(flows))
    assert card["meta"]["details"][2] == "3Г"


def test_season_frame_from_trading_day(monkeypatch):
    y = {"average": [{"td": i, "avg_pct": 0} for i in range(252)], "max_trading_days": 252,
         "current": [{"td": 221, "date": "2026-11-16", "pct": 1.0}], "years_range": "2007-2025"}

    class R:
        def json(self):
            return y
    monkeypatch.setattr(fc.requests, "get", lambda *a, **k: R())
    card = fc.season({"code": "CHMF", "name": "Северсталь", "dir": "up", "days": 91, "hits": 14, "years": 18},
                     dt.date(2026, 11, 16))
    assert abs(card["signal"]["from"] - 221 / 251) < 1e-9
    assert card["signal"]["to"] > 1                    # окно переходит через Новый год
    assert card["signal"]["headline"] == "рост в 14 из 18 лет"
    assert card["meta"]["details"] == ["Годовая", "2007–2025"]


def test_picture_never_raises(monkeypatch):
    monkeypatch.setattr(fc, "_index", lambda *a: (_ for _ in ()).throw(RuntimeError("сеть")))
    assert fc.picture("flow_record", _x(side="in", window="за год"), _bars([1.0, 2.0])) is None
