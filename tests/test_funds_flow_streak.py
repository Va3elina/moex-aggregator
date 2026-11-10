#!/usr/bin/env python3
"""Юнит-тесты серий притоков и оттоков «Денег в фондах» (signals/funds_flow_streak.py). Чистая логика без БД.

Запуск:
    pytest tests/test_funds_flow_streak.py
"""
import os
import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
os.environ.setdefault("DB_URL", "postgresql+pg8000://ci:ci@127.0.0.1:1/ci")

from signals import funds_flow_streak as fs  # noqa: E402

S = {"cat": "stocks", "sub": "Авторские", "name": "Акции · Авторские", "ids": [4, 5]}


def _bars(flows, start=date(2020, 1, 31), step=31):
    return [{"period_end": (start + timedelta(days=i * step)).isoformat(), "flow": f} for i, f in enumerate(flows)]


def test_milestones_month():
    ev = fs.events(_bars([1.0] * 2 + [-1.0] * 7), "1m")
    assert [(e["kind"], e["side"], e["n"]) for e in ev] == [("streak", "out", 3), ("streak", "out", 6)]
    assert not any(e["record"] for e in ev)          # прежних серий оттока не было — не рекорд


def test_zero_breaks_streak():
    ev = fs.events(_bars([-1.0, -1.0, 0.0, -1.0, -1.0, -1.0]), "1m")
    assert [(e["i"], e["n"]) for e in ev] == [(5, 3)]


def test_record_once_and_merged_with_milestone():
    # серия оттока 4 месяца, разрыв, потом 7: рекорд на 5-м (4+1), отметки на 3 и 6, рекорд не повторяется
    flows = [1.0] * 12 + [-1.0] * 4 + [1.0] + [-1.0] * 7
    ev = [e for e in fs.events(_bars(flows), "1m") if e["kind"] == "streak" and e["side"] == "out"]
    assert [(e["n"], e["record"]) for e in ev] == [(3, False), (3, False), (5, True), (6, False)]


def test_record_needs_history():
    flows = [-1.0] * 3 + [1.0] + [-1.0] * 4          # 8 месяцев истории < MIN_HISTORY_DAYS
    assert not any(e.get("record") for e in fs.events(_bars(flows), "1m"))


def test_break_after_long_streak_only():
    ev = fs.events(_bars([-1.0] * 6 + [1.0] + [-1.0] * 5 + [1.0]), "1m")
    br = [e for e in ev if e["kind"] == "break"]
    assert [(e["i"], e["side"], e["n"]) for e in br] == [(6, "in", 6)]


def test_closed_drops_last():
    assert fs.closed(_bars([1.0, 2.0, 3.0]), "1w") == _bars([1.0, 2.0])


def test_message_texts():
    x = {**S, "tf": "1m", "kind": "streak", "side": "out", "n": 6, "record": False}
    assert "Отток идёт 6 месяцев подряд" in fs.message(x)
    x = {**S, "tf": "1w", "kind": "streak", "side": "in", "n": 21, "record": True, "since": 2020}
    m = fs.message(x)
    assert m.startswith("🔥 ") and "Приток идёт 21 неделю подряд" in m and "с 2020 года" in m
    x = {**S, "tf": "1d", "kind": "break", "side": "in", "n": 21}
    assert "Приток после 21 рабочего дня оттока" in fs.message(x)
    x = {**S, "tf": "1d", "kind": "streak", "side": "out", "n": 22, "record": False}
    assert "Отток идёт 22 рабочих дня подряд" in fs.message(x)
    assert "tf=1d" in fs.message(x) and "period=1y" in fs.message(x)


def test_senior_only():
    a = {**S, "tf": "1d", "side": "out"}
    b = {**S, "tf": "1m", "side": "out"}
    c = {**S, "tf": "1w", "side": "in"}
    assert fs.senior_only([a, b, c]) == [b, c]


def test_pending_seeds_silently_then_reports_new(monkeypatch):
    bars = _bars([1.0] * 12 + [-1.0] * 3 + [1.0])   # последний столбец открыт и отрезается
    monkeypatch.setattr(fs, "slice_bars", lambda cats, raw: [(S, "1m", fs.closed(bars, "1m"))])
    new, upd = fs.pending({}, [], {})
    assert new == [] and upd == {"stocks|Авторские|1m": bars[-2]["period_end"]}
    new, _ = fs.pending({}, [], {"stocks|Авторские|1m": bars[-3]["period_end"]})
    assert [(x["side"], x["n"]) for x in new] == [("out", 3)]
    new, _ = fs.pending({}, [], upd)
    assert new == []
