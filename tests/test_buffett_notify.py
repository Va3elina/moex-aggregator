"""Юнит-тесты сигналов Индикатора Баффетта (signals/buffett_notify.py): рекорды, пауза, карточка — без сети.

    pytest tests/test_buffett_notify.py
"""
import datetime as dt
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from signals import buffett_notify as bn  # noqa: E402


def _rows(vals, start=dt.date(2000, 1, 31)):
    out, d = [], start
    for v in vals:
        out.append((d.isoformat(), float(v), 50.0))
        d = (d.replace(day=1) + dt.timedelta(days=62)).replace(day=1) - dt.timedelta(days=1)
    return out


def test_closed_drops_current_month():
    rows = [("2026-10-31", 23.0, 50.0), ("2026-11-18", 22.0, 50.0)]
    assert bn.closed(rows, dt.date(2026, 11, 18)) == rows[:1]


def test_record_window_and_ref():
    vals = [30] * 24 + [18] + [30] * 30 + [19]          # прошлый раз ниже — 31 месяц назад
    rows = _rows(vals)
    rec = {s: (w, ref) for s, w, ref in bn.record(rows, len(rows) - 1)}
    assert rec["min"][0] == "за 2 года"
    assert rows[rec["min"][1]][1] == 18.0


def test_pause_and_longer_window():
    sent = {"min": {"day": "2026-08-31", "rank": bn.RANK["за год"]}}
    assert not bn.due("min", "за год", "2026-09-30", sent)       # та же глубина через месяц — молчим
    assert bn.due("min", "за 5 лет", "2026-09-30", sent)         # глубже — сразу
    assert bn.due("min", "за год", "2026-12-31", sent)           # через 3+ месяца — снова


def test_phrase_since_year_for_old_ref():
    vals = [10] + [40] * 120 + [12]                            # ниже было 10 лет назад
    rows = _rows(vals)
    i = len(rows) - 1
    side, w, ref = [r for r in bn.record(rows, i) if r[0] == "min"][0]
    assert w == "за 5 лет"
    assert bn.phrase(rows, i, side, w, ref) == "минимум с 2000 года"
    c = bn.card(rows, i, side, w, ref)
    assert c["meta"]["details"][0] == "20Л"          # 10 лет назад + запас → 20 лет
    assert c["signal"]["peaks"][0]["value"] == 10.0
