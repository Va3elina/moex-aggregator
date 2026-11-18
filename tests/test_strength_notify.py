"""Юнит-тесты сигналов «Силы рынка» (signals/strength_notify.py): правила событий и карточка, без сети и докера.

    pytest tests/test_strength_notify.py
"""
import datetime as dt
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from signals import strength_notify as sn  # noqa: E402


def _rows(vals, start=dt.date(2024, 1, 1)):
    return [((start + dt.timedelta(days=i)).isoformat(), float(v), 2500.0) for i, v in enumerate(vals)]


def _events(vals):
    st, out = {}, []
    for d, v, _ in _rows(vals):
        out += sn.step(st, d, v)
    return out


def test_edge_fires_once_until_exit_and_pause():
    # зашли в край, болтаемся у порога — одно событие; вышли (>20) и через 40 дней снова — второе
    vals = [50] * 10 + [14, 16, 14, 13] + [30] * 40 + [12]
    ev = [e for e in _events(vals) if e["kind"] == "edge"]
    assert [e["side"] for e in ev] == ["low", "low"]


def test_edge_pause_blocks_quick_repeat():
    vals = [50] * 5 + [14] + [25] * 5 + [14]                 # вышли и вернулись за 6 дней — пауза 30 дней
    assert len([e for e in _events(vals) if e["kind"] == "edge"]) == 1


def test_regime_needs_both_sides():
    # 50 → 61: режима до этого не было — не событие; 61 → 39 — «меньше половины»; 39 → 60 — «больше половины»
    ev = [e for e in _events([50, 61, 55, 45, 39, 50, 60]) if e["kind"] == "regime"]
    assert [e["side"] for e in ev] == ["down", "up"]


def test_card_period_and_peaks():
    vals = [50] * 100 + [10] + [50] * 500 + [12]               # прошлый край 500 дней назад → 5 лет
    rows = _rows(vals)
    e = [x for x in _events(vals) if x["kind"] == "edge"][-1]
    c = sn.card(rows, e)
    assert c["meta"]["details"][1] == "5 лет"
    assert c["signal"]["level"] == sn.LOW
    assert c["synced"][c["signal"]["peaks"][0]["i"]]["breadth"] == 10.0
    assert c["signal"]["headline"] == "только 12% акций выше EMA200"
    vals = [50] * 100 + [10] + [50] * 100 + [12]               # прошлый край в пределах года → 1 год
    rows = _rows(vals)
    e = [x for x in _events(vals) if x["kind"] == "edge"][-1]
    assert sn.card(rows, e)["meta"]["details"][1] == "1 год"
    assert "Прошлый раз" in sn.message(rows, e)


def test_partial_failure_no_duplicate(monkeypatch, tmp_path):
    # день с двумя событиями (край и смена режима): второе не дошло — повтор шлёт только его
    monkeypatch.setattr(sn, "STATE_FILE", str(tmp_path / "st.json"))
    monkeypatch.setattr(sn, "STATE_DIR", str(tmp_path))
    monkeypatch.setattr(sn.time, "sleep", lambda s: None)
    monkeypatch.setattr(sn, "picture", lambda rows, e: None)
    vals = [70] * 40 + [10]                                        # с 70 сразу в 10: край low + режим down
    rows = _rows(vals)
    monkeypatch.setattr(sn, "load", lambda: rows)
    today = dt.date.fromisoformat(rows[-1][0]) + dt.timedelta(days=1)
    monkeypatch.setattr(sn, "load", lambda: rows[:-1])
    sn.run_once(today=today)                                       # первый прогон — молча, до предпоследнего дня
    monkeypatch.setattr(sn, "load", lambda: rows)
    sent = []
    monkeypatch.setattr(sn, "send", lambda msg, tag, **kw: (sent.append(msg), "половины" not in msg)[1])
    sn.run_once(today=today)
    assert len(sent) == 2
    sent.clear()
    monkeypatch.setattr(sn, "send", lambda msg, tag, **kw: (sent.append(msg), True)[1])
    sn.run_once(today=today)
    assert len(sent) == 1 and "половины" in sent[0]
    sent.clear()
    sn.run_once(today=today)
    assert sent == []
