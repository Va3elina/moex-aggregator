"""Сезонность по всем активам (правило витрины /hot, 08.10.2026): вход в условие и пауза 30 дней, склейка сплитов,
карточка по акции рисуется. Без сети и базы: данные подставные.

python3 -m pytest research/content_pipeline_v2/test_season_all.py -q
"""
import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DB_URL", "postgresql+pg8000://x:y@127.0.0.1:1/x")
from signals.insights import cards, detect as det, season  # noqa: E402


def _seasonal(start="2010-01-04", end="2026-10-07", split=None):
    """Акция, которая каждый год растёт с октября по декабрь (~+15%), остальное время стоит."""
    d = pd.bdate_range(start, end)
    doy = d.dayofyear.values
    g = np.where((doy >= 274) & (doy <= 365), 0.0022, 0.0) + 0.002 * np.sin(np.arange(len(d)) / 3.0) / 10
    p = 100 * np.exp(np.cumsum(g))
    s = pd.Series(p, index=d)
    if split is not None:                   # 1:100 — старые цены в 100 раз выше
        s[s.index < pd.Timestamp(split)] *= 100
    return s


def test_unsplit_glues_split():
    s = _seasonal(split="2024-04-01")
    u = season.unsplit(s)
    k = (u / u.shift(1)).dropna()
    assert k.max() < 1.5 and k.min() > 0.6 and abs(u.iloc[-1] - s.iloc[-1]) < 1e-9


def test_rule_holds_before_the_seasonal_rise():
    s = _seasonal()
    c = season.condition(s, pd.Timestamp("2026-10-01"))
    assert c and c["rising"] and c["hits"] == c["n"] >= 10 and c["med"] >= 3
    assert season.condition(s, pd.Timestamp("2026-03-02")) is None, "весной следующие 3 месяца стоят"


def test_entry_only_on_the_first_day_and_cooldown(monkeypatch):
    days = pd.bdate_range("2026-06-01", "2026-09-30")
    s = pd.Series(1.0, index=days)
    on = set(pd.bdate_range("2026-07-01", "2026-07-10")) | set(pd.bdate_range("2026-07-20", "2026-07-24")) \
        | set(pd.bdate_range("2026-08-17", "2026-08-21"))
    monkeypatch.setattr(season, "condition", lambda s_, t: {"x": 1} if pd.Timestamp(t) in on else None)
    got = [d for d, _ in season.entries(s, "2026-06-01", "2026-09-30")]
    # 01.07 — вход; 02–10.07 — условие держится, не вход; 20.07 — новый вход, но пауза 30 дней; 17.08 — вход
    assert got == [pd.Timestamp("2026-07-01"), pd.Timestamp("2026-08-17")]


def test_detector_emits_entry_with_hot_style_title():
    s = _seasonal()
    out = det.Out("2026-06-01", "2026-10-07")
    det.detect_seasonality_all(out, assets=[("GMKN", "Норильский никель", s), ("IMOEX", "Индекс МосБиржи", s)])
    items = out.items
    assert len(items) == 1, "один вход за окно; индекс Мосбиржи — у прежних детекторов"
    x = items[0]
    assert x["instrument"] == "GMKN" and x["facts"]["leg"] == "3m"
    assert x["title"].startswith("Сезонность: Норильский никель — следующие 3 месяца рос в ")
    assert " из " in x["title"] and "обычно +" in x["title"]
    assert 4 <= x["score"] <= 10


def test_stock_card_renders_without_none(monkeypatch, tmp_path):
    s = _seasonal(split="2024-04-01")
    monkeypatch.setattr(season, "series", lambda code: ("Норильский никель", season.unsplit(s)))
    c = cards.build_card({"kind": "seasonality", "code": "GMKN"}, "2026-10-01")
    text = cards.brief_text(c, focus=True)
    assert "None" not in text and c["headline"].startswith("Сезонность: Норильский никель - с 1 октября")
    ch = c["chart"]
    assert "рос в" in c["headline"] and ch["type"] == "season3m"
    assert ch["title"].startswith("Норильский никель: следующие 3 месяца рос в ") and "обычно +" in ch["title"]
    assert "серая — медианный путь" in ch["subtitle"] and "оранжевая — 2026" in ch["subtitle"]
    # серая — медианный путь на весь год, оранжевая — нынешний год до сегодня
    assert len(ch["x"]) == 365 and pd.DatetimeIndex(ch["x2"])[-1] <= pd.Timestamp("2026-10-01")
    assert "акций «Норильский никель»" in c["chart_note"][0] and "серая линия, весь год" in c["chart_note"][0]
    out = tmp_path / "season.png"
    cards.draw_chart(c, str(out))
    assert out.stat().st_size > 10_000


def test_smooth_path_wraps_by_level():
    p = pd.Series(np.r_[np.zeros(5), np.linspace(0, 10, 355), np.full(5, 10.0)], index=range(365))
    p.iloc[100] = 30.0                                  # зубец
    sm = season.smooth_path(p)
    assert len(sm) == 365 and sm.iloc[100] < 10, "зубец сглажен"
    assert abs(sm.iloc[0]) < 2 and abs(sm.iloc[-1] - 10) < 2, "края — по уровню, без провала к нулю в декабре"
