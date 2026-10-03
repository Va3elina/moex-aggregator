"""Витрина «Главное» (/hot, api/services/hot.py): правила отбора на синтетических рядах (03.10.2026).

Вадим: витрина — самое актуальное по индикаторам завода, словами скринера («Физлица нарастили шорт»,
«Мин за 2 года»), без контрактов; под графиком — что было после прошлых таких же случаев, после 2022
года — отдельно.
"""
import os
import sys
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DB_URL", "postgresql+pg8000://x:y@127.0.0.1:1/x")
from api.services import hot as H  # noqa: E402


def _days(n, start=date(2024, 1, 1)):
    return [start + timedelta(days=k) for k in range(n)]


def test_prior_extremes_window_is_strictly_before_and_slides():
    d = _days(6)
    v = [5, 1, 3, 4, 2, 6]
    allx = H.prior_extremes(d, v, None)
    assert allx[0] == (None, None) and allx[1] == (5, 5) and allx[5] == (1, 5)
    win = H.prior_extremes(d, v, 2)          # окно [d−2, d): два предыдущих дня
    assert win[3] == (1, 3) and win[5] == (2, 4)


def test_record_takes_longest_window_like_screener():
    ext = {p: (None, None) for p, _ in H.REC_WINDOWS}
    ext.update({"all": (-50, 80), "5y": (-40, 70), "2y": (-20, 60), "1y": (-10, 50)})
    assert H.record_at(-30, ext) == ("low", "2y")      # ниже 2-летнего минимума, но выше 5-летнего
    assert H.record_at(90, ext) == ("high", "all")
    assert H.record_at(0, ext) is None


def test_verb_matches_screener_wording():
    assert H.verb_for(-100, -35, -20) == "Физлица нарастили шорт"
    assert H.verb_for(-100, -10, -20) == "Физлица сократили шорт"
    assert H.verb_for(100, 60, 70) == "Физлица сократили лонг"
    assert H.verb_for(100, 80, 70) == "Физлица набрали лонг"


def test_cluster_firsts_and_price_after():
    assert H.cluster_firsts([1, 2, 3, 50, 51, 120], 40) == [1, 50, 120]
    px = [(d, 100 + k) for k, d in enumerate(_days(40))]
    assert H.price_after(px, date(2024, 1, 1), 21) == 21.0
    assert H.price_after(px, date(2024, 2, 1), 21) is None    # нет месяца после


def test_skew_is_net_share_of_all_positions():
    assert H.skew(300, -100) == 50.0
    assert H.skew(0, 0) is None


def _months(vals, start=(2024, 1)):
    y, m = start
    out = []
    for v in vals:
        out.append((f"{y}-{m:02d}", v))
        m += 1
        if m == 13:
            y, m = y + 1, 1
    return out


def test_fund_case_streak_reversal_and_month_record():
    today = date(2026, 10, 3)
    # девятый месяц оттока подряд
    streak = _months([2, 3, 1, 2, 4, 1, 2, 3, 2, 1, 3, 2] + [-1] * 9 + [0.5], start=(2025, 1))
    c = H.fund_case(streak[:-1], [], today)
    assert c and c["case"] == "streak" and c["signal"] == "Отток 9-й месяц подряд" and c["timeframe"] == "1m"
    # первый отток после 5 месяцев притока, не рекорд
    rev = _months([-5, 1, 1, 1, 1, 1, -0.5], start=(2026, 3))
    c = H.fund_case(rev, [], today)
    assert c["case"] == "reversal" and c["signal"].startswith("Первый отток после 5 мес притока")
    # рекордный месяц и одновременно разворот — пометкой
    rec = _months([1, 2, 1, 1, 1, 1, -3], start=(2026, 3))
    c = H.fund_case(rec, [], today)
    assert c["case"] == "month_record" and c["note"] == "первый после 6 мес притока"


def test_fund_case_week_record_beats_month_cases():
    today = date(2026, 10, 3)
    weeks = []
    d = date(2025, 1, 6)
    for k in range(80):
        v = -2.0 if k == 77 else (0.5 if k % 2 else -0.3)
        weeks.append((d.isoformat(), (d + timedelta(days=4)).isoformat(), v))
        d += timedelta(days=7)
    months = _months([1] * 20, start=(2025, 1))
    c = H.fund_case(months, weeks, today)
    assert c["case"] == "week_record" and c["signal"] == "Рекордный отток за неделю" and c["timeframe"] == "1w"


def test_season_years_window_per_year():
    closes = []
    for y in range(2010, 2026):
        closes.append((date(y, 10, 1), 100.0))
        closes.append((date(y, 12, 31), 110.0 if y % 2 else 95.0))
    yrs = H.season_years(closes, date(2026, 10, 3), 2010)
    assert len(yrs) == 16 and yrs[0] == (2010, -5.0) and yrs[1] == (2011, 10.0)
