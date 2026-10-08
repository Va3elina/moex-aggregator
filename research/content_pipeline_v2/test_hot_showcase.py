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


def test_screener_verb_matches_oi_screener_table():
    assert H.screener_verb(-100, "down") == "Физлица нарастили шорт"     # |net| вырос в шорте
    assert H.screener_verb(-100, "up") == "Физлица сократили шорт"
    assert H.screener_verb(100, "down") == "Физлица сократили лонг"
    assert H.screener_verb(100, "up") == "Физлица набрали лонг"


def test_peaks_sit_on_the_extreme_of_each_episode():
    vals = [0, -5, -9, -7, 0, 0, -3, -12, -4]
    assert H.peak_of(vals, [1, 2, 3, 6, 7, 8], "low", 2) == [2, 7]


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
    assert c and c["case"] == "streak" and c["signal"] == "Отток 9-й месяц подряд" and not c["chart"]["weekly"]
    assert c["chart"]["run"]["label"] == "9 мес подряд" and c["chart"]["hl"] == "2026-09"
    # первый отток после 5 месяцев притока, не рекорд
    rev = _months([-5, 1, 1, 1, 1, 1, -0.5], start=(2026, 3))
    c = H.fund_case(rev, [], today)
    assert c["case"] == "reversal" and c["signal"].startswith("Первый отток после 5 мес притока")
    # рекордный месяц и одновременно разворот — пометкой
    rec = _months([1, 2, 1, 1, 1, 1, -3], start=(2026, 3))
    c = H.fund_case(rec, [], today)
    assert c["case"] == "month_record" and c["note"] == "первый после 6 мес притока"
    assert c["chart"]["run"]["label"] == "6 мес притока" and c["chart"]["prev"] == "2026-03" and c["chart"]["level"] == 1


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
    assert c["case"] == "week_record" and c["signal"] == "Рекордный отток за неделю" and c["chart"]["weekly"]
    assert c["chart"]["level"] < 0 and c["chart"]["prev"] != c["chart"]["hl"]


def test_season_years_window_per_year():
    closes = []
    for y in range(2010, 2026):
        closes.append((date(y, 10, 1), 100.0))
        closes.append((date(y, 12, 31), 110.0 if y % 2 else 95.0))
    yrs = H.season_years(closes, date(2026, 10, 3), 2010)
    assert len(yrs) == 16 and yrs[0] == (2010, -5.0) and yrs[1] == (2011, 10.0)


def test_front_month_takes_nearest_live_contract():
    d1, d2 = date(2026, 9, 17), date(2026, 9, 21)
    rows = [(d1, 100.0, date(2026, 9, 18), False), (d1, 102.0, date(2026, 12, 18), False),
            (d2, 103.0, date(2026, 12, 18), False), (d2, 99.0, None, True)]
    assert H.front_month(rows) == [(d1, 100.0), (d2, 103.0)]
    assert H.front_month([(d1, 50.0, None, True)]) == [(d1, 50.0)]


def test_past_episodes_drop_the_current_one():
    assert H.past_episodes([3, 4, 5, 40, 41, 98, 99, 100], 100, 10) == [3, 40]
    assert H.past_episodes([3, 4, 40], 100, 10) == [3, 40]       # сегодня не сигнал — все эпизоды прошлые


def test_past_case_and_base_up():
    px = [(d, 100 + k) for k, d in enumerate(_days(120))]
    c = H.past_case(px, date(2024, 1, 1), "x")
    assert c["r"][0] == round((122 / 101 - 1) * 100, 1) and c["r"][1] is not None
    oi = H.past_case(px, date(2024, 1, 1), "x", H.OI_HORIZONS, zone="2 недели")
    assert oi["r"][0] == round((102 / 101 - 1) * 100, 1) and oi["zone"] == "2 недели"
    assert H.base_up(px, date(2024, 1, 1)) == 100
    assert H.past_case(None, date(2024, 1, 1), "x")["r"] == [None, None]


def test_fund_past_finds_earlier_streak_not_current():
    vals = [1] * 6 + [-1] * 5 + [1] * 3 + [-1] * 5          # две серии оттока по 5 мес
    months = _months(vals, start=(2023, 1))
    today = date(2024, 8, 3)
    cur = H.fund_case(months, [], today)
    assert cur["case"] == "streak"
    past = H.fund_past(months, [], today, cur, None, start=date(2023, 1, 2))
    assert len(past) == 1 and past[0][1]["chart"]["run"]["from"] == "2023-07"


def test_legs_words_and_detectors():
    assert H.leg_verb("long", True) == "Физлица набрали лонг" and H.leg_verb("nl", False) == "Физлица сократили лонг"
    assert H.leg_verb("short", True) == "Физлица нарастили шорт" and H.leg_verb("ns", False) == "Физлица сократили шорт"
    assert H.thousands(32231) == "32 тыс" and H.thousands(2733) == "2,7 тыс"
    d = _days(800)
    v = [10.0] * 500 + [20.0] + [5.0] * 298 + [15.0]
    yrs, since = H.since_years(v, d, 799, True)
    assert since == d[500] and round(yrs, 2) == round(299 / 365, 2)      # максимум с дня 500
    yrs, since = H.since_years(v, d, 500, True)
    assert since is None                                                 # выше всего за историю
    fr = [date(2024, 1, 5) + timedelta(days=7 * k) for k in range(6)]    # пятницы
    assert H.weekly_streak([1, 2, 3, 4, 3, 5], fr, 5) == 1
    assert H.weekly_streak([1, 2, 3, 4, 5, 6], fr, 5) == 5


def test_tiny_month_against_does_not_break_streak():
    vals = [-3, -2, -4, 0.2, -3, -2, -1, -2, -3]
    assert H.runs_tolerant(vals) == [(-1, 0, 8, 1)]
    assert H.runs_tolerant([-3, -2, 2.5, -3]) == [(-1, 0, 1, 0), (1, 2, 2, 0), (-1, 3, 3, 0)]   # крупный — рвёт
    c = H.fund_case(_months([1, 1, 1, 1, 1, 1] + vals, start=(2025, 1)), [], date(2026, 4, 3))
    assert c["case"] == "streak" and c["signal"] == "Отток 8 из 9 месяцев" and c["chart"]["run"]["label"] == "8 из 9 мес"


def test_unsplit_rescales_history_before_split():
    d = _days(4)
    out = H.unsplit([(d[0], 15000.0), (d[1], 15300.0), (d[2], 153.0), (d[3], 150.0)])
    assert [round(c, 2) for _, c in out] == [150.0, 153.0, 153.0, 150.0]
