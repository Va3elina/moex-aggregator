"""Частота ATR-сигналов: сетка окон × порогов → дизайн многоступенчатой системы. НЕ прод.

Считает, КАК ЧАСТО приходят сигналы при разных окнах ATR и порогах, в понятных единицах:
  - доля дней (% от торговых дней бумаги),
  - «раз в ~N дней на одну бумагу» (1 / доля),
  - «сигналов в день по ВСЕМ ликвидным» (объём канала, если слать все).

Цель — выбрать ступени: «обычные» (часто) vs «сильные» vs «экстремальные» (редко).
Окно фикс ATR=14 (выбрано ранее), но показываем и 20/30 («разное число дней»).
Период оценки — последние ~500 торг. дней (репрезентативно «как сейчас»).
"""
from __future__ import annotations
import os
import statistics as st
from collections import defaultdict

from sqlalchemy import create_engine, text
from signals import config

WINDOWS = [14, 20, 30]
TIERS = [1.5, 2.0, 2.5, 3.0, 4.0, 5.0, 7.0, 10.0]
MIN_PART = 50
MIN_REL = 0.02
GAP_MAX = 5
ATR_FLOOR_REL = 0.001
MIN_MED_NET = 500
TEST_DAYS = 500          # репрезентативный недавний период


def load(conn, sectype, clgroup):
    rows = conn.execute(text("""
        SELECT tradedate,
               (COALESCE(pos_long,0)+COALESCE(pos_short,0)) AS net,
               (COALESCE(pos_long_num,0)+COALESCE(pos_short_num,0)) AS npart
        FROM open_interest WHERE sectype=:s AND clgroup=:g AND interval=24
        ORDER BY tradedate
    """), {"s": sectype, "g": clgroup}).fetchall()
    return [(r[0], int(r[1]), int(r[2])) for r in rows]


def run():
    engine = create_engine(os.environ["DB_URL"], connect_args={"ssl_context": False})
    valid = defaultdict(int)                  # W -> валидных (бумага,день) оценок
    flags = defaultdict(int)                  # (W,T) -> срабатываний
    dates_seen = set()
    n_series = 0
    Wmax = max(WINDOWS)
    with engine.connect() as conn:
        for sectype in config.OI_ASSETS:
            for clg in ("FIZ", "YUR"):
                try:
                    s = load(conn, sectype, clg)
                except Exception:
                    continue
                if len(s) < Wmax + TEST_DAYS + 5:
                    continue
                if st.median([abs(x[1]) for x in s]) < MIN_MED_NET:
                    continue
                n_series += 1
                dates = [x[0] for x in s]; nets = [x[1] for x in s]; npart = [x[2] for x in s]
                adiff = [abs(nets[i] - nets[i - 1]) for i in range(1, len(nets))]
                start = max(Wmax, len(adiff) - TEST_DAYS)
                for i in range(start, len(adiff)):
                    di = i + 1
                    if npart[di] < MIN_PART:
                        continue
                    if (dates[di] - dates[di - 1]).days > GAP_MAX:
                        continue
                    net = nets[di]
                    if adiff[i] / max(abs(net), 1) < MIN_REL:
                        continue
                    dates_seen.add(dates[di])
                    for W in WINDOWS:
                        atr = st.fmean(adiff[i - W:i])
                        if atr <= 0 or atr < ATR_FLOOR_REL * max(abs(net), 1):
                            continue
                        valid[W] += 1
                        ratio = adiff[i] / atr
                        for T in TIERS:
                            if ratio >= T:
                                flags[(W, T)] += 1

    nday = len(dates_seen) or 1
    print(f"\n=== Частота ATR-сигналов ({n_series} ликв. серий, ~{TEST_DAYS} дней, {nday} торг.дат) ===")
    for W in WINDOWS:
        v = valid[W] or 1
        print(f"\n  ── Окно ATR = {W} дней ──")
        print(f"   {'порог':>7}{'% дней':>9}{'раз в N дней/бумага':>22}{'сигналов/день по рынку':>24}")
        for T in TIERS:
            f = flags[(W, T)]
            pct = 100 * f / v
            every = (100 / pct) if pct > 0 else 0
            per_day = f / nday
            print(f"   {('≥%.1f×'%T):>7}{pct:>8.1f}%{('раз в %.0f дн'%every) if every else '—':>22}{per_day:>20.1f}/день")
    print("\n  «раз в N дней/бумага» — как часто юзер с алертом на ОДНУ бумагу получит сигнал.")
    print("  «сигналов/день по рынку» — объём для КАНАЛА (если слать по всем ликвидным).")
    print("\n  Идея ступеней: подобрать пороги под нужную каденцию —")
    print("    «обычные» (часто, ~раз/нед на бумагу) · «сильные» (~раз/мес) · «экстремальные» (редко).")


if __name__ == "__main__":
    run()
