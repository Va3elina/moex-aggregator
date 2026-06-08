"""Walk-forward: ОБЪЁМ + резкий ход (тиры по ATR) ± OI — след информированной торговли.
НЕ прод, исследование.

Гипотеза «кто-то что-то знает»: аномальный ОБЪЁМ + крупный однодневный ход (в единицах
ATR) → цена ПРОДОЛЖАЕТ движение (информированный трейдер прав, не разворот). Если в этот
день РАСТЁТ открытый интерес (новые позиции, а не закрытие) — сильнее (умные деньги строят
позицию). Чем крупнее ATR-тир — тем «информированнее»?

Сигнал в день i (всё causal, по данным ДО/в i):
  vol_ratio = volume[i] / средний объём за 20 дней ≥ 2  (аномальный объём)
  move_atr  = |дневная доходность| / ATR(14)  → тир: T1[1,2) T2[2,3) T3[≥3)
  направление = знак дневной доходности
  OI-фильтр: вариант «+OI» требует валовой OI[i] > OI[i-1] (новые позиции)
Исход: continuation = направление × forward-доходность за H дней (>0 = продолжилось).
Walk-forward по ГОДАМ, по тирам, с OI и без. 50% = монетка.

Запуск: DB_URL=... /opt/frame/signals/.venv/bin/python -m signals.research.vol_walkforward
"""
import os
import statistics as st
from collections import defaultdict

from sqlalchemy import create_engine, text

ATRW = 14
VOLW = 20
FWD = [10, 20]
VOL_TH = 2.0
RECENT = [2024, 2025, 2026]


def load_pv(conn, sectype):
    rows = conn.execute(text("""
        SELECT begin_time::date AS d, close, COALESCE(volume, 0) AS v
        FROM candles
        WHERE sec_id IN (SELECT sec_id FROM instruments WHERE sectype=:s AND type='futures')
          AND interval = 24 AND close > 0
    """), {"s": sectype}).fetchall()
    best = {}
    for d, c, v in rows:
        if d not in best or float(v) > best[d][1]:
            best[d] = (float(c), float(v))
    return best


def load_oi_gross(conn, sectype):
    rows = conn.execute(text("""
        SELECT tradedate, SUM(COALESCE(pos_long, 0)) AS gross
        FROM open_interest WHERE sectype=:s AND interval=24 AND clgroup IN ('FIZ','YUR')
        GROUP BY tradedate
    """), {"s": sectype}).fetchall()
    return {r[0]: int(r[1]) for r in rows}


def tier(m):
    if m < 2:
        return 1
    if m < 3:
        return 2
    return 3


def run():
    engine = create_engine(os.environ["DB_URL"], connect_args={"ssl_context": False})
    # (oi_mode 'noOI'/'withOI') -> tier -> H -> year -> [continuation]
    res = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(list))))
    n_assets = 0
    with engine.connect() as conn:
        uni = conn.execute(text("""
            SELECT sectype FROM open_interest WHERE interval=24 AND clgroup='FIZ'
            GROUP BY sectype HAVING count(*) > 300 AND max(tradedate) >= DATE '2025-01-01'
        """)).fetchall()
        assets = [r[0] for r in uni]
        for sectype in assets:
            try:
                pv = load_pv(conn, sectype)
                oig = load_oi_gross(conn, sectype)
            except Exception:
                continue
            dates = sorted(pv)
            if len(dates) < ATRW + VOLW + max(FWD) + 60:
                continue
            n_assets += 1
            close = [pv[d][0] for d in dates]
            vol = [pv[d][1] for d in dates]
            rets = [0.0] + [(close[i] - close[i - 1]) / close[i - 1] for i in range(1, len(close))]
            for i in range(max(ATRW, VOLW) + 1, len(dates) - max(FWD)):
                atr = st.fmean([abs(r) for r in rets[i - ATRW:i]])
                vavg = st.fmean(vol[i - VOLW:i])
                if atr <= 0 or vavg <= 0:
                    continue
                vol_ratio = vol[i] / vavg
                if vol_ratio < VOL_TH:
                    continue
                move_atr = abs(rets[i]) / atr
                if move_atr < 1:
                    continue
                tr = tier(move_atr)
                direction = 1 if rets[i] > 0 else -1
                # OI-фильтр (валовой OI на эту и прошлую дату)
                oi_now = oig.get(dates[i])
                oi_prev = oig.get(dates[i - 1])
                oi_up = (oi_now is not None and oi_prev is not None and oi_now > oi_prev)
                yr = dates[i].year
                for H in FWD:
                    fwd = (close[i + H] - close[i]) / close[i] * 100
                    cont = direction * fwd
                    res["noOI"][tr][H][yr].append(cont)
                    if oi_up:
                        res["withOI"][tr][H][yr].append(cont)

    print(f"\nВселенная: {n_assets} фьючерсов. ATR-окно={ATRW}, объём-окно={VOLW}, "
          f"порог объёма ≥{VOL_TH}×.")
    print("continuation = знак дневного хода × forward; hit = доля >0 (движение продолжилось).")

    def agg(yd, years=None):
        xs = [x for yr, ps in yd.items() if (years is None or yr in years) for x in ps]
        if not xs:
            return (0, 0.0, 0.0)
        return (len(xs), 100 * sum(1 for x in xs if x > 0) / len(xs), st.fmean(xs))

    for mode in ("noOI", "withOI"):
        title = "ОБЪЁМ+ЦЕНА (без OI)" if mode == "noOI" else "ОБЪЁМ+ЦЕНА+OI РАСТЁТ (новые позиции)"
        print(f"\n=== {title} ===")
        print(f"{'тир':>5}{'H':>4}{'всего n':>9}{'hit все':>9}{'hit 24-26':>11}{'hit 2026':>10}{'проф2026':>10}")
        for tr in (1, 2, 3):
            for H in FWD:
                n_all, hit_all, _ = agg(res[mode][tr][H])
                _, hit_rec, _ = agg(res[mode][tr][H], RECENT)
                n26, hit26, prof26 = agg(res[mode][tr][H], [2026])
                tname = {1: "1-2×", 2: "2-3×", 3: "≥3×"}[tr]
                print(f"{tname:>5}{H:>4}{n_all:>9}{hit_all:>8.0f}%"
                      f"{hit_rec:>10.0f}%{hit26:>9.0f}%{prof26:>9.2f}")

    # по годам — тир ≥3×: hit И ПРОФИТ (профит решает при асимметрии пробоев)
    for mode in ("noOI", "withOI"):
        for H in FWD:
            print(f"\n=== По годам: ≥3×ATR, {mode}, H={H} — hit / ср.профит п.п. ===")
            yd = res[mode][3][H]
            cells = []
            for yr in sorted(yd):
                if len(yd[yr]) < 15:
                    continue
                xs = yd[yr]
                h = 100 * sum(1 for x in xs if x > 0) / len(xs)
                cells.append(f"{yr}: {h:.0f}%/{st.fmean(xs):+.1f}(n{len(xs)})")
            print("  " + "  ".join(cells))
            # сколько лет профит положителен (sign-test устойчивости)
            yrs = [yr for yr in yd if len(yd[yr]) >= 15]
            pos = sum(1 for yr in yrs if st.fmean(yd[yr]) > 0)
            print(f"  → профит>0 в {pos}/{len(yrs)} годах")


if __name__ == "__main__":
    run()
