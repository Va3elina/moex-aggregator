"""Сводный walk-forward sweep ВСЕХ OI-сигналов по всем фьючерсам. НЕ прод, исследование.

Тестирует семейства сигналов (все causal — считаются по данным ДО T) против
forward-доходности T→T+H (out-of-sample). Для каждого — оба направления
(MOMENTUM: позиция вверх → цена вверх; CONTRARIAN: наоборот). По годам, фокус 2024-2026.

Семейства:
  LEVEL   — COT-индекс чистой позиции физ (трейлинг min-max). Экстремум ≥80/≤20.
  FLOW    — z-score изменения чистой позиции физ за 5 дней. |z|≥2.
  OIMOVE  — резкий дневной сдвиг позиции |Δ|/ATR14 ≥3 (наш прод-сигнал), направленно.
  OICHG   — z-score изменения ВАЛОВОГО OI за 5 дней (открытый интерес растёт/падает).
  PARTIC  — z-score изменения числа участников физ за 5 дней (толпа заходит/выходит).

sig_sign = +1 (физ нарастили лонг / OI вырос / толпа зашла), −1 (наоборот).
  momentum_profit  = sig_sign * fwd_ret      (>0 = направление продолжилось)
  contrarian_profit = −sig_sign * fwd_ret     (>0 = разворот)
hit-rate = доля событий с profit>0. 50% = монетка.

Запуск: DB_URL=... /opt/frame/signals/.venv/bin/python -m signals.research.oi_signal_sweep
"""
import os
import statistics as st
from collections import defaultdict

from sqlalchemy import create_engine, text

from signals.research.cot_index import load_price

W = 110
FWD = [10, 20]
RECENT = [2024, 2025, 2026]


def load_full(conn, sectype):
    rows = conn.execute(text("""
        SELECT tradedate, clgroup,
               COALESCE(pos_long,0)+COALESCE(pos_short,0) AS net,
               COALESCE(pos_long_num,0)+COALESCE(pos_short_num,0) AS npart,
               COALESCE(pos_long,0) AS plong
        FROM open_interest WHERE sectype=:s AND interval=24 AND clgroup IN ('FIZ','YUR')
        ORDER BY tradedate
    """), {"s": sectype}).fetchall()
    by = {}
    for d, g, net, npart, plong in rows:
        by.setdefault(d, {})[g] = (int(net), int(npart), int(plong))
    out = []
    for d in sorted(by):
        v = by[d]
        if "FIZ" not in v or "YUR" not in v:
            continue
        net_f, npart_f, pl_f = v["FIZ"]
        _, _, pl_y = v["YUR"]
        out.append((d, net_f, pl_f + pl_y, npart_f))   # date, net_физ, gross_OI, npart_физ
    return out


def cot(vals):
    out = []
    for i in range(len(vals)):
        if i < W - 1:
            out.append(None)
            continue
        win = vals[i - W + 1:i + 1]
        lo, hi = min(win), max(win)
        out.append(None if hi == lo else (vals[i] - lo) / (hi - lo) * 100)
    return out


def zroll(vals, lag):
    """z-score изменения vals за lag дней относительно трейлинг-окна W изменений."""
    chg = [None] * len(vals)
    for i in range(lag, len(vals)):
        chg[i] = vals[i] - vals[i - lag]
    out = [None] * len(vals)
    for i in range(len(vals)):
        if i < W or chg[i] is None:
            continue
        win = [c for c in chg[i - W:i] if c is not None]
        if len(win) < W // 2:
            continue
        m = st.fmean(win)
        sd = st.pstdev(win)
        if sd > 0:
            out[i] = (chg[i] - m) / sd
    return out


def events_level(s):
    nets = [x[1] for x in s]
    idx = cot(nets)
    ev = []
    for i in range(len(s)):
        if idx[i] is None:
            continue
        if idx[i] >= 80:
            ev.append((i, +1))
        elif idx[i] <= 20:
            ev.append((i, -1))
    return ev


def events_z(series_vals, lag, zth):
    z = zroll(series_vals, lag)
    ev = []
    for i in range(len(z)):
        if z[i] is None:
            continue
        if z[i] >= zth:
            ev.append((i, +1))
        elif z[i] <= -zth:
            ev.append((i, -1))
    return ev


def events_oimove(s):
    nets = [x[1] for x in s]
    diffs = [abs(nets[i] - nets[i - 1]) for i in range(1, len(nets))]
    ev = []
    for i in range(W, len(s)):
        if i - 1 < 14:
            continue
        atr = st.fmean(diffs[i - 15:i - 1]) if i - 1 >= 14 else 0
        if atr <= 0:
            continue
        d = nets[i] - nets[i - 1]
        if abs(d) / atr >= 3:
            ev.append((i, 1 if d > 0 else -1))
    return ev


def run():
    engine = create_engine(os.environ["DB_URL"], connect_args={"ssl_context": False})
    # signal_name -> H -> direction('mom'/'con') -> year -> [profit]
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
                s = load_full(conn, sectype)
                price = load_price(conn, sectype)
            except Exception:
                continue
            if not price or len(s) < W + max(FWD) + 60:
                continue
            n_assets += 1
            dates = [x[0] for x in s]
            net_f = [x[1] for x in s]
            oi = [x[2] for x in s]
            npart = [x[3] for x in s]
            sig_events = {
                "LEVEL": events_level(s),
                "FLOW": events_z(net_f, 5, 2.0),
                "OIMOVE": events_oimove(s),
                "OICHG": events_z(oi, 5, 2.0),
                "PARTIC": events_z(npart, 5, 2.0),
            }
            for name, evs in sig_events.items():
                for (i, sign) in evs:
                    for H in FWD:
                        if i + H >= len(s):
                            continue
                        p0 = price.get(dates[i])
                        pf = price.get(dates[i + H])
                        if not p0 or not pf:
                            continue
                        ret = (pf - p0) / p0 * 100
                        yr = dates[i].year
                        res[name][H]["mom"][yr].append(sign * ret)
                        res[name][H]["con"][yr].append(-sign * ret)

    print(f"\nВселенная: {n_assets} фьючерсов с достаточной историей. Окно={W}.")
    print("hit = доля событий с прибылью; 50% = монетка. Показываю ЛУЧШЕЕ направление.")

    def agg(years_dict, years=None):
        prof = [p for yr, ps in years_dict.items() if (years is None or yr in years) for p in ps]
        if not prof:
            return (0, 0.0, 0.0)
        hit = 100 * sum(1 for p in prof if p > 0) / len(prof)
        return (len(prof), hit, st.fmean(prof))

    print(f"\n{'сигнал':>8}{'H':>4}{'напр':>6}{'всего n':>9}{'hit все':>9}"
          f"{'hit 24-26':>11}{'hit 2026':>10}{'проф2026':>10}")
    for name in ["LEVEL", "FLOW", "OIMOVE", "OICHG", "PARTIC"]:
        for H in FWD:
            # выбрать направление с лучшим hit на всей истории
            best = None
            for dirn in ("mom", "con"):
                n, hit, prof = agg(res[name][H][dirn])
                if best is None or hit > best[1]:
                    best = (dirn, hit, n, prof)
            dirn = best[0]
            n_all, hit_all, _ = agg(res[name][H][dirn])
            _, hit_rec, _ = agg(res[name][H][dirn], RECENT)
            n26, hit26, prof26 = agg(res[name][H][dirn], [2026])
            label = "моментум" if dirn == "mom" else "контр"
            print(f"{name:>8}{H:>4}{label:>8}{n_all:>9}{hit_all:>8.0f}%"
                  f"{hit_rec:>10.0f}%{hit26:>9.0f}%{prof26:>9.2f}")

    # детально по годам для каждого сигнала (лучшее направление, H=10)
    print(f"\n=== По годам (H=10, лучшее направление) ===")
    for name in ["LEVEL", "FLOW", "OIMOVE", "OICHG", "PARTIC"]:
        dirn = max(("mom", "con"), key=lambda d: agg(res[name][10][d])[1])
        yd = res[name][10][dirn]
        label = "моментум" if dirn == "mom" else "контр"
        years = sorted(yd)
        cells = []
        for yr in years:
            if len(yd[yr]) < 30:
                continue
            h = 100 * sum(1 for p in yd[yr] if p > 0) / len(yd[yr])
            cells.append(f"{yr}:{h:.0f}%")
        print(f"  {name} ({label}): " + "  ".join(cells))


if __name__ == "__main__":
    run()
