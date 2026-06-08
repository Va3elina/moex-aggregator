"""OI anomaly v2 — research / backtest (НЕ прод, ничего не меняет).

Сравнивает текущий механизм аномалий с доработками:
  OLD     — z = (Δ_1д − mean(hist)) / std(hist)        [как сейчас в detectors/oi.py]
  ROBUST  — z = (Δ_1д − median(hist)) / (1.4826·MAD)   [устойчив к выбросам]
  MULTI   — robust-z на горизонтах 1д/3д/5д (берём max|z|) [ловит тихое накопление]
  CONC    — robust-z дневного изменения «позиция на участника» (net / Σучастников)
            [мало крупных vs много мелких; данные pos_long_num/pos_short_num]

Запуск на хосте (DB_URL → 127.0.0.1, как alerts_run.sh):
  cd /opt/frame && export DB_URL=$(grep ^DB_URL= .env | cut -d= -f2- | sed 's/@db:/@127.0.0.1:/')
  /opt/frame/signals/.venv/bin/python -m signals.research.oi_anomaly_v2
"""
from __future__ import annotations
import os
import statistics as st
from collections import defaultdict

from sqlalchemy import create_engine, text

from signals import config

LOOKBACK = config.LOOKBACK_DAYS          # 60
MIN_HIST = config.MIN_HISTORY_DAYS       # 30
Z = config.Z_THRESHOLD                   # 2.5
TEST_DAYS = 180                          # сколько последних торговых дней прогоняем на каждый актив
HORIZONS = (1, 3, 5)
MIN_REL = 0.02                           # guard материальности: |move|/|net| ≥ 2% (отсев мусора на неликвиде)
MIN_PART = 50                            # для концентрации: минимум участников, иначе per-participant шумит


def _median(xs):
    return st.median(xs)


def _mad(xs):
    m = st.median(xs)
    return st.median([abs(x - m) for x in xs])


def robust_z(x, hist):
    """(x − median) / (1.4826·MAD). None если истории/разброса мало."""
    if len(hist) < MIN_HIST:
        return None
    mad = _mad(hist)
    if mad == 0:
        # вырожденный разброс — fallback на std, иначе деление на ноль
        sd = st.pstdev(hist)
        if sd == 0:
            return None
        return (x - st.fmean(hist)) / sd
    return (x - _median(hist)) / (1.4826 * mad)


def old_z(x, hist):
    """Текущий механизм: (x − mean) / std."""
    if len(hist) < 2:
        return None
    sd = st.stdev(hist)
    if sd == 0:
        return None
    return (x - st.fmean(hist)) / sd


def load_series(conn, sectype, clgroup):
    """Дневной ряд (date, net, npart) по interval=24. net=pos_long+pos_short
    (pos_short хранится отрицательным). npart=pos_long_num+pos_short_num."""
    rows = conn.execute(text("""
        SELECT tradedate,
               (COALESCE(pos_long,0) + COALESCE(pos_short,0)) AS net,
               (COALESCE(pos_long_num,0) + COALESCE(pos_short_num,0)) AS npart
        FROM open_interest
        WHERE sectype = :s AND clgroup = :g AND interval = 24
        ORDER BY tradedate
    """), {"s": sectype, "g": clgroup}).fetchall()
    return [(r[0], int(r[1]), int(r[2])) for r in rows]


def metrics_at(series, t):
    """Считает OLD/ROBUST/MULTI/CONC z для дня индекса t, используя историю [..t].
    Возвращает dict или None если мало истории."""
    window = series[max(0, t - LOOKBACK):t + 1]   # до LOOKBACK+1 точек, включая t
    if len(window) < MIN_HIST:
        return None
    nets = [w[1] for w in window]
    nparts = [w[2] for w in window]

    # --- 1-дневные диффы ---
    diffs1 = [nets[i] - nets[i - 1] for i in range(1, len(nets))]
    if len(diffs1) < 2:
        return None
    net_now = nets[-1]
    res = {"date": window[-1][0], "last_diff": diffs1[-1],
           "net": net_now, "npart": nparts[-1]}
    res["old"] = old_z(diffs1[-1], diffs1[:-1])
    res["robust"] = robust_z(diffs1[-1], diffs1[:-1])
    res["rel1"] = abs(diffs1[-1]) / max(abs(net_now), 1)   # материальность 1д-движения

    # --- мульти-горизонт (robust на 1/3/5 дней), берём max|z| ---
    best = None
    best_h = None
    best_diff = 0
    for h in HORIZONS:
        if len(nets) <= h + MIN_HIST:
            continue
        hdiffs = [nets[i] - nets[i - h] for i in range(h, len(nets))]
        if len(hdiffs) < MIN_HIST:
            continue
        zz = robust_z(hdiffs[-1], hdiffs[:-1])
        if zz is not None and (best is None or abs(zz) > abs(best)):
            best, best_h, best_diff = zz, h, hdiffs[-1]
    res["multi"], res["multi_h"] = best, best_h
    res["relm"] = abs(best_diff) / max(abs(net_now), 1)    # материальность move на лучшем горизонте

    # --- концентрация: позиция на участника ---
    pp = [nets[i] / nparts[i] if nparts[i] else None for i in range(len(nets))]
    if all(v is not None for v in pp[-(MIN_HIST + 2):]):
        ppd = [pp[i] - pp[i - 1] for i in range(1, len(pp)) if pp[i] is not None and pp[i - 1] is not None]
        if len(ppd) >= MIN_HIST:
            res["conc"] = robust_z(ppd[-1], ppd[:-1])
        else:
            res["conc"] = None
    else:
        res["conc"] = None
    return res


def run():
    engine = create_engine(os.environ["DB_URL"], connect_args={"ssl_context": False})
    agg = defaultdict(int)
    new_only = []   # поймал новый механизм, старый — нет
    with engine.connect() as conn:
        for sectype in config.OI_ASSETS:
            for clg in ("FIZ", "YUR"):
                try:
                    series = load_series(conn, sectype, clg)
                except Exception as e:
                    print(f"[skip] {sectype}/{clg}: {e}")
                    continue
                if len(series) < MIN_HIST + 5:
                    continue
                start = max(LOOKBACK + 1, len(series) - TEST_DAYS)
                for t in range(start, len(series)):
                    m = metrics_at(series, t)
                    if not m:
                        continue
                    agg["days"] += 1
                    fired_old = m["old"] is not None and abs(m["old"]) >= Z
                    # новые механизмы + guard: материальность движения (|move|/|net|)
                    # И ликвидность (≥MIN_PART участников = «толпа», иначе шум на мёртвом активе).
                    liquid = m["npart"] >= MIN_PART
                    fired_rob = (m["robust"] is not None and abs(m["robust"]) >= Z
                                 and m["rel1"] >= MIN_REL and liquid)
                    fired_multi = (m["multi"] is not None and abs(m["multi"]) >= Z
                                   and m["relm"] >= MIN_REL and liquid)
                    fired_conc = (m["conc"] is not None and abs(m["conc"]) >= Z
                                  and m["rel1"] >= MIN_REL and liquid)
                    agg["old"] += fired_old
                    agg["robust"] += fired_rob
                    agg["multi"] += fired_multi
                    agg["conc"] += fired_conc
                    # «новое поймало, старое нет»
                    if (fired_multi or fired_conc) and not fired_old:
                        new_only.append((abs(m["multi"] or 0), sectype, clg, m["date"],
                                         m["old"], m["robust"], m["multi"], m["multi_h"],
                                         m["conc"], m["last_diff"], m["net"], m["npart"]))
    # отчёт
    d = agg["days"] or 1
    print(f"\n=== Бэктест OI-аномалий: {agg['days']} (актив×день) точек, порог {Z}σ ===")
    print(f"  OLD    (mean/std, 1д)      сигналов: {agg['old']:5}  ({100*agg['old']/d:.2f}%)")
    print(f"  ROBUST (median/MAD, 1д)    сигналов: {agg['robust']:5}  ({100*agg['robust']/d:.2f}%)")
    print(f"  MULTI  (robust 1/3/5д max) сигналов: {agg['multi']:5}  ({100*agg['multi']/d:.2f}%)")
    print(f"  CONC   (концентрация)      сигналов: {agg['conc']:5}  ({100*agg['conc']/d:.2f}%)")
    print(f"\n=== Топ-15 «новое поймало, старое пропустило» (по |multi z|) ===")
    new_only.sort(reverse=True)
    for _, s, g, dt, oz, rz, mz, mh, cz, ld, net, npt in new_only[:15]:
        oz_s = f"{oz:+.1f}" if oz is not None else "  —"
        rz_s = f"{rz:+.1f}" if rz is not None else "  —"
        print(f"  {s:6}/{g}  {dt}  old={oz_s}  rob={rz_s}  "
              f"multi={mz:+.1f}(@{mh}д)  conc={('%+.1f'%cz) if cz is not None else '  —':>5}  "
              f"Δ1д={ld:+,}  net={net:+,}  уч={npt}")
    print(f"\n  Всего «новых» катчей (multi|conc, мимо old): {len(new_only)}")


if __name__ == "__main__":
    run()
