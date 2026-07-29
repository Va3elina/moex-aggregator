"""Калибровка порога среднесрочного скринера ОИ (одноразовый прогон на проде).

Считает по ЖИВЫМ данным распределение ratio для medium-горизонта и сравнивает
плотность сигналов с дневным (короткий горизонт, порог 2x) — чтобы порог
MED_SHARP_RATIO давал сопоставимую, а не лавинную частоту срабатываний.
"""
import math
import statistics
from datetime import date, timedelta

from sqlalchemy import text

from api.database import SessionLocal
from api.services.oi_screener import low_activity_set

ATR_WINDOW = 14
ATR_MIN_PART = 50
ATR_MIN_PART_YUR = 15
ATR_MIN_REL = 0.02
ATR_FLOOR_REL = 0.001

MED_WINDOW = 14
MED_BASE_WINDOW = 60
MED_MIN_BASE = 30
MED_SCALE = math.sqrt(MED_WINDOW)
MED_MIN_REL = ATR_MIN_REL * MED_SCALE

db = SessionLocal()


def series_for(clgroup):
    cutoff = date.today() - timedelta(days=150)
    rows = db.execute(text("""
        SELECT sectype, tradedate,
               (pos_long + pos_short)         AS net,
               (pos_long_num + pos_short_num) AS npart
        FROM (
            SELECT DISTINCT ON (sectype, tradedate)
                sectype, tradedate, pos_long, pos_short, pos_long_num, pos_short_num
            FROM open_interest
            WHERE clgroup = :clg AND interval = 24 AND tradedate >= :cutoff
              AND EXTRACT(ISODOW FROM tradedate) BETWEEN 1 AND 5
            ORDER BY sectype, tradedate, tradetime DESC
        ) t
        ORDER BY sectype, tradedate ASC
    """), {"clg": clgroup, "cutoff": cutoff}).fetchall()
    out = {}
    for r in rows:
        out.setdefault(r[0], []).append((r[1], r[2] or 0, r[3] or 0))
    return out


def short_ratio(nets, npart, min_part):
    if len(nets) < ATR_WINDOW + 3 or npart < min_part:
        return None, False
    diffs = [abs(nets[i] - nets[i - 1]) for i in range(1, len(nets))]
    if len(diffs) < ATR_WINDOW + 1:
        return None, False
    last = nets[-1] - nets[-2]
    net = nets[-1]
    atr = statistics.fmean(diffs[-(ATR_WINDOW + 1):-1])
    if atr <= 0 or atr < ATR_FLOOR_REL * max(abs(net), 1):
        return None, False
    ratio = abs(last) / atr
    material = abs(last) / max(abs(net), 1) >= ATR_MIN_REL
    return ratio, material


def medium_ratio(nets, npart, min_part):
    if len(nets) < MED_WINDOW + MED_MIN_BASE + 1 or npart < min_part:
        return None, False
    move = nets[-1] - nets[-1 - MED_WINDOW]
    net = nets[-1]
    diffs = [abs(nets[i] - nets[i - 1]) for i in range(1, len(nets))]
    base = diffs[-(MED_WINDOW + MED_BASE_WINDOW):-MED_WINDOW]
    if len(base) < MED_MIN_BASE:
        return None, False
    atr = statistics.fmean(base)
    if atr <= 0 or atr < ATR_FLOOR_REL * max(abs(net), 1):
        return None, False
    ratio = abs(move) / (atr * MED_SCALE)
    material = abs(move) / max(abs(net), 1) >= MED_MIN_REL
    return ratio, material


low = low_activity_set(db)

for clgroup in ("FIZ", "YUR"):
    min_part = ATR_MIN_PART_YUR if clgroup == "YUR" else ATR_MIN_PART
    ser = series_for(clgroup)
    short_vals, med_vals = [], []
    n_short_sharp = 0
    n_rows = 0
    # Прогон не только по «сегодня», но и по 30 срезам назад — иначе порог
    # подгонится под один день, а не под типичную ленту.
    for sectype, pts in ser.items():
        if sectype in low:
            continue
        for back in range(0, 30):
            sub = pts[:len(pts) - back] if back else pts
            if len(sub) < MED_WINDOW + MED_MIN_BASE + 1:
                continue
            nets = [p[1] for p in sub]
            npart = sub[-1][2]
            n_rows += 1
            sr, sm = short_ratio(nets, npart, min_part)
            if sr is not None:
                short_vals.append(sr)
                if sr >= 2 and sm:
                    n_short_sharp += 1
            mr, mm = medium_ratio(nets, npart, min_part)
            if mr is not None:
                med_vals.append((mr, mm))

    def pct(vals, q):
        if not vals:
            return float('nan')
        s = sorted(vals)
        return s[min(len(s) - 1, int(len(s) * q))]

    mr_only = [m for m, _ in med_vals]
    print(f"\n=== {clgroup} === строк-срезов: {n_rows}")
    print(f"short: n={len(short_vals)} median={statistics.median(short_vals):.2f} "
          f"p90={pct(short_vals, 0.90):.2f} sharp(>=2x & material)={n_short_sharp} "
          f"= {100 * n_short_sharp / max(n_rows, 1):.1f}% строк")
    print(f"medium: n={len(mr_only)} median={statistics.median(mr_only):.2f} "
          f"p75={pct(mr_only, 0.75):.2f} p90={pct(mr_only, 0.90):.2f} p95={pct(mr_only, 0.95):.2f}")
    for thr in (1.5, 2.0, 2.5, 3.0, 3.5, 4.0):
        n = sum(1 for m, mat in med_vals if m >= thr and mat)
        print(f"   medium sharp >= {thr}x & material: {n} = {100 * n / max(n_rows, 1):.1f}% строк")
