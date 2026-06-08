"""
Per-feature diagnostics — reads /tmp/research_raw.csv (output explore.py),
для каждого feature считает связь с forward return.

Что считаем:
  - Float features: Pearson correlation с fwd_return + значимость (t-test approx).
  - Bool features: avg fwd_return when True vs False + разница.
  - Categorical (cross_days): по-bucket avg fwd_return.

Цель — увидеть какие features РЕАЛЬНО predict'ят forward return и в какую
сторону, чтобы понять как переписать scoring (или его инвертировать).

Запуск:
  cd /opt/frame && /opt/frame/signals/.venv/bin/python -m signals.research.diagnostics
"""
from __future__ import annotations
import csv
import math
from collections import defaultdict
from statistics import fmean, stdev
from typing import Optional

CSV_PATH = "/tmp/research_raw.csv"

# Features by type
FLOAT_FEATURES = [
    "channel_pos",
    "price_vs_ema20_pct",
    "price_vs_ema50_pct",
    "fiz_net_change_5d",
    "yur_net_change_5d",
    "oi_price_corr_20d",
    "volume_z",
]
BOOL_FEATURES = [
    "ema20_above_ema50",
    "is_new_high_30",
    "is_new_low_30",
    "is_new_high_90",
    "is_new_low_90",
]


def correlation(xs: list[float], ys: list[float]) -> tuple[float, int]:
    """Pearson r + n."""
    n = len(xs)
    if n < 3 or n != len(ys):
        return 0.0, n
    mx, my = fmean(xs), fmean(ys)
    cov = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    sx = math.sqrt(sum((x - mx) ** 2 for x in xs))
    sy = math.sqrt(sum((y - my) ** 2 for y in ys))
    if sx == 0 or sy == 0:
        return 0.0, n
    return cov / (sx * sy), n


def t_stat(r: float, n: int) -> float:
    """Approx t-statistic для correlation. |t| > 2 ~ p < 0.05 if n >> 100."""
    if abs(r) >= 1 or n < 3:
        return 0.0
    return r * math.sqrt((n - 2) / (1 - r * r))


def to_float(v: str) -> Optional[float]:
    if v == "" or v is None:
        return None
    try:
        return float(v)
    except ValueError:
        return None


def to_bool(v: str) -> Optional[bool]:
    if v == "True":
        return True
    if v == "False":
        return False
    return None


def main():
    rows = []
    with open(CSV_PATH) as f:
        reader = csv.DictReader(f)
        for r in reader:
            fwd = to_float(r.get("fwd_return"))
            if fwd is None:
                continue
            rows.append(r)
    print(f"Loaded {len(rows)} rows from {CSV_PATH}\n")

    # ─────────────────────────────────────────────────────
    # FLOAT FEATURES — correlation with fwd_return
    # ─────────────────────────────────────────────────────
    print("=" * 80)
    print("FLOAT FEATURES — Pearson correlation с fwd_return")
    print("=" * 80)
    print(f"{'feature':>22} | {'corr':>7} | {'t-stat':>7} | {'n':>5} | signal?")
    print("-" * 80)
    float_results = []
    for feat in FLOAT_FEATURES:
        xs, ys = [], []
        for r in rows:
            xv = to_float(r.get(feat))
            yv = to_float(r.get("fwd_return"))
            if xv is None or yv is None:
                continue
            xs.append(xv)
            ys.append(yv)
        if not xs:
            continue
        r_val, n = correlation(xs, ys)
        t = t_stat(r_val, n)
        sig = "YES ★" if abs(t) > 2 else ("?" if abs(t) > 1 else "no")
        float_results.append((feat, r_val, t, n, sig))
        print(f"{feat:>22} | {r_val:>+7.4f} | {t:>+7.2f} | {n:>5} | {sig}")
    print()

    # ─────────────────────────────────────────────────────
    # FLOAT FEATURES — bucketed avg fwd_return
    # ─────────────────────────────────────────────────────
    print("=" * 80)
    print("FLOAT FEATURES — avg fwd_return по quintile bucket'ам")
    print("=" * 80)
    for feat in FLOAT_FEATURES:
        values = []
        for r in rows:
            xv = to_float(r.get(feat))
            yv = to_float(r.get("fwd_return"))
            if xv is None or yv is None:
                continue
            values.append((xv, yv))
        if len(values) < 100:
            continue
        # Сортируем по feature value, бьём на 5 quantile'ов
        values.sort(key=lambda v: v[0])
        n = len(values)
        chunk = n // 5
        print(f"\n  {feat}:")
        print(f"    {'quintile':>10} | {'range':>20} | {'n':>5} | {'avg_ret':>9}")
        print(f"    {'-'*10} | {'-'*20} | {'-'*5} | {'-'*9}")
        for q in range(5):
            start = q * chunk
            end = (q + 1) * chunk if q < 4 else n
            seg = values[start:end]
            if not seg:
                continue
            lo = seg[0][0]
            hi = seg[-1][0]
            avg = fmean(s[1] for s in seg)
            rng = f"{lo:>+8.2f} … {hi:>+7.2f}"
            label = f"Q{q+1}"
            print(f"    {label:>10} | {rng:>20} | {len(seg):>5} | {avg:>+7.2f}%")
    print()

    # ─────────────────────────────────────────────────────
    # BOOL FEATURES — diff in avg fwd_return between True/False
    # ─────────────────────────────────────────────────────
    print("=" * 80)
    print("BOOL FEATURES — avg fwd_return когда True vs False")
    print("=" * 80)
    print(f"{'feature':>22} | {'when True':>11} | {'n_T':>5} | {'when False':>11} | {'n_F':>5} | diff")
    print("-" * 80)
    for feat in BOOL_FEATURES:
        ts, fs = [], []
        for r in rows:
            b = to_bool(r.get(feat))
            y = to_float(r.get("fwd_return"))
            if b is None or y is None:
                continue
            (ts if b else fs).append(y)
        if not ts or not fs:
            continue
        at, af = fmean(ts), fmean(fs)
        diff = at - af
        sig = "★" if abs(diff) > 0.5 else " "
        print(
            f"{feat:>22} | {at:>+8.2f}%  | {len(ts):>5} | {af:>+8.2f}%  | {len(fs):>5} | "
            f"{diff:>+5.2f}% {sig}"
        )
    print()

    # ─────────────────────────────────────────────────────
    # cross_days — categorical
    # ─────────────────────────────────────────────────────
    print("=" * 80)
    print("CROSS DAYS — avg fwd_return когда был cross в последние 5 дней")
    print("=" * 80)
    cross_buckets = defaultdict(list)
    for r in rows:
        cd_raw = r.get("cross_days")
        y = to_float(r.get("fwd_return"))
        if y is None:
            continue
        if cd_raw == "" or cd_raw == "None":
            cd_buc = "no cross"
        else:
            try:
                cd = int(cd_raw)
                if cd > 0:
                    cd_buc = f"golden cross {cd}d ago"
                else:
                    cd_buc = f"death cross {abs(cd)}d ago"
            except ValueError:
                continue
        cross_buckets[cd_buc].append(y)
    for buc, returns in sorted(cross_buckets.items(), key=lambda kv: -len(kv[1])):
        if len(returns) < 30:
            continue
        avg = fmean(returns)
        print(f"  {buc:>26} | n={len(returns):>5} | avg_ret={avg:>+6.2f}%")
    print()

    # ─────────────────────────────────────────────────────
    # RANKED summary — top 5 features by abs(t-stat)
    # ─────────────────────────────────────────────────────
    print("=" * 80)
    print("TOP FEATURES — отранжированы по силе сигнала (|t-stat|)")
    print("=" * 80)
    float_results.sort(key=lambda x: -abs(x[2]))
    print(f"{'rank':>4} | {'feature':>22} | {'corr':>+8} | {'t':>+7} | direction")
    print("-" * 80)
    for i, (feat, r_val, t, n, sig) in enumerate(float_results, 1):
        if abs(t) < 0.5:
            direction = "—"
        elif r_val > 0:
            direction = "↑ выше → больше fwd_ret"
        else:
            direction = "↓ выше → меньше fwd_ret"
        print(f"{i:>4} | {feat:>22} | {r_val:>+8.4f} | {t:>+7.2f} | {direction}")


if __name__ == "__main__":
    main()
