"""
Diagnostics v2 — анализ всех features (extended) из research_v2_raw.csv.

Что нового vs diagnostics.py v1:
  - Читает /tmp/research_v2_raw.csv (extended explore с 65 активами + new features)
  - Анализирует НОВЫЕ features:
      participant_balance, long_share, avg_position_size,
      daily_range_pct, atr_pct, oi_total_change_5d, participants_growth_5d
  - Считает correlation + quintile bucketing + bool diff
  - Финальный ранкинг по силе сигнала (исправлен format bug)

Запуск:
  cd /opt/frame && /opt/frame/signals/.venv/bin/python -m signals.research.diagnostics_v2
"""
from __future__ import annotations
import csv
import math
from collections import defaultdict
from statistics import fmean, stdev
from typing import Optional

CSV_PATH = "/tmp/research_v2_raw.csv"

# Все float features — включая новые
FLOAT_FEATURES = [
    # Old (для сравнения)
    "channel_pos",
    "price_vs_ema20_pct",
    "price_vs_ema50_pct",
    "fiz_net_change_5d",
    "yur_net_change_5d",
    "oi_price_corr_20d",
    "volume_z",
    # NEW в v2
    "daily_range_pct",
    "atr_pct",
    "fiz_part_balance",
    "yur_part_balance",
    "fiz_long_share",
    "yur_long_share",
    "fiz_avg_long_size",
    "fiz_avg_short_size",
    "oi_total_change_5d",
    "participants_growth_5d",
]
BOOL_FEATURES = [
    "ema20_above_ema50",
    "is_new_high_30",
    "is_new_low_30",
    "is_new_high_90",
    "is_new_low_90",
]


def correlation(xs: list[float], ys: list[float]) -> tuple[float, int]:
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
    if abs(r) >= 1 or n < 3:
        return 0.0
    return r * math.sqrt((n - 2) / (1 - r * r))


def to_float(v) -> Optional[float]:
    if v in (None, "", "None"):
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def to_bool(v) -> Optional[bool]:
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
    # FLOAT FEATURES — correlation
    # ─────────────────────────────────────────────────────
    print("=" * 90)
    print("FLOAT FEATURES — Pearson r с fwd_return, в порядке силы signal")
    print("=" * 90)
    print(f"{'#':>3} {'feature':>26} | {'r':>7} | {'t-stat':>7} | {'n':>5} | signal")
    print("-" * 90)
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
        if len(xs) < 100:
            continue
        r_val, n = correlation(xs, ys)
        t = t_stat(r_val, n)
        float_results.append((feat, r_val, t, n))

    # Ранкинг по |t|
    float_results.sort(key=lambda x: -abs(x[2]))
    for rank, (feat, r_val, t, n) in enumerate(float_results, 1):
        if abs(t) > 3:
            sig = "★★★ strong"
        elif abs(t) > 2:
            sig = "★★ valid"
        elif abs(t) > 1:
            sig = "★ weak"
        else:
            sig = "—"
        print(f"{rank:>3} {feat:>26} | {r_val:>+7.4f} | {t:>+7.2f} | {n:>5} | {sig}")
    print()

    # ─────────────────────────────────────────────────────
    # FLOAT — quintile buckets (только для top-8 значимых)
    # ─────────────────────────────────────────────────────
    top_features = [f for f, r, t, n in float_results if abs(t) > 1.5][:8]
    print("=" * 90)
    print("QUINTILE BUCKETS — avg fwd_return по 5 равным группам (top-8 features)")
    print("=" * 90)
    for feat in top_features:
        values = []
        for r in rows:
            xv = to_float(r.get(feat))
            yv = to_float(r.get("fwd_return"))
            if xv is None or yv is None:
                continue
            values.append((xv, yv))
        if len(values) < 500:
            continue
        values.sort(key=lambda v: v[0])
        n = len(values)
        chunk = n // 5
        print(f"\n  {feat}:")
        print(f"    {'Q':>2} | {'range':>25} | {'n':>5} | {'avg_ret':>8}")
        print(f"    {'-'*2} | {'-'*25} | {'-'*5} | {'-'*8}")
        for q in range(5):
            start = q * chunk
            end = (q + 1) * chunk if q < 4 else n
            seg = values[start:end]
            if not seg:
                continue
            lo = seg[0][0]
            hi = seg[-1][0]
            avg = fmean(s[1] for s in seg)
            rng = f"{lo:>+10.3f} … {hi:>+9.3f}"
            print(f"    Q{q+1} | {rng:>25} | {len(seg):>5} | {avg:>+6.2f}%")
    print()

    # ─────────────────────────────────────────────────────
    # BOOL FEATURES — True vs False
    # ─────────────────────────────────────────────────────
    print("=" * 90)
    print("BOOL FEATURES — avg fwd_return: True vs False")
    print("=" * 90)
    print(f"{'feature':>22} | {'T_ret':>+8} | {'n_T':>5} | {'F_ret':>+8} | {'n_F':>5} | diff | signal")
    print("-" * 90)
    bool_results = []
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
        sig = "★★★" if abs(diff) > 1.5 else ("★★" if abs(diff) > 1.0 else ("★" if abs(diff) > 0.5 else "—"))
        bool_results.append((feat, at, len(ts), af, len(fs), diff, sig))
    bool_results.sort(key=lambda x: -abs(x[5]))
    for feat, at, nt, af, nf, diff, sig in bool_results:
        print(
            f"{feat:>22} | {at:>+7.2f}% | {nt:>5} | {af:>+7.2f}% | {nf:>5} | "
            f"{diff:>+5.2f}% | {sig}"
        )
    print()

    # ─────────────────────────────────────────────────────
    # FINAL VERDICT — recommended features to add
    # ─────────────────────────────────────────────────────
    print("=" * 90)
    print("ВЫВОДЫ — какие features добавить в scoring v3")
    print("=" * 90)
    print()
    print("Sig threshold: |t-stat| > 2 (statistically significant) или |diff| > 1.0% (bool).\n")
    print("STRONGEST FLOAT FEATURES (|t| > 2):")
    for feat, r_val, t, n in float_results:
        if abs(t) > 2:
            direction = "↑ положительный → выше fwd_ret" if r_val > 0 else "↓ отрицательный → ниже fwd_ret"
            print(f"  ★ {feat:<26} r={r_val:+.4f}  t={t:+.2f}  {direction}")
    print()
    print("STRONGEST BOOL FEATURES (|diff| > 1%):")
    for feat, at, nt, af, nf, diff, sig in bool_results:
        if abs(diff) > 1.0:
            direction = "когда True — bullish" if diff > 0 else "когда True — bearish"
            print(f"  ★ {feat:<26} diff={diff:+.2f}%  {direction}")


if __name__ == "__main__":
    main()
