"""
R&D phase 3 — type-aware scoring.

Главный урок из diagnostics_by_class.py — Simpson's paradox: features имеют
противоположное направление в разных типах активов. Universal scoring
не работает. Строим ДВЕ модели:

  - MEAN-REVERSION: equity, index, soft, energy
    (цена выше тренда → откат)
  - MOMENTUM: fx_rub, metal
    (цена выше тренда → продолжается)

Калибровка делается **per group** независимо. Train/test split **per group**
проверяет overfit отдельно.

Запуск:
  cd /opt/frame && /opt/frame/signals/.venv/bin/python -m signals.research.explore_v3

Использует /tmp/research_v2_raw.csv (уже все features) — не загружаем заново.
"""
from __future__ import annotations
import csv
import json
import math
from collections import defaultdict
from statistics import fmean
from typing import Optional

CSV_PATH = "/tmp/research_v2_raw.csv"
FWD_HORIZON_DAYS = 20
MOVE_THRESHOLD_PCT = 5.0
TRAIN_FRACTION = 0.70

# ───────────────────────────────────────────────────────────────
# Type classification (same as diagnostics_by_class.py)
# ───────────────────────────────────────────────────────────────
TYPE_MAP = {
    "USDRUBF": "fx_rub", "EURRUBF": "fx_rub", "CNYRUBF": "fx_rub",
    "Si": "fx_rub", "Eu": "fx_rub", "CR": "fx_rub",
    "ED": "fx_cross", "AU": "fx_cross", "UC": "fx_cross",
    "DX": "fx_cross", "MY": "fx_cross",
    "IMOEXF": "index", "MX": "index", "MM": "index",
    "RI": "index", "IB": "index", "VI": "index",
    "SR": "equity", "GZ": "equity", "LK": "equity", "VB": "equity",
    "GK": "equity", "SBERF": "equity", "GAZPF": "equity",
    "AL": "equity", "AF": "equity", "CH": "equity", "CE": "equity",
    "HY": "equity", "MC": "equity", "ME": "equity", "MN": "equity",
    "NM": "equity", "NK": "equity", "RN": "equity",
    "SN": "equity", "SP": "equity", "SS": "equity",
    "TN": "equity", "TT": "equity", "X5": "equity", "YD": "equity",
    "MG": "equity", "RL": "equity", "RM": "equity", "RB": "equity",
    "SE": "equity", "SF": "equity", "HS": "equity", "NA": "equity",
    "GD": "metal", "GL": "metal", "GLDRUBF": "metal",
    "SV": "metal", "PT": "metal", "PD": "metal", "HK": "metal",
    "BR": "energy", "BM": "energy", "NG": "energy",
    "FF": "energy", "NR": "energy",
    "CC": "soft", "KC": "soft", "OJ": "soft", "W4": "soft",
}

# Из diagnostics_by_class — какие features действуют какой direction в каких классах
# (см. таблицу t-stat). Группы:
MEANREV_TYPES = {"equity", "index", "soft", "energy"}
MOMENTUM_TYPES = {"fx_rub", "metal", "fx_cross"}
# fx_cross hadn't enough data alone — group with momentum (FX обычно trending)


# ───────────────────────────────────────────────────────────────
# Scoring functions
# ───────────────────────────────────────────────────────────────
def to_float(v) -> Optional[float]:
    if v in (None, "", "None"):
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def to_bool(v) -> bool:
    return v == "True"


def universal_signals(f: dict) -> float:
    """Features которые РОБАСТНЫ across all types (same direction)."""
    s = 0.0
    # is_new_low_90 — bullish во всех группах (из diagnostics v1)
    if to_bool(f.get("is_new_low_90")):    s += 2.0
    if to_bool(f.get("is_new_high_90")):   s += 1.0
    if to_bool(f.get("is_new_high_30")):   s += 0.5
    if to_bool(f.get("is_new_low_30")):    s += 0.3

    # fiz_avg_long_size — positive across most types (т=+9.87 metal, +7.39 soft)
    avg_size = to_float(f.get("fiz_avg_long_size")) or 0
    if avg_size > 100:  s += 0.5
    if avg_size > 200:  s += 0.5

    # OI flows — slightly contrarian retail / smart institutional
    fiz_5d = to_float(f.get("fiz_net_change_5d")) or 0
    yur_5d = to_float(f.get("yur_net_change_5d")) or 0
    if fiz_5d > 30:   s -= 0.3
    if fiz_5d < -30:  s += 0.3
    if yur_5d > 20:   s += 0.3
    if yur_5d < -20:  s -= 0.3

    return s


def meanrev_score(f: dict) -> float:
    """Scoring для mean-reversion класса (equity/index/soft/energy)."""
    s = universal_signals(f)

    # Price vs EMA — extremes mean revert (negative correlation t≈-3 to -5)
    p_ema50 = to_float(f.get("price_vs_ema50_pct")) or 0
    if p_ema50 > 10:    s -= 1.5
    elif p_ema50 > 5:   s -= 0.5
    if p_ema50 < -10:   s += 2.0   # oversold bounce
    elif p_ema50 < -5:  s += 0.5

    # Channel position — at top → revert (bearish), at bottom → bounce (bullish)
    cp = to_float(f.get("channel_pos")) or 0.5
    if cp > 0.85:  s -= 0.5
    if cp < 0.15:  s += 1.0

    # oi_price_corr — positive for equity/metal, NEGATIVE for energy
    # Используем направление по группе — но мы in mean-rev group it's mostly +
    corr = to_float(f.get("oi_price_corr_20d")) or 0
    if corr > 0.3:   s += 0.5
    elif corr < -0.3: s -= 0.3

    return round(s, 2)


def momentum_score(f: dict) -> float:
    """Scoring для momentum класса (fx_rub/metal/fx_cross)."""
    s = universal_signals(f)

    # Price vs EMA — выше тренда → продолжается (positive correlation)
    p_ema50 = to_float(f.get("price_vs_ema50_pct")) or 0
    if p_ema50 > 10:   s += 1.5
    elif p_ema50 > 5:  s += 0.5
    if p_ema50 < -10:  s -= 1.0
    elif p_ema50 < -5: s -= 0.3

    # Channel position — at top → continues up (bullish, в momentum)
    cp = to_float(f.get("channel_pos")) or 0.5
    if cp > 0.85:  s += 1.0
    if cp < 0.15:  s -= 0.5

    # ATR — для FX critical: low vol = strong trend (t=-13.73 для fx_rub)
    atr = to_float(f.get("atr_pct")) or 0
    if atr < 1.0:  s += 2.0     # очень спокойный FX → BULLISH
    elif atr < 1.5: s += 1.0
    if atr > 3.0:  s -= 1.0     # высокая vol → trend breakdown

    # oi_price_corr — positive в momentum classes (+4.34 fx_rub, +2.24 metal)
    corr = to_float(f.get("oi_price_corr_20d")) or 0
    if corr > 0.3:   s += 1.0
    elif corr < -0.3: s -= 0.3

    return round(s, 2)


def dispatch_score(f: dict, asset_type: str) -> tuple[float, str]:
    """Return (score, group_label)."""
    if asset_type in MOMENTUM_TYPES:
        return momentum_score(f), "momentum"
    elif asset_type in MEANREV_TYPES:
        return meanrev_score(f), "meanrev"
    else:
        return universal_signals(f), "other"


# ───────────────────────────────────────────────────────────────
# Calibration helpers
# ───────────────────────────────────────────────────────────────
def calibration_table(rows: list[dict]) -> dict:
    buckets = defaultdict(list)
    for r in rows:
        bucket = math.floor(r["_score"])
        buckets[bucket].append(r["_fwd"])
    table = {}
    for b, returns in sorted(buckets.items()):
        n = len(returns)
        rise = sum(1 for ret in returns if ret > MOVE_THRESHOLD_PCT) / n
        fall = sum(1 for ret in returns if ret < -MOVE_THRESHOLD_PCT) / n
        table[b] = {
            "n": n,
            "p_rise": round(rise * 100, 1),
            "p_fall": round(fall * 100, 1),
            "avg_ret": round(fmean(returns), 2),
        }
    return table


def split_train_test(rows: list[dict]) -> tuple[list, list]:
    sorted_rows = sorted(rows, key=lambda r: r["date"])
    split_idx = int(len(sorted_rows) * TRAIN_FRACTION)
    return sorted_rows[:split_idx], sorted_rows[split_idx:]


def print_calibration(label: str, tbl: dict):
    print(f"\n--- {label} ---")
    print(f"{'bucket':>10} | {'n':>6} | {'P(↑>5%)':>9} | {'P(↓>5%)':>9} | {'avg_ret':>9}")
    for b, s in sorted(tbl.items()):
        print(f"  [{b:>+3}, {b+1:>+3}) | {s['n']:>6} | {s['p_rise']:>7.1f}% | "
              f"{s['p_fall']:>7.1f}% | {s['avg_ret']:>+7.2f}%")


def compare_train_test(train_tbl: dict, test_tbl: dict, group_label: str):
    print(f"\n--- OVERFIT CHECK: {group_label} train vs test ---")
    print(f"{'bucket':>8} | {'tr_n':>5} | {'tr_↑':>6} | {'te_n':>5} | {'te_↑':>6} | {'diff':>6} | flag")
    all_b = sorted(set(train_tbl) | set(test_tbl))
    for b in all_b:
        t = train_tbl.get(b, {"n": 0, "p_rise": 0})
        v = test_tbl.get(b, {"n": 0, "p_rise": 0})
        diff = v["p_rise"] - t["p_rise"]
        if t["n"] >= 30 and v["n"] >= 30:
            if abs(diff) <= 5:
                flag = "OK ✓"
            elif abs(diff) <= 10:
                flag = "drift"
            else:
                flag = "OVERFIT ⚠"
        else:
            flag = "(low n)"
        print(f"  [{b:>+3},{b+1:>+3}) | {t['n']:>5} | {t['p_rise']:>5.1f}% | "
              f"{v['n']:>5} | {v['p_rise']:>5.1f}% | {diff:>+5.1f}% | {flag}")


# ───────────────────────────────────────────────────────────────
# Main
# ───────────────────────────────────────────────────────────────
def main():
    rows = []
    with open(CSV_PATH) as f:
        for r in csv.DictReader(f):
            sectype = r["sectype"]
            asset_type = TYPE_MAP.get(sectype, "other")
            score, group = dispatch_score(r, asset_type)
            fwd = to_float(r.get("fwd_return"))
            if fwd is None:
                continue
            r["_type"] = asset_type
            r["_group"] = group
            r["_score"] = score
            r["_fwd"] = fwd
            rows.append(r)
    print(f"Loaded {len(rows)} rows.")

    # Group counts
    print(f"\nGroup distribution:")
    by_group = defaultdict(list)
    for r in rows:
        by_group[r["_group"]].append(r)
    for g, gr in by_group.items():
        types = defaultdict(int)
        for r in gr:
            types[r["_type"]] += 1
        print(f"  {g:>10}: {len(gr):>5} datapoints | " +
              ", ".join(f"{t}={c}" for t, c in sorted(types.items())))

    # ─────────────────────────────────────────────────────
    # Per-group calibration + train/test split
    # ─────────────────────────────────────────────────────
    final_tables = {}
    for group_name in ["meanrev", "momentum"]:
        group_rows = by_group.get(group_name, [])
        if not group_rows:
            continue
        print("\n" + "=" * 75)
        print(f"GROUP: {group_name.upper()} (n={len(group_rows)})")
        print("=" * 75)

        # Full table
        full_tbl = calibration_table(group_rows)
        print_calibration(f"{group_name} — full dataset", full_tbl)

        # Train/test split
        train, test = split_train_test(group_rows)
        if not train or not test:
            continue
        train_tbl = calibration_table(train)
        test_tbl = calibration_table(test)
        compare_train_test(train_tbl, test_tbl, group_name)

        final_tables[group_name] = {
            "full": full_tbl,
            "train": train_tbl,
            "test": test_tbl,
            "n_total": len(group_rows),
            "n_train": len(train),
            "n_test": len(test),
        }

    # ─────────────────────────────────────────────────────
    # TOP SIGNALS per group (max scores → fwd_ret)
    # ─────────────────────────────────────────────────────
    for group_name in ["meanrev", "momentum"]:
        gr = by_group.get(group_name, [])
        if not gr:
            continue
        print(f"\n--- TOP 10 SCORES {group_name.upper()} → actual fwd_ret ---")
        print(f"{'date':>11} | {'asset':>8} | {'type':>8} | {'score':>+6} | {'fwd_ret':>+8}")
        top = sorted(gr, key=lambda r: -r["_score"])[:10]
        for r in top:
            print(f"  {r['date']:>11} | {r['sectype']:>8} | {r['_type']:>8} | "
                  f"{r['_score']:>+5.2f} | {r['_fwd']:>+6.2f}%")

        print(f"\n--- BOTTOM 10 SCORES {group_name.upper()} → actual fwd_ret ---")
        bot = sorted(gr, key=lambda r: r["_score"])[:10]
        for r in bot:
            print(f"  {r['date']:>11} | {r['sectype']:>8} | {r['_type']:>8} | "
                  f"{r['_score']:>+5.2f} | {r['_fwd']:>+6.2f}%")

    # Save
    with open("/tmp/research_v3_calibration.json", "w") as f:
        json.dump(final_tables, f, indent=2)
    print(f"\nSaved /tmp/research_v3_calibration.json")


if __name__ == "__main__":
    main()
