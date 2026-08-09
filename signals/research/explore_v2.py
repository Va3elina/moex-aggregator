"""
R&D phase 2 — расширение и валидация.

Что нового vs explore.py v1:
  1. 65 активов вместо 10 (полный ALGOPACK_OI_TICKERS).
  2. Расширенные features: participant counts, position sizes,
     daily range/ATR, OI total dynamics, и т.д.
  3. Train/test split 70/30 по времени — проверка overfit'а.
  4. Калибровочные таблицы строятся отдельно на train и test,
     потом сравниваются. Если совпадают на ±5pp — система устойчива.

Запуск:
  cd /opt/frame && /opt/frame/signals/.venv/bin/python -m signals.research.explore_v2

Выходы:
  /tmp/research_v2_raw.csv
  /tmp/research_v2_calibration.json    — обе таблицы (train, test)
  stdout — comparison train vs test
"""
from __future__ import annotations
import csv
import json
import math
import sys
import urllib3
import warnings
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from statistics import fmean, stdev
from typing import Optional

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.simplefilter("ignore")


# ───────────────────────────────────────────────────────────────
# Config — полный список 65 активов из ALGOPACK_OI_TICKERS
# ───────────────────────────────────────────────────────────────
ASSETS = [
    "CR", "CNYRUBF", "Si", "Eu", "IB", "VB", "USDRUBF", "GZ", "IMOEXF", "RB",
    "CC", "GL", "GLDRUBF", "NA", "NR", "ED", "GK", "SV", "SS", "X5",
    "MX", "MM", "NG", "GD", "SR", "SF", "GAZPF", "MN", "YD", "BR",
    "SE", "TN", "PT", "AF", "KC", "FF", "AL", "EURRUBF", "SBERF", "CE",
    "HS", "NK", "RI", "RL", "LK", "UC", "PD", "NM", "MC", "RM",
    "RN", "SP", "SN", "ME", "HY", "BM", "TT", "OJ", "MG", "W4",
    "DX", "CH", "MY", "VI", "AU",
]
PERIOD = "2y"
FWD_HORIZON = 20
MOVE_THRESHOLD = 0.05
WARMUP_DAYS = 60
TRAIN_FRACTION = 0.70  # 70% train, 30% test holdout

API_BASE = "https://localhost"
API_HOST_HEADER = "framedata.ru"


# ───────────────────────────────────────────────────────────────
def fetch_chart(sectype: str, clgroup: str) -> dict:
    r = requests.get(
        f"{API_BASE}/api/chart/{sectype}",
        params={"sectype": sectype, "inst_type": "futures", "interval": 24,
                "clgroup": clgroup, "show_oi": True, "period": PERIOD},
        headers={"Host": API_HOST_HEADER}, verify=False, timeout=60,
    )
    r.raise_for_status()
    return r.json()


def parse_date(iso: str) -> date:
    return datetime.fromisoformat(iso.replace("Z", "+00:00")).date()


def index_by_date(items: list[dict], time_key: str = "time") -> dict[date, dict]:
    out = {}
    for it in items:
        d = parse_date(it[time_key])
        out[d] = it
    return out


# ───────────────────────────────────────────────────────────────
# Indicators
# ───────────────────────────────────────────────────────────────
def ema(values: list[float], period: int) -> list[float]:
    if len(values) < period:
        return []
    alpha = 2.0 / (period + 1)
    out = [fmean(values[:period])]
    for v in values[period:]:
        out.append(out[-1] + alpha * (v - out[-1]))
    return out


def correlation(xs: list[float], ys: list[float]) -> float:
    n = len(xs)
    if n < 3 or n != len(ys):
        return 0.0
    mx, my = fmean(xs), fmean(ys)
    cov = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    sx = math.sqrt(sum((x - mx) ** 2 for x in xs))
    sy = math.sqrt(sum((y - my) ** 2 for y in ys))
    if sx == 0 or sy == 0:
        return 0.0
    return cov / (sx * sy)


def safe_div(a: float, b: float, default: float = 0.0) -> float:
    if b == 0 or b is None:
        return default
    return a / b


def pct_change(prev: Optional[float], cur: Optional[float]) -> float:
    if prev is None or cur is None or prev == 0:
        return 0.0
    return ((cur - prev) / abs(prev)) * 100


# ───────────────────────────────────────────────────────────────
# Features — extended
# ───────────────────────────────────────────────────────────────
def compute_features(
    i: int,
    dates: list[date],
    opens: list[float],
    highs: list[float],
    lows: list[float],
    closes: list[float],
    volumes: list[float],
    fiz_data: list[Optional[dict]],
    yur_data: list[Optional[dict]],
) -> Optional[dict]:
    if i < WARMUP_DAYS:
        return None

    close = closes[i]
    if close is None or close <= 0:
        return None

    closes_sofar = closes[: i + 1]
    ema20_list = ema(closes_sofar, 20)
    ema50_list = ema(closes_sofar, 50)
    if not ema20_list or not ema50_list:
        return None
    ema20, ema50 = ema20_list[-1], ema50_list[-1]

    # Extrema
    win30 = closes_sofar[-30:]
    win90 = closes_sofar[-90:] if len(closes_sofar) >= 90 else closes_sofar
    is_new_high_30 = close >= max(win30)
    is_new_low_30 = close <= min(win30)
    is_new_high_90 = close >= max(win90)
    is_new_low_90 = close <= min(win90)

    # Channel position
    high20 = max(closes_sofar[-20:])
    low20 = min(closes_sofar[-20:])
    channel_pos = safe_div(close - low20, high20 - low20, 0.5)

    # EMA distances
    price_vs_ema20 = safe_div(close - ema20, ema20, 0.0) * 100
    price_vs_ema50 = safe_div(close - ema50, ema50, 0.0) * 100

    # NEW: Daily range / ATR
    high_today = highs[i] if i < len(highs) and highs[i] else close
    low_today = lows[i] if i < len(lows) and lows[i] else close
    daily_range_pct = safe_div(high_today - low_today, close, 0.0) * 100

    # NEW: ATR proxy 14d
    tr_values = []
    for j in range(max(0, i - 14), i + 1):
        h = highs[j] if j < len(highs) and highs[j] else closes[j]
        l = lows[j] if j < len(lows) and lows[j] else closes[j]
        pc = closes[j - 1] if j > 0 else closes[j]
        tr = max(h - l, abs(h - pc), abs(l - pc))
        tr_values.append(tr)
    atr_14 = fmean(tr_values) if tr_values else 0
    atr_pct = safe_div(atr_14, close, 0.0) * 100

    # OI feature extraction helpers
    def get(arr, idx, key):
        if idx < 0 or idx >= len(arr) or arr[idx] is None:
            return None
        v = arr[idx].get(key)
        return float(v) if v is not None else None

    # FIZ features
    fiz_net = get(fiz_data, i, "net_position")
    fiz_net_5d = get(fiz_data, i - 5, "net_position")
    fiz_net_change_5d = pct_change(fiz_net_5d, fiz_net)

    fiz_long = get(fiz_data, i, "pos_long")
    fiz_short = get(fiz_data, i, "pos_short")  # negative
    fiz_long_num = get(fiz_data, i, "pos_long_num")
    fiz_short_num = get(fiz_data, i, "pos_short_num")

    # NEW: participant balance — соотношение покупателей к продавцам
    fiz_part_balance = safe_div(fiz_long_num or 0, fiz_short_num or 1, 1.0)
    # NEW: средний размер позиции
    fiz_avg_long_size = safe_div(fiz_long or 0, fiz_long_num or 1, 0.0)
    fiz_avg_short_size = safe_div(abs(fiz_short or 0), fiz_short_num or 1, 0.0)
    # NEW: доля long в total
    fiz_total = (fiz_long or 0) + abs(fiz_short or 0)
    fiz_long_share = safe_div(fiz_long or 0, fiz_total, 0.5)

    # YUR features
    yur_net = get(yur_data, i, "net_position")
    yur_net_5d = get(yur_data, i - 5, "net_position")
    yur_net_change_5d = pct_change(yur_net_5d, yur_net)

    yur_long = get(yur_data, i, "pos_long")
    yur_short = get(yur_data, i, "pos_short")
    yur_long_num = get(yur_data, i, "pos_long_num")
    yur_short_num = get(yur_data, i, "pos_short_num")
    yur_part_balance = safe_div(yur_long_num or 0, yur_short_num or 1, 1.0)
    yur_total = (yur_long or 0) + abs(yur_short or 0)
    yur_long_share = safe_div(yur_long or 0, yur_total, 0.5)

    # NEW: OI total dynamics (общая открытость растёт / падает)
    fiz_pos_now = get(fiz_data, i, "pos")
    fiz_pos_5d = get(fiz_data, i - 5, "pos")
    oi_total_change_5d = pct_change(fiz_pos_5d, fiz_pos_now)

    # NEW: participants growth (вовлечённость рынка)
    parts_now = (fiz_long_num or 0) + (fiz_short_num or 0)
    parts_5d_ago = (get(fiz_data, i - 5, "pos_long_num") or 0) + (
        get(fiz_data, i - 5, "pos_short_num") or 0
    )
    participants_growth_5d = pct_change(parts_5d_ago, parts_now)

    # OI-price correlation 20d
    closes_20 = closes_sofar[-20:]
    fiz_20 = [
        get(fiz_data, j, "net_position")
        for j in range(i - 19, i + 1)
    ]
    fiz_20_clean = [v for v in fiz_20 if v is not None]
    if len(fiz_20_clean) == 20:
        oi_price_corr_20d = correlation(closes_20, fiz_20_clean)
    else:
        oi_price_corr_20d = 0.0

    # Volume features
    vols_20 = [v for v in volumes[i - 20 : i] if v is not None and v > 0]
    if len(vols_20) >= 10:
        v_mean = fmean(vols_20)
        v_std = stdev(vols_20) if len(vols_20) >= 2 else 0
        v_now = volumes[i] or 0
        volume_z = safe_div(v_now - v_mean, v_std, 0.0)
    else:
        volume_z = 0

    return {
        # Price
        "close": close,
        "ema20": ema20,
        "ema50": ema50,
        "ema20_above_ema50": ema20 > ema50,
        "price_vs_ema20_pct": price_vs_ema20,
        "price_vs_ema50_pct": price_vs_ema50,
        "channel_pos": channel_pos,
        # Extrema
        "is_new_high_30": is_new_high_30,
        "is_new_low_30": is_new_low_30,
        "is_new_high_90": is_new_high_90,
        "is_new_low_90": is_new_low_90,
        # Volatility (NEW)
        "daily_range_pct": daily_range_pct,
        "atr_pct": atr_pct,
        # OI nets
        "fiz_net_change_5d": fiz_net_change_5d,
        "yur_net_change_5d": yur_net_change_5d,
        # OI participants (NEW)
        "fiz_part_balance": fiz_part_balance,
        "yur_part_balance": yur_part_balance,
        "fiz_long_share": fiz_long_share,
        "yur_long_share": yur_long_share,
        "fiz_avg_long_size": fiz_avg_long_size,
        "fiz_avg_short_size": fiz_avg_short_size,
        # OI dynamics (NEW)
        "oi_total_change_5d": oi_total_change_5d,
        "participants_growth_5d": participants_growth_5d,
        "oi_price_corr_20d": oi_price_corr_20d,
        # Volume
        "volume_z": volume_z,
    }


# ───────────────────────────────────────────────────────────────
# Scoring v2 — same as research/explore.py
# ───────────────────────────────────────────────────────────────
def compute_score(f: dict) -> float:
    s = 0.0
    if f["is_new_low_90"]:    s += 3.0
    if f["is_new_high_90"]:   s += 1.5
    if f["is_new_high_30"]:   s += 1.0
    if f["is_new_low_30"]:    s += 0.5
    corr = f["oi_price_corr_20d"]
    if corr > 0.3:    s += 2.0
    elif corr > 0.0:  s += 0.5
    elif corr < -0.3: s -= 0.5
    abs_dev = abs(f["price_vs_ema50_pct"])
    if abs_dev > 4:   s += 1.0
    if abs_dev > 10:  s += 1.0
    if f["fiz_net_change_5d"] > 30:  s -= 0.5
    if f["fiz_net_change_5d"] < -30: s += 0.5
    return round(s, 2)


# ───────────────────────────────────────────────────────────────
def forward_return(closes: list[float], i: int, horizon: int) -> Optional[float]:
    if i + horizon >= len(closes):
        return None
    c0, c1 = closes[i], closes[i + horizon]
    if c0 is None or c1 is None or c0 == 0:
        return None
    return (c1 / c0) - 1


# ───────────────────────────────────────────────────────────────
def analyze_asset(sectype: str) -> list[dict]:
    try:
        chart_fiz = fetch_chart(sectype, "FIZ")
        chart_yur = fetch_chart(sectype, "YUR")
    except Exception as e:
        print(f"  ERROR {sectype}: {type(e).__name__}")
        return []

    candles = chart_fiz.get("candles", [])
    if not candles:
        return []

    candles_by_date = index_by_date(candles)
    fiz_by_date = index_by_date(chart_fiz.get("open_interest", []))
    yur_by_date = index_by_date(chart_yur.get("open_interest", []))

    all_dates = sorted(candles_by_date.keys())

    opens = [float(candles_by_date[d].get("open", 0) or 0) for d in all_dates]
    highs = [float(candles_by_date[d].get("high", 0) or 0) for d in all_dates]
    lows = [float(candles_by_date[d].get("low", 0) or 0) for d in all_dates]
    closes = [float(candles_by_date[d]["close"]) for d in all_dates]
    volumes = [float(candles_by_date[d].get("volume", 0) or 0) for d in all_dates]

    fiz_data = [fiz_by_date.get(d) for d in all_dates]
    yur_data = [yur_by_date.get(d) for d in all_dates]

    rows = []
    for i in range(len(all_dates)):
        f = compute_features(i, all_dates, opens, highs, lows, closes, volumes,
                             fiz_data, yur_data)
        if f is None:
            continue
        score = compute_score(f)
        fwd_ret = forward_return(closes, i, FWD_HORIZON)
        if fwd_ret is None:
            continue
        rows.append({
            "sectype": sectype,
            "date": all_dates[i].isoformat(),
            "score": score,
            "fwd_return": round(fwd_ret * 100, 3),
            **{k: v for k, v in f.items() if not isinstance(v, list)},
        })
    return rows


# ───────────────────────────────────────────────────────────────
# Calibration table builder
# ───────────────────────────────────────────────────────────────
def calibration_table(rows: list[dict]) -> dict:
    buckets = defaultdict(list)
    for r in rows:
        bucket = math.floor(r["score"])
        buckets[bucket].append(r["fwd_return"])
    table = {}
    for b, returns in sorted(buckets.items()):
        n = len(returns)
        rise = sum(1 for ret in returns if ret > MOVE_THRESHOLD * 100) / n
        fall = sum(1 for ret in returns if ret < -MOVE_THRESHOLD * 100) / n
        table[b] = {
            "n": n,
            "p_rise": round(rise * 100, 1),
            "p_fall": round(fall * 100, 1),
            "avg_ret": round(fmean(returns), 2),
        }
    return table


def split_train_test(rows: list[dict], train_frac: float) -> tuple[list, list]:
    """Time-based split: ранние даты в train, поздние в test."""
    sorted_rows = sorted(rows, key=lambda r: r["date"])
    split_idx = int(len(sorted_rows) * train_frac)
    return sorted_rows[:split_idx], sorted_rows[split_idx:]


def print_comparison(train_tbl: dict, test_tbl: dict):
    print("\n" + "=" * 100)
    print("OVERFIT CHECK — TRAIN vs TEST калибровка (бакеты должны совпадать)")
    print("=" * 100)
    print(f"{'bucket':>8} | {'train_n':>8} | {'train_↑>5%':>11} | {'test_n':>8} | {'test_↑>5%':>11} | {'diff':>6} | flag")
    print("-" * 100)
    all_buckets = sorted(set(train_tbl) | set(test_tbl))
    for b in all_buckets:
        t = train_tbl.get(b, {"n": 0, "p_rise": 0})
        v = test_tbl.get(b, {"n": 0, "p_rise": 0})
        diff = v["p_rise"] - t["p_rise"]
        # Флаг — только для buckets с n >= 30 на обеих сторонах
        if t["n"] >= 30 and v["n"] >= 30:
            if abs(diff) <= 5:
                flag = "OK ✓"
            elif abs(diff) <= 10:
                flag = "drift"
            else:
                flag = "OVERFIT ⚠"
        else:
            flag = "(low n)"
        print(
            f"[{b:>+3}, {b+1:>+3}) | {t['n']:>8} | {t['p_rise']:>9.1f}% | "
            f"{v['n']:>8} | {v['p_rise']:>9.1f}% | {diff:>+5.1f}% | {flag}"
        )
    print("=" * 100)


# ───────────────────────────────────────────────────────────────
def main():
    all_rows = []
    for i, sectype in enumerate(ASSETS, 1):
        print(f"[{i:>2}/{len(ASSETS)}] {sectype} ...", flush=True)
        rows = analyze_asset(sectype)
        all_rows.extend(rows)

    print(f"\n## SUMMARY: {len(all_rows)} datapoints from {len(ASSETS)} assets")

    # Full table
    full_tbl = calibration_table(all_rows)
    print("\n=== CALIBRATION на FULL dataset ===")
    print(f"{'bucket':>8} | {'n':>6} | {'P(↑>5%)':>9} | {'P(↓>5%)':>9} | {'avg_ret':>9}")
    for b, s in sorted(full_tbl.items()):
        print(f"[{b:>+3}, {b+1:>+3}) | {s['n']:>6} | {s['p_rise']:>7.1f}% | "
              f"{s['p_fall']:>7.1f}% | {s['avg_ret']:>+7.2f}%")

    # Train/test
    train, test = split_train_test(all_rows, TRAIN_FRACTION)
    if train:
        train_dates = (train[0]["date"], train[-1]["date"])
        test_dates = (test[0]["date"], test[-1]["date"])
        print(f"\nTrain: {len(train)} rows, {train_dates[0]} → {train_dates[1]}")
        print(f"Test:  {len(test)} rows, {test_dates[0]} → {test_dates[1]}")
    train_tbl = calibration_table(train)
    test_tbl = calibration_table(test)
    print_comparison(train_tbl, test_tbl)

    # Save
    with open("/tmp/research_v2_calibration.json", "w") as f:
        json.dump({"train": train_tbl, "test": test_tbl, "full": full_tbl}, f, indent=2)
    fieldnames = list(all_rows[0].keys()) if all_rows else []
    with open("/tmp/research_v2_raw.csv", "w") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        w.writerows(all_rows)
    print(f"\nSaved: /tmp/research_v2_raw.csv, /tmp/research_v2_calibration.json")


if __name__ == "__main__":
    main()
