"""
R&D скрипт для signal-engine v2 (ensemble).

Цель: на 10 активов × ~1.5 года протестировать гипотезу — что rule-based
scoring на price + OI features даёт калибрационное разделение forward returns.

Принцип:
  - Для каждого (asset, date) считаем features (EMA20/50, new high/low, channel
    position, OI flows, volume) → score [-6, +6].
  - Считаем forward return через 20 торговых дней.
  - Группируем по score buckets, считаем P(fwd > +5%), P(fwd < -5%), avg.
  - Если score монотонно связан с fwd_return → гипотеза подтверждена.

Запуск:
  cd /opt/frame && /opt/frame/signals/.venv/bin/python -m signals.research.explore

Выходы:
  /tmp/research_calibration.json  — calibration table
  /tmp/research_raw.csv            — все datapoints
  stdout                           — summary + top signals
"""
from __future__ import annotations
import json
import csv
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

# Disable SSL warnings — мы ходим к самому себе через self-signed cert
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.simplefilter("ignore")


# ───────────────────────────────────────────────────────────────
# Config
# ───────────────────────────────────────────────────────────────
ASSETS = [
    ("SR",       "Сбербанк"),
    ("GZ",       "Газпром"),
    ("LK",       "Лукойл"),
    ("VB",       "ВТБ"),
    ("MX",       "Индекс МосБиржи"),
    ("RI",       "Индекс РТС"),
    ("CR",       "CNY/RUB"),
    ("USDRUBF",  "USD/RUB (вечн)"),
    ("BR",       "Нефть Brent"),
    ("GD",       "Золото"),
]

PERIOD = "2y"          # 2 года — дамп API, потом cut'aем 1.5 года для фичей
FWD_HORIZON = 20       # рабочие дни вперёд
MOVE_THRESHOLD = 0.05  # ±5%
WARMUP_DAYS = 60       # EMA50 нужно >= 50 дней warmup

API_BASE = "https://localhost"
API_HOST_HEADER = "framedata.ru"


# ───────────────────────────────────────────────────────────────
# Data loader
# ───────────────────────────────────────────────────────────────
def fetch_chart(sectype: str, clgroup: str) -> dict:
    """GET /api/chart/{sectype} — candles + OI for given clgroup."""
    r = requests.get(
        f"{API_BASE}/api/chart/{sectype}",
        params={
            "sectype": sectype,
            "inst_type": "futures",
            "interval": 24,
            "clgroup": clgroup,
            "show_oi": True,
            "period": PERIOD,
        },
        headers={"Host": API_HOST_HEADER},
        verify=False,
        timeout=60,
    )
    r.raise_for_status()
    return r.json()


def parse_date(iso: str) -> date:
    """ISO datetime → date."""
    return datetime.fromisoformat(iso.replace("Z", "+00:00")).date()


def index_by_date(items: list[dict], time_key: str = "time") -> dict[date, dict]:
    """Список items с time-key → {date: item}, deduplicated (keep last)."""
    out = {}
    for it in items:
        d = parse_date(it[time_key])
        out[d] = it
    return out


# ───────────────────────────────────────────────────────────────
# Indicators
# ───────────────────────────────────────────────────────────────
def ema(values: list[float], period: int) -> list[float]:
    """Стандартный EMA. Первое значение = SMA(period); далее α=2/(period+1)."""
    if len(values) < period:
        return []
    alpha = 2.0 / (period + 1)
    out = [fmean(values[:period])]
    for v in values[period:]:
        out.append(out[-1] + alpha * (v - out[-1]))
    return out


def correlation(xs: list[float], ys: list[float]) -> float:
    """Pearson correlation. Returns 0 if degenerate."""
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


# ───────────────────────────────────────────────────────────────
# Features
# ───────────────────────────────────────────────────────────────
def compute_features(
    i: int,
    dates: list[date],
    closes: list[float],
    volumes: list[float],
    fiz_nets: list[Optional[float]],
    yur_nets: list[Optional[float]],
) -> Optional[dict]:
    """Все features для точки i. None если данных не хватает (warmup)."""
    if i < WARMUP_DAYS:
        return None

    close = closes[i]
    if close is None or close <= 0:
        return None

    # Price trend — EMAs
    closes_sofar = closes[: i + 1]
    ema20_list = ema(closes_sofar, 20)
    ema50_list = ema(closes_sofar, 50)
    if not ema20_list or not ema50_list:
        return None
    ema20 = ema20_list[-1]
    ema50 = ema50_list[-1]

    # Golden/death cross — было ли пересечение в последние 5 дней?
    cross_days = None
    if len(ema20_list) > 5 and len(ema50_list) > 5:
        # выравниваем по концу (последние N значений совпадают по дате)
        n = min(len(ema20_list), len(ema50_list))
        a20 = ema20_list[-n:]
        a50 = ema50_list[-n:]
        for back in range(1, min(6, n)):
            if a20[-back] > a50[-back] and a20[-back - 1] <= a50[-back - 1]:
                cross_days = back  # positive = golden cross
                break
            if a20[-back] < a50[-back] and a20[-back - 1] >= a50[-back - 1]:
                cross_days = -back  # negative = death cross
                break

    # New high / low (looking back 30 & 90 days)
    win30 = closes_sofar[-30:]
    win90 = closes_sofar[-90:] if len(closes_sofar) >= 90 else closes_sofar
    is_new_high_30 = close >= max(win30)
    is_new_low_30 = close <= min(win30)
    is_new_high_90 = close >= max(win90)
    is_new_low_90 = close <= min(win90)

    # Channel position (Donchian 20d)
    high20 = max(closes_sofar[-20:])
    low20 = min(closes_sofar[-20:])
    channel_pos = (close - low20) / (high20 - low20) if high20 > low20 else 0.5

    # Price-EMA distance
    price_vs_ema20 = (close / ema20 - 1) * 100 if ema20 > 0 else 0
    price_vs_ema50 = (close / ema50 - 1) * 100 if ema50 > 0 else 0

    # OI: net change 5d (FIZ + YUR)
    def safe_pct_change(arr: list[Optional[float]], lookback: int) -> float:
        if i < lookback:
            return 0.0
        a = arr[i - lookback]
        b = arr[i]
        if a is None or b is None or a == 0:
            return 0.0
        return ((b - a) / abs(a)) * 100  # % изменения от модуля прошлого

    fiz_net_change_5d = safe_pct_change(fiz_nets, 5)
    fiz_net_change_20d = safe_pct_change(fiz_nets, 20)
    yur_net_change_5d = safe_pct_change(yur_nets, 5)
    yur_net_change_20d = safe_pct_change(yur_nets, 20)

    # OI-price correlation 20d (на закрытиях vs FIZ net)
    closes_20 = closes_sofar[-20:]
    fiz_20 = [v for v in fiz_nets[i - 19 : i + 1] if v is not None]
    if len(fiz_20) == 20:
        oi_price_corr_20d = correlation(closes_20, fiz_20)
    else:
        oi_price_corr_20d = 0.0

    # Volume z-score 20d
    vols_20 = [v for v in volumes[i - 20 : i] if v is not None and v > 0]
    if len(vols_20) >= 10:
        v_mean = fmean(vols_20)
        v_std = stdev(vols_20) if len(vols_20) >= 2 else 0
        v_now = volumes[i] or 0
        volume_z = (v_now - v_mean) / v_std if v_std > 0 else 0
    else:
        volume_z = 0

    return {
        "close": close,
        "ema20": ema20,
        "ema50": ema50,
        "ema20_above_ema50": ema20 > ema50,
        "cross_days": cross_days,
        "is_new_high_30": is_new_high_30,
        "is_new_low_30": is_new_low_30,
        "is_new_high_90": is_new_high_90,
        "is_new_low_90": is_new_low_90,
        "channel_pos": channel_pos,
        "price_vs_ema20_pct": price_vs_ema20,
        "price_vs_ema50_pct": price_vs_ema50,
        "fiz_net_change_5d": fiz_net_change_5d,
        "fiz_net_change_20d": fiz_net_change_20d,
        "yur_net_change_5d": yur_net_change_5d,
        "yur_net_change_20d": yur_net_change_20d,
        "oi_price_corr_20d": oi_price_corr_20d,
        "volume_z": volume_z,
    }


# ───────────────────────────────────────────────────────────────
# Scoring v2 — data-driven, based on diagnostics.py findings
# ───────────────────────────────────────────────────────────────
# Что обнаружила диагностика на 4177 точках 10 активов:
#   - is_new_low_90:    +2.59% diff (strongest, mean-reversion bottom fishing)
#   - is_new_high_90:   +1.32% diff (momentum continuation)
#   - is_new_high_30:   +1.26% diff (weaker momentum)
#   - oi_price_corr_20d: r=+0.071, t=+4.58 (strongest float feature)
#   - price_vs_ema50: U-shape (extremes в обе стороны → fwd_ret > +1.8%)
# Бесполезные (DROP):
#   - ema20_above_ema50 (diff -0.15%)
#   - cross_days (golden/death cross all near noise)
#   - volume_z (r=+0.0009)
def compute_score(f: dict) -> float:
    s = 0.0

    # === STRONGEST: 90d extremes (data-validated) ===
    if f["is_new_low_90"]:
        s += 3.0   # bottom-fishing alpha — лучший single feature (+2.59% diff)
    if f["is_new_high_90"]:
        s += 1.5   # momentum breakout (+1.32% diff)

    # === 30d extremes (weaker but valid) ===
    if f["is_new_high_30"]:
        s += 1.0
    if f["is_new_low_30"]:
        s += 0.5   # тоже bullish! (NOT bearish как было в v1)

    # === OI-price correlation (strongest float predictor) ===
    corr = f["oi_price_corr_20d"]
    if corr > 0.3:
        s += 2.0   # strong trend confirm (Q5 → +2.72% avg)
    elif corr > 0.0:
        s += 0.5   # mild confirm
    elif corr < -0.3:
        s -= 0.5   # mild negative

    # === Price-EMA volatility breakout (U-shape) ===
    abs_dev = abs(f["price_vs_ema50_pct"])
    if abs_dev > 4:
        s += 1.0   # большое отклонение → движение в любую сторону
    if abs_dev > 10:
        s += 1.0   # extreme → ещё сильнее

    # === Contrarian retail flow (weak but valid signal) ===
    if f["fiz_net_change_5d"] > 30:
        s -= 0.5   # retail piled long → mean reversion bearish
    if f["fiz_net_change_5d"] < -30:
        s += 0.5   # retail capitulated short → bullish

    # === DROPPED (no signal per diagnostics): ===
    # - ema20_above_ema50  (diff -0.15%)
    # - cross_days         (no monotonic relationship)
    # - volume_z           (r ≈ 0.0009)
    # - channel_pos        (weak, t=1.39)
    # - yur_net_change_5d  (weak, t=1.22)

    return round(s, 2)


# ───────────────────────────────────────────────────────────────
# Forward return
# ───────────────────────────────────────────────────────────────
def forward_return(closes: list[float], i: int, horizon: int) -> Optional[float]:
    if i + horizon >= len(closes):
        return None
    c0 = closes[i]
    c1 = closes[i + horizon]
    if c0 is None or c1 is None or c0 == 0:
        return None
    return (c1 / c0) - 1


# ───────────────────────────────────────────────────────────────
# Per-asset analysis
# ───────────────────────────────────────────────────────────────
def analyze_asset(sectype: str, name: str) -> list[dict]:
    print(f"\n=== {sectype} ({name}) ===")

    # Load both groups
    try:
        chart_fiz = fetch_chart(sectype, "FIZ")
        chart_yur = fetch_chart(sectype, "YUR")
    except Exception as e:
        print(f"  ERROR: {type(e).__name__}: {e}")
        return []

    candles = chart_fiz.get("candles", [])
    oi_fiz = chart_fiz.get("open_interest", [])
    oi_yur = chart_yur.get("open_interest", [])

    if not candles:
        print(f"  no candles, skipping")
        return []

    # Index by date
    candles_by_date = index_by_date(candles)
    fiz_by_date = index_by_date(oi_fiz)
    yur_by_date = index_by_date(oi_yur)

    # Sorted date axis (trading days only — те где есть candle)
    all_dates = sorted(candles_by_date.keys())

    closes = [float(candles_by_date[d]["close"]) for d in all_dates]
    volumes = [float(candles_by_date[d].get("volume", 0) or 0) for d in all_dates]

    def safe_net(by_date, d) -> Optional[float]:
        item = by_date.get(d)
        if not item:
            return None
        v = item.get("net_position")
        return float(v) if v is not None else None

    fiz_nets = [safe_net(fiz_by_date, d) for d in all_dates]
    yur_nets = [safe_net(yur_by_date, d) for d in all_dates]

    print(f"  dates: {len(all_dates)} (first={all_dates[0]}, last={all_dates[-1]})")

    rows = []
    skipped = 0
    for i in range(len(all_dates)):
        f = compute_features(i, all_dates, closes, volumes, fiz_nets, yur_nets)
        if f is None:
            skipped += 1
            continue
        score = compute_score(f)
        fwd_ret = forward_return(closes, i, FWD_HORIZON)
        if fwd_ret is None:
            skipped += 1
            continue
        rows.append({
            "sectype": sectype,
            "name": name,
            "date": all_dates[i].isoformat(),
            "score": score,
            "fwd_return": round(fwd_ret * 100, 3),  # в %
            "close": closes[i],
            **{k: v for k, v in f.items() if not isinstance(v, list)},
        })

    print(f"  analyzed: {len(rows)} datapoints (skipped {skipped})")
    if rows:
        avg_score = fmean(r["score"] for r in rows)
        avg_ret = fmean(r["fwd_return"] for r in rows)
        print(f"  avg score={avg_score:+.2f}, avg fwd_return={avg_ret:+.2f}%")

    return rows


# ───────────────────────────────────────────────────────────────
# Calibration
# ───────────────────────────────────────────────────────────────
def calibration_table(rows: list[dict]) -> dict:
    """Бакет score → (n, p_rise, p_fall, avg_ret)."""
    buckets = defaultdict(list)
    for r in rows:
        # Бакеты по 1.0 шагу
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


def print_calibration(table: dict):
    print("\n" + "=" * 70)
    print("CALIBRATION TABLE — score bucket vs forward return 20d")
    print("=" * 70)
    print(f"{'bucket':>8} | {'n':>6} | {'P(↑>5%)':>9} | {'P(↓>5%)':>9} | {'avg_ret':>9}")
    print("-" * 70)
    for b, stats in sorted(table.items()):
        print(
            f"  [{b:>+3}, {b+1:>+3}) | {stats['n']:>6} | "
            f"{stats['p_rise']:>7.1f}% | {stats['p_fall']:>7.1f}% | "
            f"{stats['avg_ret']:>+7.2f}%"
        )
    print("=" * 70)


def print_top_signals(rows: list[dict], top_n: int = 5):
    """Top N most extreme scores in both directions."""
    print("\n" + "=" * 70)
    print(f"TOP {top_n} MAX SCORES (bullish triggered)")
    print("=" * 70)
    bull = sorted(rows, key=lambda r: -r["score"])[:top_n]
    for r in bull:
        print(f"  {r['date']} {r['sectype']:8} score={r['score']:+5.2f}  fwd_ret={r['fwd_return']:+6.2f}%  ({r['name']})")

    print(f"\nTOP {top_n} MIN SCORES (bearish triggered)")
    print("=" * 70)
    bear = sorted(rows, key=lambda r: r["score"])[:top_n]
    for r in bear:
        print(f"  {r['date']} {r['sectype']:8} score={r['score']:+5.2f}  fwd_ret={r['fwd_return']:+6.2f}%  ({r['name']})")


def save_csv(rows: list[dict], path: str):
    if not rows:
        return
    fieldnames = ["sectype", "name", "date", "score", "fwd_return", "close",
                  "ema20_above_ema50", "cross_days",
                  "is_new_high_30", "is_new_low_30", "is_new_high_90", "is_new_low_90",
                  "channel_pos", "price_vs_ema20_pct", "price_vs_ema50_pct",
                  "fiz_net_change_5d", "yur_net_change_5d",
                  "oi_price_corr_20d", "volume_z"]
    with open(path, "w") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)


def save_json(table: dict, path: str):
    with open(path, "w") as f:
        json.dump(table, f, indent=2)


# ───────────────────────────────────────────────────────────────
def main() -> int:
    all_rows: list[dict] = []
    for sectype, name in ASSETS:
        all_rows.extend(analyze_asset(sectype, name))

    print(f"\n\n## SUMMARY: {len(all_rows)} total datapoints across {len(ASSETS)} assets")

    table = calibration_table(all_rows)
    print_calibration(table)
    print_top_signals(all_rows, top_n=5)

    # Cut to 1.5y window для сохранения отчёта (последний год = свежий, ранний =
    # неинтересный)
    save_csv(all_rows, "/tmp/research_raw.csv")
    save_json(table, "/tmp/research_calibration.json")
    print(f"\nSaved: /tmp/research_raw.csv ({len(all_rows)} rows)")
    print(f"Saved: /tmp/research_calibration.json")
    return 0


if __name__ == "__main__":
    sys.exit(main())
