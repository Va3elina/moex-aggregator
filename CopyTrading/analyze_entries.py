"""Reverse-engineer the master trader's entry style on 1000PEPEUSDT.

For every Open-Long trade timestamp from AssetChangeDetails, snap to the
nearest 1H bar and compute indicators AT THAT BAR. Compare distributions
vs random hours (baseline). What looks different = the trader's signal.

Indicators tested:
  - vol_ratio_20:  volume[t] / SMA(volume, 20)[t]
  - rsi_14:        Wilder RSI(14) on close
  - bb_pct_b_20:   (close - BB_lower) / (BB_upper - BB_lower) for 20-period BB
  - bb_width_20:   BB_width / SMA(close, 20)  (squeeze measure)
  - ret_1h:        prev-bar return
  - ret_4h:        prior 4-bar return
  - ret_24h:       prior 24-bar return
  - ema_dist_20:   (close - EMA20) / close in %
  - ema_dist_50:   (close - EMA50) / close in %
  - atr_pct_14:    ATR(14) / close (vol regime)
  - pos_in_24h:    (close - min_24h) / (max_24h - min_24h)  [Fib-like position]
  - pos_in_72h:    same on 72-hour window
"""
import warnings
warnings.filterwarnings("ignore")

from pathlib import Path
from glob import glob
import numpy as np
import pandas as pd
from scipy import stats as sp_stats

DIR = Path(__file__).parent


def load_master_pepe_entries():
    """All Open-Long 1000PEPEUSDT trades from AssetChangeDetails (sec precision)."""
    parts = []
    for f in sorted(DIR.glob("*copytrading*.csv")):
        df = pd.read_csv(f, skiprows=1)
        parts.append(df)
    df = pd.concat(parts, ignore_index=True)
    df["Time"] = pd.to_datetime(df["Time"])
    mask = ((df["Contract"] == "1000PEPEUSDT")
            & (df["Direction"] == "Open Long")
            & (df["Type"] == "Trade"))
    return df[mask].copy().sort_values("Time").reset_index(drop=True)


def load_pepe_bars():
    p = DIR / "1000pepe_1h_kucoin.csv"
    df = pd.read_csv(p, parse_dates=["datetime"]).set_index("datetime").sort_index()
    df.index = df.index.tz_localize(None)
    for c in ["open", "high", "low", "close", "volume", "turnover"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def wilder_ma(s, n):
    out = np.full(len(s), np.nan)
    if len(s) < n:
        return pd.Series(out, index=s.index)
    vals = s.values
    out[n - 1] = np.nanmean(vals[:n])
    for i in range(n, len(s)):
        out[i] = (out[i - 1] * (n - 1) + vals[i]) / n
    return pd.Series(out, index=s.index)


def compute_indicators(bars):
    d = bars.copy()
    c, h, l, v = d["close"], d["high"], d["low"], d["volume"]

    d["vol_ratio_20"] = v / v.rolling(20).mean()
    delta = c.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_g = wilder_ma(gain, 14)
    avg_l = wilder_ma(loss, 14)
    rs = avg_g / avg_l
    d["rsi_14"] = 100 - 100 / (1 + rs)

    ma20 = c.rolling(20).mean()
    sd20 = c.rolling(20).std()
    up = ma20 + 2 * sd20
    lo = ma20 - 2 * sd20
    d["bb_pct_b_20"] = (c - lo) / (up - lo)
    d["bb_width_20"] = (up - lo) / ma20

    d["ret_1h"] = c.pct_change()
    d["ret_4h"] = c.pct_change(4)
    d["ret_24h"] = c.pct_change(24)

    ema20 = c.ewm(span=20, adjust=False).mean()
    ema50 = c.ewm(span=50, adjust=False).mean()
    d["ema_dist_20"] = (c - ema20) / c * 100
    d["ema_dist_50"] = (c - ema50) / c * 100

    pc = c.shift(1)
    tr = pd.concat([h - l, (h - pc).abs(), (l - pc).abs()], axis=1).max(axis=1)
    atr = wilder_ma(tr, 14)
    d["atr_pct_14"] = atr / c * 100

    d["pos_in_24h"] = (c - l.rolling(24).min()) / (h.rolling(24).max() - l.rolling(24).min())
    d["pos_in_72h"] = (c - l.rolling(72).min()) / (h.rolling(72).max() - l.rolling(72).min())

    # Forward returns for evaluating entry quality
    for k in [1, 4, 24, 72]:
        d[f"fwd_{k}h"] = c.shift(-k) / c - 1
    return d


def snap_entries_to_bars(entries, bars):
    """Map each entry timestamp to its containing 1H bar (floor to hour)."""
    e = entries.copy()
    e["bar_time"] = e["Time"].dt.floor("h")
    e["bar_time"] = e["bar_time"].dt.tz_localize(None)
    merged = e.merge(bars, left_on="bar_time", right_index=True, how="left")
    return merged


def compare_distributions(entries_df, bars_df, indicators):
    print("\nIndicator distributions at ENTRIES vs random hours\n")
    print(f"{'indicator':18}  {'entry mean':>11}  {'entry med':>10}  "
          f"{'rand mean':>10}  {'rand med':>10}  {'t-stat':>7}  {'p':>9}")
    print("-" * 90)
    base_rand = bars_df.dropna(subset=indicators).sample(min(5000, len(bars_df)), random_state=0)
    for ind in indicators:
        e_vals = entries_df[ind].dropna().values
        r_vals = base_rand[ind].dropna().values
        if len(e_vals) < 10 or len(r_vals) < 10:
            continue
        t, p = sp_stats.ttest_ind(e_vals, r_vals, equal_var=False)
        sig = "***" if p < 0.001 else ("**" if p < 0.01 else ("*" if p < 0.05 else ""))
        print(f"{ind:18}  {e_vals.mean():>11.4f}  {np.median(e_vals):>10.4f}  "
              f"{r_vals.mean():>10.4f}  {np.median(r_vals):>10.4f}  {t:>+6.2f}  {p:>9.2e} {sig}")


def quantile_entry_distribution(entries_df, bars_df, ind, n_bins=10):
    """Show what % of entries fall in each quantile of the indicator vs uniform 10%."""
    valid = bars_df.dropna(subset=[ind])
    try:
        bin_edges = np.quantile(valid[ind].values, np.linspace(0, 1, n_bins + 1))
    except Exception:
        return
    bin_edges = np.unique(bin_edges)
    if len(bin_edges) < 3:
        return
    e_vals = entries_df[ind].dropna().values
    e_bins = np.digitize(e_vals, bin_edges[1:-1])
    counts = pd.Series(e_bins).value_counts(normalize=True).sort_index() * 100
    print(f"\n  '{ind}' entry distribution across deciles of full series (uniform=10%/bin):")
    for i in range(n_bins):
        v = counts.get(i, 0)
        bar = "█" * int(v) if v > 0 else ""
        edge_lo = bin_edges[i] if i < len(bin_edges) - 1 else bin_edges[-1]
        edge_hi = bin_edges[i + 1] if i + 1 < len(bin_edges) else bin_edges[-1]
        print(f"    D{i+1:>2} [{edge_lo:>+7.3f}..{edge_hi:>+7.3f}]: {v:>5.1f}%  {bar}")


def fibonacci_clustering(entries_df, bars_df, lookback_hours=72):
    """For each entry, compute price position vs swing high/low of prior lookback hours.
    Check if entries cluster near classic Fib levels (0.236, 0.382, 0.5, 0.618, 0.786)."""
    fib_levels = [0.236, 0.382, 0.5, 0.618, 0.786, 1.0]
    valid = entries_df.dropna(subset=[f"pos_in_{lookback_hours}h"])
    pos = valid[f"pos_in_{lookback_hours}h"].values
    print(f"\nFibonacci proximity analysis ({lookback_hours}H window):")
    print(f"  Position bucket (in 24-72H range):")
    buckets = [
        (0.00, 0.15, "near LOW (deep pullback)"),
        (0.15, 0.30, "0.236 zone (shallow)"),
        (0.30, 0.45, "0.382 zone (Fib)"),
        (0.45, 0.55, "0.500 zone (mid)"),
        (0.55, 0.70, "0.618 zone (golden)"),
        (0.70, 0.85, "0.786 zone (high)"),
        (0.85, 1.01, "near HIGH (breakout)"),
    ]
    total = len(pos)
    for lo, hi, label in buckets:
        count = ((pos >= lo) & (pos < hi)).sum()
        pct = count / total * 100
        bar = "█" * int(pct / 2)
        print(f"    [{lo:.3f}..{hi:.3f}]  {label:30}  {count:>5} entries ({pct:>5.1f}%)  {bar}")


def main():
    print(f"Loading master trader's 1000PEPE Open Long entries...")
    entries = load_master_pepe_entries()
    print(f"  {len(entries):,} entries between {entries['Time'].min()} and {entries['Time'].max()}")

    print(f"\nLoading Bybit 1H PEPE bars...")
    bars = load_pepe_bars()
    print(f"  {len(bars):,} bars from {bars.index[0]} to {bars.index[-1]}")

    print(f"\nComputing indicators...")
    bars_ind = compute_indicators(bars)

    entries_ind = snap_entries_to_bars(entries, bars_ind)
    # Drop entries outside bar range
    entries_ind = entries_ind.dropna(subset=["close"])
    print(f"  {len(entries_ind):,} entries matched to bars")

    indicators = ["vol_ratio_20", "rsi_14", "bb_pct_b_20", "bb_width_20",
                  "ret_1h", "ret_4h", "ret_24h",
                  "ema_dist_20", "ema_dist_50", "atr_pct_14",
                  "pos_in_24h", "pos_in_72h"]
    compare_distributions(entries_ind, bars_ind, indicators)

    print("\n" + "=" * 90)
    print("Detailed decile distributions for top suspect indicators")
    print("=" * 90)
    for ind in ["vol_ratio_20", "rsi_14", "ret_24h", "pos_in_24h",
                "ema_dist_20", "ema_dist_50", "bb_pct_b_20"]:
        quantile_entry_distribution(entries_ind, bars_ind, ind)

    fibonacci_clustering(entries_ind, bars_ind, 24)
    fibonacci_clustering(entries_ind, bars_ind, 72)

    # ===== Entry quality: what did fwd_24h look like after his entries? =====
    print(f"\n{'='*90}")
    print("Entry quality — forward returns AFTER his entries (vs random hours)")
    print(f"{'='*90}")
    rand_baseline = bars_ind.sample(min(5000, len(bars_ind)), random_state=1)
    for fh in ["fwd_1h", "fwd_4h", "fwd_24h", "fwd_72h"]:
        e_vals = entries_ind[fh].dropna() * 100
        r_vals = rand_baseline[fh].dropna() * 100
        t, p = sp_stats.ttest_ind(e_vals.values, r_vals.values, equal_var=False)
        sig = "***" if p < 0.001 else ("**" if p < 0.01 else ("*" if p < 0.05 else ""))
        print(f"  {fh:8}: entries avg={e_vals.mean():+6.3f}%  median={e_vals.median():+6.3f}%  "
              f"P(up)={(e_vals > 0).mean()*100:>4.1f}%  |  "
              f"random avg={r_vals.mean():+6.3f}%  |  t={t:+5.2f}  p={p:.2e} {sig}")


if __name__ == "__main__":
    main()
