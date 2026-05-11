"""Research: does anomalous volume predict continuation or reversal?

For each bar i:
  - vol_ratio = volume[i] / median(volume[i-X:i])
  - bar direction = sign of close-to-close return
  - look at forward returns over 1/5/10/20 bars

Bins of volume anomaly:  <1x | 1-1.5x | 1.5-2x | 2-3x | 3-5x | 5-10x | >10x
Split by direction (UP / DOWN bar), report mean forward return + P(up).
Compare each cell to the baseline (all bars) with a t-test.

Run on BTC daily and 1-hour separately for sample-size robustness.
"""
import warnings
warnings.filterwarnings("ignore")

from pathlib import Path
import numpy as np
import pandas as pd
from scipy import stats as sp_stats

D1_PATH = Path(__file__).parent / "btc_usd_daily_coinbase.csv"
H1_PATH = Path(__file__).parent / "btc_usd_hourly_coinbase.csv"

BINS = [0, 1.0, 1.5, 2.0, 3.0, 5.0, 10.0, np.inf]
LABELS = ["<1x", "1-1.5x", "1.5-2x", "2-3x", "3-5x", "5-10x", ">10x"]
LOOKBACK = 20  # bars to compute baseline median volume


def load_df(path, time_col):
    df = pd.read_csv(path, parse_dates=[time_col]).set_index(time_col).sort_index()
    return df[["open", "high", "low", "close", "volume"]]


def prepare(df, fwd_horizons):
    df = df.copy()
    df["vol_baseline"] = df["volume"].rolling(LOOKBACK).median()
    df["vol_ratio"] = df["volume"] / df["vol_baseline"]
    df["ret"] = df["close"].pct_change()
    df["dir"] = np.sign(df["ret"])
    for k in fwd_horizons:
        df[f"fwd_{k}"] = df["close"].shift(-k) / df["close"] - 1
    df["vol_bin"] = pd.cut(df["vol_ratio"], bins=BINS, labels=LABELS)
    keep = ["vol_ratio", "ret", "dir", "vol_bin"] + [f"fwd_{k}" for k in fwd_horizons]
    return df.dropna(subset=keep)


def report_table(df, fwd_horizons, tf_label):
    print(f"\n{'='*100}")
    print(f"  Volume-anomaly forward returns — BTC {tf_label}")
    print(f"  Sample: {len(df)} bars, lookback={LOOKBACK} {tf_label.lower()} bars (median baseline)")
    print(f"{'='*100}")

    # Baseline for the whole dataset
    all_baseline = {f"fwd_{k}": df[f"fwd_{k}"].values for k in fwd_horizons}
    p_up_all = (df["fwd_1"] > 0).mean()
    print(f"  Baseline (all bars):  N={len(df)}  "
          f"P(fwd_1>0)={p_up_all*100:.1f}%  "
          + "  ".join(f"avg fwd_{k}={df[f'fwd_{k}'].mean()*100:+.2f}%"
                       for k in fwd_horizons))

    for direction, dir_name in [(+1, "UP   bar"), (-1, "DOWN bar")]:
        sub = df[df["dir"] == direction]
        if sub.empty:
            continue
        p_up = (sub["fwd_1"] > 0).mean()
        print(f"\n  Direction {dir_name} — overall: N={len(sub)}  "
              f"P(fwd_1>0)={p_up*100:.1f}%  "
              + "  ".join(f"avg fwd_{k}={sub[f'fwd_{k}'].mean()*100:+.2f}%"
                           for k in fwd_horizons))
        hdr = "    " + "vol bin".ljust(9) + "    N    " + \
              "  P(up)  " + \
              "  ".join(f"avg fwd_{k:>2} ({'t' :>5})" for k in fwd_horizons)
        print(hdr)
        print("    " + "-" * (len(hdr) - 4))
        for vbin in LABELS:
            cell = sub[sub["vol_bin"] == vbin]
            n = len(cell)
            if n < 5:
                print(f"    {vbin:>9}  N={n:>4}  --- too few ---")
                continue
            p_up = (cell["fwd_1"] > 0).mean()
            row = f"    {vbin:>9}  N={n:>4}  {p_up*100:>5.1f}%"
            for k in fwd_horizons:
                cell_vals = cell[f"fwd_{k}"].values
                cell_mean = cell_vals.mean()
                # t-test vs full baseline
                t_stat, p_val = sp_stats.ttest_ind(cell_vals, all_baseline[f"fwd_{k}"],
                                                    equal_var=False)
                sig = "***" if p_val < 0.01 else ("**" if p_val < 0.05 else ("*" if p_val < 0.1 else ""))
                row += f"  {cell_mean*100:>+7.2f}% ({t_stat:>+5.2f}){sig:<3}"
            print(row)


def joint_table(df, fwd_horizons, tf_label):
    """Single matrix: P(next bar up) by vol bin × direction."""
    print(f"\n  P(fwd_1 > 0) joint matrix — {tf_label}")
    rows = []
    for vbin in LABELS:
        for dir_val, dir_name in [(+1, "UP"), (-1, "DOWN")]:
            cell = df[(df["vol_bin"] == vbin) & (df["dir"] == dir_val)]
            n = len(cell)
            if n < 5:
                continue
            p_up = (cell["fwd_1"] > 0).mean()
            mean_fwd5 = cell["fwd_5"].mean()
            rows.append({
                "vol_bin": vbin, "dir": dir_name, "n": n,
                "p_up_next": p_up * 100,
                "mean_fwd_5%": mean_fwd5 * 100,
                "mean_fwd_20%": cell["fwd_20"].mean() * 100,
            })
    out = pd.DataFrame(rows)
    print(out.to_string(index=False, float_format=lambda x: f"{x:+.2f}" if abs(x) < 1000 else f"{x:.0f}"))


def main():
    fwd_horizons = [1, 5, 10, 20]

    # Daily
    df_d = load_df(D1_PATH, "date")
    prepared_d = prepare(df_d, fwd_horizons)
    report_table(prepared_d, fwd_horizons, "DAILY")
    joint_table(prepared_d, fwd_horizons, "DAILY")

    # Hourly
    df_h = load_df(H1_PATH, "datetime")
    prepared_h = prepare(df_h, fwd_horizons)
    report_table(prepared_h, fwd_horizons, "HOURLY")
    joint_table(prepared_h, fwd_horizons, "HOURLY")

    # === Summary of effects ===
    print(f"\n{'='*100}")
    print("KEY FINDINGS")
    print(f"{'='*100}")
    # For each TF, for each direction, find which bin has the BIGGEST deviation in fwd_5 mean
    for label, dfx in [("DAILY", prepared_d), ("HOURLY", prepared_h)]:
        base_fwd5 = dfx["fwd_5"].mean() * 100
        print(f"\n  {label}: baseline avg fwd_5 = {base_fwd5:+.2f}%")
        for direction, dname in [(+1, "UP bars"), (-1, "DOWN bars")]:
            sub = dfx[dfx["dir"] == direction]
            base_dir_fwd5 = sub["fwd_5"].mean() * 100
            best = None
            best_dev = 0
            for vbin in LABELS:
                cell = sub[sub["vol_bin"] == vbin]
                if len(cell) < 20:
                    continue
                dev = cell["fwd_5"].mean() * 100 - base_fwd5
                if abs(dev) > abs(best_dev):
                    best_dev, best = dev, vbin
            if best:
                cell = sub[sub["vol_bin"] == best]
                p_up = (cell["fwd_1"] > 0).mean()
                print(f"    {dname:10}: strongest deviation in {best}: "
                      f"avg fwd_5 = {cell['fwd_5'].mean()*100:+.2f}% "
                      f"(Δ vs base = {best_dev:+.2f}pp), N={len(cell)}, "
                      f"P(fwd_1>0)={p_up*100:.1f}%")


if __name__ == "__main__":
    main()
