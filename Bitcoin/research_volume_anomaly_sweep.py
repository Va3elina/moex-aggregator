"""Volume-anomaly research — SWEEP over lookback X.

For each lookback X in a grid, recompute vol_ratio = volume / median(volume[-X:]),
re-bin, and measure forward-return statistics per (vol_bin × direction).
Output: how t-statistic and edge evolve with X, and the OPTIMAL X per cell.

Goal: find the lookback at which the volume-anomaly signal is strongest
(not just use X=20 by convention).
"""
import warnings
warnings.filterwarnings("ignore")

from pathlib import Path
import numpy as np
import pandas as pd
from scipy import stats as sp_stats

D1_PATH = Path(__file__).parent / "btc_usd_daily_coinbase.csv"
H1_PATH = Path(__file__).parent / "btc_usd_hourly_coinbase.csv"

LOOKBACKS = [5, 10, 14, 20, 30, 50, 75, 100, 150, 200]
BINS = [0, 1.0, 1.5, 2.0, 3.0, 5.0, 10.0, np.inf]
LABELS = ["<1x", "1-1.5x", "1.5-2x", "2-3x", "3-5x", "5-10x", ">10x"]
FWD_HORIZON = 5  # primary horizon for the edge metric
MIN_N = 20  # minimum sample size per cell to be considered


def load_df(path, time_col):
    df = pd.read_csv(path, parse_dates=[time_col]).set_index(time_col).sort_index()
    return df[["close", "volume"]]


def compute_forward_returns(df, horizons):
    out = df.copy()
    out["ret"] = out["close"].pct_change()
    out["dir"] = np.sign(out["ret"])
    for k in horizons:
        out[f"fwd_{k}"] = out["close"].shift(-k) / out["close"] - 1
    return out


def sweep_one(df, lookbacks, fwd_h):
    """For each (lookback, vol_bin, direction): compute mean fwd_h and t-stat vs baseline."""
    base_fwd = df[f"fwd_{fwd_h}"].dropna().values
    base_mean = base_fwd.mean()
    base_p_up = (df[f"fwd_{fwd_h}"] > 0).mean()

    records = []
    for X in lookbacks:
        vr = df["volume"] / df["volume"].rolling(X).median()
        vb = pd.cut(vr, bins=BINS, labels=LABELS)
        for vbin in LABELS:
            for direction, dir_name in [(+1, "UP"), (-1, "DOWN")]:
                mask = (vb == vbin) & (df["dir"] == direction) & df[f"fwd_{fwd_h}"].notna()
                cell = df.loc[mask, f"fwd_{fwd_h}"].values
                n = len(cell)
                if n < MIN_N:
                    continue
                mean_v = cell.mean()
                p_up = (cell > 0).mean()
                t, p = sp_stats.ttest_ind(cell, base_fwd, equal_var=False)
                records.append({
                    "X": X, "vol_bin": vbin, "dir": dir_name, "n": n,
                    "mean_fwd": mean_v * 100,
                    "delta_vs_base": (mean_v - base_mean) * 100,
                    "p_up_pct": p_up * 100,
                    "delta_p_up": (p_up - base_p_up) * 100,
                    "t_stat": t, "p_value": p,
                })
    return pd.DataFrame(records), base_mean * 100, base_p_up * 100


def print_t_matrix(records, tf_name, fwd_h):
    print(f"\n{'='*120}")
    print(f"  {tf_name}: t-statistic of mean fwd_{fwd_h} vs baseline, by (lookback X × vol_bin × dir)")
    print(f"  (positive t = forward return HIGHER than baseline; negative = LOWER)")
    print(f"{'='*120}")
    for direction in ["UP", "DOWN"]:
        print(f"\n  Direction = {direction} bars")
        # pivot: rows=vol_bin, cols=X, values=t_stat
        sub = records[records["dir"] == direction]
        pvt = sub.pivot_table(index="vol_bin", columns="X", values="t_stat",
                                observed=False)
        pvt = pvt.reindex(LABELS)
        # also count per cell
        cnt = sub.pivot_table(index="vol_bin", columns="X", values="n", observed=False)
        cnt = cnt.reindex(LABELS)

        cols = sorted(pvt.columns)
        hdr = f"    {'vol_bin':<9}  " + "  ".join(f"X={x:>3}".rjust(9) for x in cols)
        print(hdr)
        print("    " + "-" * (len(hdr) - 4))
        for vbin in LABELS:
            if vbin not in pvt.index:
                continue
            row = f"    {vbin:<9}  "
            for x in cols:
                t = pvt.loc[vbin, x]
                n = cnt.loc[vbin, x] if (vbin, x) in cnt.stack().index else np.nan
                if pd.isna(t) or pd.isna(n):
                    row += "        ·".rjust(11)
                else:
                    star = ("***" if abs(t) >= 2.58 else
                            "**" if abs(t) >= 1.96 else
                            "*" if abs(t) >= 1.64 else "")
                    row += f"  {t:>+5.2f}{star:<3}".ljust(11)
            print(row)


def best_per_cell(records, fwd_h):
    """For each (dir, vol_bin), find the lookback X maximizing |t_stat|."""
    out = []
    for direction in ["UP", "DOWN"]:
        for vbin in LABELS:
            sub = records[(records["dir"] == direction) & (records["vol_bin"] == vbin)]
            if sub.empty:
                continue
            sub_abs = sub.assign(abs_t=sub["t_stat"].abs()).sort_values("abs_t", ascending=False)
            best = sub_abs.iloc[0]
            out.append({
                "dir": direction, "vol_bin": vbin,
                "best_X": int(best["X"]),
                "n_at_best": int(best["n"]),
                "t": best["t_stat"],
                "p": best["p_value"],
                f"mean_fwd_{fwd_h}%": best["mean_fwd"],
                "delta_pp_vs_base": best["delta_vs_base"],
                "p_up%": best["p_up_pct"],
                "delta_p_up_pp": best["delta_p_up"],
            })
    return pd.DataFrame(out)


def main():
    fwd_h = FWD_HORIZON
    horizons = [1, fwd_h, 10, 20]

    for path, time_col, tf_name in [
        (D1_PATH, "date", "BTC DAILY"),
        (H1_PATH, "datetime", "BTC HOURLY"),
    ]:
        print(f"\n{'#'*120}")
        print(f"# {tf_name}")
        print(f"{'#'*120}")
        df = load_df(path, time_col)
        df = compute_forward_returns(df, horizons)
        records, base_mean, base_p_up = sweep_one(df, LOOKBACKS, fwd_h)
        print(f"  Bars: {len(df)}    Baseline avg fwd_{fwd_h} = {base_mean:+.3f}%, "
              f"P(fwd_1>0) baseline = {base_p_up:.1f}%")

        print_t_matrix(records, tf_name, fwd_h)

        best = best_per_cell(records, fwd_h)
        # Filter only cells with significant signal
        sig_best = best[best["p"] < 0.05].sort_values("p")
        print(f"\n  STATISTICALLY SIGNIFICANT cells (p<0.05), sorted by p-value:")
        if sig_best.empty:
            print("    none")
        else:
            print(sig_best.to_string(index=False, float_format=lambda x: f"{x:+.3f}" if abs(x) < 10 else f"{x:.1f}"))


if __name__ == "__main__":
    main()
