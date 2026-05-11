"""Research: do market cap & volume fundamentals predict forward returns?

Merge BTC daily OHLCV (Coinbase) with daily market cap (CoinMetrics).
Compute:
  - mcap level (USD)
  - log mcap
  - mcap regime: rolling-z-score over 200 days (where is mcap vs recent history)
  - turnover ratio: usd_volume / mcap   ← key feature, "how much of total cap traded"
  - turnover ratio vs its own rolling mean

For each feature, bin and measure forward returns over 1/5/10/20 days.
Then cross with the volume-anomaly signal (red bar + 2-5x vol_ratio) to see
if mcap/turnover regime changes the edge.
"""
import warnings
warnings.filterwarnings("ignore")

from pathlib import Path
import numpy as np
import pandas as pd
from scipy import stats as sp_stats

CB_PATH = Path(__file__).parent / "btc_usd_daily_coinbase.csv"
CM_PATH = Path(__file__).parent / "btc_daily_coinmetrics.csv"


def load_merged():
    cb = pd.read_csv(CB_PATH, parse_dates=["date"]).set_index("date").sort_index()
    cm = pd.read_csv(CM_PATH, parse_dates=["date"]).set_index("date").sort_index()
    df = cb.join(cm, how="inner", rsuffix="_cm")
    df = df.dropna(subset=["close", "volume", "CapMrktCurUSD"])
    return df


def compute_features(df):
    df = df.copy()
    df["usd_volume"] = df["close"] * df["volume"]  # Coinbase USD volume
    df["mcap"] = df["CapMrktCurUSD"].astype(float)
    df["mcap_log"] = np.log(df["mcap"])
    df["turnover"] = df["usd_volume"] / df["mcap"]  # fraction of mcap traded on Coinbase

    # 200-day rolling z-score for log-mcap (regime indicator)
    df["mcap_z200"] = (df["mcap_log"] - df["mcap_log"].rolling(200).mean()) \
                       / df["mcap_log"].rolling(200).std()

    # Turnover anomaly vs its own 20-day median
    df["turnover_z20"] = (df["turnover"] - df["turnover"].rolling(20).median()) \
                          / df["turnover"].rolling(20).std()

    # Forward returns
    df["ret"] = df["close"].pct_change()
    df["dir"] = np.sign(df["ret"])
    for k in [1, 5, 10, 20]:
        df[f"fwd_{k}"] = df["close"].shift(-k) / df["close"] - 1

    # Vol-anomaly signal from earlier research
    df["vol_ratio_20"] = df["volume"] / df["volume"].rolling(20).median()
    return df.dropna(subset=["mcap_z200", "fwd_5"])


def quantile_table(df, feat, label, n_bins=5):
    """Group by quantile of `feat`, report forward returns per bin."""
    q_labels = [f"Q{i+1}" for i in range(n_bins)]
    df = df.copy()
    df["bin"] = pd.qcut(df[feat], n_bins, labels=q_labels, duplicates="drop")
    base_fwd5 = df["fwd_5"].mean() * 100
    base_fwd1 = (df["fwd_1"] > 0).mean() * 100
    print(f"\n  Quantile bins of {label}  (baseline: avg fwd_5={base_fwd5:+.3f}%, P(fwd_1>0)={base_fwd1:.1f}%)")
    rows = []
    for b in q_labels:
        cell = df[df["bin"] == b]
        if len(cell) < 20:
            continue
        # Boundary values for the bin
        lo, hi = df.loc[df["bin"] == b, feat].min(), df.loc[df["bin"] == b, feat].max()
        t, p = sp_stats.ttest_ind(cell["fwd_5"].values, df["fwd_5"].values, equal_var=False)
        rows.append({
            "bin": b, "N": len(cell),
            f"{label}_lo": lo, f"{label}_hi": hi,
            "P_fwd1_up%": (cell["fwd_1"] > 0).mean() * 100,
            "avg_fwd_5%": cell["fwd_5"].mean() * 100,
            "avg_fwd_20%": cell["fwd_20"].mean() * 100,
            "t_vs_base": t, "p_value": p,
        })
    out = pd.DataFrame(rows)
    print(out.to_string(index=False, float_format=lambda x: f"{x:+.3f}" if abs(x) < 100 else f"{x:.2e}"))


def cross_signal_x_regime(df, signal_mask, signal_name, regime_feat, regime_label, n_bins=4):
    """Compute the volume-anomaly signal edge within each regime bin."""
    if signal_mask.sum() < 30:
        print(f"\n  Signal '{signal_name}' too sparse: N={signal_mask.sum()}")
        return
    q_labels = [f"Q{i+1}" for i in range(n_bins)]
    df = df.copy()
    df["regime"] = pd.qcut(df[regime_feat], n_bins, labels=q_labels, duplicates="drop")
    sig_df = df[signal_mask]
    base_sig_fwd5 = sig_df["fwd_5"].mean() * 100
    base_all_fwd5 = df["fwd_5"].mean() * 100
    print(f"\n  '{signal_name}' edge by {regime_label} quantile")
    print(f"    Overall signal stats: N={len(sig_df)}, "
          f"avg fwd_5={base_sig_fwd5:+.3f}% (vs {base_all_fwd5:+.3f}% baseline)")
    rows = []
    for b in q_labels:
        cell = sig_df[sig_df["regime"] == b]
        n = len(cell)
        if n < 10:
            rows.append({"regime": b, "N": n})
            continue
        t, p = sp_stats.ttest_ind(cell["fwd_5"].values, df["fwd_5"].values, equal_var=False)
        rows.append({
            "regime": b, "N": n,
            "P_fwd1_up%": (cell["fwd_1"] > 0).mean() * 100,
            "avg_fwd_5%": cell["fwd_5"].mean() * 100,
            "avg_fwd_20%": cell["fwd_20"].mean() * 100,
            "t_vs_all": t, "p": p,
        })
    out = pd.DataFrame(rows)
    print(out.to_string(index=False, float_format=lambda x: f"{x:+.3f}" if abs(x) < 100 else f"{x:.2e}"))


def main():
    df = compute_features(load_merged())
    print(f"Merged dataset: {len(df)} daily bars  ({df.index[0].date()} .. {df.index[-1].date()})")
    print(f"  Mcap range: ${df['mcap'].min()/1e9:.1f}B .. ${df['mcap'].max()/1e9:.1f}B")
    print(f"  Turnover range: {df['turnover'].min()*100:.3f}% .. {df['turnover'].max()*100:.2f}%")

    # === Q1: do forward returns depend on mcap level? ===
    print(f"\n{'='*100}")
    print("Q1: Forward returns by absolute market-cap level (log mcap)")
    print(f"{'='*100}")
    quantile_table(df, "mcap_log", "log_mcap")

    # === Q2: do forward returns depend on mcap regime (z-score vs 200d) ===
    print(f"\n{'='*100}")
    print("Q2: Forward returns by mcap REGIME (z-score vs 200-day baseline)")
    print(f"{'='*100}")
    print("  Lower Z = mcap unusually LOW (after crash). Higher Z = mcap unusually HIGH (after rally).")
    quantile_table(df, "mcap_z200", "mcap_z200")

    # === Q3: turnover ratio (volume/mcap) ===
    print(f"\n{'='*100}")
    print("Q3: Forward returns by TURNOVER ratio (Coinbase USD volume / mcap)")
    print(f"{'='*100}")
    print("  Higher turnover = more of mcap traded that day. Proxy for engagement/speculation.")
    quantile_table(df, "turnover", "turnover")

    # === Q4: turnover anomaly (z-score vs 20-day) ===
    print(f"\n{'='*100}")
    print("Q4: Forward returns by TURNOVER anomaly (z-score vs 20d median)")
    print(f"{'='*100}")
    quantile_table(df, "turnover_z20", "turnover_z20")

    # === Q5: cross volume-anomaly signal with mcap regime ===
    print(f"\n{'='*100}")
    print("Q5: Does the vol-anomaly signal edge depend on mcap regime?")
    print(f"{'='*100}")
    # Signal: red bar + vol_ratio 2-5x (the one we found ~marginal on daily)
    sig_redvol = (df["dir"] == -1) & (df["vol_ratio_20"] >= 2) & (df["vol_ratio_20"] < 5)
    cross_signal_x_regime(df, sig_redvol, "RED bar + 2-5x vol", "mcap_z200", "mcap_regime")

    # === Q6: cross vol-anomaly with turnover regime ===
    print(f"\n{'='*100}")
    print("Q6: Does the vol-anomaly signal edge depend on TURNOVER regime?")
    print(f"{'='*100}")
    cross_signal_x_regime(df, sig_redvol, "RED bar + 2-5x vol", "turnover", "turnover")

    # === Q7: green bar version ===
    print(f"\n{'='*100}")
    print("Q7: Does the GREEN bar + 2-5x vol signal edge depend on mcap regime?")
    print(f"{'='*100}")
    sig_grnvol = (df["dir"] == +1) & (df["vol_ratio_20"] >= 2) & (df["vol_ratio_20"] < 5)
    cross_signal_x_regime(df, sig_grnvol, "GREEN bar + 2-5x vol", "mcap_z200", "mcap_regime")


if __name__ == "__main__":
    main()
