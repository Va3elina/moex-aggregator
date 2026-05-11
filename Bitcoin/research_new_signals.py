"""Systematic search for new statistically significant predictive signals.

Computes ~25 candidate features on BTC daily, runs quantile analysis vs
forward returns (fwd_1, fwd_5, fwd_20). Applies Bonferroni correction for
the large multiple-comparison count. Reports only signals that survive.

Features grouped by family:
  CAL: calendar (DOW, DOM, month, halving phase)
  VOL: volatility (ATR rank, realized vol, range/close)
  REV: mean-reversion / extension (price vs MA50/200, distance to EMA200/ATR)
  MOM: momentum (multi-bar returns, streaks, vol-adjusted momentum)
  CDL: candle (body, wicks, gaps)
  FUN: fundamentals (already-explored, included for comparison)
"""
import warnings
warnings.filterwarnings("ignore")

from pathlib import Path
import numpy as np
import pandas as pd
from scipy import stats as sp_stats

DIR = Path(__file__).parent
CB_PATH = DIR / "btc_usd_daily_coinbase.csv"
CM_PATH = DIR / "btc_daily_coinmetrics.csv"

ALPHA = 0.05
N_BINS = 5
MIN_N_PER_BIN = 30
FWD_HORIZONS = [1, 5, 20]

# BTC halving block heights and approximate dates
HALVINGS = [
    pd.Timestamp("2012-11-28"),
    pd.Timestamp("2016-07-09"),
    pd.Timestamp("2020-05-11"),
    pd.Timestamp("2024-04-20"),
    pd.Timestamp("2028-04-01"),  # estimate
]


def wilder_ma(series, n):
    out = np.full(len(series), np.nan)
    if len(series) < n: return pd.Series(out, index=series.index)
    vals = series.values
    out[n - 1] = np.nanmean(vals[:n])
    for i in range(n, len(series)):
        out[i] = (out[i - 1] * (n - 1) + vals[i]) / n
    return pd.Series(out, index=series.index)


def compute_atr(df, period=14):
    h, l, c = df["high"], df["low"], df["close"]
    pc = c.shift(1)
    tr = pd.concat([(h - l), (h - pc).abs(), (l - pc).abs()], axis=1).max(axis=1)
    return wilder_ma(tr, period)


def halving_phase_pct(date):
    for i, h in enumerate(HALVINGS[:-1]):
        nh = HALVINGS[i + 1]
        if h <= date < nh:
            return (date - h).days / (nh - h).days
    return np.nan


def load_data():
    df = pd.read_csv(CB_PATH, parse_dates=["date"]).set_index("date").sort_index()
    cm = pd.read_csv(CM_PATH, parse_dates=["date"]).set_index("date").sort_index()
    cm["mcap"] = pd.to_numeric(cm["CapMrktCurUSD"], errors="coerce")
    df = df.join(cm[["mcap"]], how="left")
    return df


def build_features(df):
    d = df.copy()
    c, o, h, l = d["close"], d["open"], d["high"], d["low"]
    rng = (h - l).replace(0, np.nan)
    body = (c - o).abs()

    # Returns
    d["ret"] = c.pct_change()
    d["log_ret"] = np.log(c / c.shift(1))
    d["dir"] = np.sign(d["ret"])
    for k in FWD_HORIZONS:
        d[f"fwd_{k}"] = c.shift(-k) / c - 1

    # === CALENDAR ===
    d["CAL_dow"] = d.index.dayofweek            # 0=Mon, 6=Sun
    d["CAL_dom"] = d.index.day                  # 1..31
    d["CAL_month"] = d.index.month              # 1..12
    d["CAL_halving_pct"] = d.index.to_series().apply(halving_phase_pct)
    d["CAL_year_pct"] = d.index.dayofyear / 365.25

    # === VOLATILITY ===
    atr_14 = compute_atr(d, 14)
    atr_50 = compute_atr(d, 50)
    d["VOL_atr_pct_rank_200"] = atr_14.rolling(200).rank(pct=True)
    d["VOL_realized_std_30"] = d["ret"].rolling(30).std()
    d["VOL_realized_std_z200"] = ((d["VOL_realized_std_30"] - d["VOL_realized_std_30"].rolling(200).mean())
                                   / d["VOL_realized_std_30"].rolling(200).std())
    d["VOL_range_close_ratio"] = rng / c
    d["VOL_atr_ratio_14_50"] = atr_14 / atr_50          # short vs long vol

    # === MEAN-REVERSION / EXTENSION ===
    ma50 = c.rolling(50).mean()
    ma200 = c.rolling(200).mean()
    ema200 = c.ewm(span=200, adjust=False).mean()
    d["REV_z_vs_ma50"] = (c - ma50) / c.rolling(50).std()
    d["REV_z_vs_ma200"] = (c - ma200) / c.rolling(200).std()
    d["REV_dist_ema200_atr"] = (c - ema200) / atr_14
    d["REV_dd_from_200d_high"] = c / c.rolling(200).max() - 1
    d["REV_rise_from_200d_low"] = c / c.rolling(200).min() - 1

    # === MOMENTUM / STREAKS ===
    d["MOM_ret_5"] = c.pct_change(5)
    d["MOM_ret_20"] = c.pct_change(20)
    d["MOM_ret_60"] = c.pct_change(60)
    d["MOM_ret_120"] = c.pct_change(120)
    # Volatility-adjusted momentum (Sharpe of last 20 daily returns)
    d["MOM_sharpe_20"] = d["ret"].rolling(20).mean() / d["ret"].rolling(20).std()
    d["MOM_sharpe_60"] = d["ret"].rolling(60).mean() / d["ret"].rolling(60).std()

    # Consecutive same-direction streak
    sign = np.sign(d["ret"]).fillna(0)
    streak = np.zeros(len(sign))
    cur = 0
    last = 0
    for i, s in enumerate(sign.values):
        if s == 0:
            streak[i] = cur
            continue
        if s == last:
            cur += int(s)
        else:
            cur = int(s)
        streak[i] = cur
        last = s
    d["MOM_streak"] = streak

    # === CANDLE PATTERN ===
    d["CDL_body_pct"] = body / rng
    d["CDL_upper_wick_pct"] = (h - c.clip(o, c)) / rng  # wick above body
    d["CDL_lower_wick_pct"] = (c.clip(c, o) - l) / rng
    d["CDL_upper_wick_pct"] = ((h - d[["close", "open"]].max(axis=1)) / rng)
    d["CDL_lower_wick_pct"] = ((d[["close", "open"]].min(axis=1) - l) / rng)
    d["CDL_gap_pct"] = o / c.shift(1) - 1   # gap-up (>0) or gap-down (<0) vs prev close

    return d


def analyze_continuous(d, feat, n_bins=N_BINS):
    """Bin continuous feature into quantiles, compute forward-return stats per bin."""
    df = d.dropna(subset=[feat, "fwd_5"]).copy()
    if len(df) < n_bins * MIN_N_PER_BIN:
        return None
    try:
        df["bin"] = pd.qcut(df[feat], n_bins, labels=False, duplicates="drop")
    except ValueError:
        return None
    rows = []
    for b in range(n_bins):
        cell = df[df["bin"] == b]
        n = len(cell)
        if n < MIN_N_PER_BIN:
            continue
        feat_range = (df.loc[df["bin"] == b, feat].min(),
                       df.loc[df["bin"] == b, feat].max())
        for fh in FWD_HORIZONS:
            fwd_col = f"fwd_{fh}"
            t, p = sp_stats.ttest_ind(cell[fwd_col].dropna().values,
                                       df[fwd_col].dropna().values,
                                       equal_var=False)
            rows.append({"feat": feat, "bin": b, "N": n,
                         "lo": feat_range[0], "hi": feat_range[1],
                         "fwd_h": fh,
                         "mean%": cell[fwd_col].mean() * 100,
                         "p_up_%": (cell["fwd_1"] > 0).mean() * 100,
                         "t": t, "p": p})
    return pd.DataFrame(rows)


def analyze_discrete(d, feat, fh=5):
    df = d.dropna(subset=[feat, f"fwd_{fh}"]).copy()
    fwd_col = f"fwd_{fh}"
    rows = []
    for v, cell in df.groupby(feat):
        if len(cell) < 50:
            continue
        t, p = sp_stats.ttest_ind(cell[fwd_col].dropna().values,
                                   df[fwd_col].dropna().values,
                                   equal_var=False)
        rows.append({"feat": feat, "value": v, "N": len(cell),
                     "fwd_h": fh,
                     "mean%": cell[fwd_col].mean() * 100,
                     "p_up_%": (cell["fwd_1"] > 0).mean() * 100,
                     "t": t, "p": p})
    return pd.DataFrame(rows)


def main():
    df = build_features(load_data())
    print(f"Loaded {len(df)} daily bars ({df.index[0].date()} .. {df.index[-1].date()})\n")
    base_fwd5 = df["fwd_5"].mean() * 100
    print(f"Baseline: avg fwd_5 = {base_fwd5:+.3f}%, P(fwd_1>0) = {(df['fwd_1']>0).mean()*100:.1f}%\n")

    # Continuous features list
    continuous_feats = [
        # VOL
        "VOL_atr_pct_rank_200", "VOL_realized_std_30", "VOL_realized_std_z200",
        "VOL_range_close_ratio", "VOL_atr_ratio_14_50",
        # REV
        "REV_z_vs_ma50", "REV_z_vs_ma200", "REV_dist_ema200_atr",
        "REV_dd_from_200d_high", "REV_rise_from_200d_low",
        # MOM
        "MOM_ret_5", "MOM_ret_20", "MOM_ret_60", "MOM_ret_120",
        "MOM_sharpe_20", "MOM_sharpe_60",
        # CDL
        "CDL_body_pct", "CDL_upper_wick_pct", "CDL_lower_wick_pct", "CDL_gap_pct",
        # CAL continuous
        "CAL_halving_pct", "CAL_year_pct",
    ]
    discrete_feats = ["CAL_dow", "CAL_dom", "CAL_month", "MOM_streak"]

    # Run all and collect
    all_results = []
    for feat in continuous_feats:
        res = analyze_continuous(df, feat)
        if res is not None:
            all_results.append(res)
    for feat in discrete_feats:
        for fh in FWD_HORIZONS:
            res = analyze_discrete(df, feat, fh)
            if res is not None and len(res) > 0:
                all_results.append(res)

    full = pd.concat(all_results, ignore_index=True)

    # Bonferroni correction
    n_tests = len(full)
    alpha_bonf = ALPHA / n_tests
    print(f"Total tests run: {n_tests}, Bonferroni threshold p < {alpha_bonf:.2e}\n")

    # ====== Section 1: Bonferroni-significant ======
    survive = full[full["p"] < alpha_bonf].sort_values("p")
    print(f"{'='*120}")
    print(f"Section 1: BONFERRONI-survivors (p < {alpha_bonf:.2e}) — {len(survive)} signals")
    print(f"{'='*120}")
    if len(survive) > 0:
        for col in ("lo", "hi"):
            if col in survive.columns:
                survive[col] = survive[col].apply(lambda x: f"{x:+.4f}" if pd.notna(x) else "")
        print(survive[["feat", "bin" if "bin" in survive.columns else "value",
                         "N", "lo" if "lo" in survive.columns else "value",
                         "hi" if "hi" in survive.columns else "value",
                         "fwd_h", "mean%", "p_up_%", "t", "p"]].to_string(
            index=False, float_format=lambda x: f"{x:+.3f}" if abs(x) < 100 else f"{x:.2e}"))
    else:
        print("  None.")

    # ====== Section 2: very strong but not Bonferroni (p < 0.001) ======
    strong = full[(full["p"] < 0.001) & (full["p"] >= alpha_bonf)].sort_values("p")
    print(f"\n{'='*120}")
    print(f"Section 2: VERY STRONG but below Bonferroni threshold (p < 0.001) — {len(strong)} signals")
    print(f"{'='*120}")
    if len(strong) > 0:
        print(strong[["feat", "bin" if "bin" in strong.columns else "value",
                       "N", "fwd_h", "mean%", "p_up_%", "t", "p"]].head(30).to_string(
            index=False, float_format=lambda x: f"{x:+.3f}" if abs(x) < 100 else f"{x:.2e}"))

    # ====== Section 3: by-feature t-stat profile (continuous features only) ======
    print(f"\n{'='*120}")
    print("Section 3: t-statistic profile per continuous feature (fwd_5 only)")
    print("  Q1 (low) → Q5 (high) bin sequence shows the shape of the relationship")
    print(f"{'='*120}")
    cont_fwd5 = full[(full["fwd_h"] == 5) & (full["feat"].isin(continuous_feats))]
    print(f"{'feature':28}  {'Q1 t':>7}  {'Q2 t':>7}  {'Q3 t':>7}  {'Q4 t':>7}  {'Q5 t':>7}  "
          f"{'spread Q5-Q1 mean%':>20}")
    print("-" * 100)
    for feat in continuous_feats:
        sub = cont_fwd5[cont_fwd5["feat"] == feat].sort_values("bin")
        if sub.empty:
            continue
        t_vals = sub["t"].tolist()
        # pad to 5
        t_vals = t_vals + [np.nan] * (5 - len(t_vals))
        means = sub["mean%"].tolist()
        spread = (means[-1] - means[0]) if len(means) >= 2 else 0
        ts = "  ".join(f"{v:>+7.2f}" if pd.notna(v) else "      ·" for v in t_vals[:5])
        print(f"{feat:28}  {ts}  {spread:>+19.2f}")

    # ====== Section 4: calendar (discrete) ======
    print(f"\n{'='*120}")
    print("Section 4: calendar/discrete features at fwd_5")
    print(f"{'='*120}")
    for feat in discrete_feats:
        sub = full[(full["feat"] == feat) & (full["fwd_h"] == 5)].sort_values("value")
        if sub.empty:
            continue
        print(f"\n  {feat}:")
        for _, r in sub.iterrows():
            sig = "***" if r["p"] < 0.001 else ("**" if r["p"] < 0.01 else ("*" if r["p"] < 0.05 else ""))
            print(f"    {r['value']:>3}  N={int(r['N']):>5}  mean fwd_5={r['mean%']:>+6.2f}%  "
                  f"P(up_1)={r['p_up_%']:>4.1f}%  t={r['t']:>+5.2f}  p={r['p']:.4f} {sig}")


if __name__ == "__main__":
    main()
