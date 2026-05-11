"""Out-of-sample validation of top fundamental signals.

Two axes of validation:
  1. CROSS-ASSET: do stable_30d_growth and mcap_z200 work on ETH, LTC, BCH
     the same way they work on BTC? Universal regime signal vs BTC-only quirk.
  2. CROSS-TIME: split BTC history in halves, do signals work in BOTH halves?
     (rigorous in-sample / out-of-sample test)

Plus one new signal:
  3. BTC DOMINANCE — share of BTC in total (BTC+ETH+LTC+BCH) mcap.
     Test: does falling dominance precede altcoin rallies?
"""
import warnings
warnings.filterwarnings("ignore")

from pathlib import Path
import numpy as np
import pandas as pd
from scipy import stats as sp_stats

DIR = Path(__file__).parent
BTC_CM_PATH = DIR / "btc_daily_coinmetrics.csv"
ALT_CM_PATH = DIR / "altcoins_daily_coinmetrics.csv"
STB_PATH = DIR / "stablecoins_daily_coinmetrics.csv"
N_BINS = 5
FWD = [5, 20]


def load_asset_data():
    """Returns dict: asset -> DataFrame with PriceUSD and mcap."""
    out = {}
    btc = pd.read_csv(BTC_CM_PATH, parse_dates=["date"]).set_index("date").sort_index()
    btc["mcap"] = pd.to_numeric(btc["CapMrktCurUSD"], errors="coerce")
    btc["price"] = pd.to_numeric(btc["PriceUSD"], errors="coerce")
    out["BTC"] = btc[["price", "mcap"]].dropna()

    alts = pd.read_csv(ALT_CM_PATH, parse_dates=["date"]).sort_values("date")
    for asset, sub in alts.groupby("asset"):
        sub = sub.set_index("date").sort_index()
        sub["mcap"] = pd.to_numeric(sub["CapMrktCurUSD"], errors="coerce")
        sub["price"] = pd.to_numeric(sub["PriceUSD"], errors="coerce")
        out[asset.upper()] = sub[["price", "mcap"]].dropna()
    return out


def load_stables():
    stb = pd.read_csv(STB_PATH, parse_dates=["date"]).sort_values("date")
    pvt = stb.pivot_table(index="date", columns="asset",
                            values="CapMrktCurUSD", aggfunc="last")
    for c in pvt.columns:
        pvt[c] = pd.to_numeric(pvt[c], errors="coerce")
    total = pvt.sum(axis=1, min_count=1)
    return pd.DataFrame({
        "stable_total": total,
        "stable_30d_growth": total.pct_change(30),
    })


def build_features(df, stables):
    d = df.copy()
    d["log_mcap"] = np.log(d["mcap"])
    d["mcap_z200"] = ((d["log_mcap"] - d["log_mcap"].rolling(200).mean())
                       / d["log_mcap"].rolling(200).std())
    d = d.join(stables, how="left")
    d["ret"] = d["price"].pct_change()
    for k in FWD:
        d[f"fwd_{k}"] = d["price"].shift(-k) / d["price"] - 1
    return d


def quantile_test(df, feat, fwd_col):
    df = df.dropna(subset=[feat, fwd_col]).copy()
    if len(df) < N_BINS * 30:
        return None
    try:
        df["bin"] = pd.qcut(df[feat], N_BINS, labels=False, duplicates="drop")
    except ValueError:
        return None
    rows = []
    for b in range(N_BINS):
        cell = df[df["bin"] == b]
        if len(cell) < 20:
            continue
        t, p = sp_stats.ttest_ind(cell[fwd_col].dropna().values,
                                   df[fwd_col].dropna().values, equal_var=False)
        rows.append({"bin": b, "N": len(cell),
                     "mean%": cell[fwd_col].mean() * 100,
                     "t": t, "p": p})
    return pd.DataFrame(rows) if rows else None


def report_quantiles(label, res):
    if res is None or len(res) == 0:
        print(f"  {label}: insufficient data")
        return
    t_vals = res["t"].tolist() + [np.nan] * (N_BINS - len(res))
    means = res["mean%"].tolist() + [np.nan] * (N_BINS - len(res))
    spread = (res["mean%"].iloc[-1] - res["mean%"].iloc[0]) if len(res) >= 2 else 0
    p_min = res["p"].min()
    ts = "  ".join(f"{v:>+5.2f}" if pd.notna(v) else "    ·" for v in t_vals[:N_BINS])
    ms = "  ".join(f"{v:>+5.2f}" if pd.notna(v) else "    ·" for v in means[:N_BINS])
    print(f"  {label:36}  t:{ts}  mean%:{ms}  spread:{spread:>+6.2f}  pmin:{p_min:.4f}")


def main():
    assets = load_asset_data()
    stables = load_stables()

    print("=" * 130)
    print("PART 1: CROSS-ASSET — do BTC fundamental signals work on ETH/LTC/BCH?")
    print("=" * 130)

    for fwd in FWD:
        print(f"\n  Forward horizon: {fwd} bars\n")
        print(f"  {'signal × asset':38}  {'Q1 t':>5}  {'Q2 t':>5}  {'Q3 t':>5}  {'Q4 t':>5}  {'Q5 t':>5}"
              f"  {'Q1 mean':>5}  {'Q2 mean':>5}  {'Q3 mean':>5}  {'Q4 mean':>5}  {'Q5 mean':>5}  {'spread':>7}  {'pmin':>7}")
        print("  " + "-" * 130)
        for sig_name in ["stable_30d_growth", "mcap_z200"]:
            for asset, df in assets.items():
                d = build_features(df, stables)
                res = quantile_test(d, sig_name, f"fwd_{fwd}")
                report_quantiles(f"{sig_name} × {asset}", res)

    print("\n" + "=" * 130)
    print("PART 2: CROSS-TIME — do signals work in BOTH halves of BTC history?")
    print("=" * 130)
    btc = build_features(assets["BTC"], stables).dropna(subset=["mcap_z200"])
    midpoint = btc.index[len(btc) // 2]
    print(f"  Split point: {midpoint.date()} (early half vs late half)")
    halves = {"EARLY (~2016-2020)": btc[btc.index < midpoint],
              "LATE  (~2020-2026)": btc[btc.index >= midpoint]}

    for fwd in FWD:
        print(f"\n  Forward horizon: {fwd} bars\n")
        print(f"  {'signal × period':38}  {'Q1 t':>5}  {'Q2 t':>5}  {'Q3 t':>5}  {'Q4 t':>5}  {'Q5 t':>5}"
              f"  {'Q1 mean':>5}  {'Q2 mean':>5}  {'Q3 mean':>5}  {'Q4 mean':>5}  {'Q5 mean':>5}  {'spread':>7}  {'pmin':>7}")
        print("  " + "-" * 130)
        for sig_name in ["stable_30d_growth", "mcap_z200"]:
            for half_name, df in halves.items():
                res = quantile_test(df, sig_name, f"fwd_{fwd}")
                report_quantiles(f"{sig_name} × {half_name}", res)

    # ===== PART 3: BTC DOMINANCE as NEW SIGNAL =====
    print("\n" + "=" * 130)
    print("PART 3: NEW SIGNAL — BTC dominance (BTC mcap / (BTC + ETH + LTC + BCH) mcap)")
    print("=" * 130)

    # Build combined mcap
    btc_m = assets["BTC"]["mcap"]
    eth_m = assets["ETH"]["mcap"] if "ETH" in assets else pd.Series()
    ltc_m = assets["LTC"]["mcap"] if "LTC" in assets else pd.Series()
    bch_m = assets["BCH"]["mcap"] if "BCH" in assets else pd.Series()

    all_dates = btc_m.index.union(eth_m.index).union(ltc_m.index).union(bch_m.index)
    btc_m = btc_m.reindex(all_dates).ffill()
    eth_m = eth_m.reindex(all_dates).ffill().fillna(0)
    ltc_m = ltc_m.reindex(all_dates).ffill().fillna(0)
    bch_m = bch_m.reindex(all_dates).ffill().fillna(0)
    total_m = btc_m + eth_m + ltc_m + bch_m
    dominance = btc_m / total_m
    dominance_z30 = (dominance - dominance.rolling(30).mean()) / dominance.rolling(30).std()
    dominance_change_30d = dominance - dominance.shift(30)

    # Test dominance impact on BTC AND on ETH forward returns
    for asset in ["BTC", "ETH"]:
        if asset not in assets:
            continue
        d = build_features(assets[asset], stables)
        d["dominance"] = dominance.reindex(d.index)
        d["dominance_change_30d"] = dominance_change_30d.reindex(d.index)
        d["dominance_z30"] = dominance_z30.reindex(d.index)

        for fwd in FWD:
            print(f"\n  Forward horizon fwd_{fwd} on {asset}:")
            for feat in ["dominance", "dominance_change_30d", "dominance_z30"]:
                res = quantile_test(d, feat, f"fwd_{fwd}")
                report_quantiles(f"{feat} × {asset}", res)

    # ===== PART 4: Pearson correlations summary =====
    print("\n" + "=" * 130)
    print("PART 4: Pearson correlations of universal signals with forward returns, per asset")
    print("=" * 130)
    print(f"  {'signal':22}  {'asset':5}  {'fwd_5':>7}  {'fwd_20':>7}  N")
    print("  " + "-" * 65)
    for asset, df in assets.items():
        d = build_features(df, stables)
        d["dominance"] = dominance.reindex(d.index)
        d["dominance_change_30d"] = dominance_change_30d.reindex(d.index)
        for sig in ["stable_30d_growth", "mcap_z200", "dominance_change_30d"]:
            valid = d.dropna(subset=[sig, "fwd_5", "fwd_20"])
            if len(valid) < 100:
                continue
            c5 = valid[sig].corr(valid["fwd_5"])
            c20 = valid[sig].corr(valid["fwd_20"])
            print(f"  {sig:22}  {asset:5}  {c5:>+6.3f}  {c20:>+6.3f}  N={len(valid)}")


if __name__ == "__main__":
    main()
