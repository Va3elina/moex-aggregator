"""Research forward-return dependence on fundamental BTC data:

1. mcap regime (mcap_z200) — previous research
2. exchange net flow (FlowIn − FlowOut) USD, normalized by mcap
3. exchange flow trend (30d rolling)
4. stablecoin total mcap (USDT + USDC) and its 30d growth — "money on sidelines"
5. global trading volume (CryptoCompare aggregate) vs Coinbase share
6. global turnover (global USD volume / mcap)
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
GLB_PATH = DIR / "btc_daily_global_cryptocompare.csv"
STB_PATH = DIR / "stablecoins_daily_coinmetrics.csv"


def load_all():
    cb = pd.read_csv(CB_PATH, parse_dates=["date"]).set_index("date").sort_index()
    cb = cb[["close", "volume"]].rename(columns={"close": "close_cb", "volume": "vol_btc_cb"})
    cm = pd.read_csv(CM_PATH, parse_dates=["date"]).set_index("date").sort_index()
    cm = cm.rename(columns={"CapMrktCurUSD": "mcap",
                              "FlowInExUSD": "flow_in",
                              "FlowOutExUSD": "flow_out"})
    glb = pd.read_csv(GLB_PATH, parse_dates=["date"]).set_index("date").sort_index()
    glb = glb[["volume_btc", "volume_usd"]].rename(
        columns={"volume_btc": "vol_btc_global", "volume_usd": "vol_usd_global"})
    stb = pd.read_csv(STB_PATH, parse_dates=["date"]).sort_values("date")
    stb_pvt = stb.pivot_table(index="date", columns="asset", values="CapMrktCurUSD",
                                aggfunc="last")
    stb_pvt.columns = [f"mcap_{c}" for c in stb_pvt.columns]

    df = cb.join(cm[["PriceUSD", "mcap", "SplyCur", "flow_in", "flow_out"]], how="inner")
    df = df.join(glb, how="left")
    df = df.join(stb_pvt, how="left")
    df["mcap"] = pd.to_numeric(df["mcap"], errors="coerce")
    for c in ["flow_in", "flow_out", "mcap_usdt", "mcap_usdc",
              "vol_btc_global", "vol_usd_global"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def compute_features(df):
    df = df.copy()
    # Forward returns
    df["ret"] = df["close_cb"].pct_change()
    df["dir"] = np.sign(df["ret"])
    for k in [1, 5, 10, 20]:
        df[f"fwd_{k}"] = df["close_cb"].shift(-k) / df["close_cb"] - 1

    # 1. Mcap regime
    df["mcap_log"] = np.log(df["mcap"])
    df["mcap_z200"] = ((df["mcap_log"] - df["mcap_log"].rolling(200).mean())
                       / df["mcap_log"].rolling(200).std())

    # 2. Exchange net flow (positive = depositing TO exchange ⇒ bearish)
    df["net_flow"] = df["flow_in"] - df["flow_out"]
    df["net_flow_per_mcap"] = df["net_flow"] / df["mcap"]
    df["net_flow_z30"] = ((df["net_flow_per_mcap"]
                            - df["net_flow_per_mcap"].rolling(30).mean())
                           / df["net_flow_per_mcap"].rolling(30).std())

    # 3. Flow trend: 30-day cumulative net flow vs mcap (drift)
    df["net_flow_30d_per_mcap"] = (df["net_flow"].rolling(30).sum() / df["mcap"])

    # 4. Stablecoin total + 30d growth
    df["stable_total"] = df["mcap_usdt"].fillna(0) + df["mcap_usdc"].fillna(0)
    df["stable_30d_growth"] = df["stable_total"].pct_change(30)

    # 5. Volume coverage: Coinbase share
    df["vol_usd_cb"] = df["close_cb"] * df["vol_btc_cb"]
    df["coinbase_share"] = df["vol_usd_cb"] / df["vol_usd_global"]
    df["coinbase_share"] = df["coinbase_share"].clip(0, 1)

    # 6. Global turnover (USD volume / mcap)
    df["turnover_global"] = df["vol_usd_global"] / df["mcap"]
    df["turnover_global_z20"] = ((df["turnover_global"]
                                   - df["turnover_global"].rolling(20).median())
                                  / df["turnover_global"].rolling(20).std())

    return df


def quantile_report(df, feat, label, n_bins=5, fwd="fwd_5"):
    df = df.dropna(subset=[feat, fwd]).copy()
    if len(df) < 100:
        print(f"  {label}: too few rows ({len(df)})")
        return
    q_labels = [f"Q{i+1}" for i in range(n_bins)]
    df["bin"] = pd.qcut(df[feat], n_bins, labels=q_labels, duplicates="drop")
    base = df[fwd].mean() * 100
    p_up_base = (df["fwd_1"] > 0).mean() * 100

    print(f"\n  Quantiles of {label} (N={len(df)}, baseline avg {fwd}={base:+.3f}%, "
          f"P(fwd_1>0)={p_up_base:.1f}%)")
    rows = []
    for b in q_labels:
        cell = df[df["bin"] == b]
        n = len(cell)
        if n < 20:
            continue
        lo = df.loc[df["bin"] == b, feat].min()
        hi = df.loc[df["bin"] == b, feat].max()
        t, p = sp_stats.ttest_ind(cell[fwd].values, df[fwd].values, equal_var=False)
        rows.append({
            "bin": b, "N": n,
            f"{label}_lo": lo, f"{label}_hi": hi,
            "P_up1%": (cell["fwd_1"] > 0).mean() * 100,
            f"avg_{fwd}%": cell[fwd].mean() * 100,
            "avg_fwd_20%": cell["fwd_20"].mean() * 100,
            "t": t, "p": p,
        })
    out = pd.DataFrame(rows)
    fmt = lambda x: f"{x:+.3f}" if abs(x) < 100 else (f"{x:.2e}" if abs(x) < 1e10 else f"{x:.2e}")
    print(out.to_string(index=False, float_format=fmt))


def main():
    df = compute_features(load_all())
    print(f"Merged dataset: {len(df)} daily bars  "
          f"({df.index[0].date()} .. {df.index[-1].date()})")
    print(f"  Mcap: ${df['mcap'].min()/1e9:.1f}B .. ${df['mcap'].max()/1e9:.0f}B")
    print(f"  Stablecoin total: ${df['stable_total'].dropna().min()/1e9:.1f}B "
          f".. ${df['stable_total'].dropna().max()/1e9:.0f}B")
    print(f"  Coinbase share of global vol: median {df['coinbase_share'].median()*100:.1f}%, "
          f"range {df['coinbase_share'].quantile(0.05)*100:.1f}-{df['coinbase_share'].quantile(0.95)*100:.1f}%")

    print(f"\n{'='*100}")
    print("Q1: Mcap regime (z-score vs 200d) — REFERENCE (previously found edge)")
    print(f"{'='*100}")
    quantile_report(df, "mcap_z200", "mcap_z200")

    print(f"\n{'='*100}")
    print("Q2: Exchange NET FLOW (USD, normalized by mcap)")
    print("  Positive = depositing TO exchanges = sell pressure brewing")
    print(f"{'='*100}")
    quantile_report(df, "net_flow_per_mcap", "net_flow_pct")

    print(f"\n{'='*100}")
    print("Q3: 30-day cumulative net flow (longer trend, normalized)")
    print(f"{'='*100}")
    quantile_report(df, "net_flow_30d_per_mcap", "net_flow_30d_pct")

    print(f"\n{'='*100}")
    print("Q4: Stablecoin total mcap (USDT+USDC) — money on sidelines")
    print(f"{'='*100}")
    quantile_report(df, "stable_total", "stable_total_usd")

    print(f"\n{'='*100}")
    print("Q5: Stablecoin 30-day GROWTH rate — minting vs burning")
    print("  Positive = stables MINTED = new buying power entering")
    print(f"{'='*100}")
    quantile_report(df, "stable_30d_growth", "stable_30d_growth")

    print(f"\n{'='*100}")
    print("Q6: Coinbase SHARE of global volume (Coinbase USD / global USD)")
    print(f"{'='*100}")
    quantile_report(df, "coinbase_share", "coinbase_share")

    print(f"\n{'='*100}")
    print("Q7: Global turnover (global USD volume / mcap)")
    print(f"{'='*100}")
    quantile_report(df, "turnover_global", "turnover_global")

    print(f"\n{'='*100}")
    print("Q8: Global turnover ANOMALY (z-score vs 20-day median)")
    print(f"{'='*100}")
    quantile_report(df, "turnover_global_z20", "turnover_z20")

    # ===== Correlation matrix between fundamentals and forward returns =====
    print(f"\n{'='*100}")
    print("Pearson correlations between fundamentals and forward returns")
    print(f"{'='*100}")
    feats = ["mcap_z200", "net_flow_per_mcap", "net_flow_30d_per_mcap",
              "stable_total", "stable_30d_growth", "coinbase_share",
              "turnover_global", "turnover_global_z20"]
    fwd_cols = ["fwd_1", "fwd_5", "fwd_10", "fwd_20"]
    corr = df[feats + fwd_cols].corr().loc[feats, fwd_cols]
    print(corr.to_string(float_format=lambda x: f"{x:+.3f}"))


if __name__ == "__main__":
    main()
