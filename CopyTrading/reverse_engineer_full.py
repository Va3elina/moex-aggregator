"""Maximum-effort reverse engineering of the master trader.

Pipeline:
1. Load 1H data for top-10 traded pairs (+ BTC for cross-asset context).
2. Compute ~35 features per bar on every pair, plus BTC-context features
   merged by timestamp (BTC's own RSI, return, vol regime).
3. Build positive samples (his Open Long events snapped to 1H bars on that
   contract) and negative samples (random non-entry bars on same contracts).
4. Train Gradient Boosting + Random Forest classifier → feature importance
   reveals what actually drove his entries.
5. K-means cluster his entries into 3-5 groups, profile each (= distinct
   sub-setups he uses).
6. Per-cluster forward-return analysis: which sub-setups have edge.
7. Backtest top-importance feature combo as standalone strategy on each pair.
"""
import warnings
warnings.filterwarnings("ignore")

from pathlib import Path
import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import cross_val_score, train_test_split
from sklearn.metrics import roc_auc_score, classification_report

DIR = Path(__file__).parent
PAIRS_DIR = DIR / "pairs_1h"
BTC_PATH = DIR.parent / "Bitcoin" / "btc_usd_hourly_coinbase.csv"

BYBIT_PAIRS = ["1000PEPEUSDT", "DOGEUSDT", "SOLUSDT", "1000FLOKIUSDT",
               "1000BONKUSDT", "SHIB1000USDT", "WIFUSDT", "WLDUSDT",
               "XRPUSDT", "POPCATUSDT"]


def wilder_ma(s, n):
    out = np.full(len(s), np.nan)
    if len(s) < n:
        return pd.Series(out, index=s.index)
    vals = s.values
    out[n - 1] = np.nanmean(vals[:n])
    for i in range(n, len(s)):
        out[i] = (out[i - 1] * (n - 1) + vals[i]) / n
    return pd.Series(out, index=s.index)


def compute_features(bars, prefix=""):
    """35 indicators per bar."""
    d = bars.copy()
    c, h, l, o, v = d["close"], d["high"], d["low"], d["open"], d["volume"]
    f = pd.DataFrame(index=d.index)
    # Volume
    f[f"{prefix}vol_ratio_20"] = v / v.rolling(20).mean()
    f[f"{prefix}vol_ratio_50"] = v / v.rolling(50).mean()
    f[f"{prefix}vol_z_20"] = (v - v.rolling(20).mean()) / v.rolling(20).std()
    # RSI
    delta = c.diff()
    gain = delta.clip(lower=0); loss = -delta.clip(upper=0)
    avg_g = wilder_ma(gain, 14); avg_l = wilder_ma(loss, 14)
    f[f"{prefix}rsi_14"] = 100 - 100 / (1 + avg_g / avg_l)
    avg_g7 = wilder_ma(gain, 7); avg_l7 = wilder_ma(loss, 7)
    f[f"{prefix}rsi_7"] = 100 - 100 / (1 + avg_g7 / avg_l7)
    # MACD
    ema12 = c.ewm(span=12, adjust=False).mean()
    ema26 = c.ewm(span=26, adjust=False).mean()
    macd = ema12 - ema26
    sig = macd.ewm(span=9, adjust=False).mean()
    f[f"{prefix}macd"] = macd / c * 100
    f[f"{prefix}macd_hist"] = (macd - sig) / c * 100
    # Bollinger
    ma20 = c.rolling(20).mean(); sd20 = c.rolling(20).std()
    up = ma20 + 2 * sd20; lo = ma20 - 2 * sd20
    f[f"{prefix}bb_pct_b"] = (c - lo) / (up - lo)
    f[f"{prefix}bb_width"] = (up - lo) / ma20
    # EMAs
    ema20 = c.ewm(span=20, adjust=False).mean()
    ema50 = c.ewm(span=50, adjust=False).mean()
    ema200 = c.ewm(span=200, adjust=False).mean()
    f[f"{prefix}ema_dist_20"] = (c - ema20) / c * 100
    f[f"{prefix}ema_dist_50"] = (c - ema50) / c * 100
    f[f"{prefix}ema_dist_200"] = (c - ema200) / c * 100
    f[f"{prefix}ema_20_50"] = (ema20 - ema50) / c * 100
    f[f"{prefix}ema_50_200"] = (ema50 - ema200) / c * 100
    # Returns multi-TF
    f[f"{prefix}ret_1h"] = c.pct_change()
    f[f"{prefix}ret_4h"] = c.pct_change(4)
    f[f"{prefix}ret_12h"] = c.pct_change(12)
    f[f"{prefix}ret_24h"] = c.pct_change(24)
    f[f"{prefix}ret_72h"] = c.pct_change(72)
    f[f"{prefix}ret_168h"] = c.pct_change(168)  # 1 week
    # ATR
    pc = c.shift(1)
    tr = pd.concat([h - l, (h - pc).abs(), (l - pc).abs()], axis=1).max(axis=1)
    atr14 = wilder_ma(tr, 14)
    f[f"{prefix}atr_pct"] = atr14 / c * 100
    f[f"{prefix}atr_z_50"] = (atr14 - atr14.rolling(50).mean()) / atr14.rolling(50).std()
    # Position in range
    f[f"{prefix}pos_24h"] = (c - l.rolling(24).min()) / (h.rolling(24).max() - l.rolling(24).min())
    f[f"{prefix}pos_72h"] = (c - l.rolling(72).min()) / (h.rolling(72).max() - l.rolling(72).min())
    f[f"{prefix}pos_168h"] = (c - l.rolling(168).min()) / (h.rolling(168).max() - l.rolling(168).min())
    # Drawdown
    f[f"{prefix}dd_24h"] = c / h.rolling(24).max() - 1
    f[f"{prefix}dd_72h"] = c / h.rolling(72).max() - 1
    # Stochastic %K
    f[f"{prefix}stoch_k"] = (c - l.rolling(14).min()) / (h.rolling(14).max() - l.rolling(14).min()) * 100
    # Candle shape
    rng = (h - l).replace(0, np.nan)
    f[f"{prefix}body_pct"] = (c - o).abs() / rng
    f[f"{prefix}upper_wick"] = (h - pd.concat([c, o], axis=1).max(axis=1)) / rng
    f[f"{prefix}lower_wick"] = (pd.concat([c, o], axis=1).min(axis=1) - l) / rng
    f[f"{prefix}is_bull"] = (c > o).astype(int)
    return f


def load_pair(bybit_name):
    p = PAIRS_DIR / f"{bybit_name.lower()}_1h.csv"
    df = pd.read_csv(p, parse_dates=["datetime"]).set_index("datetime").sort_index()
    df.index = df.index.tz_localize(None)
    for c in ["open", "high", "low", "close", "volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def load_btc():
    df = pd.read_csv(BTC_PATH, parse_dates=["datetime"]).set_index("datetime").sort_index()
    df.index = df.index.tz_localize(None)
    return df


def load_his_opens():
    parts = []
    for f in sorted(DIR.glob("*copytrading*.csv")):
        parts.append(pd.read_csv(f, skiprows=1))
    df = pd.concat(parts, ignore_index=True)
    df["Time"] = pd.to_datetime(df["Time"])
    mask = ((df["Direction"] == "Open Long") & (df["Type"] == "Trade")
            & (df["Contract"].isin(BYBIT_PAIRS)))
    out = df[mask].copy().sort_values("Time").reset_index(drop=True)
    out["bar_time"] = out["Time"].dt.floor("h").dt.tz_localize(None)
    return out


def build_dataset(his_opens, all_features_by_pair, btc_features):
    """Build pos+neg sample set for ML."""
    pos_rows = []
    for pair_name, feats in all_features_by_pair.items():
        pair_entries = his_opens[his_opens["Contract"] == pair_name]
        entry_bars = pair_entries["bar_time"].drop_duplicates()
        valid = entry_bars[entry_bars.isin(feats.index)]
        rows = feats.loc[valid].copy()
        rows["pair"] = pair_name
        rows["label"] = 1
        # join BTC context
        rows = rows.join(btc_features, how="left")
        pos_rows.append(rows)
    pos = pd.concat(pos_rows, ignore_index=False)
    pos = pos.dropna()

    # Negative samples: random non-entry bars from same pairs
    neg_rows = []
    rng = np.random.default_rng(42)
    n_per_pair = max(100, len(pos) // len(all_features_by_pair) * 3)  # 3:1 neg:pos
    for pair_name, feats in all_features_by_pair.items():
        pair_entries = his_opens[his_opens["Contract"] == pair_name]
        entry_bars = set(pair_entries["bar_time"].drop_duplicates().tolist())
        valid_bars = feats.dropna().index.tolist()
        non_entry = [b for b in valid_bars if b not in entry_bars]
        n_take = min(n_per_pair, len(non_entry))
        sampled = rng.choice(len(non_entry), size=n_take, replace=False)
        sample_bars = [non_entry[i] for i in sampled]
        rows = feats.loc[sample_bars].copy()
        rows["pair"] = pair_name
        rows["label"] = 0
        rows = rows.join(btc_features, how="left")
        neg_rows.append(rows)
    neg = pd.concat(neg_rows, ignore_index=False).dropna()

    df = pd.concat([pos, neg], ignore_index=True)
    return df, pos, neg


def main():
    print("Loading BTC + 10 pairs + his trades...")
    btc = load_btc()
    btc_f = compute_features(btc, prefix="BTC_")
    print(f"  BTC features: {btc_f.shape}")

    all_features = {}
    for pair in BYBIT_PAIRS:
        try:
            bars = load_pair(pair)
            all_features[pair] = compute_features(bars, prefix="px_")
        except Exception as e:
            print(f"  {pair}: skipped ({e})")
    print(f"  pairs loaded: {len(all_features)}")

    his = load_his_opens()
    print(f"  his opens (top pairs only): {len(his)}")

    print("\nBuilding ML dataset...")
    df, pos, neg = build_dataset(his, all_features, btc_f)
    print(f"  positives: {len(pos)},  negatives: {len(neg)},  total: {len(df)}")

    feature_cols = [c for c in df.columns if c not in ("pair", "label")]
    X = df[feature_cols].values
    y = df["label"].values

    # ===== Gradient Boosting feature importance =====
    print("\n[ML 1/2] Training Gradient Boosting...")
    Xtr, Xte, ytr, yte = train_test_split(X, y, test_size=0.25, random_state=0,
                                            stratify=y)
    gb = GradientBoostingClassifier(n_estimators=200, max_depth=4, learning_rate=0.05,
                                      random_state=0)
    gb.fit(Xtr, ytr)
    proba = gb.predict_proba(Xte)[:, 1]
    auc = roc_auc_score(yte, proba)
    print(f"  Test AUC: {auc:.4f}")
    imp = pd.Series(gb.feature_importances_, index=feature_cols).sort_values(ascending=False)
    print("\n  Top 20 feature importances (GB):")
    for f, v in imp.head(20).items():
        bar = "█" * int(v * 200) if v > 0 else ""
        print(f"    {f:25}  {v*100:>5.2f}%  {bar}")

    # ===== Random Forest for confirmation =====
    print("\n[ML 2/2] Training Random Forest...")
    rf = RandomForestClassifier(n_estimators=300, max_depth=8, random_state=0, n_jobs=-1)
    rf.fit(Xtr, ytr)
    rf_proba = rf.predict_proba(Xte)[:, 1]
    rf_auc = roc_auc_score(yte, rf_proba)
    print(f"  Test AUC: {rf_auc:.4f}")
    rf_imp = pd.Series(rf.feature_importances_, index=feature_cols).sort_values(ascending=False)
    print("  Top 15 feature importances (RF):")
    for f, v in rf_imp.head(15).items():
        print(f"    {f:25}  {v*100:>5.2f}%")

    # ===== K-means clustering of his entries =====
    print("\n[CLUSTER] K-means on his ENTRIES across all pairs...")
    # Use top-15 features to reduce noise
    top_feats = imp.head(15).index.tolist()
    pos_X = pos[top_feats].values
    scaler = StandardScaler()
    pos_Xs = scaler.fit_transform(pos_X)
    for k in [3, 4, 5]:
        km = KMeans(n_clusters=k, random_state=0, n_init=10)
        labels = km.fit_predict(pos_Xs)
        print(f"\n  K={k}:")
        for c in range(k):
            cell = pos.iloc[labels == c]
            n = len(cell)
            print(f"    Cluster {c}: N={n:>5} ({n/len(pos)*100:>4.1f}%)  "
                  f"mean profile:")
            for f in top_feats[:8]:  # top 8 features per cluster
                m_in = cell[f].mean()
                m_out = pos[f].mean()
                diff = m_in - m_out
                sign = "+" if diff > 0 else ""
                print(f"      {f:22}  mean={m_in:>+7.3f}  (vs all-entries {m_out:>+7.3f}, "
                      f"diff {sign}{diff:>+6.3f})")

    # ===== K=4 clusters + forward return per cluster =====
    print("\n[QUALITY] forward returns per cluster (K=4)")
    km = KMeans(n_clusters=4, random_state=0, n_init=10)
    cluster_labels = km.fit_predict(pos_Xs)
    pos_clusters = pos.copy()
    pos_clusters["cluster"] = cluster_labels

    # Compute forward returns by joining back to pair bars
    fwd_ret_24h_per_cluster = {}
    for c in range(4):
        cell = pos_clusters[pos_clusters["cluster"] == c]
        rets = []
        for idx, row in cell.iterrows():
            pair = row["pair"]
            t = idx
            try:
                feats = all_features[pair]
                # need raw close for fwd return
                bars = load_pair(pair)
                close_now = bars["close"].loc[t]
                close_24h = bars["close"].loc[t:].iloc[24] if len(bars["close"].loc[t:]) >= 25 else np.nan
                if pd.notna(close_24h):
                    rets.append(close_24h / close_now - 1)
            except (KeyError, IndexError):
                continue
        rets = np.array(rets)
        if len(rets) > 0:
            fwd_ret_24h_per_cluster[c] = {"n": len(rets),
                                            "mean": rets.mean() * 100,
                                            "p_up": (rets > 0).mean() * 100,
                                            "med": np.median(rets) * 100}
    print(f"\n  Cluster forward-24h return summary:")
    for c, s in sorted(fwd_ret_24h_per_cluster.items()):
        print(f"    Cluster {c}: N={s['n']}  mean={s['mean']:+6.2f}%  "
              f"median={s['med']:+6.2f}%  P(up)={s['p_up']:.1f}%")


if __name__ == "__main__":
    main()
