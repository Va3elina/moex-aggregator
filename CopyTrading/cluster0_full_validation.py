"""All 4 validation tests:

1. WALK-FORWARD: split year of data by quarters, test Cluster 0 each quarter
2. NARROWER SETUP: find subset of Cluster 0 with even higher WR
3. FRESH DATA: test on Oct 2025 - May 2026 (out-of-sample, +7 months)
4. HIS REAL TRADES BY CLUSTER: assign each of his 7525 round-trips to a
   cluster, see realized P&L per cluster (does Cluster 0 actually win in $?)
"""
import warnings
warnings.filterwarnings("ignore")

from pathlib import Path
import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

DIR = Path(__file__).parent
PAIRS_DIR = DIR / "pairs_1h"
FRESH_DIR = DIR / "pairs_1h_fresh"
BTC_PATH = DIR.parent / "Bitcoin" / "btc_usd_hourly_coinbase.csv"
COPY_PATH = DIR / "zip_contents/tradingTools_773060_20241001_20250831/tradingTools_copyTradingTradesPLHistory_67110813_20241001_20250831_0.csv"

INITIAL = 100.0
FEE = 0.0006
SLIPPAGE = 0.0005


def wilder_ma(s, n):
    out = np.full(len(s), np.nan)
    if len(s) < n: return pd.Series(out, index=s.index)
    vals = s.values
    out[n - 1] = np.nanmean(vals[:n])
    for i in range(n, len(s)):
        out[i] = (out[i - 1] * (n - 1) + vals[i]) / n
    return pd.Series(out, index=s.index)


def features(bars):
    d = bars.copy()
    c, h, l, o = d["close"], d["high"], d["low"], d["open"]
    rng = (h - l).replace(0, np.nan)
    d["lower_wick"] = (pd.concat([c, o], axis=1).min(axis=1) - l) / rng
    pc = c.shift(1)
    tr = pd.concat([h - l, (h - pc).abs(), (l - pc).abs()], axis=1).max(axis=1)
    atr14 = wilder_ma(tr, 14)
    d["atr_pct"] = atr14 / c * 100
    ema50 = c.ewm(span=50, adjust=False).mean()
    ema200 = c.ewm(span=200, adjust=False).mean()
    d["ema_50_200"] = (ema50 - ema200) / c * 100
    d["ret_4h"] = c.pct_change(4)
    d["ret_72h"] = c.pct_change(72)
    return d


def cluster0_signal(d, regime_f,
                    regime_ema_min=3.0, regime_ret_min=0.02,
                    asset_ema_min=8.0, pullback_max=-0.02,
                    atr_min=2.5, wick_min=0.25):
    j = d.join(regime_f, how="left", rsuffix="_R")
    return ((j["ema_50_200_R"] > regime_ema_min) &
            (j["ret_72h_R"] > regime_ret_min) &
            (j["ema_50_200"] > asset_ema_min) &
            (j["ret_4h"] < pullback_max) &
            (j["atr_pct"] > atr_min) &
            (j["lower_wick"] > wick_min)).fillna(False)


def backtest(bars, signals, hold=24):
    closes = bars["close"].values
    cash, pos, e_idx, e_pr, held = INITIAL, 0.0, 0, 0, 0
    trades = []
    for i in range(len(bars)):
        if pos > 0:
            held += 1
            if held >= hold:
                eff = closes[i] * (1 - SLIPPAGE)
                cash += pos * eff * (1 - FEE)
                trades.append({"ret_pct": (eff / e_pr - 1) * 100,
                                "entry_t": bars.index[e_idx], "exit_t": bars.index[i]})
                pos = 0.0; held = 0
        if pos == 0 and signals.iloc[i]:
            eff = closes[i] * (1 + SLIPPAGE)
            qty = (cash * 0.95) / eff
            cost = qty * eff * (1 + FEE)
            if cost <= cash:
                cash -= cost; pos = qty; e_pr = eff; e_idx = i
    eq = cash + pos * closes[-1]
    return pd.DataFrame(trades), eq


def load_pair(name, fresh=False):
    p = (FRESH_DIR if fresh else PAIRS_DIR) / f"{name.lower()}_1h.csv"
    df = pd.read_csv(p, parse_dates=["datetime"]).set_index("datetime").sort_index()
    df.index = df.index.tz_localize(None)
    for c in ["open", "high", "low", "close", "volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def load_btc(fresh=False):
    if fresh:
        p = FRESH_DIR / "btc_fresh_1h.csv"
        df = pd.read_csv(p, parse_dates=["datetime"]).set_index("datetime").sort_index()
    else:
        df = pd.read_csv(BTC_PATH, parse_dates=["datetime"]).set_index("datetime").sort_index()
    df.index = df.index.tz_localize(None)
    for c in ["open", "high", "low", "close", "volume"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


# ============ TEST 1: Walk-forward ============
def walk_forward():
    print("=" * 100)
    print("TEST 1: WALK-FORWARD — does Cluster 0 work in every quarter?")
    print("=" * 100)
    btc = load_btc()
    btc_f = features(btc)[["ema_50_200", "ret_4h", "ret_72h", "atr_pct"]]
    pairs = ["1000PEPEUSDT", "1000FLOKIUSDT", "1000BONKUSDT", "WIFUSDT",
             "POPCATUSDT", "SHIB1000USDT", "WLDUSDT"]
    folds = [
        ("Q4 2024", "2024-10-01", "2025-01-01"),
        ("Q1 2025", "2025-01-01", "2025-04-01"),
        ("Q2 2025", "2025-04-01", "2025-07-01"),
        ("Q3 2025", "2025-07-01", "2025-10-01"),
    ]
    print(f"\n{'fold':12}  {'pair':14}  {'sigs':>5}  {'trades':>6}  {'WR%':>5}  "
          f"{'avg%':>6}  {'total':>9}  {'B&H':>8}")
    print("-" * 90)
    fold_totals = {f[0]: [] for f in folds}
    for fold_name, start, end in folds:
        for pair in pairs:
            try:
                bars = features(load_pair(pair))
            except FileNotFoundError:
                continue
            mask = (bars.index >= start) & (bars.index < end)
            window = bars[mask]
            if len(window) < 100:
                continue
            sig = cluster0_signal(window, btc_f)
            n_sig = int(sig.sum())
            if n_sig == 0:
                continue
            trades, eq = backtest(window, sig)
            bh = window["close"].iloc[-1] / window["close"].iloc[0] * INITIAL
            n = len(trades)
            if n == 0:
                continue
            wr = (trades["ret_pct"] > 0).sum() / n * 100
            avg = trades["ret_pct"].mean()
            print(f"{fold_name:12}  {pair:14}  {n_sig:>5}  {n:>6}  {wr:>4.1f}%  "
                  f"{avg:>+5.2f}%  ${eq:>7.2f}  ${bh:>6.2f}")
            fold_totals[fold_name].append((eq, bh, n, wr))
    print(f"\n  Per-fold summary (across {len(pairs)} pairs):")
    for fn in fold_totals:
        rs = fold_totals[fn]
        if not rs:
            print(f"    {fn}: no trades")
            continue
        total_eq = sum(r[0] for r in rs)
        total_bh = sum(r[1] for r in rs)
        n_trades = sum(r[2] for r in rs)
        avg_wr = sum(r[3] * r[2] for r in rs) / n_trades if n_trades else 0
        n_pairs = len(rs)
        ret_strat = (total_eq / (n_pairs * INITIAL) - 1) * 100
        ret_bh = (total_bh / (n_pairs * INITIAL) - 1) * 100
        verdict = "✓ beats B&H" if ret_strat > ret_bh else "✗ B&H wins"
        print(f"    {fn}: {n_pairs} pairs, {n_trades} trades, "
              f"WR {avg_wr:.1f}%, strat {ret_strat:+5.1f}%, B&H {ret_bh:+5.1f}%  {verdict}")


# ============ TEST 2: Narrower setup ============
def narrower_setup():
    print("\n" + "=" * 100)
    print("TEST 2: NARROWER SETUP — search for highest-WR subset of Cluster 0")
    print("=" * 100)
    btc = load_btc()
    btc_f = features(btc)[["ema_50_200", "ret_4h", "ret_72h", "atr_pct"]]
    pairs = ["1000PEPEUSDT", "1000FLOKIUSDT", "1000BONKUSDT", "WIFUSDT",
             "POPCATUSDT", "SHIB1000USDT", "WLDUSDT", "DOGEUSDT", "SOLUSDT"]

    # Grid of tighter thresholds
    print(f"\n  Grid search (Cluster 0 with stricter thresholds, hold 24h):")
    print(f"  {'cfg':45}  {'sigs':>5}  {'trades':>6}  {'WR%':>5}  "
          f"{'avg%':>6}  {'total':>8}")
    configs = [
        # base
        ("BASE: rg_ema>3 rg_ret>2 ass_ema>8 pull<-2 atr>2.5 wick>0.25",
            3.0, 0.02, 8.0, -0.02, 2.5, 0.25),
        # stricter regime
        ("STRICT regime: rg_ema>5 rg_ret>4",
            5.0, 0.04, 8.0, -0.02, 2.5, 0.25),
        # stricter alt trend
        ("STRICT alt: ass_ema>12 wick>0.35",
            3.0, 0.02, 12.0, -0.02, 2.5, 0.35),
        # bigger pullback
        ("BIG pullback: pull<-4 atr>3",
            3.0, 0.02, 8.0, -0.04, 3.0, 0.25),
        # combo strict
        ("ALL STRICT: rg_ema>5 ass_ema>12 pull<-3 wick>0.35",
            5.0, 0.03, 12.0, -0.03, 2.5, 0.35),
        # require explosive vol
        ("HIGH ATR: atr>4 wick>0.30",
            3.0, 0.02, 8.0, -0.02, 4.0, 0.30),
    ]
    for label, rgE, rgR, asE, pl, at, wk in configs:
        total_sigs = 0
        total_trades = []
        total_eq = 0
        n_pairs_with_data = 0
        for pair in pairs:
            try:
                bars = features(load_pair(pair))
            except FileNotFoundError:
                continue
            sig = cluster0_signal(bars, btc_f, rgE, rgR, asE, pl, at, wk)
            total_sigs += int(sig.sum())
            if sig.sum() == 0:
                continue
            trades, eq = backtest(bars, sig)
            if len(trades):
                total_trades.append(trades)
                total_eq += eq
                n_pairs_with_data += 1
        if not total_trades:
            print(f"  {label:45}  {total_sigs:>5}  no trades")
            continue
        all_trades = pd.concat(total_trades, ignore_index=True)
        n = len(all_trades)
        wr = (all_trades["ret_pct"] > 0).sum() / n * 100
        avg = all_trades["ret_pct"].mean()
        ret = (total_eq / (n_pairs_with_data * INITIAL) - 1) * 100
        print(f"  {label:45}  {total_sigs:>5}  {n:>6}  {wr:>4.1f}%  "
              f"{avg:>+5.2f}%  {ret:>+6.1f}%")


# ============ TEST 3: Fresh out-of-sample data ============
def fresh_data_test():
    print("\n" + "=" * 100)
    print("TEST 3: FRESH OUT-OF-SAMPLE (Oct 2025 - May 2026, +7 months past training data)")
    print("=" * 100)
    btc_fresh = load_btc(fresh=True)
    btc_f = features(btc_fresh)[["ema_50_200", "ret_4h", "ret_72h", "atr_pct"]]
    print(f"  BTC fresh range: {btc_fresh.index[0]} → {btc_fresh.index[-1]}")
    pairs = ["1000PEPEUSDT", "DOGEUSDT", "1000FLOKIUSDT", "1000BONKUSDT",
             "SHIB1000USDT", "WIFUSDT", "POPCATUSDT", "SOLUSDT"]

    print(f"\n  {'pair':14}  {'sigs':>5}  {'trades':>6}  {'WR%':>5}  "
          f"{'avg%':>6}  {'total$':>8}  {'B&H$':>8}  {'verdict'}")
    print("  " + "-" * 80)
    rows = []
    for pair in pairs:
        try:
            bars = features(load_pair(pair, fresh=True))
        except FileNotFoundError:
            continue
        sig = cluster0_signal(bars, btc_f)
        n_sig = int(sig.sum())
        if n_sig == 0:
            print(f"  {pair:14}  no signals")
            continue
        trades, eq = backtest(bars, sig)
        bh = bars["close"].iloc[-1] / bars["close"].iloc[0] * INITIAL
        n = len(trades)
        if n == 0:
            print(f"  {pair:14}  {n_sig} signals but no completed trades")
            continue
        wr = (trades["ret_pct"] > 0).sum() / n * 100
        avg = trades["ret_pct"].mean()
        verdict = "✓ beats B&H" if eq > bh else ("≈" if abs(eq - bh) < 5 else "✗")
        print(f"  {pair:14}  {n_sig:>5}  {n:>6}  {wr:>4.1f}%  "
              f"{avg:>+5.2f}%  ${eq:>6.2f}  ${bh:>6.2f}  {verdict}")
        rows.append((pair, n, wr, eq, bh))
    if rows:
        df = pd.DataFrame(rows, columns=["pair", "n", "wr", "eq", "bh"])
        avg_wr = (df["wr"] * df["n"]).sum() / df["n"].sum()
        print(f"\n  Aggregate: {df['n'].sum()} trades, WR {avg_wr:.1f}%, "
              f"strat avg ${df['eq'].mean():.2f} vs B&H ${df['bh'].mean():.2f}")


# ============ TEST 4: His real trades by cluster ============
def his_real_trades_by_cluster():
    print("\n" + "=" * 100)
    print("TEST 4: HIS REAL CLOSED TRADES — assign to cluster, see realized P&L")
    print("=" * 100)
    btc = load_btc()
    btc_f = features(btc)[["ema_50_200", "ret_4h", "ret_72h", "atr_pct"]]

    copy = pd.read_csv(COPY_PATH, skiprows=1)
    copy["Opened On"] = pd.to_datetime(copy["Opened On"])
    copy["Closed On"] = pd.to_datetime(copy["Closed On"])
    for c in ["Qty", "Entry Price", "Closing Price", "Closed P&L", "Fees"]:
        copy[c] = pd.to_numeric(copy[c], errors="coerce")
    copy["net_pnl"] = copy["Closed P&L"] - copy["Fees"]
    # match trades whose contract we have features for
    pair_features = {}
    avail_pairs = ["1000PEPEUSDT", "DOGEUSDT", "SOLUSDT", "1000FLOKIUSDT",
                   "1000BONKUSDT", "SHIB1000USDT", "WIFUSDT", "WLDUSDT",
                   "XRPUSDT", "POPCATUSDT"]
    for p in avail_pairs:
        try:
            pair_features[p] = features(load_pair(p))
        except FileNotFoundError:
            pass

    rows = []
    for _, t in copy.iterrows():
        pair = t["Positions"]
        if pair not in pair_features:
            continue
        # Snap entry date (only date precision in this CSV) to bar 12:00 UTC
        bar_t = (t["Opened On"]).replace(hour=12, minute=0, second=0)
        feats = pair_features[pair]
        if bar_t not in feats.index:
            # find nearest
            mask = (feats.index >= bar_t - pd.Timedelta(hours=1)) & (feats.index <= bar_t + pd.Timedelta(hours=2))
            sub = feats[mask]
            if sub.empty: continue
            row = sub.iloc[0]
            bar_t = sub.index[0]
        else:
            row = feats.loc[bar_t]
        btc_row = btc_f.reindex([bar_t]).iloc[0] if bar_t in btc_f.index else None
        if btc_row is None or btc_row.isna().any() or row[["ema_50_200", "ret_4h", "ret_72h", "atr_pct", "lower_wick"]].isna().any():
            continue
        rows.append({
            "pair": pair,
            "net_pnl": t["net_pnl"],
            "is_cluster0": (btc_row["ema_50_200"] > 3.0 and
                            btc_row["ret_72h"] > 0.02 and
                            row["ema_50_200"] > 8.0 and
                            row["ret_4h"] < -0.02 and
                            row["atr_pct"] > 2.5 and
                            row["lower_wick"] > 0.25),
            "btc_ema_50_200": btc_row["ema_50_200"],
            "btc_ret_72h": btc_row["ret_72h"],
            "pair_ema_50_200": row["ema_50_200"],
            "pair_ret_4h": row["ret_4h"],
            "pair_atr_pct": row["atr_pct"],
            "pair_lower_wick": row["lower_wick"],
            "close_by": t["Close By"],
        })
    df = pd.DataFrame(rows)
    print(f"\n  Matched {len(df)} of his closed round-trips to feature data")
    print(f"\n  Cluster 0 (strict ML setup): {df['is_cluster0'].sum()} trades")
    cl0 = df[df["is_cluster0"]]
    other = df[~df["is_cluster0"]]
    print(f"  Cluster 0 net P&L: ${cl0['net_pnl'].sum():+,.2f}  "
          f"(WR {(cl0['net_pnl']>0).mean()*100:.1f}%, "
          f"avg ${cl0['net_pnl'].mean():+.2f})")
    print(f"  Other trades  : {len(other)}, net P&L: ${other['net_pnl'].sum():+,.2f}  "
          f"(WR {(other['net_pnl']>0).mean()*100:.1f}%, "
          f"avg ${other['net_pnl'].mean():+.2f})")

    # Per condition satisfied — looser version
    df["btc_bull"] = df["btc_ema_50_200"] > 3.0
    df["btc_up"] = df["btc_ret_72h"] > 0.02
    df["alt_uptrend"] = df["pair_ema_50_200"] > 8.0
    df["pullback"] = df["pair_ret_4h"] < -0.02
    df["high_atr"] = df["pair_atr_pct"] > 2.5
    df["wick"] = df["pair_lower_wick"] > 0.25
    df["conditions_met"] = (df[["btc_bull", "btc_up", "alt_uptrend",
                                    "pullback", "high_atr", "wick"]].sum(axis=1))

    print(f"\n  By number of Cluster 0 conditions met (out of 6):")
    print(f"  {'# conditions':>13}  {'N':>5}  {'net P&L':>12}  {'WR%':>5}  {'avg/trade':>10}")
    for k in range(7):
        sub = df[df["conditions_met"] == k]
        if len(sub) == 0:
            continue
        wr = (sub["net_pnl"] > 0).mean() * 100
        print(f"  {k:>13}  {len(sub):>5}  ${sub['net_pnl'].sum():>+10.2f}  "
              f"{wr:>4.1f}%  ${sub['net_pnl'].mean():>+9.2f}")

    # Show conditions impact: total $ if he'd only traded each cluster
    print(f"\n  By Close By reason — within Cluster 0 (6/6) vs Other (≤5/6):")
    for label, sub in [("Cluster 0 (all 6)", df[df["conditions_met"] == 6]),
                          ("Almost (5/6)", df[df["conditions_met"] == 5]),
                          ("Half (3-4/6)", df[df["conditions_met"].isin([3, 4])]),
                          ("Weak (0-2/6)", df[df["conditions_met"] <= 2])]:
        if len(sub) == 0:
            continue
        by_close = sub.groupby("close_by")["net_pnl"].agg(["count", "sum", "mean"]).reset_index()
        print(f"\n    {label}  (N={len(sub)}, net=${sub['net_pnl'].sum():+.2f}):")
        for _, r in by_close.iterrows():
            print(f"      {r['close_by']:30}  N={int(r['count']):>4}  total=${r['sum']:+8.2f}  avg=${r['mean']:+6.2f}")


def main():
    walk_forward()
    narrower_setup()
    fresh_data_test()
    his_real_trades_by_cluster()


if __name__ == "__main__":
    main()
