"""Clean analysis using the official Bybit copyTradingTradesPLHistory file.

This is the canonical record of what the Master Trader ("ITEKCrypto") did
as far as the follower was concerned, expressed as 7525 proper round-trips
with Opened On / Closed On / Entry / Exit / Closed P&L / Close By reason.

Also reports the follower's OWN manual perpetual trades (from ClosedPL)
to separate them from copy-trading activity.
"""
import warnings
warnings.filterwarnings("ignore")

from pathlib import Path
import numpy as np
import pandas as pd

DIR = Path(__file__).parent
COPY_PATH = DIR / "zip_contents/tradingTools_773060_20241001_20250831/tradingTools_copyTradingTradesPLHistory_67110813_20241001_20250831_0.csv"
OWN_PATH = DIR / "zip_contents/unifiedAccount_773060_20241001_20250831/unifiedAccount_uSDTPerpetualClosedPL_67110813_20241001_20250831_0.csv"


def load_copy():
    df = pd.read_csv(COPY_PATH, skiprows=1)
    df["Opened On"] = pd.to_datetime(df["Opened On"])
    df["Closed On"] = pd.to_datetime(df["Closed On"])
    for c in ["Qty", "Entry Price", "Closing Price", "Closed P&L", "Fees"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["hold_seconds"] = (df["Closed On"] - df["Opened On"]).dt.total_seconds()
    df["notional"] = df["Qty"].abs() * df["Entry Price"]
    df["ret_pct"] = (df["Closing Price"] / df["Entry Price"] - 1) * 100
    # detect side from sign of qty or by close reason? Bybit: Qty positive = long, negative = short usually
    # Let's check by examining if entry/close direction matches
    df["side"] = df["Qty"].apply(lambda q: "long" if q > 0 else "short")
    df["net_pnl"] = df["Closed P&L"] - df["Fees"]
    return df


def load_own():
    df = pd.read_csv(OWN_PATH, skiprows=1)
    df["Filled/Settlement Time(UTC+0)"] = pd.to_datetime(df["Filled/Settlement Time(UTC+0)"])
    df["Create Time"] = pd.to_datetime(df["Create Time"])
    for c in ["Qty", "Entry Price", "Realized P&L", "Filled Price",
              "Opening Fee", "Closing Fee", "Funding Fee"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["net_pnl"] = df["Realized P&L"] - df["Opening Fee"] - df["Closing Fee"] - df["Funding Fee"]
    df["ret_pct"] = (df["Filled Price"] / df["Entry Price"] - 1) * 100 * \
                     np.where(df["Trade Type"] == "BUY", 1, -1)
    return df


def report_master(df):
    masters = df["Master Trader"].value_counts()
    print(f"  Master Trader(s) followed: {masters.to_dict()}")
    print(f"  Period: {df['Opened On'].min().date()} → {df['Closed On'].max().date()}")
    print(f"  Total round-trips: {len(df):,}")


def analyze_copy(df):
    print("=" * 100)
    print("COPY-TRADING — Master Trader analysis (7525 clean round-trips)")
    print("=" * 100)
    report_master(df)

    print(f"\n  Total NET P&L (Closed P&L − Fees): ${df['net_pnl'].sum():+,.2f}")
    print(f"  Total Closed P&L (gross)         : ${df['Closed P&L'].sum():+,.2f}")
    print(f"  Total Fees paid                  : ${df['Fees'].sum():,.2f}  "
          f"({df['Fees'].sum() / abs(df['Closed P&L'].sum()) * 100:.1f}% of |gross|)")
    n = len(df)
    wins = (df["net_pnl"] > 0).sum()
    losses = (df["net_pnl"] < 0).sum()
    wr = wins / n * 100
    pf = df[df["net_pnl"] > 0]["net_pnl"].sum() / abs(df[df["net_pnl"] < 0]["net_pnl"].sum())
    print(f"  Wins / Losses: {wins:,} / {losses:,}  →  Win rate {wr:.1f}%")
    print(f"  Profit factor: {pf:.2f}")
    print(f"  Avg win:  ${df[df['net_pnl']>0]['net_pnl'].mean():+,.2f}")
    print(f"  Avg loss: ${df[df['net_pnl']<0]['net_pnl'].mean():+,.2f}")
    print(f"  Expected value per trade: ${df['net_pnl'].mean():+,.2f}")

    # By side
    print("\n  By side:")
    for side, g in df.groupby("side"):
        wn = (g["net_pnl"] > 0).sum()
        print(f"    {side}: N={len(g):,}  WR={wn/len(g)*100:>4.1f}%  "
              f"avg=${g['net_pnl'].mean():+8.2f}  total=${g['net_pnl'].sum():+12,.2f}")

    # Close By reason
    print("\n  By Close-By reason:")
    for reason, g in df.groupby("Close By"):
        wn = (g["net_pnl"] > 0).sum()
        print(f"    {reason:35}  N={len(g):>5}  WR={wn/len(g)*100:>4.1f}%  "
              f"total=${g['net_pnl'].sum():+12,.2f}  avg=${g['net_pnl'].mean():+7.2f}")

    # By hold time
    bins = [0, 60, 300, 1800, 3600, 14400, 86400, 604800, np.inf]
    labels = ["<1m", "1-5m", "5-30m", "30m-1h", "1-4h", "4-24h", "1-7d", ">7d"]
    df["hold_bin"] = pd.cut(df["hold_seconds"], bins=bins, labels=labels)
    print("\n  By hold time:")
    for bn, g in df.groupby("hold_bin"):
        if len(g) == 0: continue
        wn = (g["net_pnl"] > 0).sum()
        print(f"    {bn:>8}: N={len(g):>5}  WR={wn/len(g)*100:>4.1f}%  "
              f"avg_ret={g['ret_pct'].mean():+5.2f}%  total=${g['net_pnl'].sum():+11,.2f}")

    # Top pairs
    print("\n  Top 15 pairs by NET P&L:")
    by_pair = df.groupby("Positions").agg(
        n=("net_pnl", "size"),
        wr=("net_pnl", lambda x: (x > 0).mean() * 100),
        total=("net_pnl", "sum"),
        avg=("net_pnl", "mean"),
    ).sort_values("total", ascending=False)
    for p, r in by_pair.head(15).iterrows():
        print(f"    {p:18}  N={int(r['n']):>4}  WR={r['wr']:>4.1f}%  "
              f"avg=${r['avg']:+8.2f}  total=${r['total']:+12,.2f}")
    print("\n  Worst 10 pairs by NET P&L:")
    for p, r in by_pair.tail(10).iterrows():
        print(f"    {p:18}  N={int(r['n']):>4}  WR={r['wr']:>4.1f}%  "
              f"avg=${r['avg']:+8.2f}  total=${r['total']:+12,.2f}")

    # Monthly P&L
    print("\n  Monthly NET P&L (by Closed On):")
    monthly = df.set_index("Closed On")["net_pnl"].resample("ME").sum()
    monthly_trades = df.set_index("Closed On")["net_pnl"].resample("ME").size()
    cum = 0
    for d, v in monthly.items():
        n_mo = monthly_trades.loc[d]
        cum += v
        sign = "+" if v >= 0 else "-"
        bar = "█" * max(0, int(abs(v) / 100)) if v >= 0 else "▒" * max(0, int(abs(v) / 100))
        print(f"    {d.strftime('%Y-%m'):>8}  trades={n_mo:>4}  PnL=${v:+8.2f}  cum=${cum:+9.2f}  {bar}")

    return df


def analyze_own(df):
    print("\n" + "=" * 100)
    print("USER'S OWN manual perpetual trades (separately from copy-trading)")
    print("=" * 100)
    if len(df) == 0:
        print("  No own trades.")
        return
    print(f"  Total: {len(df)} closed positions")
    print(f"  Net P&L: ${df['net_pnl'].sum():+,.2f}")
    print(f"  Period: {df['Filled/Settlement Time(UTC+0)'].min().date()} → "
          f"{df['Filled/Settlement Time(UTC+0)'].max().date()}")
    print(f"\n  All own trades:")
    for _, r in df.iterrows():
        print(f"    {r['Filled/Settlement Time(UTC+0)']}  {r['Contracts']:12}  "
              f"{r['Trade Type']:4}  qty={r['Qty']:>10.4f}  "
              f"entry={r['Entry Price']:>10.4f}  exit={r['Filled Price']:>10.4f}  "
              f"PnL=${r['net_pnl']:+8.2f}")


def main():
    copy = load_copy()
    analyze_copy(copy)
    own = load_own()
    analyze_own(own)


if __name__ == "__main__":
    main()
