"""Per-master-trader breakdown: which leader was profitable, which sank the account."""
import warnings
warnings.filterwarnings("ignore")

from pathlib import Path
import numpy as np
import pandas as pd

DIR = Path(__file__).parent
COPY_PATH = DIR / "zip_contents/tradingTools_773060_20241001_20250831/tradingTools_copyTradingTradesPLHistory_67110813_20241001_20250831_0.csv"


def load():
    df = pd.read_csv(COPY_PATH, skiprows=1)
    df["Opened On"] = pd.to_datetime(df["Opened On"])
    df["Closed On"] = pd.to_datetime(df["Closed On"])
    for c in ["Qty", "Entry Price", "Closing Price", "Closed P&L", "Fees"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["hold_seconds"] = (df["Closed On"] - df["Opened On"]).dt.total_seconds()
    df["net_pnl"] = df["Closed P&L"] - df["Fees"]
    df["ret_pct"] = (df["Closing Price"] / df["Entry Price"] - 1) * 100
    return df


def main():
    df = load()
    print("=" * 110)
    print("PER-MASTER-TRADER breakdown — who actually made money for the follower")
    print("=" * 110)

    # Aggregate per master
    agg = df.groupby("Master Trader").agg(
        n=("net_pnl", "size"),
        wr=("net_pnl", lambda x: (x > 0).mean() * 100),
        gross=("Closed P&L", "sum"),
        fees=("Fees", "sum"),
        net=("net_pnl", "sum"),
        avg=("net_pnl", "mean"),
        avg_hold_h=("hold_seconds", lambda x: x.mean() / 3600),
        med_hold_h=("hold_seconds", lambda x: x.median() / 3600),
        liq_rate=("Close By", lambda x: (x == "ClosedByLiq").mean() * 100),
        manual_close_rate=("Close By", lambda x: (x == "ClosedByUserSelf").mean() * 100),
        avg_notional=("Entry Price", lambda x: 0),  # placeholder
        first_trade=("Opened On", "min"),
        last_trade=("Closed On", "max"),
    ).sort_values("net", ascending=False)

    print(f"\n{'master':22}  {'trades':>6}  {'WR%':>5}  {'gross$':>10}  {'fees$':>9}  "
          f"{'NET$':>10}  {'avg$':>7}  {'avg_hold_h':>10}  {'liq%':>5}  {'man%':>5}  "
          f"{'period':>23}")
    print("-" * 145)
    for master, r in agg.iterrows():
        period = f"{r['first_trade'].strftime('%Y-%m')} → {r['last_trade'].strftime('%Y-%m')}"
        print(f"{master:22}  {int(r['n']):>6}  {r['wr']:>4.1f}%  "
              f"{r['gross']:>+9.2f}  {r['fees']:>+8.2f}  {r['net']:>+9.2f}  "
              f"{r['avg']:>+6.2f}  {r['avg_hold_h']:>9.1f}  "
              f"{r['liq_rate']:>4.1f}%  {r['manual_close_rate']:>4.1f}%  "
              f"{period:>23}")

    # Monthly breakdown per master
    print("\n" + "=" * 110)
    print("MONTHLY breakdown per master (P&L USD)")
    print("=" * 110)
    df["month"] = df["Closed On"].dt.to_period("M").astype(str)
    pivot = df.pivot_table(index="month", columns="Master Trader",
                            values="net_pnl", aggfunc="sum", fill_value=0)
    pivot["TOTAL"] = pivot.sum(axis=1)
    print(pivot.to_string(float_format=lambda x: f"{x:+8.0f}"))

    # Worst single trades
    print("\n" + "=" * 110)
    print("TOP 20 LOSING TRADES (single round-trips that hurt most)")
    print("=" * 110)
    worst = df.nsmallest(20, "net_pnl")[["Master Trader", "Positions", "Qty",
                                            "Opened On", "Entry Price",
                                            "Closed On", "Closing Price",
                                            "Close By", "net_pnl", "hold_seconds"]]
    worst["hold_days"] = worst["hold_seconds"] / 86400
    for _, r in worst.iterrows():
        print(f"  {str(r['Opened On'])[:16]} → {str(r['Closed On'])[:16]}  "
              f"({r['hold_days']:>5.1f}d)  {r['Master Trader']:22}  "
              f"{r['Positions']:13}  qty={r['Qty']:>10.4f}  "
              f"{r['Entry Price']:>10.4f} → {r['Closing Price']:>10.4f}  "
              f"{r['Close By']:25}  ${r['net_pnl']:+8.2f}")

    # Best single trades for comparison
    print("\n" + "=" * 110)
    print("TOP 10 WINNING TRADES")
    print("=" * 110)
    best = df.nlargest(10, "net_pnl")[["Master Trader", "Positions", "Qty",
                                          "Opened On", "Closed On", "Entry Price",
                                          "Closing Price", "Close By", "net_pnl",
                                          "hold_seconds"]]
    best["hold_days"] = best["hold_seconds"] / 86400
    for _, r in best.iterrows():
        print(f"  {str(r['Opened On'])[:16]} → {str(r['Closed On'])[:16]}  "
              f"({r['hold_days']:>5.1f}d)  {r['Master Trader']:22}  "
              f"{r['Positions']:13}  ${r['net_pnl']:+8.2f}  "
              f"({r['Close By']})")


if __name__ == "__main__":
    main()
