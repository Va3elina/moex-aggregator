"""Analyze a Bybit copy-trading export to understand how the leader trades.

Inputs: two CSVs with columns
  Uid, Time, Currency, Contract, Type, Direction, Quantity, Position,
  Filled Price, Funding, Fee Paid, Cash Flow, Change, Wallet Balance,
  Fee Rate, Trade ID, Order ID

Analysis:
  1. Universe & frequency: which pairs, how many trades, long/short bias
  2. Position sizing: notional per trade, equity %
  3. Hold time: open → close duration
  4. Round-trip P&L: match opens to closes per contract via FIFO
  5. Win rate by pair, by side, by hold-time bucket
  6. Equity curve: from Wallet Balance directly
  7. Concurrency: how many positions open at once
  8. Time-of-day / weekday patterns
"""
import warnings
warnings.filterwarnings("ignore")

from pathlib import Path
from collections import deque, defaultdict
import numpy as np
import pandas as pd

DIR = Path(__file__).parent
FILES = sorted(DIR.glob("*copytrading*.csv"))


def load_all():
    parts = []
    for f in FILES:
        df = pd.read_csv(f, skiprows=1)  # skip the "UID:..." header line
        parts.append(df)
    df = pd.concat(parts, ignore_index=True)
    df["Time"] = pd.to_datetime(df["Time"])
    df = df.sort_values("Time").reset_index(drop=True)
    for c in ["Quantity", "Filled Price", "Fee Paid", "Cash Flow", "Change",
              "Wallet Balance", "Funding"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def basic_stats(df):
    print("=" * 100)
    print("PART 1 — Trading universe and activity")
    print("=" * 100)
    trades = df[df["Type"] == "Trade"].copy()
    pre = df[df["Type"] != "Trade"].copy()
    print(f"  Total rows           : {len(df):,}")
    print(f"  Trade rows           : {len(trades):,}")
    print(f"  Non-trade rows       : {len(pre):,} (funding, fees, etc.)")
    print(f"  Date range           : {df['Time'].min()} → {df['Time'].max()}")
    print(f"  Total days covered   : {(df['Time'].max() - df['Time'].min()).days}")
    print(f"  Wallet balance start : ${df['Wallet Balance'].iloc[0]:,.2f}")
    print(f"  Wallet balance end   : ${df['Wallet Balance'].iloc[-1]:,.2f}")
    pct = (df['Wallet Balance'].iloc[-1] / df['Wallet Balance'].iloc[0] - 1) * 100
    print(f"  Total return         : {pct:+.2f}%")

    print("\n  Direction breakdown:")
    print(trades["Direction"].value_counts().to_string())

    print("\n  Top 20 pairs by trade count:")
    pair_counts = trades["Contract"].value_counts().head(20)
    for pair, n in pair_counts.items():
        long_n = ((trades["Contract"] == pair) & trades["Direction"].str.contains("Long")).sum()
        short_n = ((trades["Contract"] == pair) & trades["Direction"].str.contains("Short")).sum()
        print(f"    {pair:18}  {n:>5} trades  (L:{long_n:>4}  S:{short_n:>4})")
    print(f"\n  Total unique pairs traded: {trades['Contract'].nunique()}")


def match_round_trips(df):
    """Match each Close to prior Open(s) on same contract via FIFO.
    Returns list of round-trip trades with entry/exit prices, qty, hold time, PnL."""
    trades = df[df["Type"] == "Trade"].sort_values("Time").reset_index(drop=True)
    # FIFO queue per (contract, side)
    open_queue = defaultdict(deque)  # key = (contract, "long"/"short")
    rounds = []
    for _, r in trades.iterrows():
        contract = r["Contract"]
        direction = r["Direction"]
        qty = r["Quantity"]
        price = r["Filled Price"]
        time = r["Time"]
        fee = r["Fee Paid"]
        cash = r["Cash Flow"]

        if "Open" in direction:
            side = "long" if "Long" in direction else "short"
            open_queue[(contract, side)].append({
                "open_time": time, "open_price": price,
                "open_qty": qty, "open_fee": fee,
            })
        elif "Close" in direction:
            side = "long" if "Long" in direction else "short"
            remaining = qty
            q = open_queue[(contract, side)]
            while remaining > 0 and q:
                head = q[0]
                used = min(remaining, head["open_qty"])
                # PnL for this leg
                if side == "long":
                    pnl_per_unit = price - head["open_price"]
                else:
                    pnl_per_unit = head["open_price"] - price
                gross = pnl_per_unit * used
                # Allocate fees proportionally
                close_fee_frac = used / qty
                close_fee = fee * close_fee_frac
                open_fee_frac = used / head["open_qty"]
                open_fee = head["open_fee"] * open_fee_frac
                net = gross - close_fee - open_fee

                rounds.append({
                    "contract": contract, "side": side,
                    "open_time": head["open_time"], "close_time": time,
                    "open_price": head["open_price"], "close_price": price,
                    "qty": used, "notional": used * head["open_price"],
                    "gross_pnl": gross, "fees": open_fee + close_fee,
                    "net_pnl": net, "ret_pct": (pnl_per_unit / head["open_price"]) * 100 * (1 if side == "long" else 1),
                    "hold_seconds": (time - head["open_time"]).total_seconds(),
                })

                head["open_qty"] -= used
                head["open_fee"] -= open_fee
                remaining -= used
                if head["open_qty"] <= 1e-9:
                    q.popleft()
            # Any residual `remaining` means flipping (opening opposite side).
            # We'll ignore that residual; usually copy-trading is netting.

    # Any positions still in queues are OPEN at end of data
    still_open = []
    for (c, s), q in open_queue.items():
        for o in q:
            still_open.append({"contract": c, "side": s, "qty": o["open_qty"],
                                "open_time": o["open_time"], "open_price": o["open_price"]})
    return pd.DataFrame(rounds), pd.DataFrame(still_open)


def analyze_round_trips(rounds):
    print("\n" + "=" * 100)
    print("PART 2 — Round-trip trades (matched Open → Close FIFO per contract+side)")
    print("=" * 100)
    n = len(rounds)
    print(f"  Total closed round-trips: {n:,}")
    wins = rounds[rounds["net_pnl"] > 0]
    losses = rounds[rounds["net_pnl"] < 0]
    wr = len(wins) / n * 100 if n else 0
    total_net = rounds["net_pnl"].sum()
    total_fees = rounds["fees"].sum()
    avg_win = wins["net_pnl"].mean() if len(wins) else 0
    avg_loss = losses["net_pnl"].mean() if len(losses) else 0
    pf = wins["net_pnl"].sum() / abs(losses["net_pnl"].sum()) if len(losses) else float("inf")

    print(f"  Wins / Losses        : {len(wins):,} / {len(losses):,}")
    print(f"  Win rate             : {wr:.1f}%")
    print(f"  Total NET PnL        : ${total_net:+,.2f}")
    print(f"  Total fees paid      : ${total_fees:,.2f}  ({total_fees/abs(total_net)*100:.1f}% of |PnL|)")
    print(f"  Avg WIN  $           : ${avg_win:+,.2f}")
    print(f"  Avg LOSS $           : ${avg_loss:+,.2f}")
    print(f"  Win/Loss ratio (R)   : {abs(avg_win/avg_loss):.2f}" if avg_loss else "  ratio: ∞")
    print(f"  Profit factor        : {pf:.2f}")
    print(f"  Expected value/trade : ${total_net/n:+,.2f}")

    # by side
    print("\n  Performance by side:")
    for side, g in rounds.groupby("side"):
        wn = (g["net_pnl"] > 0).sum()
        print(f"    {side:6}: N={len(g):,}  WR={wn/len(g)*100:>4.1f}%  "
              f"avg PnL=${g['net_pnl'].mean():+8.2f}  total=${g['net_pnl'].sum():+12,.2f}")

    # hold time
    print("\n  Hold-time distribution:")
    print(f"    Min  : {rounds['hold_seconds'].min():>8.0f} sec  "
          f"({rounds['hold_seconds'].min()/60:.1f} min)")
    print(f"    Med  : {rounds['hold_seconds'].median():>8.0f} sec  "
          f"({rounds['hold_seconds'].median()/3600:.1f} h)")
    print(f"    Mean : {rounds['hold_seconds'].mean():>8.0f} sec  "
          f"({rounds['hold_seconds'].mean()/3600:.1f} h)")
    print(f"    P95  : {rounds['hold_seconds'].quantile(0.95):>8.0f} sec  "
          f"({rounds['hold_seconds'].quantile(0.95)/86400:.1f} days)")
    print(f"    Max  : {rounds['hold_seconds'].max():>8.0f} sec  "
          f"({rounds['hold_seconds'].max()/86400:.1f} days)")

    # By hold-time bucket
    bins = [0, 60, 300, 1800, 3600, 14400, 86400, 604800, np.inf]
    labels = ["<1m", "1-5m", "5-30m", "30m-1h", "1-4h", "4-24h", "1-7d", ">7d"]
    rounds["hold_bin"] = pd.cut(rounds["hold_seconds"], bins=bins, labels=labels)
    print("\n  By hold-time bucket:")
    for bin_label, g in rounds.groupby("hold_bin"):
        if len(g) == 0:
            continue
        wn = (g["net_pnl"] > 0).sum()
        print(f"    {bin_label:>8}: N={len(g):>5}  WR={wn/len(g)*100:>4.1f}%  "
              f"avg_ret={g['ret_pct'].mean():+5.2f}%  total PnL=${g['net_pnl'].sum():+10,.2f}")

    # Top pairs by net PnL
    print("\n  Top 15 pairs by NET PnL:")
    by_pair = rounds.groupby("contract").agg(
        n=("net_pnl", "size"),
        wr=("net_pnl", lambda x: (x > 0).mean() * 100),
        net=("net_pnl", "sum"),
        avg=("net_pnl", "mean"),
    ).sort_values("net", ascending=False)
    for pair, r in by_pair.head(15).iterrows():
        print(f"    {pair:18}  N={int(r['n']):>4}  WR={r['wr']:>4.1f}%  "
              f"avg=${r['avg']:+8.2f}  total=${r['net']:+12,.2f}")
    print("\n  Worst 10 pairs by NET PnL:")
    for pair, r in by_pair.tail(10).iterrows():
        print(f"    {pair:18}  N={int(r['n']):>4}  WR={r['wr']:>4.1f}%  "
              f"avg=${r['avg']:+8.2f}  total=${r['net']:+12,.2f}")
    return rounds


def analyze_temporal(df, rounds):
    print("\n" + "=" * 100)
    print("PART 3 — Temporal patterns")
    print("=" * 100)
    rounds["open_hour"] = rounds["open_time"].dt.hour
    rounds["open_dow"] = rounds["open_time"].dt.dayofweek

    print("\n  By hour of OPEN (UTC):")
    print(f"  {'hour':>4}  {'N':>5}  {'WR%':>5}  {'avg_ret%':>8}  {'total_PnL':>12}")
    for h in range(24):
        sub = rounds[rounds["open_hour"] == h]
        if len(sub) == 0:
            continue
        wn = (sub["net_pnl"] > 0).sum()
        print(f"  {h:>4}  {len(sub):>5}  {wn/len(sub)*100:>4.1f}%  "
              f"{sub['ret_pct'].mean():+7.2f}%  ${sub['net_pnl'].sum():+11,.2f}")

    print("\n  By day-of-week (0=Mon, 6=Sun):")
    for d in range(7):
        sub = rounds[rounds["open_dow"] == d]
        if len(sub) == 0:
            continue
        wn = (sub["net_pnl"] > 0).sum()
        print(f"    DOW {d}: N={len(sub):>5}  WR={wn/len(sub)*100:>4.1f}%  "
              f"avg_ret={sub['ret_pct'].mean():+5.2f}%  total=${sub['net_pnl'].sum():+11,.2f}")


def analyze_equity(df):
    print("\n" + "=" * 100)
    print("PART 4 — Equity curve & drawdown (from Wallet Balance)")
    print("=" * 100)
    eq = df.set_index("Time")["Wallet Balance"].resample("1D").last().ffill()
    peak = eq.cummax()
    dd = (eq / peak - 1) * 100
    print(f"  Days observed       : {len(eq)}")
    print(f"  Start balance       : ${eq.iloc[0]:,.2f}")
    print(f"  Final balance       : ${eq.iloc[-1]:,.2f}")
    print(f"  Peak balance        : ${eq.max():,.2f} on {eq.idxmax().date()}")
    print(f"  Min balance         : ${eq.min():,.2f} on {eq.idxmin().date()}")
    print(f"  Total return        : {(eq.iloc[-1]/eq.iloc[0]-1)*100:+.2f}%")
    print(f"  Max drawdown        : {dd.min():+.2f}%  (trough {dd.idxmin().date()})")

    # Daily returns for Sharpe estimation
    daily_ret = eq.pct_change().dropna()
    if len(daily_ret) > 2 and daily_ret.std() > 0:
        sharpe = daily_ret.mean() / daily_ret.std() * np.sqrt(365)
        print(f"  Daily ret mean      : {daily_ret.mean()*100:+.4f}% / day")
        print(f"  Daily ret std       : {daily_ret.std()*100:.4f}%")
        print(f"  Sharpe (annualized) : {sharpe:.2f}")

    # Monthly returns
    monthly = eq.resample("ME").last().pct_change() * 100
    print("\n  Monthly returns:")
    for d, r in monthly.dropna().items():
        bar = "█" * max(0, int(r / 2)) if r >= 0 else "▒" * max(0, int(-r / 2))
        sign = "+" if r >= 0 else "-"
        print(f"    {d.strftime('%Y-%m'):>8}  {r:>+7.2f}%  {bar}")


def analyze_concurrency(df):
    print("\n" + "=" * 100)
    print("PART 5 — Concurrent positions over time")
    print("=" * 100)
    trades = df[df["Type"] == "Trade"].copy()
    # For each contract, track running position size
    positions = defaultdict(float)
    concurrent_count = []
    for _, r in trades.iterrows():
        if pd.isna(r["Direction"]):
            continue
        c = r["Contract"]
        if "Open Long" in r["Direction"]:
            positions[c] += r["Quantity"]
        elif "Close Long" in r["Direction"]:
            positions[c] -= r["Quantity"]
        elif "Open Short" in r["Direction"]:
            positions[c] -= r["Quantity"]
        elif "Close Short" in r["Direction"]:
            positions[c] += r["Quantity"]
        active = sum(1 for v in positions.values() if abs(v) > 1e-9)
        concurrent_count.append({"time": r["Time"], "concurrent": active})
    if not concurrent_count:
        return
    cc = pd.DataFrame(concurrent_count)
    print(f"  Concurrent positions: max={cc['concurrent'].max()}, "
          f"median={cc['concurrent'].median():.1f}, "
          f"mean={cc['concurrent'].mean():.1f}")
    # Distribution
    print("\n  Distribution of concurrent positions:")
    vc = cc["concurrent"].value_counts().sort_index()
    for n_pos, count in vc.head(20).items():
        pct = count / len(cc) * 100
        bar = "█" * int(pct / 2)
        print(f"    {n_pos:>3}: {count:>6} times ({pct:>5.2f}%)  {bar}")


def main():
    df = load_all()
    basic_stats(df)
    rounds, open_legs = match_round_trips(df)
    if len(rounds):
        rounds = analyze_round_trips(rounds)
        analyze_temporal(df, rounds)
    analyze_equity(df)
    analyze_concurrency(df)
    if len(open_legs):
        print("\n  Still-open positions at end of data:")
        print(open_legs.to_string(index=False))


if __name__ == "__main__":
    main()
