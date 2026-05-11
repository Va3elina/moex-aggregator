"""Wavelet trend follower — test individual improvements & combos across crypto.

Baseline: W1 (s5 up AND s6 up).
Individual improvements:
  I1) Slope strength: require |slope|/price > threshold (skip tiny moves)
  I2) Triple scale: s4 AND s5 AND s6 all up
  I3) Cooldown: wait N bars after exit before next entry
  I4) Momentum filter: close > EMA50
  I5) Soft exit: exit when slope < -threshold (not on flip)

Combinations:
  C1) I2 + I4 (triple + momentum)
  C2) I2 + I3 (triple + cooldown)
  C3) I2 + I4 + I1 (triple + momentum + strong slope)
  C4) I1 + I3 + I4 (strong slope + cooldown + momentum)
  C5) I2 + I5 (triple + soft exit)

Output: table of {asset × variant} → return, DD, WR, PF, N trades.
"""
import warnings; warnings.filterwarnings("ignore")
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(__file__).parent
ASSETS = {
    'BTC': 'btc_usd_daily_coinbase.csv',
    'ETH': 'eth_usd_daily_coinbase.csv',
    'BCH': 'bch_usd_daily_coinbase.csv',
    'LTC': 'ltc_usd_daily_coinbase.csv',
    'SOL': 'sol_usd_daily_coinbase.csv',
}
FEE = 0.0005
SLIP = 0.0003
INITIAL = 100.0


def ema(series, n):
    s = series.values
    out = np.full(len(s), np.nan)
    if len(s) < n: return pd.Series(out, index=series.index)
    out[n-1] = s[:n].mean()
    k = 2.0 / (n + 1.0)
    for i in range(n, len(s)):
        out[i] = s[i] * k + out[i-1] * (1 - k)
    return pd.Series(out, index=series.index)


def atrous_haar(x, J=6):
    n = len(x)
    smooths = [np.asarray(x, dtype=float).copy()]
    for j in range(1, J + 1):
        gap = 2 ** (j - 1)
        prev = smooths[-1]
        s = np.full(n, np.nan)
        for i in range(n):
            if i - gap < 0:
                s[i] = prev[i]
            else:
                s[i] = 0.5 * (prev[i] + prev[i - gap])
        smooths.append(s)
    return smooths


def slope(arr, lookback=5):
    out = np.full(len(arr), np.nan)
    for i in range(lookback, len(arr)):
        if not np.isnan(arr[i]) and not np.isnan(arr[i - lookback]):
            out[i] = arr[i] - arr[i - lookback]
    return out


def build_signal_long(df,
                       use_s4=False,
                       slope_strength=0.0,      # min slope value (log-units per `lookback` bars)
                       cooldown_bars=0,
                       momentum_ema=0,          # 0 = no filter; >0 = require close > EMA(close, n)
                       soft_exit_thresh=0.0):
    """Return boolean array: True = hold long.
    Entry: all required scales slope > slope_strength
    Exit: any required scale slope < -soft_exit_thresh
    Cooldown: must have been out for >= cooldown_bars before re-entry"""
    log_c = np.log(df['close'].values)
    smooths = atrous_haar(log_c, J=6)
    sl4 = slope(smooths[4], 5)
    sl5 = slope(smooths[5], 5)
    sl6 = slope(smooths[6], 5)

    if momentum_ema > 0:
        ema_series = ema(df['close'], momentum_ema).values
        mom_ok = df['close'].values > ema_series
    else:
        mom_ok = np.ones(len(df), dtype=bool)

    n = len(df)
    target = np.zeros(n, dtype=float)
    state = 0  # 0=flat, 1=long
    bars_since_exit = 9999
    for i in range(n):
        # required scale slopes
        scales = [sl5[i], sl6[i]]
        if use_s4:
            scales.append(sl4[i])
        if any(np.isnan(v) for v in scales):
            target[i] = state; continue

        all_up_strong = all(s > slope_strength for s in scales)
        any_down_soft = any(s < -soft_exit_thresh for s in scales)

        if state == 0:
            if all_up_strong and bars_since_exit >= cooldown_bars and mom_ok[i]:
                state = 1
        elif state == 1:
            if any_down_soft or (not mom_ok[i] and momentum_ema > 0):
                state = 0
                bars_since_exit = 0
            else:
                bars_since_exit = 9999  # not relevant while in trade

        target[i] = state
        if state == 0:
            bars_since_exit += 1
    return target


def backtest(df, target):
    closes = df['close'].values
    dates = df.index
    cash = INITIAL
    qty = 0.0
    eq_curve = np.zeros(len(closes))
    trades = []
    entry_value = 0.0
    entry_date = None
    for i in range(len(closes)):
        c = closes[i]; date = dates[i]
        eq_curve[i] = cash + qty * c
        tgt = target[i]
        if tgt == 1 and qty == 0:
            eff = c * (1 + SLIP)
            spend = cash * 0.99
            buy_qty = spend / eff
            cost = buy_qty * eff * (1 + FEE)
            cash -= cost
            qty = buy_qty
            entry_value = cost
            entry_date = date
        elif tgt == 0 and qty > 0:
            eff = c * (1 - SLIP)
            proceeds = qty * eff * (1 - FEE)
            cash += proceeds
            pnl_pct = (proceeds / entry_value - 1) * 100
            trades.append({'entry_date': entry_date, 'exit_date': date,
                          'pnl_pct': pnl_pct, 'hold_days': (date - entry_date).days})
            qty = 0; entry_value = 0; entry_date = None
    if qty > 0:
        c = closes[-1]
        eff = c * (1 - SLIP)
        proceeds = qty * eff * (1 - FEE)
        cash += proceeds
        pnl_pct = (proceeds / entry_value - 1) * 100
        trades.append({'entry_date': entry_date, 'exit_date': dates[-1],
                      'pnl_pct': pnl_pct, 'hold_days': (dates[-1] - entry_date).days})
    final_eq = cash
    eq = pd.Series(eq_curve, index=dates)
    return pd.DataFrame(trades), final_eq, eq


def metrics(trades, final_eq, eq_curve):
    if len(trades) == 0:
        return {'N': 0, 'WR': 0, 'PF': 0, 'avg': 0, 'ret': (final_eq/INITIAL-1)*100, 'DD': 0}
    wr = (trades['pnl_pct'] > 0).sum() / len(trades) * 100
    pos = trades[trades['pnl_pct'] > 0]['pnl_pct'].sum()
    neg = abs(trades[trades['pnl_pct'] < 0]['pnl_pct'].sum())
    pf = pos / neg if neg > 0 else float('inf')
    ret = (final_eq / INITIAL - 1) * 100
    dd = ((eq_curve.cummax() - eq_curve) / eq_curve.cummax()).max() * 100
    return {'N': len(trades), 'WR': wr, 'PF': pf, 'avg': trades['pnl_pct'].mean(),
            'ret': ret, 'DD': dd}


def buy_hold(df):
    bh_qty = INITIAL / df['close'].iloc[0]
    bh_eq = bh_qty * df['close']
    bh_ret = (bh_eq.iloc[-1] / INITIAL - 1) * 100
    bh_dd = ((bh_eq.cummax() - bh_eq) / bh_eq.cummax()).max() * 100
    return bh_ret, bh_dd


def load(path):
    df = pd.read_csv(ROOT / path, parse_dates=['date']).set_index('date').sort_index()
    df = df[df['close'] > 0].dropna(subset=['close'])
    return df


# ---- Variant definitions ----
VARIANTS = [
    # (label, kwargs)
    ('W1 baseline (s5∧s6)',           dict()),
    ('I1 slope ≥ 0.005',              dict(slope_strength=0.005)),
    ('I1 slope ≥ 0.010',              dict(slope_strength=0.010)),
    ('I2 triple s4∧s5∧s6',            dict(use_s4=True)),
    ('I3 cooldown 5 bars',            dict(cooldown_bars=5)),
    ('I3 cooldown 10 bars',           dict(cooldown_bars=10)),
    ('I4 momentum EMA50',             dict(momentum_ema=50)),
    ('I4 momentum EMA100',            dict(momentum_ema=100)),
    ('I5 soft exit (-0.005)',         dict(soft_exit_thresh=0.005)),
    ('I5 soft exit (-0.010)',         dict(soft_exit_thresh=0.010)),
    ('C1 triple + EMA50',             dict(use_s4=True, momentum_ema=50)),
    ('C2 triple + cooldown 5',        dict(use_s4=True, cooldown_bars=5)),
    ('C3 triple + EMA50 + slope0.005',dict(use_s4=True, momentum_ema=50, slope_strength=0.005)),
    ('C4 slope0.005 + cool5 + EMA50', dict(slope_strength=0.005, cooldown_bars=5, momentum_ema=50)),
    ('C5 triple + soft exit 0.005',   dict(use_s4=True, soft_exit_thresh=0.005)),
    ('C6 all-in (triple+EMA+soft)',   dict(use_s4=True, momentum_ema=50, soft_exit_thresh=0.005)),
]


def main():
    # Build asset → df map
    asset_dfs = {a: load(p) for a, p in ASSETS.items()}

    # Compute buy & hold baseline
    print("\n" + "=" * 110)
    print("Buy & Hold baseline per asset")
    print("=" * 110)
    print(f"  {'asset':5}  {'days':>5}  {'period':>26}  {'B&H return':>12}  {'B&H DD':>8}")
    print("  " + "-" * 70)
    for a, df in asset_dfs.items():
        ret, dd = buy_hold(df)
        period = f"{df.index[0].date()} → {df.index[-1].date()}"
        print(f"  {a:5}  {len(df):>5}  {period:>26}  {ret:>+11.0f}%  {dd:>6.1f}%")

    # Run all variants on all assets, collect into matrix
    print("\n" + "=" * 110)
    print("All variants × all assets — returns")
    print("=" * 110)
    rows = []
    for label, kwargs in VARIANTS:
        row = {'variant': label}
        for a, df in asset_dfs.items():
            target = build_signal_long(df, **kwargs)
            tr, eq, eqc = backtest(df, target)
            m = metrics(tr, eq, eqc)
            row[a] = m
        rows.append(row)

    # Print returns table
    print(f"  {'variant':36} | " + " | ".join(f"{a:>10}" for a in ASSETS) + " |")
    print("  " + "-" * (36 + 13 * len(ASSETS)))
    for row in rows:
        line = f"  {row['variant']:36} | "
        line += " | ".join(f"{row[a]['ret']:>9.0f}%" for a in ASSETS) + " |"
        print(line)

    # Print DD table
    print("\n" + "=" * 110)
    print("All variants × all assets — max drawdown")
    print("=" * 110)
    print(f"  {'variant':36} | " + " | ".join(f"{a:>10}" for a in ASSETS) + " |")
    print("  " + "-" * (36 + 13 * len(ASSETS)))
    for row in rows:
        line = f"  {row['variant']:36} | "
        line += " | ".join(f"{row[a]['DD']:>9.1f}%" for a in ASSETS) + " |"
        print(line)

    # Print WR / PF / N for BTC (deep dive)
    for asset in ['BTC', 'ETH']:
        print("\n" + "=" * 110)
        print(f"{asset} — detailed metrics per variant")
        print("=" * 110)
        print(f"  {'variant':36}  {'N':>4}  {'WR%':>5}  {'PF':>6}  {'avg%':>7}  {'ret%':>10}  {'DD%':>6}")
        print("  " + "-" * 80)
        for row in rows:
            m = row[asset]
            pf_str = f"{m['PF']:>5.2f}" if m['PF'] != float('inf') else "  inf"
            print(f"  {row['variant']:36}  {m['N']:>4}  {m['WR']:>4.1f}  {pf_str}  "
                  f"{m['avg']:>+6.1f}  {m['ret']:>+9.0f}%  {m['DD']:>5.1f}")

    # Best variant per asset (by Sharpe-like ratio: return / DD)
    print("\n" + "=" * 110)
    print("Best variant per asset (by return / DD)")
    print("=" * 110)
    print(f"  {'asset':6}  {'best variant':40}  {'ret':>10}  {'DD':>6}  {'ret/DD':>8}")
    print("  " + "-" * 80)
    for a in ASSETS:
        best = None; best_score = -1e9
        for row in rows:
            m = row[a]
            if m['DD'] <= 0 or m['N'] == 0: continue
            score = m['ret'] / m['DD']
            if score > best_score:
                best_score = score; best = row
        if best is None:
            print(f"  {a:6}  (no valid variant)")
            continue
        m = best[a]
        print(f"  {a:6}  {best['variant']:40}  {m['ret']:>+9.0f}%  {m['DD']:>5.1f}%  {m['ret']/m['DD']:>7.1f}")


if __name__ == "__main__":
    main()
