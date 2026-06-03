"""V1 enhanced with options-derived signals.

Original V1: weekly EMA200 cross + VIX ≥ 76 + RSI exit (3 trades over 11y)
V1 enhanced:
  + DVOL ≥ 70 → entry (since 2023)
  + VRP ≤ -10 → entry (since 2023)
  + VIX ≥ 30 → entry (entire history)

Compare:
  V0: Buy & Hold
  V1: Original
  V1+VIX30: + VIX ≥ 30 trigger
  V1+VRP: + VRP ≤ -10 trigger (limited to 2023+ data)
  V1+ALL: all entries combined
"""
import warnings; warnings.filterwarnings("ignore")
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(__file__).parent
FEE, SLIP, INITIAL = 0.0005, 0.0003, 100.0


def ema(arr, n):
    if hasattr(arr, 'values'): arr = arr.values
    out = np.full(len(arr), np.nan)
    if len(arr) < n: return out
    out[n-1] = arr[:n].mean(); k = 2.0/(n+1.0)
    for i in range(n, len(arr)):
        out[i] = arr[i]*k + out[i-1]*(1-k)
    return out

def rsi_a(close, n=14):
    s = pd.Series(close)
    delta = s.diff(); gain = delta.clip(lower=0); loss = -delta.clip(upper=0)
    avg_g = gain.ewm(alpha=1/n, adjust=False).mean()
    avg_l = loss.ewm(alpha=1/n, adjust=False).mean()
    return (100 - 100/(1 + avg_g/avg_l)).values


def load_data():
    btc = pd.read_csv(ROOT/'btc_usd_daily_coinbase.csv', parse_dates=['date']).set_index('date').sort_index()
    btc = btc[btc['close']>0].dropna(subset=['close'])

    vix = pd.read_csv(ROOT / 'vix_daily.csv', skiprows=3,
                       names=['Date','close','high','low','open','volume'])
    vix['Date'] = pd.to_datetime(vix['Date'])
    vix = vix.set_index('Date')

    dvol = pd.read_csv(ROOT/'btc_dvol_daily.csv', parse_dates=['date']).set_index('date').sort_index()

    return btc, vix, dvol


def backtest_strategy(btc, signal_array, exit_signal_array, dates):
    """signal_array: bool, when to enter long.
    exit_signal_array: bool, when to exit.
    """
    closes = btc['close'].values
    lows = btc['low'].values
    n = len(closes)
    cash = INITIAL; qty = 0.0
    eq = np.zeros(n); trades = []
    ev = 0; ed = None; intra_min = 0

    for i in range(n):
        c = closes[i]
        eq[i] = cash + qty*c
        if qty > 0 and (intra_min == 0 or lows[i] < intra_min):
            intra_min = lows[i]

        if qty > 0 and exit_signal_array[i]:
            eff = c * (1 - SLIP)
            proceeds = qty * eff * (1 - FEE)
            cash += proceeds
            pnl = (proceeds/ev - 1) * 100
            mae = (intra_min/(ev/qty) - 1) * 100 if qty > 0 else 0
            trades.append({'entry_date': ed, 'exit_date': dates[i],
                          'pnl_pct': pnl, 'mae_pct': mae,
                          'hold_d': (dates[i] - ed).total_seconds()/86400})
            qty = 0; ev = 0; ed = None; intra_min = 0
            continue

        if qty == 0 and signal_array[i]:
            eff = c * (1 + SLIP)
            spend = cash * 0.99
            bq = spend/eff; cost = bq*eff*(1+FEE)
            cash -= cost; qty = bq
            ev = cost; ed = dates[i]
            intra_min = lows[i]

    if qty > 0:
        c = closes[-1]
        eff = c * (1 - SLIP)
        proceeds = qty * eff * (1 - FEE)
        cash += proceeds
        pnl = (proceeds/ev - 1) * 100
        mae = (intra_min/(ev/qty) - 1) * 100 if qty > 0 else 0
        trades.append({'entry_date': ed, 'exit_date': dates[-1],
                      'pnl_pct': pnl, 'mae_pct': mae,
                      'hold_d': (dates[-1] - ed).total_seconds()/86400})

    return pd.DataFrame(trades), cash, pd.Series(eq, index=dates)


def report(label, tr, fe, eq):
    if len(tr) == 0:
        print(f"  {label:60}  no trades")
        return
    total = (fe/INITIAL - 1) * 100
    dd = ((eq.cummax() - eq)/eq.cummax()).max() * 100
    wr = (tr['pnl_pct']>0).sum()/len(tr) * 100
    pos = tr[tr['pnl_pct']>0]['pnl_pct'].sum()
    neg = abs(tr[tr['pnl_pct']<0]['pnl_pct'].sum())
    pf = pos/neg if neg > 0 else float('inf')
    mae = tr['mae_pct'].min()
    eq_26 = eq[eq.index >= pd.Timestamp(2026,1,1)]
    y26 = (eq_26.iloc[-1]/eq_26.iloc[0] - 1)*100 if len(eq_26)>1 else 0
    eq_rec = eq[eq.index >= pd.Timestamp(2023,9,1)]  # since DVOL data starts
    rec = (eq_rec.iloc[-1]/eq_rec.iloc[0] - 1)*100 if len(eq_rec)>1 else 0
    pf_str = f"{pf:>5.2f}" if pf != float('inf') else "  inf"
    print(f"  {label:60}  N={len(tr):>3}  WR={wr:>4.0f}%  PF={pf_str}  total={total:>+8.0f}%  DD={dd:>4.1f}%  since2023={rec:>+6.0f}%  2026={y26:>+6.1f}%  worstMAE={mae:>+5.1f}%")


def make_v1_entry_signal(btc, vix, vix_threshold=76):
    """Replicate original V1 entry logic (weekly EMA200 cross + VIX threshold)."""
    closes = btc['close'].values
    dates = btc.index
    weekly_close = btc['close'].resample('W-MON', label='left', closed='left').last().dropna()
    w_ema200 = pd.Series(ema(weekly_close.values, 200), index=weekly_close.index)
    cross_under = ((weekly_close < w_ema200) & (weekly_close.shift(1) >= w_ema200.shift(1)))

    cu_d = cross_under.shift(1).reindex(dates, method='ffill').fillna(False).astype(bool)
    cu_strict = (cu_d & ~cu_d.shift(1).fillna(False)).values

    vix_high = vix['high'].reindex(dates, method='ffill').values
    vix_trigger = vix_high >= vix_threshold

    return cu_strict | vix_trigger


def make_exit_signal(btc):
    """RSI SMA crossunder 70 (weekly)."""
    closes = btc['close'].values
    dates = btc.index
    weekly_close = btc['close'].resample('W-MON', label='left', closed='left').last().dropna()
    w_rsi = pd.Series(rsi_a(weekly_close.values, 14), index=weekly_close.index)
    w_rsi_sma = w_rsi.rolling(14).mean()
    exit_rsi = ((w_rsi_sma < 70) & (w_rsi_sma.shift(1) >= 70))
    exit_d = exit_rsi.shift(1).reindex(dates, method='ffill').fillna(False).astype(bool)
    return (exit_d & ~exit_d.shift(1).fillna(False)).values


def main():
    btc, vix, dvol = load_data()
    print("=" * 130)
    print("V1 Strategy ENHANCED with options-derived signals")
    print("=" * 130)
    print(f"\nData ranges:")
    print(f"  BTC: {btc.index[0].date()} → {btc.index[-1].date()} ({len(btc)} days)")
    print(f"  VIX: {vix.index[0].date()} → {vix.index[-1].date()} ({len(vix)} days)")
    print(f"  DVOL: {dvol.index[0].date()} → {dvol.index[-1].date()} ({len(dvol)} days)")

    # Buy & Hold benchmark
    bh = INITIAL / btc['close'].iloc[0] * btc['close']
    bh_total = (bh.iloc[-1]/INITIAL - 1) * 100
    bh_dd = ((bh.cummax() - bh)/bh.cummax()).max() * 100
    print(f"\n  Buy & Hold BTC: total={bh_total:+.0f}%, DD={bh_dd:.0f}%")
    print()

    # Compute realized vol
    rv30 = btc['close'].pct_change().rolling(30).std() * np.sqrt(365) * 100
    rv30_aligned = rv30.reindex(btc.index, method='ffill')

    # Align DVOL to BTC dates
    dvol_close = dvol['close'].reindex(btc.index, method='ffill')
    vrp = dvol_close - rv30_aligned

    dates = btc.index
    exit_signal = make_exit_signal(btc)

    # === V0: original V1 (only weekly cross + VIX 76)
    v1_signal = make_v1_entry_signal(btc, vix, vix_threshold=76)
    tr, fe, eq = backtest_strategy(btc, v1_signal, exit_signal, dates)
    print(f"  V1 ORIGINAL (weekly cross + VIX≥76):")
    report("V1 original", tr, fe, eq)

    # === V2: lower VIX (already tested)
    v2_signal = make_v1_entry_signal(btc, vix, vix_threshold=50)
    tr, fe, eq = backtest_strategy(btc, v2_signal, exit_signal, dates)
    print(f"\n  V2 (weekly cross + VIX≥50):")
    report("V2 (VIX≥50)", tr, fe, eq)

    # === V3: add VIX 30
    v3_signal = make_v1_entry_signal(btc, vix, vix_threshold=30)
    tr, fe, eq = backtest_strategy(btc, v3_signal, exit_signal, dates)
    print(f"\n  V3 (weekly cross + VIX≥30):")
    report("V3 (VIX≥30)", tr, fe, eq)

    # === V4: V1 + VIX≥30 OR DVOL ≥ 70 OR VRP ≤ -10
    vix_high = vix['high'].reindex(dates, method='ffill').values
    dvol_vals = dvol_close.values
    vrp_vals = vrp.values

    v4_signal = v1_signal | (vix_high >= 30) | (dvol_vals >= 70) | (vrp_vals <= -10)
    tr, fe, eq = backtest_strategy(btc, v4_signal, exit_signal, dates)
    print(f"\n  V4 (V1 + VIX≥30 + DVOL≥70 + VRP≤-10):")
    report("V4 (all options signals)", tr, fe, eq)

    # === V5: just VRP ≤ -10 (since 2023)
    v5_signal = (vrp_vals <= -10)
    tr, fe, eq = backtest_strategy(btc, v5_signal, exit_signal, dates)
    print(f"\n  V5 (VRP ≤ -10 ONLY):")
    report("V5 (VRP only)", tr, fe, eq)

    # === V6: V1 + VRP ≤ -10
    v6_signal = v1_signal | (vrp_vals <= -10)
    tr, fe, eq = backtest_strategy(btc, v6_signal, exit_signal, dates)
    print(f"\n  V6 (V1 + VRP≤-10):")
    report("V6 (V1+VRP)", tr, fe, eq)

    # === Show trade-by-trade for best variants
    print(f"\n\n  Trade-by-trade detail (V4 = V1 + ALL options):")
    v4_signal = v1_signal | (vix_high >= 30) | (dvol_vals >= 70) | (vrp_vals <= -10)
    tr, fe, eq = backtest_strategy(btc, v4_signal, exit_signal, dates)
    if len(tr) > 0:
        print(f"    {'entry':>11}  {'exit':>11}  {'days':>5}  {'PnL':>8}  {'MAE':>7}")
        for _, t in tr.iterrows():
            print(f"    {t['entry_date'].date()}  {t['exit_date'].date()}  "
                  f"{int(t['hold_d']):>4}d  {t['pnl_pct']:>+7.1f}%  {t['mae_pct']:>+6.1f}%")


if __name__ == "__main__":
    main()
