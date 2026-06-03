"""Backtest GAZP VRP (per-asset, options-derived) strategies.

VRP = ATM_IV (from GAZP options via Black-76) - RV30 (GAZP stock realized vol).

Test:
  A. VRP signal both directions (crypto-style LONG on low VRP, vs RU-style LONG on high)
  B. ATM IV level signal (high IV = panic = mean revert, like RVI worked)
  C. Combine VRP with proven FIZ contrarian signal
"""
import warnings; warnings.filterwarnings("ignore")
import numpy as np
import pandas as pd
from pathlib import Path

ROOT = Path(__file__).parent
INITIAL = 100.0
FEE = 0.0003
SLIP = 0.0001


def load():
    iv = pd.read_csv(ROOT/'gazp_atm_iv_daily.csv', parse_dates=['date']).set_index('date').sort_index()
    gazp = pd.read_csv(ROOT/'gazp_daily.csv', parse_dates=['date']).set_index('date').sort_index()
    futoi = pd.read_csv(ROOT/'gazp_futoi_signals.csv', parse_dates=['date']).set_index('date').sort_index()

    df = gazp[['close']].copy()
    df['ret'] = df['close'].pct_change()
    df['rv30'] = df['ret'].rolling(30).std() * np.sqrt(252) * 100  # RU: 252 trading days
    df['atm_iv'] = iv['atm_iv'].reindex(df.index)
    df['vrp'] = df['atm_iv'] - df['rv30']
    df['fiz_lsr'] = (futoi['fiz_long'] / futoi['fiz_short'].replace(0, 1)).reindex(df.index)
    return df.dropna(subset=['atm_iv', 'close']).copy()


def backtest(df, entry_mask, min_hold, max_hold, exit_mask=None):
    closes = df['close'].values
    dates = df.index
    n = len(df)
    em = entry_mask.values if hasattr(entry_mask, 'values') else entry_mask
    xm = exit_mask.values if (exit_mask is not None and hasattr(exit_mask, 'values')) else exit_mask
    cash = INITIAL
    eq = np.full(n, INITIAL, dtype=float)
    in_pos = False; entry_idx = 0; entry_px = 0
    trades = []
    for i in range(n):
        c = closes[i]
        if in_pos:
            eq[i] = cash * (c/entry_px)
            hold = i - entry_idx
            do_exit = False
            if hold >= max_hold: do_exit = True
            elif hold >= min_hold and xm is not None and xm[i]: do_exit = True
            if do_exit:
                exit_p = c*(1-SLIP)
                ret = exit_p/entry_px - 1 - 2*FEE
                cash *= (1+ret)
                trades.append({'entry': dates[entry_idx], 'exit': dates[i], 'hold': hold, 'pnl': ret*100})
                in_pos = False; eq[i] = cash
        else:
            eq[i] = cash
        if not in_pos and em[i]:
            entry_idx = i; entry_px = c*(1+SLIP); in_pos = True
    return pd.Series(eq, index=dates), pd.DataFrame(trades)


def summ(eq, tr, label):
    total = (eq.iloc[-1]/INITIAL-1)*100
    dd = ((eq.cummax()-eq)/eq.cummax()).max()*100
    n = len(tr); wr = (tr['pnl']>0).sum()/max(n,1)*100; avg = tr['pnl'].mean() if n else 0
    return f"{label:<48}  total={total:>+7.0f}%  DD={dd:>4.0f}%  N={n:>3}  WR={wr:>3.0f}%  avg={avg:>+5.1f}%"


def main():
    df = load()
    print(f"Period: {df.index[0].date()} → {df.index[-1].date()}  ({len(df)} days)")
    bh = (df['close'].iloc[-1]/df['close'].iloc[0]-1)*100
    print(f"GAZP B&H: {bh:+.0f}%\n")

    print("ATM IV / VRP distribution:")
    for q in [0.05, 0.25, 0.5, 0.75, 0.95]:
        print(f"  p{int(q*100):>2}: ATM_IV={df['atm_iv'].quantile(q):>6.1f}%  "
              f"RV30={df['rv30'].quantile(q):>6.1f}%  VRP={df['vrp'].quantile(q):>+6.1f}%")
    print()

    # Forward returns by VRP quintile
    dfx = df.copy()
    for h in [20, 60]:
        dfx[f'fwd{h}'] = dfx['close'].pct_change(h).shift(-h)*100
    dfx['vrp_q'] = pd.qcut(dfx['vrp'], 5, labels=['Q1_low','Q2','Q3','Q4','Q5_high'])
    print("Forward returns by VRP quintile:")
    print(f"  {'bucket':<10} N    {'20d%':>8} {'P_up20':>7}  {'60d%':>8} {'P_up60':>7}")
    for b in ['Q1_low','Q2','Q3','Q4','Q5_high']:
        s = dfx[dfx['vrp_q']==b]
        r20=s['fwd20'].dropna(); r60=s['fwd60'].dropna()
        print(f"  {b:<10} {len(s):>3}  {r20.mean():>+7.1f}% {(r20>0).mean()*100:>6.0f}%  "
              f"{r60.mean():>+7.1f}% {(r60>0).mean()*100:>6.0f}%")
    print()

    dfx['iv_q'] = pd.qcut(dfx['atm_iv'], 5, labels=['Q1_low','Q2','Q3','Q4','Q5_high'])
    print("Forward returns by ATM IV level quintile:")
    print(f"  {'bucket':<10} N    {'20d%':>8} {'P_up20':>7}  {'60d%':>8} {'P_up60':>7}")
    for b in ['Q1_low','Q2','Q3','Q4','Q5_high']:
        s = dfx[dfx['iv_q']==b]
        r20=s['fwd20'].dropna(); r60=s['fwd60'].dropna()
        print(f"  {b:<10} {len(s):>3}  {r20.mean():>+7.1f}% {(r20>0).mean()*100:>6.0f}%  "
              f"{r60.mean():>+7.1f}% {(r60>0).mean()*100:>6.0f}%")
    print()

    print("="*90)
    print("BACKTESTS")
    print("="*90)

    # A: crypto-style LONG on LOW VRP
    print("\nA) LONG on LOW VRP (crypto-style, IV cheap vs realized):")
    for thr in [-5, 0, 5, 10]:
        em = (df['vrp'] <= thr).fillna(False)
        xm = (df['vrp'] >= thr+15).fillna(False)
        eq, tr = backtest(df, em, 10, 60, xm)
        print("  " + summ(eq, tr, f"VRP <= {thr}"))

    # B: RU-style LONG on HIGH IV (panic mean-revert)
    print("\nB) LONG on HIGH ATM IV (panic mean-revert, RVI-style):")
    for thr in [35, 40, 50, 60]:
        em = (df['atm_iv'] >= thr).fillna(False)
        xm = (df['atm_iv'] <= thr*0.7).fillna(False)
        eq, tr = backtest(df, em, 10, 60, xm)
        print("  " + summ(eq, tr, f"ATM_IV >= {thr}"))

    # B2: LONG on HIGH VRP (RU opposite)
    print("\nB2) LONG on HIGH VRP (RU-opposite of crypto):")
    for thr in [10, 15, 20, 25]:
        em = (df['vrp'] >= thr).fillna(False)
        xm = (df['vrp'] <= thr-10).fillna(False)
        eq, tr = backtest(df, em, 10, 60, xm)
        print("  " + summ(eq, tr, f"VRP >= {thr}"))

    # C: combine FIZ extreme with VRP filter
    print("\nC) FIZ LSR <= 1.4 (proven) + VRP filter variations:")
    base = (df['fiz_lsr'] <= 1.4).fillna(False)
    xm = (df['fiz_lsr'] >= 3.5).fillna(False)
    eq, tr = backtest(df, base, 10, 60, xm)
    print("  " + summ(eq, tr, "FIZ<=1.4 alone (baseline)"))
    for vthr in [0, 5, 10]:
        em = (base & (df['vrp'] >= vthr)).fillna(False)
        eq, tr = backtest(df, em, 10, 60, xm)
        print("  " + summ(eq, tr, f"FIZ<=1.4 AND VRP>={vthr}"))
    for ivthr in [30, 40]:
        em = (base & (df['atm_iv'] >= ivthr)).fillna(False)
        eq, tr = backtest(df, em, 10, 60, xm)
        print("  " + summ(eq, tr, f"FIZ<=1.4 AND ATM_IV>={ivthr}"))


if __name__ == "__main__":
    main()
