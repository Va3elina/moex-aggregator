"""Backtest IMOEX options/vol signals: term_ratio, VRP, RVI>=40 (proven RU long),
and FIZ contrarian. Adapted from backtest_sber_vrp.py.

Long-only IMOEX (via spot index proxy) entries with min/max hold + exit condition.
"""
import warnings; warnings.filterwarnings("ignore")
import numpy as np
import pandas as pd
from pathlib import Path

ROOT = Path(__file__).parent
IMOEX_D = Path('/home/user/moex-aggregator/CopyTrading/moex_daily/imoex_d.csv')
INITIAL = 100.0
FEE = 0.0003
SLIP = 0.0001


def load():
    spot = pd.read_csv(IMOEX_D, parse_dates=['date']).set_index('date').sort_index()
    ind = pd.read_csv(ROOT/'imoex_option_indicators_daily.csv', parse_dates=['date']).set_index('date').sort_index()
    futoi = pd.read_csv(ROOT/'imoex_futoi_history.csv', parse_dates=['date']).set_index('date').sort_index()
    rvi = pd.read_csv(ROOT/'rvi_daily.csv', parse_dates=['TRADEDATE']).set_index('TRADEDATE').sort_index()

    df = spot[['close']].copy()
    df['ret'] = df['close'].pct_change()
    df['rv30'] = df['ret'].rolling(30).std() * np.sqrt(252) * 100
    for c in ['atm_iv', 'term_ratio_30_90', 'iv_30d', 'iv_90d']:
        if c in ind.columns:
            df[c] = ind[c].reindex(df.index)
    df['vrp'] = df['atm_iv'] - df['rv30']
    df['fiz_lsr'] = (futoi['fiz_long'] / futoi['fiz_short'].replace(0, 1)).reindex(df.index)
    df['rvi'] = rvi['close'].reindex(df.index)
    return df.dropna(subset=['close']).copy()


def backtest(df, entry_mask, min_hold, max_hold, exit_mask=None):
    closes = df['close'].values
    dates = df.index
    n = len(df)
    em = entry_mask.reindex(df.index).fillna(False).values
    xm = exit_mask.reindex(df.index).fillna(False).values if exit_mask is not None else None
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
    return f"{label:<46}  total={total:>+7.0f}%  DD={dd:>4.0f}%  N={n:>3}  WR={wr:>3.0f}%  avg={avg:>+5.1f}%"


def main():
    df = load()
    sub = df.dropna(subset=['atm_iv'])
    print(f"Indicator period: {sub.index[0].date()} -> {sub.index[-1].date()}  ({len(sub)} days w/ ATM IV)")
    bh = (df['close'].iloc[-1]/df['close'].iloc[0]-1)*100
    print(f"IMOEX B&H (full): {bh:+.0f}%\n")

    print("Distribution (days w/ options):")
    for q in [0.05, 0.25, 0.5, 0.75, 0.95]:
        print(f"  p{int(q*100):>2}: ATM_IV={sub['atm_iv'].quantile(q):>6.1f}  "
              f"RVI={sub['rvi'].quantile(q):>6.1f}  VRP={sub['vrp'].quantile(q):>+6.1f}  "
              f"term={sub['term_ratio_30_90'].quantile(q):>5.2f}")
    print()

    print("="*94)
    print("BACKTESTS (long-only IMOEX)")
    print("="*94)

    print("\n1) RVI >= X (panic mean-revert, proven RU long):")
    for thr in [30, 40, 50, 60]:
        em = df['rvi'] >= thr
        xm = df['rvi'] <= thr*0.7
        eq, tr = backtest(df, em, 10, 60, xm)
        print("  " + summ(eq, tr, f"RVI >= {thr}"))

    print("\n2) ATM IV >= X (our built IV, same logic as RVI):")
    for thr in [25, 30, 40, 50]:
        em = df['atm_iv'] >= thr
        xm = df['atm_iv'] <= thr*0.7
        eq, tr = backtest(df, em, 10, 60, xm)
        print("  " + summ(eq, tr, f"ATM_IV >= {thr}"))

    print("\n3) Term structure ratio (iv30/iv90):")
    print("   3a) LONG on backwardation (term>=X, stress):")
    for thr in [1.0, 1.05, 1.1]:
        em = df['term_ratio_30_90'] >= thr
        xm = df['term_ratio_30_90'] <= 1.0
        eq, tr = backtest(df, em, 10, 60, xm)
        print("  " + summ(eq, tr, f"term >= {thr}"))
    print("   3b) LONG on contango (term<=X, calm):")
    for thr in [0.95, 0.9]:
        em = df['term_ratio_30_90'] <= thr
        xm = df['term_ratio_30_90'] >= 1.0
        eq, tr = backtest(df, em, 10, 60, xm)
        print("  " + summ(eq, tr, f"term <= {thr}"))

    print("\n4) VRP signal:")
    print("   4a) LONG on HIGH VRP (RU-style):")
    for thr in [5, 10, 15]:
        em = df['vrp'] >= thr
        xm = df['vrp'] <= thr-8
        eq, tr = backtest(df, em, 10, 60, xm)
        print("  " + summ(eq, tr, f"VRP >= {thr}"))
    print("   4b) LONG on LOW VRP (crypto-style):")
    for thr in [-5, 0, 5]:
        em = df['vrp'] <= thr
        xm = df['vrp'] >= thr+12
        eq, tr = backtest(df, em, 10, 60, xm)
        print("  " + summ(eq, tr, f"VRP <= {thr}"))

    print("\n5) FIZ contrarian (retail crowd fade):")
    print("   5a) LONG when FIZ net long is LOW (retail bearish -> bullish):")
    qlo = df['fiz_lsr'].quantile(0.3)
    for thr in [round(qlo, 2), 1.0, 1.4]:
        em = df['fiz_lsr'] <= thr
        xm = df['fiz_lsr'] >= df['fiz_lsr'].quantile(0.7)
        eq, tr = backtest(df, em, 10, 60, xm)
        print("  " + summ(eq, tr, f"FIZ_LSR <= {thr}"))
    print("   5b) SHORT-avoidance: LONG when FIZ very HIGH (momentum):")
    for thr in [round(df['fiz_lsr'].quantile(0.8), 2)]:
        em = df['fiz_lsr'] >= thr
        xm = df['fiz_lsr'] <= df['fiz_lsr'].quantile(0.5)
        eq, tr = backtest(df, em, 10, 60, xm)
        print("  " + summ(eq, tr, f"FIZ_LSR >= {thr}"))

    print("\n6) Combo RVI>=40 AND FIZ low:")
    em = (df['rvi'] >= 40) & (df['fiz_lsr'] <= df['fiz_lsr'].quantile(0.5))
    xm = df['rvi'] <= 28
    eq, tr = backtest(df, em, 10, 60, xm)
    print("  " + summ(eq, tr, "RVI>=40 AND FIZ<=median"))


if __name__ == "__main__":
    main()
