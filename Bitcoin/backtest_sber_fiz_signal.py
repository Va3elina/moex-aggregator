"""Backtest SBER LONG strategy on FIZ (retail) contrarian signal.

Signal hypothesis (validated in analyze_sber_futoi.py):
  - FIZ LSR (long/short ratio) at extreme LOW = retail panic = local bottom → LONG
  - FIZ LSR at extreme HIGH = retail euphoria = local top → EXIT / SHORT

Strategy: LONG SBER when FIZ_LSR < threshold_low, exit after fixed hold OR when FIZ_LSR > threshold_high.

We test on 2020-01 → 2026-05 (1500+ days).
"""
import pandas as pd
import numpy as np
from pathlib import Path

ROOT = Path(__file__).parent
INITIAL = 100.0
FEE_PCT = 0.0005   # Bybit-like, 0.05% per side (MOEX brokers typically lower but safer)
SLIP_PCT = 0.0003

sber = pd.read_csv(ROOT/'sber_daily.csv', parse_dates=['date']).set_index('date').sort_index()
futoi = pd.read_csv(ROOT/'sber_futoi_signals.csv', parse_dates=['date']).set_index('date').sort_index()


def make_df():
    df = sber[['close']].join(futoi[['fiz_long', 'fiz_short', 'yur_long', 'yur_short']], how='inner')
    df['fiz_lsr'] = df['fiz_long'] / df['fiz_short'].replace(0, 1)
    df['yur_lsr'] = df['yur_long'] / df['yur_short'].replace(0, 1)
    # 30d rolling vol for context
    df['ret'] = df['close'].pct_change()
    df['rv30'] = df['ret'].rolling(30).std() * np.sqrt(252) * 100
    return df.dropna(subset=['fiz_lsr', 'close']).copy()


def backtest(df, entry_lsr_max, exit_lsr_min, min_hold, max_hold, label=""):
    closes = df['close'].values
    dates = df.index
    lsr = df['fiz_lsr'].values
    n = len(df)
    cash = INITIAL
    eq = np.full(n, INITIAL, dtype=float)
    in_pos = False
    entry_idx = 0
    entry_px = 0
    trades = []
    for i in range(n):
        c = closes[i]
        if in_pos:
            eq[i] = cash * (c / entry_px)
            hold = i - entry_idx
            do_exit = False
            reason = ""
            if hold >= max_hold:
                do_exit = True; reason = "maxhold"
            elif hold >= min_hold and lsr[i] >= exit_lsr_min:
                do_exit = True; reason = "fiz_euph"
            if do_exit:
                exit_p = c * (1 - SLIP_PCT)
                ret = exit_p/entry_px - 1 - 2*FEE_PCT
                cash *= (1 + ret)
                trades.append({'entry': dates[entry_idx], 'exit': dates[i], 'hold_d': hold,
                              'entry_lsr': lsr[entry_idx], 'exit_lsr': lsr[i],
                              'pnl_pct': ret * 100, 'reason': reason})
                in_pos = False
                eq[i] = cash
        else:
            eq[i] = cash
        if not in_pos and lsr[i] <= entry_lsr_max:
            entry_idx = i
            entry_px = c * (1 + SLIP_PCT)
            in_pos = True
    return pd.Series(eq, index=dates), pd.DataFrame(trades)


def summarize(eq, tr, label):
    total = (eq.iloc[-1]/INITIAL - 1) * 100
    dd = ((eq.cummax() - eq)/eq.cummax()).max() * 100
    n = len(tr)
    wr = (tr['pnl_pct']>0).sum()/max(n, 1)*100
    avg = tr['pnl_pct'].mean() if n else 0
    return f"{label:<40}  total={total:>+8.0f}%  DD={dd:>4.0f}%  N={n:>3}  WR={wr:>3.0f}%  avg={avg:>+5.1f}%"


def main():
    df = make_df()
    print(f"Period: {df.index[0].date()} → {df.index[-1].date()}  ({len(df)} days)")
    bh = (df['close'].iloc[-1]/df['close'].iloc[0] - 1) * 100
    print(f"SBER Buy & Hold: {bh:+.0f}%\n")

    # Print FIZ LSR distribution
    print("FIZ LSR distribution:")
    for q in [0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95]:
        print(f"  p{int(q*100)}: {df['fiz_lsr'].quantile(q):.2f}")
    print()

    # Sweep entry threshold
    print("="*100)
    print("Entry threshold sweep (FIZ LSR <= X = retail panic short → LONG):")
    print("Exit: FIZ LSR >= 3.0 (retail euphoric) after min_hold OR max_hold=60")
    print("="*100)
    for entry_thr in [1.0, 1.2, 1.4, 1.6, 1.8, 2.0]:
        eq, tr = backtest(df, entry_lsr_max=entry_thr, exit_lsr_min=3.0,
                           min_hold=10, max_hold=60)
        print(summarize(eq, tr, f"FIZ LSR <= {entry_thr}"))

    print("\n" + "="*100)
    print("Exit threshold sweep (when to take profit on retail euphoria):")
    print("Entry: FIZ LSR <= 1.4, max_hold=60")
    print("="*100)
    for exit_thr in [2.0, 2.5, 3.0, 3.5, 4.0, 5.0, 99]:
        eq, tr = backtest(df, entry_lsr_max=1.4, exit_lsr_min=exit_thr,
                           min_hold=10, max_hold=60)
        print(summarize(eq, tr, f"Exit on FIZ LSR >= {exit_thr}"))

    print("\n" + "="*100)
    print("Hold period sweep (entry 1.4, exit 3.0):")
    print("="*100)
    for mh in [20, 40, 60, 90, 120, 180]:
        eq, tr = backtest(df, entry_lsr_max=1.4, exit_lsr_min=3.0,
                           min_hold=10, max_hold=mh)
        print(summarize(eq, tr, f"max_hold = {mh}d"))

    # Best combo
    print("\n" + "="*100)
    print("BEST: FIZ LSR <= 1.4 entry, >= 3.0 exit, hold 10-60d")
    print("="*100)
    eq, tr = backtest(df, entry_lsr_max=1.4, exit_lsr_min=3.0, min_hold=10, max_hold=60)
    print(summarize(eq, tr, "BEST"))
    print("\nTrades:")
    for _, t in tr.iterrows():
        print(f"  {t['entry'].date()} → {t['exit'].date()} ({int(t['hold_d']):>3}d)  "
              f"FIZ LSR {t['entry_lsr']:.2f}→{t['exit_lsr']:.2f}  "
              f"PnL {t['pnl_pct']:>+6.1f}%  ({t['reason']})")


if __name__ == "__main__":
    main()
