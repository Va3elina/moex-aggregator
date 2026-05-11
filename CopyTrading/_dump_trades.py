"""Dump all trades for each pair in 2025-2026 window for comparison with TradingView."""
import warnings; warnings.filterwarnings("ignore")
from pathlib import Path
import numpy as np
import pandas as pd

DIR = Path(__file__).parent / "moex_1h"
INITIAL = 100.0; FEE = 0.0005; SLIPPAGE = 0.0003

def wilder_ma(s, n):
    out = np.full(len(s), np.nan)
    if len(s) < n: return pd.Series(out, index=s.index)
    v = s.values; out[n-1] = np.nanmean(v[:n])
    for i in range(n, len(s)): out[i] = (out[i-1]*(n-1) + v[i]) / n
    return pd.Series(out, index=s.index)

def features(bars):
    d = bars.copy(); c,h,l,o = d['close'],d['high'],d['low'],d['open']
    rng = (h-l).replace(0, np.nan)
    d['lower_wick'] = (pd.concat([c,o], axis=1).min(axis=1) - l) / rng
    pc = c.shift(1); tr = pd.concat([h-l, (h-pc).abs(), (l-pc).abs()], axis=1).max(axis=1)
    atr14 = wilder_ma(tr, 14); d['atr_pct'] = atr14 / c * 100
    e50 = c.ewm(span=50, adjust=False).mean(); e200 = c.ewm(span=200, adjust=False).mean()
    d['ema_50_200'] = (e50 - e200) / c * 100
    d['ret_4h'] = c.pct_change(4); d['ret_72h'] = c.pct_change(72)
    return d

def signal(d, R):
    j = d.join(R, how='left', rsuffix='_R')
    return ((j['ema_50_200_R']>2.0) & (j['ret_72h_R']>0.02) &
            (j['ema_50_200']>1.0) & (j['ret_4h']<-0.01) &
            (j['atr_pct']>0.5) & (j['lower_wick']>0.25)).fillna(False)

def backtest_with_dates(bars, sig, leverage=3.0, hold=48):
    closes = bars['close'].values
    times = bars.index
    cash, pos, ep, held, notional = INITIAL, 0.0, 0, 0, 0
    trades = []
    for i in range(len(bars)):
        if pos > 0:
            held += 1
            if held >= hold:
                eff = closes[i] * (1 - SLIPPAGE)
                proceeds = pos * eff
                pnl = proceeds - pos * ep
                cash += pnl - proceeds * FEE
                trades.append({
                    'entry_date': times[entry_i], 'exit_date': times[i],
                    'entry_price': ep, 'exit_price': eff,
                    'ret_pct': (eff/ep - 1)*100*leverage,
                    'pnl_rub': (eff/ep - 1) * notional,  # P&L in initial capital terms
                })
                pos = 0; held = 0
        if pos == 0 and sig.iloc[i]:
            eff = closes[i] * (1 + SLIPPAGE)
            notional = cash * leverage * 0.95
            pos = notional / eff
            ep = eff
            entry_i = i
            cash -= notional * FEE
    return pd.DataFrame(trades), cash + pos * closes[-1] if pos > 0 else cash

def load(name):
    p = DIR / f'{name.lower()}_1h.csv'
    df = pd.read_csv(p, parse_dates=['datetime']).set_index('datetime').sort_index()
    for c in ['open','high','low','close','volume']: df[c] = pd.to_numeric(df[c], errors='coerce')
    return df

START = pd.Timestamp('2025-01-03')
END = pd.Timestamp('2026-05-11')

def main():
    imoex_f = features(load('imoex'))[['ema_50_200','ret_4h','ret_72h','atr_pct']]
    for p in ['AFKS','WUSH','DELI','SMLT','POSI','ASTR','VKCO','SOFL']:
        bars = features(load(p))
        sig = signal(bars, imoex_f)
        trades, _ = backtest_with_dates(bars, sig, leverage=3.0, hold=48)
        if len(trades) == 0:
            print(f"\n=== {p} === 0 trades"); continue
        # Filter to 2025+
        trades_window = trades[(trades['entry_date'] >= START) & (trades['entry_date'] <= END)]
        n_wins = (trades_window['ret_pct'] > 0).sum()
        n_total = len(trades_window)
        wr = n_wins/n_total*100 if n_total else 0
        net = trades_window['ret_pct'].sum()
        print(f"\n=== {p} — {n_total} сделок | win {n_wins}/{n_total} ({wr:.0f}%) | "
              f"итог {net:+.2f}% (3x leverage) ===")
        if n_total:
            for i, r in trades_window.iterrows():
                marker = '✓' if r['ret_pct'] > 0 else '✗'
                print(f"  {marker} {r['entry_date'].strftime('%Y-%m-%d %H:%M')} → "
                      f"{r['exit_date'].strftime('%Y-%m-%d %H:%M')}  "
                      f"{r['entry_price']:>8.2f} → {r['exit_price']:>8.2f}  "
                      f"{r['ret_pct']:+6.2f}%")

if __name__ == "__main__":
    main()
