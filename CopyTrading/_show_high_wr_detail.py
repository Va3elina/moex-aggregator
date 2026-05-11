"""Show per-pair detail for the best high-WR momentum config."""
import warnings; warnings.filterwarnings("ignore")
from pathlib import Path
import numpy as np
import pandas as pd

DIR = Path(__file__).parent / "moex_daily"
INITIAL = 100.0; FEE = 0.0005; SLIPPAGE = 0.0003


def wilder_ma(s, n):
    out = np.full(len(s), np.nan)
    if len(s) < n: return pd.Series(out, index=s.index)
    v = s.values; out[n-1] = np.nanmean(v[:n])
    for i in range(n, len(s)): out[i] = (out[i-1]*(n-1) + v[i]) / n
    return pd.Series(out, index=s.index)


def features(bars):
    d = bars.copy(); c,h,l,o = d['close'],d['high'],d['low'],d['open']
    delta = c.diff()
    gain = delta.clip(lower=0); loss = -delta.clip(upper=0)
    avg_g = wilder_ma(gain, 14); avg_l = wilder_ma(loss, 14)
    d['rsi_14'] = 100 - 100/(1 + avg_g/avg_l)
    pc = c.shift(1); tr = pd.concat([h-l, (h-pc).abs(), (l-pc).abs()], axis=1).max(axis=1)
    atr14 = wilder_ma(tr, 14)
    d['atr_z_50'] = (atr14 - atr14.rolling(50).mean()) / atr14.rolling(50).std()
    ma20 = c.rolling(20).mean(); sd20 = c.rolling(20).std()
    up = ma20 + 2*sd20; lo = ma20 - 2*sd20
    d['bb_pct_b'] = (c - lo) / (up - lo)
    d['ret_20'] = c.pct_change(20)
    d['ret_60'] = c.pct_change(60)
    d['pos_60'] = (c - l.rolling(60).min()) / (h.rolling(60).max() - l.rolling(60).min())
    e20 = c.ewm(span=20, adjust=False).mean()
    e50 = c.ewm(span=50, adjust=False).mean()
    e200 = c.ewm(span=200, adjust=False).mean()
    d['all_emas_aligned'] = ((e20 > e50) & (e50 > e200)).astype(int)
    return d


def signal(d):
    return ((d['rsi_14'] > 70) & (d['pos_60'] > 0.8) &
            (d['ret_20'] > 0.02) & (d['ret_60'] > 0.20) &
            (d['bb_pct_b'] > 0.5) & (d['all_emas_aligned'] == 1)).fillna(False)


def backtest(bars, sig, hold=20):
    closes = bars['close'].values; times = bars.index
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
                trades.append({'entry': times[entry_i], 'exit': times[i],
                                'price_pct': (eff/ep - 1)*100})
                pos = 0; held = 0
        if pos == 0 and sig.iloc[i]:
            eff = closes[i] * (1 + SLIPPAGE)
            notional = cash * 0.95
            pos = notional / eff
            ep = eff; entry_i = i
            cash -= notional * FEE
    eq = cash + (pos * (closes[-1] - ep) if pos > 0 else 0)
    return pd.DataFrame(trades), eq


def load(name):
    p = DIR / f'{name.lower()}_d.csv'
    df = pd.read_csv(p, parse_dates=['date']).set_index('date').sort_index()
    for c in ['open','high','low','close','volume']: df[c] = pd.to_numeric(df[c], errors='coerce')
    return df


PAIRS = ['AFKS','SMLT','POSI','ASTR','VKCO','SOFL','WUSH','DELI']
print("HIGH-WR config: hold=20d, RSI>70, pos>0.8, ret_20>2%, ret_60>20%, EMA20>50>200")
print("=" * 100)
print(f"{'Pair':6}  {'Trades':>6}  {'Per/yr':>7}  {'WR%':>5}  {'Avg%':>7}  "
      f"{'Strategy$':>9}  {'B&H$':>8}  {'×B&H':>6}")
print("-" * 80)

total_eq = 0; total_bh = 0; total_tr = []; n_pairs = 0
for p in PAIRS:
    bars = features(load(p))
    sig = signal(bars)
    tr, eq = backtest(bars, sig)
    bh = bars['close'].iloc[-1] / bars['close'].iloc[0] * INITIAL
    yrs = (bars.index[-1] - bars.index[0]).days / 365.25
    if len(tr) == 0:
        print(f"{p:6}  {'0':>6}  {'-':>7}  {'-':>5}  {'-':>7}  ${eq:>7.2f}  ${bh:>6.2f}")
        continue
    wr = (tr['price_pct']>0).sum() / len(tr) * 100
    ratio = eq / bh
    print(f"{p:6}  {len(tr):>6}  {len(tr)/yrs:>5.1f}/y  {wr:>4.1f}%  "
          f"{tr['price_pct'].mean():>+6.2f}%  ${eq:>7.2f}  ${bh:>6.2f}  {ratio:>5.2f}x")
    total_eq += eq; total_bh += bh; total_tr.append(tr); n_pairs += 1

if total_tr:
    all_tr = pd.concat(total_tr)
    print("-" * 80)
    print(f"AGGREGATE: {len(all_tr)} trades, WR {(all_tr['price_pct']>0).sum()/len(all_tr)*100:.1f}%, "
          f"avg {all_tr['price_pct'].mean():+.2f}%/trade")
    print(f"  Portfolio (equal $100 per pair): strategy ${total_eq:.2f} vs B&H ${total_bh:.2f}")
    print(f"  Strategy CAGR: {((total_eq/n_pairs/INITIAL)**(1/5)-1)*100:+.1f}%/year (assuming 5y avg history)")
