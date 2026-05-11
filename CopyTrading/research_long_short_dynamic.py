"""Comprehensive research: LONG + SHORT with volume/trend filters + dynamic exits.

LONG entry: trend up + momentum + volume confirmation
  RSI > rsi_long, pos_60 > pos_long, ret_20 > ret_long
  EMA20 > EMA50 > EMA200 (uptrend confirmed)
  vol_tier in [1, 2]  (avoid 0 = no volume and 3 = blow-off)

SHORT entry: trend down + weakness + volume confirmation
  RSI < rsi_short, pos_60 < pos_short, ret_20 < ret_short
  EMA20 < EMA50 < EMA200 (downtrend confirmed)
  vol_tier in [1, 2]

Dynamic exits:
  LONG: TP ceiling X% + give-back Y% | EMA20 break fallback
  SHORT: mirror (TP ceiling on negative side + cover-back) | EMA20 cross UP

Grid search both sides separately, find best per-pair config.
"""
import warnings; warnings.filterwarnings("ignore")
from pathlib import Path
from itertools import product
import numpy as np
import pandas as pd

DIR = Path(__file__).parent / "moex_daily"
INITIAL = 100.0; FEE = 0.0005; SLIPPAGE = 0.0003
PAIRS = ['AFKS','SMLT','POSI','ASTR','VKCO','SOFL','WUSH','DELI']


def wilder_ma(s, n):
    out = np.full(len(s), np.nan)
    if len(s) < n: return pd.Series(out, index=s.index)
    v = s.values; out[n-1] = np.nanmean(v[:n])
    for i in range(n, len(s)): out[i] = (out[i-1]*(n-1) + v[i]) / n
    return pd.Series(out, index=s.index)


def features(bars):
    d = bars.copy(); c,h,l,o,v = d['close'],d['high'],d['low'],d['open'],d['volume']
    # RSI
    delta = c.diff()
    gain = delta.clip(lower=0); loss = -delta.clip(upper=0)
    avg_g = wilder_ma(gain, 14); avg_l = wilder_ma(loss, 14)
    d['rsi_14'] = 100 - 100/(1 + avg_g/avg_l)
    # ATR
    pc = c.shift(1); tr = pd.concat([h-l, (h-pc).abs(), (l-pc).abs()], axis=1).max(axis=1)
    d['atr'] = wilder_ma(tr, 14)
    # ADX
    up_move = h - h.shift(1); dn_move = l.shift(1) - l
    plus_dm = pd.Series(np.where((up_move > dn_move) & (up_move > 0), up_move, 0), index=d.index)
    minus_dm = pd.Series(np.where((dn_move > up_move) & (dn_move > 0), dn_move, 0), index=d.index)
    plus_di = 100 * wilder_ma(plus_dm, 14) / d['atr']
    minus_di = 100 * wilder_ma(minus_dm, 14) / d['atr']
    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di)
    d['adx'] = wilder_ma(dx, 14)
    # BB
    ma20 = c.rolling(20).mean(); sd20 = c.rolling(20).std()
    d['bb_pct_b'] = (c - (ma20 - 2*sd20)) / (4*sd20)
    # EMAs
    d['ema_20'] = c.ewm(span=20, adjust=False).mean()
    d['ema_50'] = c.ewm(span=50, adjust=False).mean()
    d['ema_200'] = c.ewm(span=200, adjust=False).mean()
    d['trend_up'] = (d['ema_20'] > d['ema_50']) & (d['ema_50'] > d['ema_200'])
    d['trend_dn'] = (d['ema_20'] < d['ema_50']) & (d['ema_50'] < d['ema_200'])
    # Volume
    d['vol_ratio'] = v / v.rolling(20).mean()
    d['vol_tier'] = 0
    d.loc[d['vol_ratio'] >= 1.5, 'vol_tier'] = 1
    d.loc[d['vol_ratio'] >= 3.25, 'vol_tier'] = 2
    d.loc[d['vol_ratio'] >= 6.0, 'vol_tier'] = 3
    # Returns + position
    d['ret_20'] = c.pct_change(20)
    d['pos_60'] = (c - l.rolling(60).min()) / (h.rolling(60).max() - l.rolling(60).min())
    return d


def long_signal(d, rsi_min=60, pos_min=0.7, ret_min=0.03, adx_min=15, require_trend=True):
    s = ((d['rsi_14'] > rsi_min) & (d['pos_60'] > pos_min) &
          (d['ret_20'] > ret_min) & (d['adx'] > adx_min) &
          (d['vol_tier'].isin([1, 2])))
    if require_trend:
        s = s & d['trend_up']
    return s.fillna(False)


def short_signal(d, rsi_max=40, pos_max=0.3, ret_max=-0.03, adx_min=15, require_trend=True):
    s = ((d['rsi_14'] < rsi_max) & (d['pos_60'] < pos_max) &
          (d['ret_20'] < ret_max) & (d['adx'] > adx_min) &
          (d['vol_tier'].isin([1, 2])))
    if require_trend:
        s = s & d['trend_dn']
    return s.fillna(False)


def backtest_dynamic(bars, sig, side='long', tp_pct=5.0, gb_pct=30.0,
                       max_hold=40):
    """Adaptive: TP ceiling activates trailing, EMA20 fallback, max safety."""
    closes = bars['close'].values; highs = bars['high'].values; lows = bars['low'].values
    ema20 = bars['ema_20'].values
    cash, pos, ep, held, peak = INITIAL, 0.0, 0, 0, 0
    tp_active = False
    trades = []
    times = bars.index

    for i in range(len(bars)):
        if pos != 0:
            held += 1
            c, h, l = closes[i], highs[i], lows[i]

            if side == 'long':
                peak = max(peak, h)
                peak_pnl = (peak / ep - 1) * 100
                cur_pnl = (c / ep - 1) * 100
                if not tp_active and peak_pnl >= tp_pct:
                    tp_active = True
                gb_level = ep + (peak - ep) * (1 - gb_pct / 100)

                exit_p, reason = None, None
                if tp_active and l <= gb_level:
                    exit_p = gb_level; reason = 'giveback'
                elif not tp_active and c < ema20[i]:
                    exit_p = c; reason = 'EMA20'
                elif held >= max_hold:
                    exit_p = c; reason = 'maxtime'

                if exit_p is not None:
                    eff = exit_p * (1 - SLIPPAGE)
                    cash += pos * eff * (1 - FEE)
                    trades.append({'side': 'long', 'entry_t': times[ei], 'exit_t': times[i],
                                    'entry_p': ep, 'exit_p': eff,
                                    'pnl_pct': (eff/ep - 1) * 100, 'hold': held,
                                    'reason': reason})
                    pos = 0; held = 0; tp_active = False; peak = 0
            else:  # short
                peak = min(peak, l) if peak > 0 else l
                # for short: profit when price falls below entry
                peak_pnl = (ep / peak - 1) * 100  # positive if price dropped
                cur_pnl = (ep / c - 1) * 100
                if not tp_active and peak_pnl >= tp_pct:
                    tp_active = True
                # cover-back: for short, exit if price rises X% from lowest point
                gb_level = peak + (ep - peak) * (gb_pct / 100)

                exit_p, reason = None, None
                if tp_active and h >= gb_level:
                    exit_p = gb_level; reason = 'coverback'
                elif not tp_active and c > ema20[i]:
                    exit_p = c; reason = 'EMA20'
                elif held >= max_hold:
                    exit_p = c; reason = 'maxtime'

                if exit_p is not None:
                    eff = exit_p * (1 + SLIPPAGE)  # cover (buy) with slippage up
                    # short pnl: positive when exit < entry
                    pnl = abs(pos) * (ep - eff)
                    cash += pnl - abs(pos) * eff * FEE
                    trades.append({'side': 'short', 'entry_t': times[ei], 'exit_t': times[i],
                                    'entry_p': ep, 'exit_p': eff,
                                    'pnl_pct': (ep/eff - 1) * 100, 'hold': held,
                                    'reason': reason})
                    pos = 0; held = 0; tp_active = False; peak = 0

        if pos == 0 and sig.iloc[i]:
            if side == 'long':
                eff = closes[i] * (1 + SLIPPAGE)
                qty = (cash * 0.95) / eff
                cost = qty * eff * (1 + FEE)
                if cost <= cash:
                    cash -= cost; pos = qty; ep = eff; ei = i; peak = closes[i]
            else:
                eff = closes[i] * (1 - SLIPPAGE)
                qty = (cash * 0.95) / eff
                # short: receive cash, owe shares
                pos = -qty
                cash -= qty * eff * FEE  # only fee on entry
                ep = eff; ei = i; peak = closes[i]

    return pd.DataFrame(trades), cash + (pos * closes[-1] if pos > 0 else 0)


def load(name):
    p = DIR / f'{name.lower()}_d.csv'
    df = pd.read_csv(p, parse_dates=['date']).set_index('date').sort_index()
    for c in ['open','high','low','close','volume']: df[c] = pd.to_numeric(df[c], errors='coerce')
    return df


def aggregate_long(pairs, **kw):
    all_t = []
    for p in pairs:
        bars = features(load(p))
        sig = long_signal(bars, **{k: v for k, v in kw.items() if k in
                                     ['rsi_min','pos_min','ret_min','adx_min','require_trend']})
        tr, _ = backtest_dynamic(bars, sig, side='long',
                                   tp_pct=kw.get('tp_pct', 5.0),
                                   gb_pct=kw.get('gb_pct', 30.0))
        if len(tr): all_t.append(tr)
    if not all_t: return None
    df = pd.concat(all_t); n = len(df)
    return {'n': n, 'wr': (df['pnl_pct']>0).sum()/n*100,
            'avg': df['pnl_pct'].mean(),
            'pf': df[df['pnl_pct']>0]['pnl_pct'].sum() /
                   abs(df[df['pnl_pct']<0]['pnl_pct'].sum()) if (df['pnl_pct']<0).any() else float('inf'),
            'max_loss': df['pnl_pct'].min(), 'max_win': df['pnl_pct'].max(),
            'avg_hold': df['hold'].mean()}


def aggregate_short(pairs, **kw):
    all_t = []
    for p in pairs:
        bars = features(load(p))
        sig = short_signal(bars, **{k: v for k, v in kw.items() if k in
                                      ['rsi_max','pos_max','ret_max','adx_min','require_trend']})
        tr, _ = backtest_dynamic(bars, sig, side='short',
                                   tp_pct=kw.get('tp_pct', 5.0),
                                   gb_pct=kw.get('gb_pct', 30.0))
        if len(tr): all_t.append(tr)
    if not all_t: return None
    df = pd.concat(all_t); n = len(df)
    return {'n': n, 'wr': (df['pnl_pct']>0).sum()/n*100,
            'avg': df['pnl_pct'].mean(),
            'pf': df[df['pnl_pct']>0]['pnl_pct'].sum() /
                   abs(df[df['pnl_pct']<0]['pnl_pct'].sum()) if (df['pnl_pct']<0).any() else float('inf'),
            'max_loss': df['pnl_pct'].min(), 'max_win': df['pnl_pct'].max(),
            'avg_hold': df['hold'].mean()}


def main():
    print("=" * 110)
    print("LONG SIDE — grid search with volume + trend filters + adaptive exits")
    print("=" * 110)
    results = []
    for rsi, pos, ret, adx, trend, tp, gb in product(
            [55, 60, 65, 70], [0.7, 0.8, 0.9], [0.02, 0.05, 0.10],
            [15, 20, 25], [True, False], [5, 10], [25, 30, 40]):
        m = aggregate_long(PAIRS, rsi_min=rsi, pos_min=pos, ret_min=ret,
                            adx_min=adx, require_trend=trend, tp_pct=tp, gb_pct=gb)
        if m and m['n'] >= 30:
            results.append({'rsi':rsi,'pos':pos,'ret':ret,'adx':adx,
                              'trend':trend,'tp':tp,'gb':gb, **m})
    df = pd.DataFrame(results)
    print(f"  {len(df)} configs tested with min 30 trades\n")
    print("TOP 10 by WR (long):")
    print(df.nlargest(10, 'wr').to_string(index=False, float_format=lambda x: f"{x:.2f}"))

    print("\nTOP 10 by avg/trade (long):")
    print(df.nlargest(10, 'avg').to_string(index=False, float_format=lambda x: f"{x:.2f}"))

    # Best WR config detail
    best_long = df.nlargest(1, 'wr').iloc[0]
    print(f"\nBest long config: rsi>{best_long['rsi']}, pos>{best_long['pos']}, "
          f"ret>{best_long['ret']*100}%, adx>{best_long['adx']}, "
          f"trend={best_long['trend']}, TP={best_long['tp']}%, GB={best_long['gb']}%")
    print(f"  → {int(best_long['n'])} trades, WR={best_long['wr']:.1f}%, "
          f"avg={best_long['avg']:+.2f}%, PF={best_long['pf']:.2f}")

    print("\n" + "=" * 110)
    print("SHORT SIDE — mirror grid")
    print("=" * 110)
    results_s = []
    for rsi, pos, ret, adx, trend, tp, gb in product(
            [30, 35, 40, 45], [0.1, 0.2, 0.3], [-0.10, -0.05, -0.02],
            [15, 20, 25], [True, False], [5, 10], [25, 30, 40]):
        m = aggregate_short(PAIRS, rsi_max=rsi, pos_max=pos, ret_max=ret,
                             adx_min=adx, require_trend=trend, tp_pct=tp, gb_pct=gb)
        if m and m['n'] >= 20:
            results_s.append({'rsi':rsi,'pos':pos,'ret':ret,'adx':adx,
                                'trend':trend,'tp':tp,'gb':gb, **m})
    df_s = pd.DataFrame(results_s)
    print(f"  {len(df_s)} short configs tested with min 20 trades\n")
    if len(df_s):
        print("TOP 10 by WR (short):")
        print(df_s.nlargest(10, 'wr').to_string(index=False, float_format=lambda x: f"{x:.2f}"))
        print("\nTOP 10 by avg/trade (short):")
        print(df_s.nlargest(10, 'avg').to_string(index=False, float_format=lambda x: f"{x:.2f}"))


if __name__ == "__main__":
    main()
