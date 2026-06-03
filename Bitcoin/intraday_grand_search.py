"""Intraday TF exploration: BTC 1H/4H/15min, ETH 1H/4H.

Strategies tested:
  W1: wavelet trend (slope5∧slope6 > 0)
  8a: bands breakout (k=1.5, std50)
  8a + ATR(3x) trail
  8a + EMA50 filter
  Triple slope (sl4∧sl5∧sl6 > 0)
  Price > s5 (simplest)
  Volume spike + breakout
  Multi-TF: daily slope6 filter + intraday entry

Reports per (asset, TF, strategy): total, DD, recent 4y, 2026, N, WR, PF.
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
    out[n-1] = arr[:n].mean()
    k = 2.0/(n+1.0)
    for i in range(n, len(arr)):
        out[i] = arr[i]*k + out[i-1]*(1-k)
    return out

def atr_vec(high, low, close, n=14):
    pc = np.empty_like(close); pc[0] = close[0]; pc[1:] = close[:-1]
    tr = np.maximum(high-low, np.maximum(np.abs(high-pc), np.abs(low-pc)))
    out = np.full(len(close), np.nan)
    out[0] = tr[0]; a = 1.0/n
    for i in range(1, len(close)):
        out[i] = a*tr[i] + (1-a)*out[i-1]
    return out

def atrous_haar(x, J=7):
    sm = [x.astype(float).copy()]
    for j in range(1, J+1):
        gap = 2**(j-1); prev = sm[-1]; s = prev.copy()
        if len(x) > gap: s[gap:] = 0.5*(prev[gap:] + prev[:-gap])
        sm.append(s)
    return sm

def slope_a(arr, lb=5):
    out = np.full(len(arr), np.nan)
    if len(arr) > lb: out[lb:] = arr[lb:] - arr[:-lb]
    return out


def load_hourly(asset):
    name = f"{asset.lower()}_usd_hourly_coinbase.csv"
    df = pd.read_csv(ROOT/name, parse_dates=['datetime']).set_index('datetime').sort_index()
    df.index = df.index.tz_localize(None)
    return df[df['close']>0].dropna(subset=['close'])

def load_15min(asset):
    name = f"{asset.lower()}_usd_15min_coinbase.csv"
    df = pd.read_csv(ROOT/name, parse_dates=['datetime']).set_index('datetime').sort_index()
    df.index = df.index.tz_localize(None)
    return df[df['close']>0].dropna(subset=['close'])

def resample(df, rule):
    return df.resample(rule).agg({'open':'first','high':'max','low':'min','close':'last','volume':'sum'}).dropna()


def precompute(df):
    log_c = np.log(df['close'].values)
    sm = atrous_haar(log_c, J=7)
    return {
        'log_c': log_c, 'price': df['close'].values,
        'high': df['high'].values, 'low': df['low'].values,
        'volume': df['volume'].values, 'index': df.index,
        's4': sm[4], 's5': sm[5], 's6': sm[6], 's7': sm[7],
        'sl4': slope_a(sm[4], 5), 'sl5': slope_a(sm[5], 5),
        'sl6': slope_a(sm[6], 5), 'sl7': slope_a(sm[7], 5),
        'std50': pd.Series(log_c - sm[5]).rolling(50).std().values,
        'ema50': ema(df['close'].values, 50),
        'ema100': ema(df['close'].values, 100),
        'ema200': ema(df['close'].values, 200),
        'atr14': atr_vec(df['high'].values, df['low'].values, df['close'].values, 14),
        'vol_ma_30': pd.Series(df['volume'].values).rolling(30).mean().values,
    }


def state_machine(entry, exit_):
    n = len(entry); state = np.zeros(n); s = 0
    for i in range(n):
        if s == 1 and exit_[i]: s = 0
        elif s == 0 and entry[i]: s = 1
        state[i] = float(s)
    return state


def state_machine_atr(entry, price, atr14, mult):
    n = len(price); state = np.zeros(n); s = 0
    trail = 0.0; high = 0.0
    for i in range(n):
        if s == 1:
            if not np.isnan(atr14[i]):
                if price[i] > high: high = price[i]
                cand = high - mult * atr14[i]
                if cand > trail: trail = cand
                if price[i] < trail: s = 0
        else:
            if entry[i]:
                s = 1; high = price[i]
                trail = price[i] - mult * atr14[i] if not np.isnan(atr14[i]) else 0
        state[i] = float(s)
    return state


# ===== Strategy variants =====
def v_W1(p):
    """slope5∧slope6 > 0."""
    entry = (p['sl5'] > 0) & (p['sl6'] > 0)
    exit_ = (p['sl5'] <= 0) | (p['sl6'] <= 0)
    mask = np.isnan(p['sl5']) | np.isnan(p['sl6'])
    return state_machine(np.where(mask, False, entry), np.where(mask, False, exit_))

def v_8a(p, k=1.5):
    std = p['std50']; upper = p['s5'] + k*std
    entry = p['log_c'] > upper
    exit_ = p['log_c'] < p['s5']
    mask = np.isnan(std)
    return state_machine(np.where(mask, False, entry), np.where(mask, False, exit_))

def v_8a_atr(p, k=1.5, mult=3.0):
    std = p['std50']; upper = p['s5'] + k*std
    entry = p['log_c'] > upper
    return state_machine_atr(np.where(np.isnan(std), False, entry), p['price'], p['atr14'], mult)

def v_8a_ema(p, k=1.5, ema_key='ema100'):
    std = p['std50']; upper = p['s5'] + k*std
    entry = (p['log_c'] > upper) & (p['price'] > p[ema_key])
    exit_ = (p['log_c'] < p['s5']) | (p['price'] < p[ema_key])
    mask = np.isnan(std) | np.isnan(p[ema_key])
    return state_machine(np.where(mask, False, entry), np.where(mask, False, exit_))

def v_triple(p):
    entry = (p['sl4'] > 0) & (p['sl5'] > 0) & (p['sl6'] > 0)
    exit_ = (p['sl4'] <= 0) | (p['sl5'] <= 0) | (p['sl6'] <= 0)
    mask = np.isnan(p['sl4']) | np.isnan(p['sl5']) | np.isnan(p['sl6'])
    return state_machine(np.where(mask, False, entry), np.where(mask, False, exit_))

def v_price_gt_s5(p):
    entry = p['log_c'] > p['s5']
    exit_ = p['log_c'] < p['s5']
    mask = np.isnan(p['s5'])
    return state_machine(np.where(mask, False, entry), np.where(mask, False, exit_))

def v_price_gt_s5_s6(p):
    entry = (p['log_c'] > p['s5']) & (p['s5'] > p['s6'])
    exit_ = (p['log_c'] < p['s5']) | (p['s5'] < p['s6'])
    mask = np.isnan(p['s5']) | np.isnan(p['s6'])
    return state_machine(np.where(mask, False, entry), np.where(mask, False, exit_))

def v_volume_breakout(p):
    """Buy when (1H vol > 3× 30h avg AND price breaks above s5+1σ).
    Sell when price < s5."""
    std = p['std50']
    vol_ratio = p['volume'] / p['vol_ma_30']
    entry = (vol_ratio > 3.0) & (p['log_c'] > (p['s5'] + 1.0 * std))
    exit_ = p['log_c'] < p['s5']
    mask = np.isnan(std) | np.isnan(p['vol_ma_30'])
    return state_machine(np.where(mask, False, entry), np.where(mask, False, exit_))


# ===== Backtest =====
def backtest(p, target):
    closes = p['price']; lows = p['low']; dates = p['index']
    n = len(closes)
    cash = INITIAL; qty = 0.0
    eq = np.zeros(n); trades = []
    ev = 0; ed = None; intra_min = 0
    for i in range(n):
        c = closes[i]
        eq[i] = cash + qty*c
        if qty > 0 and (intra_min == 0 or lows[i] < intra_min):
            intra_min = lows[i]
        if target[i] == 1 and qty == 0:
            eff = c*(1+SLIP); spend = cash*0.99
            bq = spend/eff; cost = bq*eff*(1+FEE)
            cash -= cost; qty = bq
            ev = cost; ed = dates[i]
            intra_min = lows[i]
        elif target[i] == 0 and qty > 0:
            eff = c*(1-SLIP); proceeds = qty*eff*(1-FEE)
            cash += proceeds
            pnl = (proceeds/ev - 1) * 100
            mae = (intra_min/(ev/qty) - 1) * 100 if qty > 0 else 0
            trades.append({'entry': ed, 'exit': dates[i], 'pnl_pct': pnl, 'mae_pct': mae,
                          'hold_d': (dates[i] - ed).total_seconds()/86400})
            qty = 0; ev = 0; ed = None; intra_min = 0
    if qty > 0:
        c = closes[-1]; eff = c*(1-SLIP); proceeds = qty*eff*(1-FEE)
        cash += proceeds
        pnl = (proceeds/ev - 1) * 100
        mae = (intra_min/(ev/qty) - 1) * 100 if qty > 0 else 0
        trades.append({'entry': ed, 'exit': dates[-1], 'pnl_pct': pnl, 'mae_pct': mae,
                      'hold_d': (dates[-1] - ed).total_seconds()/86400})
    return pd.DataFrame(trades), cash, pd.Series(eq, index=dates)


def metrics(tr, fe, eq):
    if len(tr) == 0:
        return None
    total = (fe/INITIAL - 1) * 100
    dd = ((eq.cummax() - eq)/eq.cummax()).max() * 100
    wr = (tr['pnl_pct']>0).sum()/len(tr) * 100
    pos = tr[tr['pnl_pct']>0]['pnl_pct'].sum()
    neg = abs(tr[tr['pnl_pct']<0]['pnl_pct'].sum())
    pf = pos/neg if neg > 0 else float('inf')
    mae = tr['mae_pct'].min()
    eq_26 = eq[eq.index >= pd.Timestamp(2026,1,1)]
    y26 = (eq_26.iloc[-1]/eq_26.iloc[0] - 1)*100 if len(eq_26)>1 else 0
    eq_rec = eq[eq.index >= pd.Timestamp(2022,1,1)]
    rec = (eq_rec.iloc[-1]/eq_rec.iloc[0] - 1)*100 if len(eq_rec)>1 else 0
    avg_hold = tr['hold_d'].mean()
    return {'N': len(tr), 'WR': wr, 'PF': pf, 'total': total, 'dd': dd,
            'mae': mae, 'rec4y': rec, 'y26': y26, 'avgHold': avg_hold}


def report(label, m):
    if m is None:
        print(f"  {label:42}  (no trades)"); return
    pf_str = f"{m['PF']:>5.2f}" if m['PF'] != float('inf') else "  inf"
    print(f"  {label:42}  N={m['N']:>5}  WR={m['WR']:>4.1f}%  PF={pf_str}  total={m['total']:>+7.0f}%  DD={m['dd']:>4.1f}%  rec4y={m['rec4y']:>+5.0f}%  2026={m['y26']:>+6.1f}%  hold={m['avgHold']:>5.1f}d")


def main():
    btc_h = load_hourly('BTC')
    eth_h = load_hourly('ETH')
    btc_4h = resample(btc_h, '4h')
    eth_4h = resample(eth_h, '4h')
    btc_15m = load_15min('BTC')

    datasets = [
        ('BTC 15min', btc_15m),
        ('BTC 1H',    btc_h),
        ('BTC 4H',    btc_4h),
        ('ETH 1H',    eth_h),
        ('ETH 4H',    eth_4h),
    ]

    variants = [
        ('W1 (s5∧s6 slopes up)',        v_W1),
        ('Triple (s4∧s5∧s6 up)',        v_triple),
        ('Price > s5 only',              v_price_gt_s5),
        ('Price > s5 AND s5>s6',         v_price_gt_s5_s6),
        ('8a (k=1.5) bands breakout',    v_8a),
        ('8a + ATR(3x) trail',           v_8a_atr),
        ('8a + EMA100 filter',           lambda p: v_8a_ema(p, 1.5, 'ema100')),
        ('8a + EMA200 filter',           lambda p: v_8a_ema(p, 1.5, 'ema200')),
        ('Volume spike breakout',        v_volume_breakout),
    ]

    for tf_label, df in datasets:
        n_bars = len(df)
        # Skip 15min for memory if too big
        if n_bars > 200000:
            df_use = df.iloc[-150000:]  # last ~150k bars
            print(f"\n{'='*150}\n{tf_label} (LIMITED to last 150k bars of {n_bars})\n{'='*150}")
        else:
            df_use = df
            print(f"\n{'='*150}\n{tf_label} ({n_bars} bars, {df.index[0]} → {df.index[-1]})\n{'='*150}")

        p = precompute(df_use)

        # B&H benchmark
        bh = INITIAL / df_use['close'].iloc[0] * df_use['close']
        bh_total = (bh.iloc[-1]/INITIAL - 1) * 100
        bh_dd = ((bh.cummax() - bh)/bh.cummax()).max() * 100
        bh_26 = bh[bh.index >= pd.Timestamp(2026,1,1)]
        bh_26_ret = (bh_26.iloc[-1]/bh_26.iloc[0] - 1)*100 if len(bh_26)>1 else 0
        print(f"  Buy & Hold: total={bh_total:+.0f}%, DD={bh_dd:.0f}%, 2026={bh_26_ret:+.1f}%")

        for label, fn in variants:
            target = fn(p)
            tr, fe, eq = backtest(p, target)
            m = metrics(tr, fe, eq)
            report(label, m)


if __name__ == "__main__":
    main()
