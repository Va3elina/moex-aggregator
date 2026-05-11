"""Test different failsafe rules."""
import warnings; warnings.filterwarnings("ignore")
from pathlib import Path
import numpy as np
import pandas as pd
import sys
sys.path.insert(0, str(Path(__file__).parent))
from backtest_v4 import features, load, PAIRS

INITIAL = 100.0; FEE = 0.0005; SLIPPAGE = 0.0003


def backtest_custom_failsafe(bars, sig, side='long',
                                 fs_rule=None, fs_bars=3, fs_threshold=0):
    """fs_rule: None | 'pnl_below' | 'no_peak' | 'mae_max'.
    pnl_below: current pnl < fs_threshold at fs_bars
    no_peak: max_pnl during first fs_bars <= 0
    mae_max: max adverse excursion > fs_threshold
    """
    closes = bars['close'].values; highs = bars['high'].values; lows = bars['low'].values
    ema20 = bars['ema_20'].values
    cash, pos, ep, held, peak, trough = INITIAL, 0.0, 0, 0, 0, 0
    max_pnl_during = -1e9; min_pnl_during = 1e9
    tp_active = False
    trades = []
    for i in range(len(bars)):
        if pos != 0:
            held += 1
            c, h, l = closes[i], highs[i], lows[i]
            if side == 'long':
                peak = max(peak, h)
                cur_pnl = (c/ep - 1) * 100
                peak_pnl = (peak/ep - 1) * 100
                trough_low = (l/ep - 1) * 100
                max_pnl_during = max(max_pnl_during, peak_pnl)
                min_pnl_during = min(min_pnl_during, trough_low)
                if not tp_active and peak_pnl >= 5.0: tp_active = True
                gb_level = ep + (peak - ep) * 0.75
                exit_p, reason = None, None
                # Failsafe checks
                if fs_rule == 'pnl_below' and held == fs_bars and cur_pnl < fs_threshold:
                    exit_p = c; reason = 'fs_pnl'
                elif fs_rule == 'no_peak' and held == fs_bars and max_pnl_during <= 0:
                    exit_p = c; reason = 'fs_nopeak'
                elif fs_rule == 'mae_max' and min_pnl_during < fs_threshold:
                    exit_p = c; reason = 'fs_mae'
                elif tp_active and l <= gb_level:
                    exit_p = gb_level; reason = 'giveback'
                elif not tp_active and c < ema20[i]:
                    exit_p = c; reason = 'EMA20'
                elif held >= 40:
                    exit_p = c; reason = 'maxtime'
                if exit_p is not None:
                    eff = exit_p * (1 - SLIPPAGE)
                    cash += pos * eff * (1 - FEE)
                    trades.append({'side':'long','pnl_pct':(eff/ep-1)*100,
                                    'reason':reason,'hold':held,
                                    'max_pnl':max_pnl_during,'min_pnl':min_pnl_during})
                    pos = 0; held = 0; tp_active = False; peak = 0
                    max_pnl_during = -1e9; min_pnl_during = 1e9
            else:
                peak = min(peak, l) if peak > 0 else l  # peak = lowest low (best for short)
                cur_pnl = (ep/c - 1) * 100
                peak_pnl = (ep/peak - 1) * 100  # gain when price falls
                worst = (ep/h - 1) * 100  # negative gain when price rises
                max_pnl_during = max(max_pnl_during, peak_pnl)
                min_pnl_during = min(min_pnl_during, worst)
                if not tp_active and peak_pnl >= 5.0: tp_active = True
                cb_level = peak + (ep - peak) * 0.25
                exit_p, reason = None, None
                if fs_rule == 'pnl_below' and held == fs_bars and cur_pnl < fs_threshold:
                    exit_p = c; reason = 'fs_pnl'
                elif fs_rule == 'no_peak' and held == fs_bars and max_pnl_during <= 0:
                    exit_p = c; reason = 'fs_nopeak'
                elif fs_rule == 'mae_max' and min_pnl_during < fs_threshold:
                    exit_p = c; reason = 'fs_mae'
                elif tp_active and h >= cb_level:
                    exit_p = cb_level; reason = 'coverback'
                elif not tp_active and c > ema20[i]:
                    exit_p = c; reason = 'EMA20'
                elif held >= 40:
                    exit_p = c; reason = 'maxtime'
                if exit_p is not None:
                    eff = exit_p * (1 + SLIPPAGE)
                    pnl = abs(pos) * (ep - eff)
                    cash += pnl - abs(pos) * eff * FEE
                    trades.append({'side':'short','pnl_pct':(ep/eff-1)*100,
                                    'reason':reason,'hold':held,
                                    'max_pnl':max_pnl_during,'min_pnl':min_pnl_during})
                    pos = 0; held = 0; tp_active = False; peak = 0
                    max_pnl_during = -1e9; min_pnl_during = 1e9

        if pos == 0 and sig.iloc[i]:
            if side == 'long':
                eff = closes[i] * (1 + SLIPPAGE)
                qty = (cash * 0.95) / eff
                cost = qty * eff * (1 + FEE)
                if cost <= cash:
                    cash -= cost; pos = qty; ep = eff; peak = closes[i]
            else:
                eff = closes[i] * (1 - SLIPPAGE)
                qty = (cash * 0.95) / eff
                pos = -qty
                cash -= qty * eff * FEE
                ep = eff; peak = closes[i]
    return pd.DataFrame(trades)


def long_sig(bars):
    return ((bars['rsi_14'] > 70) & (bars['pos_60'] > 0.7) & (bars['ret_20'] > 0.10) &
             (bars['adx'] > 25) & bars['trend_up'] &
             bars['vol_tier'].isin([1, 2])).fillna(False)


def short_sig(bars):
    return ((bars['rsi_14'] < 30) & (bars['pos_60'] < 0.1) & (bars['ret_20'] < -0.10) &
             (bars['adx'] > 25) & bars['trend_dn'] &
             bars['vol_tier'].isin([1, 2])).fillna(False)


def run_test(fs_rule, fs_bars=3, fs_threshold=0, label=""):
    all_t = []
    for p in PAIRS:
        bars = features(load(p))
        tr_l = backtest_custom_failsafe(bars, long_sig(bars), 'long', fs_rule, fs_bars, fs_threshold)
        tr_s = backtest_custom_failsafe(bars, short_sig(bars), 'short', fs_rule, fs_bars, fs_threshold)
        if len(tr_l): all_t.append(tr_l)
        if len(tr_s): all_t.append(tr_s)
    df = pd.concat(all_t)
    wr = (df['pnl_pct']>0).sum()/len(df)*100
    pos_pnl = df[df['pnl_pct']>0]['pnl_pct'].sum()
    neg_pnl = abs(df[df['pnl_pct']<0]['pnl_pct'].sum())
    pf = pos_pnl/neg_pnl if neg_pnl > 0 else float('inf')
    max_loss = df['pnl_pct'].min()
    return {'label': label, 'n': len(df), 'wr': wr, 'pf': pf,
            'avg': df['pnl_pct'].mean(), 'max_loss': max_loss}


print("FAILSAFE VARIATIONS TEST")
print("=" * 90)
configs = [
    ('none', None, 0, 0, 'Baseline (no failsafe)'),
    ('no_peak_3', 'no_peak', 3, 0, 'No-peak rule: exit if max_pnl<=0 after 3 bars'),
    ('no_peak_5', 'no_peak', 5, 0, 'No-peak rule: exit if max_pnl<=0 after 5 bars'),
    ('no_peak_7', 'no_peak', 7, 0, 'No-peak rule: exit if max_pnl<=0 after 7 bars'),
    ('mae_5', 'mae_max', 0, -5.0, 'MAE rule: exit if drawdown >5% during trade'),
    ('mae_7', 'mae_max', 0, -7.0, 'MAE rule: exit if drawdown >7%'),
    ('mae_10', 'mae_max', 0, -10.0, 'MAE rule: exit if drawdown >10%'),
    ('pnl_below_2_-2', 'pnl_below', 2, -2.0, 'PNL: at day 2, exit if pnl<-2%'),
    ('pnl_below_3_-3', 'pnl_below', 3, -3.0, 'PNL: at day 3, exit if pnl<-3%'),
    ('pnl_below_5_-2', 'pnl_below', 5, -2.0, 'PNL: at day 5, exit if pnl<-2%'),
]
print(f"  {'label':50}  {'N':>4}  {'WR%':>5}  {'PF':>5}  {'avg':>6}  {'maxLoss':>8}")
print("  " + "-" * 90)
for name, rule, bars, thresh, label in configs:
    r = run_test(rule, bars, thresh, label)
    print(f"  {label:50}  {r['n']:>4}  {r['wr']:>5.1f}  {r['pf']:>5.2f}  "
          f"{r['avg']:>+5.2f}  {r['max_loss']:>+7.2f}")
