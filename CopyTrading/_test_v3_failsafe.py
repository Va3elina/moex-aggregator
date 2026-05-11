"""Test: V3 entry conditions + V4 failsafe only (no ATR floor)."""
import warnings; warnings.filterwarnings("ignore")
from pathlib import Path
import numpy as np
import pandas as pd
import sys
sys.path.insert(0, str(Path(__file__).parent))
from backtest_v4 import features, backtest_v4, load, PAIRS

print("TEST: V3 entry conditions + V4 failsafe (best of both)")
print("=" * 70)
all_t = []
for p in PAIRS:
    bars = features(load(p))
    sig_l = ((bars['rsi_14'] > 70) & (bars['pos_60'] > 0.7) & (bars['ret_20'] > 0.10) &
             (bars['adx'] > 25) & bars['trend_up'] &
             bars['vol_tier'].isin([1, 2])).fillna(False)
    sig_s = ((bars['rsi_14'] < 30) & (bars['pos_60'] < 0.1) & (bars['ret_20'] < -0.10) &
             (bars['adx'] > 25) & bars['trend_dn'] &
             bars['vol_tier'].isin([1, 2])).fillna(False)
    tr_l = backtest_v4(bars, sig_l, 'long', failsafe_bars=3, failsafe_min_pnl=1.0)
    tr_s = backtest_v4(bars, sig_s, 'short', failsafe_bars=3, failsafe_min_pnl=1.0)
    if len(tr_l): all_t.append(tr_l)
    if len(tr_s): all_t.append(tr_s)
df = pd.concat(all_t)
wr = (df['pnl_pct'] > 0).sum() / len(df) * 100
pos_pnl = df[df['pnl_pct'] > 0]['pnl_pct'].sum()
neg_pnl = abs(df[df['pnl_pct'] < 0]['pnl_pct'].sum())
pf = pos_pnl / neg_pnl
print(f"V3+failsafe: {len(df)} trades, WR={wr:.1f}%, PF={pf:.2f}, "
      f"avg={df['pnl_pct'].mean():+.2f}%")
print(f"  Max loss: {df['pnl_pct'].min():+.2f}%")
print(f"  Failsafe exits: {(df['reason']=='failsafe').sum()}")
print()
print("Breakdown by reason:")
for r in ['coverback', 'giveback', 'EMA20', 'failsafe', 'maxtime']:
    sub = df[df['reason'] == r]
    if len(sub) == 0:
        continue
    wrr = (sub['pnl_pct'] > 0).sum() / len(sub) * 100
    print(f"  {r:12}: {len(sub):>4} trades, WR {wrr:>5.1f}%, avg {sub['pnl_pct'].mean():+5.2f}%")
print()
# Comparison
print("Side breakdown:")
long_df = df[df['side'] == 'long']
short_df = df[df['side'] == 'short']
if len(long_df):
    lwr = (long_df['pnl_pct'] > 0).sum() / len(long_df) * 100
    print(f"  LONG : {len(long_df)} trades, WR={lwr:.1f}%, avg={long_df['pnl_pct'].mean():+.2f}%")
if len(short_df):
    swr = (short_df['pnl_pct'] > 0).sum() / len(short_df) * 100
    print(f"  SHORT: {len(short_df)} trades, WR={swr:.1f}%, avg={short_df['pnl_pct'].mean():+.2f}%")
