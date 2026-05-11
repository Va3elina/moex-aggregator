"""Try better variants for BTC 1H — wider stops, different entry logic."""
import warnings; warnings.filterwarnings("ignore")
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from btc_1h_volume_atr import (load_hourly, load_daily, hourly_features,
                                  daily_features, join_daily_to_hourly,
                                  backtest_tier, INITIAL)
import numpy as np
import pandas as pd


def main():
    h_bars = load_hourly()
    d_bars = load_daily()
    h_f = hourly_features(h_bars)
    d_f = daily_features(d_bars)
    df = join_daily_to_hourly(h_f, d_f).dropna(subset=['atr_25', 'vol_tier'])

    print(f"BTC 1H: {len(df)} bars")
    print(f"Vol tier distribution:")
    print(df['vol_tier'].value_counts().sort_index())

    # Test WIDER stops + bigger TPs
    print("\n" + "=" * 110)
    print("Wider stops with various TP/trail combos (strong_daily filter)")
    print("=" * 110)
    print(f"  {'config':50}  {'trades':>6}  {'WR%':>5}  {'PF':>5}  {'avg%':>6}  {'final$':>10}")
    print("  " + "-" * 100)
    configs = [
        # label, stop, {1: tp, 2: tp, 3: tp}, trail, activate
        ('stop3 tp5 trail1.0',      3.0, {1: 5.0, 2: 5.0, 3: 5.0},   1.0, 2.0),
        ('stop3 tp8 trail1.0',      3.0, {1: 8.0, 2: 8.0, 3: 8.0},   1.0, 2.0),
        ('stop5 tp10 trail1.5',     5.0, {1: 10.0, 2: 10.0, 3: 10.0}, 1.5, 3.0),
        ('stop5 tp_huge trail3.0',  5.0, {1: 100, 2: 100, 3: 100},    3.0, 5.0),
        ('stop3 tp_huge trail2.0',  3.0, {1: 100, 2: 100, 3: 100},    2.0, 3.0),
        ('stop2 tp_huge trail1.5',  2.0, {1: 100, 2: 100, 3: 100},    1.5, 2.0),
        ('stop10 tp_huge trail5.0', 10.0, {1: 100, 2: 100, 3: 100},   5.0, 8.0),
        # Trail very wide — let big winners run
        ('stop3 tp_huge trail2 act0.5', 3.0, {1: 100, 2: 100, 3: 100}, 2.0, 0.5),
    ]
    for label, stop, tps, trail, activate in configs:
        tr, eq = backtest_tier(df, trend_filter='strong_daily',
                                  stop_atr=stop, tp_atr_per_tier=tps,
                                  trail_atr=trail, trail_activate_atr=activate)
        if len(tr) == 0: continue
        wr = (tr['pnl_pct']>0).sum() / len(tr) * 100
        pos_pnl = tr[tr['pnl_pct']>0]['pnl_pct'].sum()
        neg_pnl = abs(tr[tr['pnl_pct']<0]['pnl_pct'].sum())
        pf = pos_pnl/neg_pnl if neg_pnl>0 else float('inf')
        print(f"  {label:50}  {len(tr):>6}  {wr:>4.1f}%  {pf:>4.2f}  "
              f"{tr['pnl_pct'].mean():>+5.2f}%  ${eq:>8.2f}")

    # Tier-specific: only trade tier 1 (most common) with various configs
    print("\n" + "=" * 110)
    print("ONLY tier 1 (vol 1.5-3.25x — most frequent) with various stops")
    print("=" * 110)
    print(f"  {'config':50}  {'trades':>6}  {'WR%':>5}  {'PF':>5}  {'avg%':>6}  {'final$':>10}")
    for label, stop, tp_val, trail, activate in [
        ('T1 only: stop1 tp2.5',    1.0, 2.5, 0.5, 1.0),
        ('T1 only: stop2 tp4',      2.0, 4.0, 1.0, 2.0),
        ('T1 only: stop3 tp6',      3.0, 6.0, 1.5, 3.0),
        ('T1 only: stop3 tp_huge trail1.5', 3.0, 100, 1.5, 2.0),
    ]:
        tr, eq = backtest_tier(df, min_tier=1, max_tier=1,
                                  trend_filter='strong_daily',
                                  stop_atr=stop, tp_atr_per_tier={1: tp_val, 2: tp_val, 3: tp_val},
                                  trail_atr=trail, trail_activate_atr=activate)
        if len(tr) == 0:
            print(f"  {label:50}  0 trades"); continue
        wr = (tr['pnl_pct']>0).sum() / len(tr) * 100
        pos_pnl = tr[tr['pnl_pct']>0]['pnl_pct'].sum()
        neg_pnl = abs(tr[tr['pnl_pct']<0]['pnl_pct'].sum())
        pf = pos_pnl/neg_pnl if neg_pnl>0 else float('inf')
        print(f"  {label:50}  {len(tr):>6}  {wr:>4.1f}%  {pf:>4.2f}  "
              f"{tr['pnl_pct'].mean():>+5.2f}%  ${eq:>8.2f}")

    # Check what tier 1 trades actually do
    print("\n  Debug: how many bars have each tier?")
    print(f"    vol_tier=0: {(df['vol_tier']==0).sum()}")
    print(f"    vol_tier=1: {(df['vol_tier']==1).sum()}")
    print(f"    vol_tier=2: {(df['vol_tier']==2).sum()}")
    print(f"    vol_tier=3: {(df['vol_tier']==3).sum()}")


if __name__ == "__main__":
    main()
