"""Segment BTC 1H Nov 2023 - May 2026 into 'legs' (directional moves) using ZigZag.

For each leg: start, end, duration, % move, type (up/down).
Then classify and look at statistics — what kinds of moves dominate?
"""
import warnings; warnings.filterwarnings("ignore")
from pathlib import Path
import numpy as np
import pandas as pd

H1_PATH = Path(__file__).parent.parent / "Bitcoin" / "btc_usd_hourly_coinbase.csv"


def zigzag(bars, threshold_pct=5.0):
    """Detect swing points where price reverses by threshold_pct from last extreme.
    Returns list of (datetime, price, type) where type='H' or 'L'."""
    highs = bars['high'].values
    lows = bars['low'].values
    times = bars.index
    pivots = []
    # Initialize with first bar
    current_high = highs[0]
    current_low = lows[0]
    current_high_idx = 0
    current_low_idx = 0
    direction = 0  # 0=undetermined, 1=up, -1=down

    for i in range(1, len(bars)):
        h = highs[i]; l = lows[i]
        if direction >= 0:
            # Was going up, looking for higher highs OR a reversal
            if h > current_high:
                current_high = h; current_high_idx = i
            # Check for downward reversal
            if (current_high - l) / current_high * 100 >= threshold_pct:
                pivots.append({'idx': current_high_idx, 'time': times[current_high_idx],
                                 'price': current_high, 'type': 'H'})
                current_low = l; current_low_idx = i
                direction = -1
        if direction <= 0:
            # Was going down, looking for lower lows OR a reversal
            if l < current_low:
                current_low = l; current_low_idx = i
            if (h - current_low) / current_low * 100 >= threshold_pct:
                pivots.append({'idx': current_low_idx, 'time': times[current_low_idx],
                                 'price': current_low, 'type': 'L'})
                current_high = h; current_high_idx = i
                direction = 1
    return pd.DataFrame(pivots)


def legs_from_pivots(pivots, bars):
    """Pair consecutive pivots into legs."""
    legs = []
    for i in range(1, len(pivots)):
        a = pivots.iloc[i-1]; b = pivots.iloc[i]
        leg_type = 'UP' if b['type'] == 'H' else 'DOWN'
        bars_in_leg = bars.iloc[a['idx']:b['idx']+1]
        legs.append({
            'leg_n': i,
            'start_time': a['time'],
            'end_time': b['time'],
            'start_price': a['price'],
            'end_price': b['price'],
            'type': leg_type,
            'pct_move': (b['price'] / a['price'] - 1) * 100,
            'duration_hours': b['idx'] - a['idx'],
            'duration_days': (b['idx'] - a['idx']) / 24,
            'max_drawdown_within': ((bars_in_leg['low'].min() / a['price'] - 1) * 100) if leg_type == 'UP'
                                   else ((bars_in_leg['high'].max() / a['price'] - 1) * 100),
        })
    return pd.DataFrame(legs)


def load_bars():
    df = pd.read_csv(H1_PATH, parse_dates=['datetime']).set_index('datetime').sort_index()
    df.index = df.index.tz_localize(None)
    for c in ['open','high','low','close','volume']: df[c] = pd.to_numeric(df[c], errors='coerce')
    return df


def main():
    bars = load_bars()
    bars = bars[bars.index >= '2023-11-01']
    print(f"BTC 1H: {len(bars)} bars  {bars.index[0]} → {bars.index[-1]}")

    print("\n" + "=" * 100)
    print("Segmentation at different ZigZag thresholds:")
    print("=" * 100)

    for threshold in [3.0, 5.0, 8.0, 12.0, 20.0]:
        pivots = zigzag(bars, threshold)
        legs = legs_from_pivots(pivots, bars)
        ups = legs[legs['type'] == 'UP']
        downs = legs[legs['type'] == 'DOWN']
        print(f"\n--- Threshold {threshold}% ---")
        print(f"  Total legs: {len(legs)}")
        print(f"  UP legs:    {len(ups):>3}  avg move {ups['pct_move'].mean():>+6.1f}%  "
              f"median duration {ups['duration_days'].median():.1f}d")
        print(f"  DOWN legs:  {len(downs):>3}  avg move {downs['pct_move'].mean():>+6.1f}%  "
              f"median duration {downs['duration_days'].median():.1f}d")

    # Pick 5% threshold and show all legs
    print("\n" + "=" * 100)
    print("All legs at 5% threshold (BTC 1H Nov 2023 - May 2026):")
    print("=" * 100)
    pivots = zigzag(bars, 5.0)
    legs = legs_from_pivots(pivots, bars)
    print(f"  {'#':>3}  {'type':>4}  {'start':>16}  {'end':>16}  {'move%':>7}  "
          f"{'days':>5}  {'maxDD':>7}")
    print("  " + "-" * 80)
    for _, r in legs.iterrows():
        print(f"  {int(r['leg_n']):>3}  {r['type']:>4}  "
              f"{r['start_time'].strftime('%Y-%m-%d %H'):>16}  "
              f"{r['end_time'].strftime('%Y-%m-%d %H'):>16}  "
              f"{r['pct_move']:>+6.1f}%  {r['duration_days']:>4.1f}  "
              f"{r['max_drawdown_within']:>+6.1f}%")

    # Now classify
    print("\n" + "=" * 100)
    print("LEG CLASSIFICATION (5% threshold)")
    print("=" * 100)
    print(f"\n  UP-leg classification (by magnitude × duration):")
    ups = legs[legs['type'] == 'UP'].copy()
    ups['ratio'] = ups['pct_move'] / ups['duration_days']  # %/day
    for label, mask in [
        ("BIG fast (>15% in <7d)",   (ups['pct_move'] > 15) & (ups['duration_days'] < 7)),
        ("BIG slow (>15% over 7d+)",  (ups['pct_move'] > 15) & (ups['duration_days'] >= 7)),
        ("MID (8-15%)",               (ups['pct_move'] >= 8) & (ups['pct_move'] <= 15)),
        ("SMALL (5-8%)",              (ups['pct_move'] >= 5) & (ups['pct_move'] < 8)),
    ]:
        sub = ups[mask]
        if len(sub) == 0: continue
        print(f"    {label:30}  N={len(sub)}  avg={sub['pct_move'].mean():+.1f}%  "
              f"med_dur={sub['duration_days'].median():.1f}d  "
              f"avg_ratio={sub['ratio'].mean():.2f}%/day")

    print(f"\n  DOWN-leg classification:")
    downs = legs[legs['type'] == 'DOWN'].copy()
    downs['ratio'] = downs['pct_move'] / downs['duration_days']
    for label, mask in [
        ("DEEP fast (>10% drop, <5d)", (downs['pct_move'] < -10) & (downs['duration_days'] < 5)),
        ("DEEP slow (>10% over 5d+)",   (downs['pct_move'] < -10) & (downs['duration_days'] >= 5)),
        ("MEDIUM (-7 to -10%)",         (downs['pct_move'] >= -10) & (downs['pct_move'] <= -7)),
        ("SHALLOW (-5 to -7%)",         (downs['pct_move'] > -7)),
    ]:
        sub = downs[mask]
        if len(sub) == 0: continue
        print(f"    {label:30}  N={len(sub)}  avg={sub['pct_move'].mean():+.1f}%  "
              f"med_dur={sub['duration_days'].median():.1f}d")

    # What follows each type of leg?
    print(f"\n  What follows each UP-leg type? (next leg is always DOWN)")
    print(f"  {'after UP-leg type':35}  {'next DOWN avg':>14}  {'next DOWN days':>15}")
    legs_indexed = legs.reset_index(drop=True)
    for label, mask in [
        ("BIG fast (>15%, <7d)",   (legs_indexed['type'] == 'UP') & (legs_indexed['pct_move'] > 15) & (legs_indexed['duration_days'] < 7)),
        ("BIG slow (>15%, 7d+)",   (legs_indexed['type'] == 'UP') & (legs_indexed['pct_move'] > 15) & (legs_indexed['duration_days'] >= 7)),
        ("MID (8-15%)",            (legs_indexed['type'] == 'UP') & (legs_indexed['pct_move'].between(8, 15))),
        ("SMALL (5-8%)",           (legs_indexed['type'] == 'UP') & (legs_indexed['pct_move'].between(5, 8))),
    ]:
        idx_list = legs_indexed[mask].index.tolist()
        next_downs = [legs_indexed.iloc[idx+1] for idx in idx_list
                       if idx+1 < len(legs_indexed) and legs_indexed.iloc[idx+1]['type'] == 'DOWN']
        if not next_downs: continue
        next_moves = [d['pct_move'] for d in next_downs]
        next_days = [d['duration_days'] for d in next_downs]
        print(f"  {label:35}  {np.mean(next_moves):>+13.1f}%  {np.median(next_days):>14.1f}d")

    # Save legs CSV
    out_csv = Path(__file__).parent / "btc_legs_5pct.csv"
    legs.to_csv(out_csv, index=False)
    print(f"\n  Saved legs to {out_csv.name}")


if __name__ == "__main__":
    main()
