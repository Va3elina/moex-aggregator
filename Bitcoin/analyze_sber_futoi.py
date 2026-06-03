"""Analyze SBER futoi for smart money signals.

Key insight: on FORTS clearing YUR and FIZ are zero-sum (yur_net = -fiz_net always).
So "smart_money_ratio = yur/fiz" is uninformative.

Useful metrics:
  1. YUR change day-over-day (institutional re-positioning)
  2. YUR long/short ratio (direction of institutional bias)
  3. FIZ long/short ratio extremes (retail capitulation/euphoria)
  4. Crowd ratio: pos_long_num vs total_n (how many participants are long)
"""
import pandas as pd
import numpy as np
from pathlib import Path

ROOT = Path(__file__).parent
futoi = pd.read_csv(ROOT/'sber_futoi_history.csv', parse_dates=['date']).set_index('date').sort_index()
sber = pd.read_csv(ROOT/'sber_daily.csv', parse_dates=['date']).set_index('date').sort_index()

print(f"futoi: {futoi.index[0].date()} → {futoi.index[-1].date()}, {len(futoi)} rows")
print(f"SBER:  {sber.index[0].date()} → {sber.index[-1].date()}, {len(sber)} rows\n")

# Build derived metrics
df = futoi.copy()
df['yur_total'] = df['yur_long'] + df['yur_short']
df['fiz_total'] = df['fiz_long'] + df['fiz_short']

# Ratios
df['yur_lsr'] = df['yur_long'] / df['yur_short'].replace(0, 1)  # long/short ratio
df['fiz_lsr'] = df['fiz_long'] / df['fiz_short'].replace(0, 1)

# Change day-over-day
df['yur_net_chg'] = df['yur_net'].diff()
df['yur_net_chg_pct'] = df['yur_net'].diff() / df['yur_total'].shift(1) * 100

# Smoothed
df['yur_net_chg_7d'] = df['yur_net_chg'].rolling(7).mean()

# (skip participation — not in CSV)

# Print extreme values
print("=" * 100)
print("YUR (institutional) Long/Short Ratio percentiles:")
for q in [0.05, 0.1, 0.5, 0.9, 0.95]:
    print(f"  p{int(q*100)}: {df['yur_lsr'].quantile(q):.3f}")

print("\nFIZ (retail) Long/Short Ratio percentiles:")
for q in [0.05, 0.1, 0.5, 0.9, 0.95]:
    print(f"  p{int(q*100)}: {df['fiz_lsr'].quantile(q):.3f}")

# Forward returns analysis
sber_close = sber[['close']].rename(columns={'close': 'sber_close'})
merged = df.join(sber_close, how='inner')
for h in [5, 10, 20, 60]:
    merged[f'fwd_{h}d'] = merged['sber_close'].pct_change(h).shift(-h) * 100

print("\n" + "=" * 100)
print("FORWARD RETURNS by YUR LSR percentile (institutional positioning):")
print("=" * 100)
merged['yur_lsr_q'] = pd.qcut(merged['yur_lsr'], 5, labels=['Q1_short', 'Q2', 'Q3', 'Q4', 'Q5_long'])
print(f"\n  {'YUR LSR bucket':<14}  N    "
      f"{'mean 20d %':>10}  {'P(up) 20d':>10}  "
      f"{'mean 60d %':>10}  {'P(up) 60d':>10}")
for bucket in ['Q1_short', 'Q2', 'Q3', 'Q4', 'Q5_long']:
    sub = merged[merged['yur_lsr_q'] == bucket]
    r20 = sub['fwd_20d'].dropna()
    r60 = sub['fwd_60d'].dropna()
    print(f"  {bucket:<14}  {len(sub):>3}  "
          f"{r20.mean():>+9.2f}%  {(r20>0).mean()*100:>8.0f}%  "
          f"{r60.mean():>+9.2f}%  {(r60>0).mean()*100:>8.0f}%")

print("\n" + "=" * 100)
print("FORWARD RETURNS by FIZ LSR percentile (retail positioning — CONTRARIAN expected):")
print("=" * 100)
merged['fiz_lsr_q'] = pd.qcut(merged['fiz_lsr'], 5, labels=['Q1_short', 'Q2', 'Q3', 'Q4', 'Q5_long'])
print(f"\n  {'FIZ LSR bucket':<14}  N    "
      f"{'mean 20d %':>10}  {'P(up) 20d':>10}  "
      f"{'mean 60d %':>10}  {'P(up) 60d':>10}")
for bucket in ['Q1_short', 'Q2', 'Q3', 'Q4', 'Q5_long']:
    sub = merged[merged['fiz_lsr_q'] == bucket]
    r20 = sub['fwd_20d'].dropna()
    r60 = sub['fwd_60d'].dropna()
    print(f"  {bucket:<14}  {len(sub):>3}  "
          f"{r20.mean():>+9.2f}%  {(r20>0).mean()*100:>8.0f}%  "
          f"{r60.mean():>+9.2f}%  {(r60>0).mean()*100:>8.0f}%")

# YUR change as signal (institutional re-positioning)
print("\n" + "=" * 100)
print("FORWARD RETURNS by 7d YUR NET CHANGE (smoothed institutional flow):")
print("=" * 100)
mask = merged['yur_net_chg_7d'].notna()
merged.loc[mask, 'yur_chg_q'] = pd.qcut(merged.loc[mask, 'yur_net_chg_7d'], 5,
                                          labels=['Q1_selling', 'Q2', 'Q3', 'Q4', 'Q5_buying'])
print(f"\n  {'YUR 7d flow':<14}  N    "
      f"{'mean 20d %':>10}  {'P(up) 20d':>10}  "
      f"{'mean 60d %':>10}  {'P(up) 60d':>10}")
for bucket in ['Q1_selling', 'Q2', 'Q3', 'Q4', 'Q5_buying']:
    sub = merged[merged['yur_chg_q'] == bucket]
    r20 = sub['fwd_20d'].dropna()
    r60 = sub['fwd_60d'].dropna()
    print(f"  {bucket:<14}  {len(sub):>3}  "
          f"{r20.mean():>+9.2f}%  {(r20>0).mean()*100:>8.0f}%  "
          f"{r60.mean():>+9.2f}%  {(r60>0).mean()*100:>8.0f}%")

# Save derived metrics
out = ROOT / 'sber_futoi_signals.csv'
df.to_csv(out)
print(f"\nDerived metrics saved to {out.name}")
