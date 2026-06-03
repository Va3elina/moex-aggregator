"""Analyze GAZP futoi for smart money signals + build gazp_futoi_signals.csv.

Mirror of analyze_sber_futoi.py. YUR/FIZ are zero-sum on FORTS clearing.
Key test: does FIZ-LSR contrarian (retail panic at low LSR = LONG) work on GAZP
the same way it does on SBER (FIZ<=1.4 = panic)?
"""
import pandas as pd
import numpy as np
from pathlib import Path

ROOT = Path(__file__).parent
futoi = pd.read_csv(ROOT/'gazp_futoi_history.csv', parse_dates=['date']).set_index('date').sort_index()
gazp = pd.read_csv(ROOT/'gazp_daily.csv', parse_dates=['date']).set_index('date').sort_index()

print(f"futoi: {futoi.index[0].date()} -> {futoi.index[-1].date()}, {len(futoi)} rows")
print(f"GAZP:  {gazp.index[0].date()} -> {gazp.index[-1].date()}, {len(gazp)} rows\n")

df = futoi.copy()
df['yur_total'] = df['yur_long'] + df['yur_short']
df['fiz_total'] = df['fiz_long'] + df['fiz_short']
df['yur_lsr'] = df['yur_long'] / df['yur_short'].replace(0, 1)
df['fiz_lsr'] = df['fiz_long'] / df['fiz_short'].replace(0, 1)
df['yur_net_chg'] = df['yur_net'].diff()
df['yur_net_chg_pct'] = df['yur_net'].diff() / df['yur_total'].shift(1) * 100
df['yur_net_chg_7d'] = df['yur_net_chg'].rolling(7).mean()

print("FIZ (retail) Long/Short Ratio percentiles:")
for q in [0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95]:
    print(f"  p{int(q*100)}: {df['fiz_lsr'].quantile(q):.3f}")
print("\nYUR (institutional) Long/Short Ratio percentiles:")
for q in [0.05, 0.1, 0.5, 0.9, 0.95]:
    print(f"  p{int(q*100)}: {df['yur_lsr'].quantile(q):.3f}")

g_close = gazp[['close']].rename(columns={'close': 'gazp_close'})
merged = df.join(g_close, how='inner')
for h in [5, 10, 20, 60]:
    merged[f'fwd_{h}d'] = merged['gazp_close'].pct_change(h).shift(-h) * 100

print("\n" + "=" * 90)
print("FORWARD RETURNS by FIZ LSR quintile (retail - CONTRARIAN expected):")
print("=" * 90)
merged['fiz_lsr_q'] = pd.qcut(merged['fiz_lsr'], 5, labels=['Q1_short', 'Q2', 'Q3', 'Q4', 'Q5_long'])
print(f"  {'FIZ LSR bucket':<14}  N    {'20d %':>10}  {'P(up)20':>8}  {'60d %':>10}  {'P(up)60':>8}")
for bucket in ['Q1_short', 'Q2', 'Q3', 'Q4', 'Q5_long']:
    sub = merged[merged['fiz_lsr_q'] == bucket]
    r20 = sub['fwd_20d'].dropna(); r60 = sub['fwd_60d'].dropna()
    print(f"  {bucket:<14}  {len(sub):>3}  {r20.mean():>+9.2f}%  {(r20>0).mean()*100:>6.0f}%  "
          f"{r60.mean():>+9.2f}%  {(r60>0).mean()*100:>6.0f}%")

print("\n" + "=" * 90)
print("FORWARD RETURNS by YUR LSR quintile (institutional):")
print("=" * 90)
merged['yur_lsr_q'] = pd.qcut(merged['yur_lsr'], 5, labels=['Q1_short', 'Q2', 'Q3', 'Q4', 'Q5_long'])
print(f"  {'YUR LSR bucket':<14}  N    {'20d %':>10}  {'P(up)20':>8}  {'60d %':>10}  {'P(up)60':>8}")
for bucket in ['Q1_short', 'Q2', 'Q3', 'Q4', 'Q5_long']:
    sub = merged[merged['yur_lsr_q'] == bucket]
    r20 = sub['fwd_20d'].dropna(); r60 = sub['fwd_60d'].dropna()
    print(f"  {bucket:<14}  {len(sub):>3}  {r20.mean():>+9.2f}%  {(r20>0).mean()*100:>6.0f}%  "
          f"{r60.mean():>+9.2f}%  {(r60>0).mean()*100:>6.0f}%")

out = ROOT / 'gazp_futoi_signals.csv'
df.to_csv(out)
print(f"\nDerived metrics saved to {out.name}")
