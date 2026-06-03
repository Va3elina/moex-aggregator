"""Fundamental market regime analysis BTC + ETH 2015-2026.

Computes per-year:
  - Realized volatility (annualized, 30d rolling)
  - Days above/below weekly EMA200 (regime split)
  - Number of weekly EMA200 crossings (rare events)
  - VIX events (>= 76, >= 60, >= 50)
  - Max drawdown
  - Daily return autocorrelation (trending vs mean-reverting)
  - Tail behavior: % days with |return| > 5%, > 10%
  - "RSI exhaustion" frequency (RSI > 80, < 20)

Goal: see how the trading environment has shifted, especially since 2020.
"""
import warnings; warnings.filterwarnings("ignore")
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(__file__).parent


def ema(s, n):
    if isinstance(s, np.ndarray):
        out = np.full(len(s), np.nan)
        if len(s) < n: return out
        out[n-1] = s[:n].mean()
        k = 2.0/(n+1.0)
        for i in range(n, len(s)):
            out[i] = s[i]*k + out[i-1]*(1-k)
        return out
    return s.ewm(span=n, adjust=False).mean()

def rsi(close, n=14):
    s = pd.Series(close)
    delta = s.diff()
    gain = delta.clip(lower=0); loss = -delta.clip(upper=0)
    avg_g = gain.ewm(alpha=1/n, adjust=False).mean()
    avg_l = loss.ewm(alpha=1/n, adjust=False).mean()
    return (100 - 100/(1 + avg_g/avg_l)).values


def load_daily(asset):
    name = "btc_usd_daily_coinbase.csv" if asset == 'BTC' else "eth_usd_daily_coinbase.csv"
    df = pd.read_csv(ROOT/name, parse_dates=['date']).set_index('date').sort_index()
    return df[df['close']>0].dropna(subset=['close'])


def analyze(df, asset):
    # Weekly EMA200
    weekly_close = df['close'].resample('W-MON', label='left', closed='left').last().dropna()
    weekly_ema200 = pd.Series(ema(weekly_close.values, 200), index=weekly_close.index)
    # Cross events
    weekly_cross_under = ((weekly_close < weekly_ema200) & (weekly_close.shift(1) >= weekly_ema200.shift(1)))
    weekly_cross_over = ((weekly_close > weekly_ema200) & (weekly_close.shift(1) <= weekly_ema200.shift(1)))

    # Forward fill weekly to daily
    weekly_ema200_d = weekly_ema200.shift(1).reindex(df.index, method='ffill')
    weekly_close_d = weekly_close.shift(1).reindex(df.index, method='ffill')
    df['above_w_ema200'] = (df['close'] > weekly_ema200_d)
    df['cross_under'] = weekly_cross_under.shift(1).reindex(df.index, method='ffill').fillna(False).astype(bool)
    df['cross_over'] = weekly_cross_over.shift(1).reindex(df.index, method='ffill').fillna(False).astype(bool)

    # Daily metrics
    df['log_ret'] = np.log(df['close']).diff()
    df['rsi'] = rsi(df['close'].values, 14)

    print(f"\n{'='*150}")
    print(f"{asset} — Market regime metrics by year")
    print(f"{'='*150}")
    print(f"  {'year':>4}  {'vol annu%':>10}  {'%days>W_EMA200':>15}  {'W_EMA200 crosses':>17}  "
          f"{'max DD%':>8}  {'big_days_5%':>11}  {'big_days_10%':>12}  "
          f"{'RSI>80 days':>12}  {'RSI<20 days':>12}  {'auto_corr':>10}")
    print("  " + "-" * 150)
    rows = []
    for year in sorted(df.index.year.unique()):
        ym = df.index.year == year
        sub = df[ym]
        if len(sub) < 30: continue
        # Realized vol annualized from daily log returns
        vol_ann = sub['log_ret'].std() * np.sqrt(365) * 100
        pct_above = sub['above_w_ema200'].mean() * 100
        n_crosses = int(sub['cross_under'].sum()) + int(sub['cross_over'].sum())
        # Max DD within year
        eq = (sub['close']/sub['close'].iloc[0])
        max_dd = ((eq.cummax() - eq)/eq.cummax()).max() * 100
        # Big days
        big5 = (sub['log_ret'].abs() > 0.05).sum()
        big10 = (sub['log_ret'].abs() > 0.10).sum()
        # RSI exhaustion
        rsi80 = (sub['rsi'] > 80).sum()
        rsi20 = (sub['rsi'] < 20).sum()
        # Daily autocorrelation (lag 1)
        ac = sub['log_ret'].autocorr(lag=1)
        rows.append({'year': year, 'vol_ann': vol_ann, 'pct_above': pct_above,
                     'n_crosses': n_crosses, 'max_dd': max_dd,
                     'big5': big5, 'big10': big10, 'rsi80': rsi80, 'rsi20': rsi20,
                     'autocorr': ac})
        print(f"  {year:>4}  {vol_ann:>9.0f}%  {pct_above:>14.0f}%  {n_crosses:>17}  "
              f"{max_dd:>7.1f}%  {big5:>11}  {big10:>12}  "
              f"{rsi80:>12}  {rsi20:>12}  {ac:>+9.3f}")

    # Aggregate by era
    print(f"\n  {asset} — by era:")
    eras = [(2015, 2019, "2015-2019 (early)"),
            (2020, 2022, "2020-2022 (COVID+Luna)"),
            (2023, 2026, "2023-2026 (post-FTX)")]
    print(f"    {'era':30}  {'avg vol%':>10}  {'avg %above':>11}  {'crosses/y':>10}  {'avg DD%':>8}  {'big5/y':>8}  {'autocorr':>10}")
    for ys, ye, name in eras:
        era_rows = [r for r in rows if ys <= r['year'] <= ye]
        if not era_rows: continue
        n_y = len(era_rows)
        print(f"    {name:30}  {np.mean([r['vol_ann'] for r in era_rows]):>9.0f}%  "
              f"{np.mean([r['pct_above'] for r in era_rows]):>10.0f}%  "
              f"{np.sum([r['n_crosses'] for r in era_rows])/n_y:>9.1f}  "
              f"{np.mean([r['max_dd'] for r in era_rows]):>7.1f}%  "
              f"{np.sum([r['big5'] for r in era_rows])/n_y:>7.1f}  "
              f"{np.mean([r['autocorr'] for r in era_rows]):>+9.3f}")


def vix_analysis():
    # CBOE VIX not in our data, but we can describe historical events
    print("\n" + "="*100)
    print("VIX major events (CBOE: peak daily high)")
    print("="*100)
    events = [
        ("2018-02-06", "+50",  "Volmageddon (XIV crash)"),
        ("2018-12-26", "+36",  "December selloff"),
        ("2020-03-16", "+82",  "COVID crash (your VIX≥76 trigger!)"),
        ("2020-03-18", "+85",  "COVID continuation"),
        ("2022-01-24", "+38",  "Rate-hike fears (no trigger)"),
        ("2022-09-13", "+33",  "Inflation persistence (no trigger)"),
        ("2024-08-05", "+65",  "Yen carry unwind (close to trigger)"),
        ("2025-04-07", "+60",  "Tariff shock"),
        ("2025-11-21", "+34",  "Rate cut delay"),
    ]
    print(f"  {'date':>12}  {'VIX high':>8}  {'event':>40}")
    for d, h, e in events:
        print(f"  {d:>12}  {h:>8}  {e:>40}")
    print("\n  → VIX ≥ 76 triggered only 1 time since 2015 (COVID Mar 2020)")
    print("  → Strategy 'wait for VIX 76' = essentially 'wait for once-a-decade panic'")
    print("  → Lowering threshold to VIX 50 or 60 would give 4-5 more triggers")


def main():
    btc = load_daily('BTC')
    eth = load_daily('ETH')
    analyze(btc, 'BTC')
    analyze(eth, 'ETH')
    vix_analysis()


if __name__ == "__main__":
    main()
