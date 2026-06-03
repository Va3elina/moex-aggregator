"""Trend-following deep analysis: user's strategy across timeframes + walk-forward.

Strategy principles (from user's Pine):
  - Entry 1 (33%): price crosses BELOW long EMA (200) — buy oversold
  - Entry 2 (+67%): EMA20 crosses above EMA5 — momentum confirmation
  - Exit: SMA(RSI, 14) crosses below 70 — momentum cooling

Tests:
  - 4 TF combinations: W/D, D/D, 4H/4H, 4H/1H
  - 2 assets: BTC, ETH
  - Walk-forward 4 periods: 2015-2017, 2018-2020, 2021-2023, 2024-2026
  - Per-period metrics + total
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
    out[n-1] = arr[:n].mean(); k = 2.0/(n+1.0)
    for i in range(n, len(arr)):
        out[i] = arr[i]*k + out[i-1]*(1-k)
    return out

def rsi_a(close, n=14):
    s = pd.Series(close)
    delta = s.diff(); gain = delta.clip(lower=0); loss = -delta.clip(upper=0)
    avg_g = gain.ewm(alpha=1/n, adjust=False).mean()
    avg_l = loss.ewm(alpha=1/n, adjust=False).mean()
    return (100 - 100/(1 + avg_g/avg_l)).values


def load_daily(asset):
    name = "btc_usd_daily_coinbase.csv" if asset == 'BTC' else "eth_usd_daily_coinbase.csv"
    df = pd.read_csv(ROOT/name, parse_dates=['date']).set_index('date').sort_index()
    return df[df['close']>0].dropna(subset=['close'])

def load_hourly(asset):
    name = "btc_usd_hourly_coinbase.csv" if asset == 'BTC' else "eth_usd_hourly_coinbase.csv"
    df = pd.read_csv(ROOT/name, parse_dates=['datetime']).set_index('datetime').sort_index()
    df.index = df.index.tz_localize(None)
    return df[df['close']>0].dropna(subset=['close'])

def resample_to(hourly, rule='4h'):
    return hourly.resample(rule).agg({'open':'first','high':'max','low':'min','close':'last','volume':'sum'}).dropna()


def backtest_original_logic(engine_df, logic_resample_rule,
                              ema_long=200, ema_mid=20, ema_short=5,
                              rsi_period=14, rsi_smooth=14, rsi_exit=70,
                              use_pyramid=True):
    """Replicate user's strategy without VIX. Logic computed on a higher TF, applied to engine TF.

    Args:
      engine_df: OHLCV at execution timeframe
      logic_resample_rule: 'W-MON', 'D', '4h', None (None = use engine TF)
    """
    closes = engine_df['close'].values
    dates = engine_df.index

    # Compute logic indicators at higher TF
    if logic_resample_rule is None or logic_resample_rule == 'engine':
        logic_close = engine_df['close']
    else:
        logic_close = engine_df['close'].resample(logic_resample_rule, label='left', closed='left').last().dropna()

    l_ema200 = pd.Series(ema(logic_close.values, ema_long), index=logic_close.index)
    l_ema20  = pd.Series(ema(logic_close.values, ema_mid),  index=logic_close.index)
    l_ema5   = pd.Series(ema(logic_close.values, ema_short), index=logic_close.index)
    l_rsi    = pd.Series(rsi_a(logic_close.values, rsi_period), index=logic_close.index)
    l_rsi_sma = l_rsi.rolling(rsi_smooth).mean()

    cross_under_ema200 = ((logic_close < l_ema200) & (logic_close.shift(1) >= l_ema200.shift(1)))
    cross_ema20_over_5 = ((l_ema20 > l_ema5) & (l_ema20.shift(1) <= l_ema5.shift(1)))
    exit_rsi = ((l_rsi_sma < rsi_exit) & (l_rsi_sma.shift(1) >= rsi_exit))

    # Forward-fill into engine TF (with 1-bar lag to avoid look-ahead)
    cross_under_d = cross_under_ema200.shift(1).reindex(dates, method='ffill').fillna(False).astype(bool)
    cross_ema_d   = cross_ema20_over_5.shift(1).reindex(dates, method='ffill').fillna(False).astype(bool)
    exit_rsi_d    = exit_rsi.shift(1).reindex(dates, method='ffill').fillna(False).astype(bool)

    # Detect first day signal becomes True (Pine fires once)
    sig = {}
    for name, s in [('xu', cross_under_d), ('xc', cross_ema_d), ('xe', exit_rsi_d)]:
        prev = s.shift(1).fillna(False)
        sig[name] = (s & (~prev)).values

    cash = INITIAL; qty = 0.0
    eq = np.zeros(len(closes)); trades = []
    entry_value = 0.0; entry_date = None
    entries_taken = 0
    intra_min = 0
    for i in range(len(closes)):
        c = closes[i]
        eq[i] = cash + qty*c
        if qty > 0:
            low = engine_df['low'].values[i]
            if intra_min == 0 or low < intra_min: intra_min = low

        # Exit
        if qty > 0 and sig['xe'][i]:
            eff = c * (1 - SLIP)
            proceeds = qty * eff * (1 - FEE)
            cash += proceeds
            pnl = (proceeds/entry_value - 1) * 100
            mae = (intra_min/(entry_value/qty) - 1) * 100 if qty > 0 else 0
            trades.append({'entry_date': entry_date, 'exit_date': dates[i],
                          'pnl_pct': pnl, 'mae_pct': mae,
                          'hold_d': (dates[i] - entry_date).total_seconds()/86400})
            qty = 0; entry_value = 0; entry_date = None
            entries_taken = 0; intra_min = 0
            continue

        # Entry 33% on EMA200 cross-under
        if qty == 0 and sig['xu'][i]:
            eff = c * (1 + SLIP)
            spend = cash * (0.33 if use_pyramid else 0.99)
            bq = spend/eff; cost = bq*eff*(1+FEE)
            cash -= cost; qty += bq
            entry_value = cost; entry_date = dates[i]
            entries_taken = 1
            intra_min = engine_df['low'].values[i]
            continue

        # Add 67% on EMA20>EMA5
        if use_pyramid and qty > 0 and entries_taken == 1 and sig['xc'][i]:
            eff = c * (1 + SLIP)
            spend = cash * 0.99
            bq = spend/eff; cost = bq*eff*(1+FEE)
            if cost <= cash:
                cash -= cost; qty += bq
                entry_value += cost
                entries_taken = 2

    if qty > 0:
        c = closes[-1]
        eff = c * (1 - SLIP)
        proceeds = qty * eff * (1 - FEE)
        cash += proceeds
        pnl = (proceeds/entry_value - 1) * 100
        mae = (intra_min/(entry_value/qty) - 1) * 100 if qty > 0 else 0
        trades.append({'entry_date': entry_date, 'exit_date': dates[-1],
                      'pnl_pct': pnl, 'mae_pct': mae,
                      'hold_d': (dates[-1] - entry_date).total_seconds()/86400})

    return pd.DataFrame(trades), cash, pd.Series(eq, index=dates)


def metrics_period(trades, eq, period_start, period_end):
    """Returns dict of metrics for the period."""
    eq_p = eq[(eq.index >= period_start) & (eq.index < period_end)]
    if len(eq_p) < 2:
        return {'ret': 0, 'dd': 0, 'N': 0, 'wr': 0, 'mae': 0}
    ret = (eq_p.iloc[-1]/eq_p.iloc[0] - 1) * 100
    dd = ((eq_p.cummax() - eq_p)/eq_p.cummax()).max() * 100
    tr_p = trades[(trades['exit_date'] >= period_start) & (trades['exit_date'] < period_end)] if len(trades) else pd.DataFrame()
    n = len(tr_p)
    wr = (tr_p['pnl_pct']>0).sum()/n*100 if n else 0
    mae = tr_p['mae_pct'].min() if n else 0
    return {'ret': ret, 'dd': dd, 'N': n, 'wr': wr, 'mae': mae}


def report_tf_combo(asset, tf_label, df, logic_rule, periods):
    print(f"\n  {asset} — {tf_label}:")
    tr, fe, eq = backtest_original_logic(df, logic_rule)
    total = (fe/INITIAL - 1) * 100
    total_dd = ((eq.cummax() - eq)/eq.cummax()).max() * 100
    wr = (tr['pnl_pct']>0).sum()/max(len(tr),1)*100
    print(f"    ОВЕРОЛЛ: N={len(tr)}  WR={wr:.0f}%  total={total:>+8.0f}%  DD={total_dd:>4.1f}%")
    print(f"    {'Период':>12}  {'ret':>9}  {'DD':>5}  {'N':>3}  {'WR':>4}  {'MAE':>6}")
    for label, ps, pe in periods:
        m = metrics_period(tr, eq, ps, pe)
        print(f"    {label:>12}  {m['ret']:>+8.0f}%  {m['dd']:>4.1f}%  {m['N']:>3}  {m['wr']:>3.0f}%  {m['mae']:>+5.1f}%")


def main():
    btc_daily = load_daily('BTC')
    eth_daily = load_daily('ETH')
    btc_hourly = load_hourly('BTC')
    eth_hourly = load_hourly('ETH')
    btc_4h = resample_to(btc_hourly, '4h')
    eth_4h = resample_to(eth_hourly, '4h')

    # 4 walk-forward periods (independent)
    periods = [
        ('2015-2017', pd.Timestamp(2015,1,1), pd.Timestamp(2018,1,1)),
        ('2018-2020', pd.Timestamp(2018,1,1), pd.Timestamp(2021,1,1)),
        ('2021-2023', pd.Timestamp(2021,1,1), pd.Timestamp(2024,1,1)),
        ('2024-2026', pd.Timestamp(2024,1,1), pd.Timestamp(2027,1,1)),
    ]

    # TF combinations to test
    tf_combos = [
        ('W logic / D engine', 'daily', 'W-MON'),  # original
        ('D logic / D engine', 'daily', 'engine'),
        ('D logic / 4H engine', '4h', 'D'),
        ('4H logic / 4H engine', '4h', 'engine'),
        ('4H logic / 1H engine', 'hourly', '4h'),
        ('W logic / 4H engine', '4h', 'W-MON'),
        ('D logic / 1H engine', 'hourly', 'D'),
    ]

    for asset, dailies, h4s, hourlys in [
        ('BTC', btc_daily, btc_4h, btc_hourly),
        ('ETH', eth_daily, eth_4h, eth_hourly),
    ]:
        print(f"\n{'=' * 100}")
        print(f"{asset} — TF/engine combinations with walk-forward")
        print(f"{'=' * 100}")

        for label, engine_tf, logic_rule in tf_combos:
            df = {'daily': dailies, '4h': h4s, 'hourly': hourlys}[engine_tf]
            report_tf_combo(asset, label, df, logic_rule, periods)

    # === EMA period sensitivity (on W logic / D engine, BTC) ===
    print(f"\n\n{'=' * 100}")
    print("BTC — EMA period sensitivity (W logic / D engine)")
    print(f"{'=' * 100}")
    print(f"  {'EMA config (long/mid/short)':30}  {'total':>9}  {'DD':>5}  {'N':>3}  {'WR':>4}  {'2024-26':>9}")
    for el, em, es in [(200, 20, 5), (100, 20, 5), (50, 20, 5),
                        (200, 50, 10), (100, 30, 7), (50, 10, 3)]:
        tr, fe, eq = backtest_original_logic(btc_daily, 'W-MON',
                                              ema_long=el, ema_mid=em, ema_short=es)
        total = (fe/INITIAL - 1)*100
        dd = ((eq.cummax() - eq)/eq.cummax()).max()*100
        wr = (tr['pnl_pct']>0).sum()/max(len(tr),1)*100
        m26 = metrics_period(tr, eq, pd.Timestamp(2024,1,1), pd.Timestamp(2027,1,1))
        print(f"  {f'{el}/{em}/{es}':30}  {total:>+8.0f}%  {dd:>4.0f}%  {len(tr):>3}  {wr:>3.0f}%  {m26['ret']:>+8.0f}%")

    # === RSI exit threshold sensitivity ===
    print(f"\n\n{'=' * 100}")
    print("BTC — RSI exit threshold sensitivity (W logic / D engine, EMA200/20/5)")
    print(f"{'=' * 100}")
    print(f"  {'RSI exit cross under':30}  {'total':>9}  {'DD':>5}  {'N':>3}  {'WR':>4}")
    for rsi_x in [50, 60, 65, 70, 75, 80]:
        tr, fe, eq = backtest_original_logic(btc_daily, 'W-MON', rsi_exit=rsi_x)
        total = (fe/INITIAL - 1)*100
        dd = ((eq.cummax() - eq)/eq.cummax()).max()*100
        wr = (tr['pnl_pct']>0).sum()/max(len(tr),1)*100
        print(f"  {f'crossunder({rsi_x})':30}  {total:>+8.0f}%  {dd:>4.0f}%  {len(tr):>3}  {wr:>3.0f}%")

    # === Pyramiding vs all-in ===
    print(f"\n\n{'=' * 100}")
    print("BTC — Pyramiding 33/67 vs all-in 100% (W logic / D engine)")
    print(f"{'=' * 100}")
    print(f"  {'mode':30}  {'total':>9}  {'DD':>5}  {'N':>3}  {'WR':>4}  {'MAE':>6}")
    for label, pyr in [('Pyramid 33/67', True), ('All-in 100% on first cross', False)]:
        tr, fe, eq = backtest_original_logic(btc_daily, 'W-MON', use_pyramid=pyr)
        total = (fe/INITIAL - 1)*100
        dd = ((eq.cummax() - eq)/eq.cummax()).max()*100
        wr = (tr['pnl_pct']>0).sum()/max(len(tr),1)*100
        mae = tr['mae_pct'].min() if len(tr) else 0
        print(f"  {label:30}  {total:>+8.0f}%  {dd:>4.0f}%  {len(tr):>3}  {wr:>3.0f}%  {mae:>+5.1f}%")


if __name__ == "__main__":
    main()
