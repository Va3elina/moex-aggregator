"""BTC 1H volume-anomaly tier strategy with daily trend filter and ATR trailing stop.

Entry conditions on 1H:
  - volume_ratio = vol / SMA(vol, 20) hits a tier
  - candle_strength > 0.30 (|close-open|/(high-low))
  - close > open (green bullish bar)
  - DAILY trend filter: daily close > daily EMA200 (forward-filled, day-shifted to avoid look-ahead)
  - Optional: daily RSI > 50, etc.

Position sizing per tier (from hackathon):
  - Tier 1 (vol >= 1.5x): 100% of equity
  - Tier 2 (vol >= 3.25x): 75%
  - Tier 3 (vol >= 6x): 50%

Exit:
  - Initial stop: -1 ATR(25) from entry
  - Take profit: +N ATR by tier (T1: 2.5, T2: 8, T3: 9)
  - Trailing stop: after price moves +0.5 ATR favorable, trail at high - 0.5 ATR
  - Stop hit OR TP hit OR trailing stop hit → exit

Test multiple variations to find best params.
"""
import warnings; warnings.filterwarnings("ignore")
from pathlib import Path
import numpy as np
import pandas as pd

H1_PATH = Path(__file__).parent.parent / "Bitcoin" / "btc_usd_hourly_coinbase.csv"
D1_PATH = Path(__file__).parent.parent / "Bitcoin" / "btc_usd_daily_coinbase.csv"
INITIAL = 100.0; FEE = 0.0010; SLIPPAGE = 0.0005


def wilder_ma(s, n):
    out = np.full(len(s), np.nan)
    if len(s) < n: return pd.Series(out, index=s.index)
    v = s.values; out[n-1] = np.nanmean(v[:n])
    for i in range(n, len(s)): out[i] = (out[i-1]*(n-1) + v[i]) / n
    return pd.Series(out, index=s.index)


def hourly_features(bars):
    d = bars.copy(); c,h,l,o,v = d['close'],d['high'],d['low'],d['open'],d['volume']
    rng = (h - l).replace(0, np.nan)
    d['body_pct'] = (c - o).abs() / rng
    d['lower_wick'] = (pd.concat([c, o], axis=1).min(axis=1) - l) / rng
    d['is_bull'] = c > o
    pc = c.shift(1); tr = pd.concat([h-l, (h-pc).abs(), (l-pc).abs()], axis=1).max(axis=1)
    d['atr_25'] = wilder_ma(tr, 25)
    # Volume
    d['vol_ma_20'] = v.rolling(20).mean()
    d['vol_ratio'] = v / d['vol_ma_20']
    d['vol_tier'] = 0
    d.loc[d['vol_ratio'] >= 1.5, 'vol_tier'] = 1
    d.loc[d['vol_ratio'] >= 3.25, 'vol_tier'] = 2
    d.loc[d['vol_ratio'] >= 6.0, 'vol_tier'] = 3
    return d


def daily_features(bars):
    d = bars.copy(); c = d['close']
    d['ema_50'] = c.ewm(span=50, adjust=False).mean()
    d['ema_200'] = c.ewm(span=200, adjust=False).mean()
    d['trend_up_daily'] = c > d['ema_200']
    d['trend_strong_daily'] = (d['ema_50'] > d['ema_200']) & d['trend_up_daily']
    delta = c.diff()
    gain = delta.clip(lower=0); loss = -delta.clip(upper=0)
    avg_g = wilder_ma(gain, 14); avg_l = wilder_ma(loss, 14)
    d['rsi_14_d'] = 100 - 100/(1 + avg_g/avg_l)
    return d[['ema_50','ema_200','trend_up_daily','trend_strong_daily','rsi_14_d']]


def join_daily_to_hourly(hourly, daily):
    """Shift daily by 1 day, then ffill — avoid look-ahead."""
    daily_shifted = daily.copy()
    daily_shifted.index = daily_shifted.index + pd.Timedelta(days=1)
    return hourly.join(daily_shifted.reindex(hourly.index, method='ffill'))


def load_hourly():
    df = pd.read_csv(H1_PATH, parse_dates=['datetime']).set_index('datetime').sort_index()
    df.index = df.index.tz_localize(None)
    for c in ['open','high','low','close','volume']: df[c] = pd.to_numeric(df[c], errors='coerce')
    return df


def load_daily():
    df = pd.read_csv(D1_PATH, parse_dates=['date']).set_index('date').sort_index()
    for c in ['open','high','low','close','volume']: df[c] = pd.to_numeric(df[c], errors='coerce')
    return df


def backtest_tier(df, min_tier=1, max_tier=3, candle_min=0.30, require_bull=True,
                    trend_filter='strong_daily',  # 'none' | 'up_daily' | 'strong_daily'
                    stop_atr=1.0, tp_atr_per_tier={1: 2.5, 2: 8.0, 3: 9.0},
                    trail_atr=0.5, trail_activate_atr=1.0,
                    size_per_tier={1: 1.00, 2: 0.75, 3: 0.50}):
    closes = df['close'].values; highs = df['high'].values; lows = df['low'].values
    opens = df['open'].values
    atr = df['atr_25'].values
    vol_tier = df['vol_tier'].values
    candle = df['body_pct'].values
    is_bull = df['is_bull'].values
    trend_up = df['trend_up_daily'].values if 'trend_up_daily' in df else None
    trend_strong = df['trend_strong_daily'].values if 'trend_strong_daily' in df else None
    times = df.index

    cash, pos, ep, e_atr, e_tier, e_max, e_idx = INITIAL, 0.0, 0, 0, 0, 0, 0
    stop = 0; tp = 0; trail_active = False
    trades = []
    for i in range(len(df)):
        c, h, l = closes[i], highs[i], lows[i]
        if pos > 0:
            e_max = max(e_max, h)
            # Trailing stop activation: when price moves > trail_activate_atr ATR above entry
            if not trail_active and (e_max - ep) >= trail_activate_atr * e_atr:
                trail_active = True
            if trail_active:
                stop = max(stop, e_max - trail_atr * e_atr)

            exit_p, reason = None, None
            # Stop loss first (gap-down or intrabar)
            if opens[i] <= stop:
                exit_p = opens[i]; reason = 'stop_gap'
            elif opens[i] >= tp:
                exit_p = opens[i]; reason = 'tp_gap'
            elif l <= stop:
                exit_p = stop; reason = 'stop'
            elif h >= tp:
                exit_p = tp; reason = 'tp'

            if exit_p is not None:
                eff = exit_p * (1 - SLIPPAGE)
                cash += pos * eff * (1 - FEE)
                trades.append({'entry_t': times[e_idx], 'exit_t': times[i],
                                'tier': e_tier, 'entry_p': ep, 'exit_p': eff,
                                'pnl_pct': (eff/ep - 1) * 100, 'hold': i - e_idx,
                                'reason': reason})
                pos = 0; trail_active = False; e_max = 0

        if pos == 0:
            # Check entry conditions
            tier = int(vol_tier[i]) if not pd.isna(vol_tier[i]) else 0
            if tier < min_tier or tier > max_tier: continue
            if pd.isna(candle[i]) or candle[i] < candle_min: continue
            if require_bull and not is_bull[i]: continue
            if pd.isna(atr[i]): continue
            # Trend filter
            if trend_filter == 'up_daily' and trend_up is not None:
                if pd.isna(trend_up[i]) or not trend_up[i]: continue
            elif trend_filter == 'strong_daily' and trend_strong is not None:
                if pd.isna(trend_strong[i]) or not trend_strong[i]: continue

            eff = c * (1 + SLIPPAGE)
            size_frac = size_per_tier.get(tier, 1.0)
            qty = (cash * size_frac) / eff
            cost = qty * eff * (1 + FEE)
            if cost <= cash:
                cash -= cost; pos = qty; ep = eff; e_atr = atr[i]; e_tier = tier
                e_idx = i; e_max = c
                stop = ep - stop_atr * e_atr
                tp = ep + tp_atr_per_tier.get(tier, 2.5) * e_atr
                trail_active = False

    final_eq = cash + (pos * closes[-1] if pos > 0 else 0)
    return pd.DataFrame(trades), final_eq


def main():
    print("Loading BTC hourly + daily...")
    h_bars = load_hourly()
    d_bars = load_daily()
    print(f"  Hourly: {len(h_bars)} bars  {h_bars.index[0]} → {h_bars.index[-1]}")
    print(f"  Daily : {len(d_bars)} bars  {d_bars.index[0].date()} → {d_bars.index[-1].date()}")

    h_f = hourly_features(h_bars)
    d_f = daily_features(d_bars)
    df = join_daily_to_hourly(h_f, d_f)
    df = df.dropna(subset=['atr_25', 'vol_tier'])
    print(f"  Merged: {len(df)} bars\n")

    # Test 1: No trend filter (baseline — like hackathon original)
    print("=" * 100)
    print("TEST 1: TREND FILTER comparison (all tier 1-3, hold via TP+stop+trail)")
    print("=" * 100)
    print(f"  {'trend filter':22}  {'trades':>6}  {'WR%':>5}  {'PF':>5}  {'avg%':>6}  {'final$':>9}  {'max loss':>8}")
    print("  " + "-" * 90)
    for tf in ['none', 'up_daily', 'strong_daily']:
        tr, eq = backtest_tier(df, trend_filter=tf)
        if len(tr) == 0: continue
        wr = (tr['pnl_pct']>0).sum() / len(tr) * 100
        pos_pnl = tr[tr['pnl_pct']>0]['pnl_pct'].sum()
        neg_pnl = abs(tr[tr['pnl_pct']<0]['pnl_pct'].sum())
        pf = pos_pnl/neg_pnl if neg_pnl>0 else float('inf')
        print(f"  {tf:22}  {len(tr):>6}  {wr:>4.1f}%  {pf:>4.2f}  "
              f"{tr['pnl_pct'].mean():>+5.2f}%  ${eq:>7.2f}  {tr['pnl_pct'].min():>+7.2f}%")

    # Test 2: tier breakdown (with strong_daily filter)
    print("\n" + "=" * 100)
    print("TEST 2: TIER breakdown (strong_daily filter)")
    print("=" * 100)
    print(f"  {'tier':>6}  {'trades':>6}  {'WR%':>5}  {'avg%':>6}  {'reasons':>30}")
    tr_all, _ = backtest_tier(df, trend_filter='strong_daily')
    for t in [1, 2, 3]:
        sub = tr_all[tr_all['tier'] == t]
        if len(sub) == 0:
            print(f"  Tier {t}: no trades"); continue
        wr = (sub['pnl_pct']>0).sum() / len(sub) * 100
        reasons = dict(sub['reason'].value_counts())
        print(f"  Tier {t}  {len(sub):>6}  {wr:>4.1f}%  {sub['pnl_pct'].mean():>+5.2f}%  {reasons}")

    # Test 3: stop/TP ATR variations
    print("\n" + "=" * 100)
    print("TEST 3: stop/TP variations (strong_daily filter, all tiers)")
    print("=" * 100)
    print(f"  {'config':40}  {'trades':>6}  {'WR%':>5}  {'PF':>5}  {'avg%':>6}  {'final$':>9}")
    configs = [
        ('stop1 tp2.5/8/9 trail0.5',  1.0, {1: 2.5, 2: 8.0, 3: 9.0}, 0.5, 1.0),
        ('stop1 tp3/5/7 trail0.5',    1.0, {1: 3.0, 2: 5.0, 3: 7.0}, 0.5, 1.0),
        ('stop1 tp5/10/15 trail0.5',  1.0, {1: 5.0, 2: 10.0, 3: 15.0}, 0.5, 1.0),
        ('stop1.5 tp2.5/8/9 trail0.5', 1.5, {1: 2.5, 2: 8.0, 3: 9.0}, 0.5, 1.0),
        ('stop2 tp2.5/8/9 trail0.5',  2.0, {1: 2.5, 2: 8.0, 3: 9.0}, 0.5, 1.0),
        ('stop1 tp big trail1.0',     1.0, {1: 10.0, 2: 20.0, 3: 30.0}, 1.0, 1.5),
        ('stop1 trail very tight',    1.0, {1: 100, 2: 100, 3: 100}, 0.3, 0.5),
    ]
    for label, stop_atr, tp_atrs, trail, activate in configs:
        tr, eq = backtest_tier(df, trend_filter='strong_daily',
                                  stop_atr=stop_atr, tp_atr_per_tier=tp_atrs,
                                  trail_atr=trail, trail_activate_atr=activate)
        if len(tr) == 0: continue
        wr = (tr['pnl_pct']>0).sum() / len(tr) * 100
        pos_pnl = tr[tr['pnl_pct']>0]['pnl_pct'].sum()
        neg_pnl = abs(tr[tr['pnl_pct']<0]['pnl_pct'].sum())
        pf = pos_pnl/neg_pnl if neg_pnl>0 else float('inf')
        print(f"  {label:40}  {len(tr):>6}  {wr:>4.1f}%  {pf:>4.2f}  "
              f"{tr['pnl_pct'].mean():>+5.2f}%  ${eq:>7.2f}")


if __name__ == "__main__":
    main()
