"""Multi-factor strategy: trend + seasonality + volume + liquidation proxy + flow analysis.

Factors:
  F1 — Trend: wavelet s5/s6 slope direction
  F2 — Seasonality: position in halving cycle (months since last halving)
  F3 — Volume regime: z-score of 30-day rolling volume
  F4 — Flow imbalance: (FlowOutEx - FlowInEx) from CoinMetrics, smoothed
  F5 — Liquidation proxy: detect liquidation cascades from hourly volume spikes
  F6 — Volatility regime: 30-day realized vol z-score

Strategy: composite score from all factors. Long when score > threshold.
"""
import warnings; warnings.filterwarnings("ignore")
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(__file__).parent
FEE, SLIP, INITIAL = 0.0005, 0.0003, 100.0


def atrous_haar(x, J=6):
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


def load_btc():
    df = pd.read_csv(ROOT/'btc_usd_daily_coinbase.csv', parse_dates=['date']).set_index('date').sort_index()
    return df[df['close']>0].dropna(subset=['close'])

def load_btc_hourly():
    df = pd.read_csv(ROOT/'btc_usd_hourly_coinbase.csv', parse_dates=['datetime']).set_index('datetime').sort_index()
    df.index = df.index.tz_localize(None)
    return df[df['close']>0].dropna(subset=['close'])

def load_coinmetrics():
    df = pd.read_csv(ROOT/'btc_daily_coinmetrics.csv', parse_dates=['date']).set_index('date').sort_index()
    return df


# ============ FACTOR COMPUTATION ============

def compute_F1_trend(daily):
    """Trend factor: slope of s5 and s6 wavelet. Returns score in [-1, 1]."""
    log_c = np.log(daily['close'].values)
    sm = atrous_haar(log_c, J=6)
    sl5 = slope_a(sm[5], 5)
    sl6 = slope_a(sm[6], 5)
    f1 = np.zeros(len(daily))
    f1[(sl5 > 0) & (sl6 > 0)] = 1.0
    f1[(sl5 < 0) & (sl6 < 0)] = -1.0
    f1[(sl5 > 0) & (sl6 < 0)] = 0.0  # mixed
    f1[(sl5 < 0) & (sl6 > 0)] = 0.0
    return pd.Series(f1, index=daily.index, name='F1_trend')


def compute_F2_seasonality(daily):
    """Seasonality factor based on halving cycle position.
    Historical halvings: 2012-11-28, 2016-07-09, 2020-05-11, 2024-04-19.
    Best months post-halving: 6-18 months (peak rally typically there).
    """
    halvings = [pd.Timestamp(2012, 11, 28), pd.Timestamp(2016, 7, 9),
                 pd.Timestamp(2020, 5, 11), pd.Timestamp(2024, 4, 19)]
    f2 = np.zeros(len(daily))
    for i, d in enumerate(daily.index):
        # Find most recent halving before this date
        prev_halvings = [h for h in halvings if h <= d]
        if not prev_halvings:
            f2[i] = 0; continue
        last_h = max(prev_halvings)
        months_since = (d - last_h).days / 30
        # Bullish zone: 6-18 months after halving (peak phase)
        # Bearish zone: 30-42 months (deep bear before next halving)
        if 6 <= months_since <= 18:
            f2[i] = 1.0
        elif 18 < months_since <= 24:
            f2[i] = 0.5
        elif 24 < months_since <= 30:
            f2[i] = 0.0
        elif 30 < months_since <= 42:
            f2[i] = -0.5
        else:
            f2[i] = 0.3  # neutral-positive otherwise
    return pd.Series(f2, index=daily.index, name='F2_seasonality')


def compute_F3_volume(daily):
    """Volume z-score over 30-day rolling window."""
    vol = daily['volume']
    vol_ma = vol.rolling(30).mean()
    vol_std = vol.rolling(30).std()
    z = (vol - vol_ma) / vol_std
    z = z.clip(-3, 3) / 3  # normalize to [-1, 1]
    return z.fillna(0).rename('F3_volume')


def compute_F4_flow(daily, cm):
    """Net exchange flow: FlowOut - FlowIn (positive = accumulation)."""
    # Align CoinMetrics to daily index
    cm_aligned = cm.reindex(daily.index, method='ffill')
    net_flow = (cm_aligned['FlowOutExUSD'] - cm_aligned['FlowInExUSD'])
    # Smooth and normalize
    nf_ma = net_flow.rolling(30).mean()
    nf_std = net_flow.rolling(30).std()
    z = ((net_flow - nf_ma) / nf_std).clip(-3, 3) / 3
    return z.fillna(0).rename('F4_flow')


def compute_F5_liquidation_proxy(daily, hourly):
    """Detect liquidation cascades from hourly data.
    Event: hour where (volume > 3× 24h avg) AND (price drop > 3%).
    Score = -1 if many cascades in last 7 days (washed out → bottom)
            +1 if many cascades in last 1 day (continuing downtrend → bearish)
            normalize by count.
    """
    # Compute hourly volume z-score and return
    hourly = hourly.copy()
    hourly['vol_ma_24h'] = hourly['volume'].rolling(24).mean()
    hourly['vol_ratio'] = hourly['volume'] / hourly['vol_ma_24h']
    hourly['ret_1h'] = hourly['close'].pct_change()
    # Cascade: vol > 3× avg AND price drop > 3%
    cascades = (hourly['vol_ratio'] > 3) & (hourly['ret_1h'] < -0.03)
    # Count cascades per day
    cascades_per_day = cascades.resample('D').sum()
    cascades_per_day.index = pd.to_datetime(cascades_per_day.index)
    # Aligned to daily
    cascades_aligned = cascades_per_day.reindex(daily.index, method='ffill').fillna(0)
    # Count in last 7 days vs last 1 day
    last_7d = cascades_aligned.rolling(7).sum()
    # Many cascades in last week → washed out → bullish (negative score → positive after sign flip)
    # Score: -last_7d / 5 → more cascades → more positive score (BTC oversold)
    score = (last_7d / 5).clip(0, 1)
    return score.rename('F5_liquidation')


def compute_F6_volatility(daily):
    """Volatility regime: 30-day realized vol z-score.
    Low vol → bullish (calm market → accumulation).
    High vol → bearish (panic → wait).
    """
    ret = np.log(daily['close'] / daily['close'].shift(1))
    rv30 = ret.rolling(30).std() * np.sqrt(365)
    rv_ma = rv30.rolling(180).mean()
    rv_std = rv30.rolling(180).std()
    z = -((rv30 - rv_ma) / rv_std).clip(-3, 3) / 3  # negative sign: low vol = positive
    return z.fillna(0).rename('F6_volatility')


# ============ STRATEGY ============

def backtest_composite(daily, weights, threshold=0.5, exit_threshold=0.0):
    """Long when composite score > threshold, exit when < exit_threshold."""
    factors = pd.DataFrame({
        'F1': compute_F1_trend(daily),
        'F2': compute_F2_seasonality(daily),
        'F3': compute_F3_volume(daily),
        'F4': compute_F4_flow(daily, load_coinmetrics()),
        'F5': compute_F5_liquidation_proxy(daily, load_btc_hourly()),
        'F6': compute_F6_volatility(daily),
    })
    factors = factors.fillna(0)

    # Composite score with weights
    w = pd.Series(weights, index=['F1','F2','F3','F4','F5','F6'])
    score = (factors * w).sum(axis=1) / w.abs().sum()

    closes = daily['close'].values
    dates = daily.index
    n = len(closes)
    cash = INITIAL; qty = 0.0
    eq = np.zeros(n); trades = []
    ev = 0; ed = None; intra_min = 0

    for i in range(n):
        c = closes[i]
        eq[i] = cash + qty*c
        if qty > 0:
            low = daily['low'].values[i]
            if intra_min == 0 or low < intra_min: intra_min = low

        s = score.iloc[i]
        if qty > 0 and s < exit_threshold:
            eff = c * (1 - SLIP)
            proceeds = qty * eff * (1 - FEE)
            cash += proceeds
            pnl = (proceeds/ev - 1) * 100
            mae = (intra_min/(ev/qty) - 1) * 100 if qty > 0 else 0
            trades.append({'entry_date': ed, 'exit_date': dates[i],
                          'pnl_pct': pnl, 'mae_pct': mae,
                          'hold_d': (dates[i] - ed).total_seconds()/86400})
            qty = 0; ev = 0; ed = None; intra_min = 0
        elif qty == 0 and s > threshold:
            eff = c * (1 + SLIP)
            spend = cash * 0.99
            bq = spend/eff; cost = bq*eff*(1+FEE)
            cash -= cost; qty = bq
            ev = cost; ed = dates[i]
            intra_min = daily['low'].values[i]

    if qty > 0:
        c = closes[-1]; eff = c * (1 - SLIP)
        proceeds = qty * eff * (1 - FEE)
        cash += proceeds
        pnl = (proceeds/ev - 1) * 100
        mae = (intra_min/(ev/qty) - 1) * 100 if qty > 0 else 0
        trades.append({'entry_date': ed, 'exit_date': dates[-1],
                      'pnl_pct': pnl, 'mae_pct': mae,
                      'hold_d': (dates[-1] - ed).total_seconds()/86400})

    return pd.DataFrame(trades), cash, pd.Series(eq, index=dates), factors, score


def report(label, tr, fe, eq, dates):
    if len(tr) == 0:
        print(f"  {label}: no trades")
        return
    total = (fe/INITIAL - 1) * 100
    dd = ((eq.cummax() - eq)/eq.cummax()).max() * 100
    wr = (tr['pnl_pct']>0).sum()/len(tr) * 100
    mae = tr['mae_pct'].min()
    pos = tr[tr['pnl_pct']>0]['pnl_pct'].sum()
    neg = abs(tr[tr['pnl_pct']<0]['pnl_pct'].sum())
    pf = pos/neg if neg > 0 else float('inf')
    eq_26 = eq[eq.index >= pd.Timestamp(2026,1,1)]
    y26 = (eq_26.iloc[-1]/eq_26.iloc[0] - 1)*100 if len(eq_26)>1 else 0
    eq_rec = eq[eq.index >= pd.Timestamp(2022,1,1)]
    rec = (eq_rec.iloc[-1]/eq_rec.iloc[0] - 1)*100 if len(eq_rec)>1 else 0
    pf_str = f"{pf:>5.2f}" if pf != float('inf') else "  inf"
    print(f"  {label:50}  N={len(tr):>3}  WR={wr:>4.1f}%  PF={pf_str}  total={total:>+7.0f}%  DD={dd:>4.1f}%  rec4y={rec:>+5.0f}%  2026={y26:>+6.1f}%  worstMAE={mae:>+5.1f}%")


def main():
    daily = load_btc()
    daily = daily[daily.index >= pd.Timestamp(2015, 7, 20)]

    print("="*150)
    print("MULTI-FACTOR STRATEGY — BTC daily")
    print("  Factors: F1=trend, F2=seasonality, F3=volume, F4=flow, F5=liquidations, F6=volatility")
    print("="*150)

    # Show factor distributions
    print("\nFactor sample (2023-2026):")
    f1 = compute_F1_trend(daily)
    f2 = compute_F2_seasonality(daily)
    f3 = compute_F3_volume(daily)
    f4 = compute_F4_flow(daily, load_coinmetrics())
    f5 = compute_F5_liquidation_proxy(daily, load_btc_hourly())
    f6 = compute_F6_volatility(daily)
    fdf = pd.concat([f1, f2, f3, f4, f5, f6], axis=1)
    recent = fdf[fdf.index >= '2024-01-01']
    print(f"\n  Sample values (Q1 2024 → present):")
    print(recent.describe().round(3))
    print(f"\n  Last 10 days:")
    print(recent.tail(10).round(2))

    # === Test individual factors ===
    print("\n" + "="*150)
    print("Single-factor strategies (long when factor > 0.3, exit when < 0)")
    print("="*150)
    for i, w_label in enumerate([
        ('F1 only (trend)',         [1,0,0,0,0,0]),
        ('F2 only (seasonality)',   [0,1,0,0,0,0]),
        ('F3 only (volume z)',      [0,0,1,0,0,0]),
        ('F4 only (flow)',          [0,0,0,1,0,0]),
        ('F5 only (liquidations)',  [0,0,0,0,1,0]),
        ('F6 only (low vol)',       [0,0,0,0,0,1]),
    ]):
        label, w = w_label
        tr, fe, eq, _, _ = backtest_composite(daily, w, threshold=0.3, exit_threshold=0)
        report(label, tr, fe, eq, daily.index)

    # === Test 2-factor combinations ===
    print("\n" + "="*150)
    print("2-factor and 3-factor combinations")
    print("="*150)
    for w_label in [
        ('F1+F2 (trend+season)',         [1,1,0,0,0,0]),
        ('F1+F4 (trend+flow)',           [1,0,0,1,0,0]),
        ('F1+F5 (trend+liq)',            [1,0,0,0,1,0]),
        ('F1+F6 (trend+vol)',            [1,0,0,0,0,1]),
        ('F1+F2+F4 (trend+season+flow)', [1,1,0,1,0,0]),
        ('F1+F2+F5 (trend+season+liq)',  [1,1,0,0,1,0]),
        ('F1+F2+F6 (trend+season+vol)',  [1,1,0,0,0,1]),
        ('F1+F2+F4+F6',                  [1,1,0,1,0,1]),
        ('ALL factors equal weights',    [1,1,1,1,1,1]),
        ('Trend heavy (F1=2)',           [2,1,0,1,0,1]),
        ('Season heavy (F2=2)',          [1,2,0,1,0,1]),
    ]:
        label, w = w_label
        tr, fe, eq, _, _ = backtest_composite(daily, w, threshold=0.3, exit_threshold=0)
        report(label, tr, fe, eq, daily.index)

    # === Sweep threshold for best combo ===
    print("\n" + "="*150)
    print("Threshold sweep for best multi-factor combo (F1+F2+F4+F6)")
    print("="*150)
    for thr in [0.1, 0.2, 0.3, 0.4, 0.5, 0.6]:
        for exit_thr in [-0.1, 0, 0.1]:
            tr, fe, eq, _, _ = backtest_composite(daily, [1,1,0,1,0,1],
                                                    threshold=thr, exit_threshold=exit_thr)
            label = f"thr_in={thr} thr_out={exit_thr}"
            report(label, tr, fe, eq, daily.index)

    # === Show buy & hold for comparison ===
    bh = INITIAL / daily['close'].iloc[0] * daily['close']
    bh_total = (bh.iloc[-1]/INITIAL - 1) * 100
    bh_dd = ((bh.cummax() - bh)/bh.cummax()).max() * 100
    bh_26 = bh[bh.index >= pd.Timestamp(2026,1,1)]
    bh_26_ret = (bh_26.iloc[-1]/bh_26.iloc[0] - 1)*100 if len(bh_26)>1 else 0
    print(f"\n  Buy & Hold:                                          total={bh_total:>+7.0f}%  DD={bh_dd:.1f}%  2026={bh_26_ret:+.1f}%")


if __name__ == "__main__":
    main()
