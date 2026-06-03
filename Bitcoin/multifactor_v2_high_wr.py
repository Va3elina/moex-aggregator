"""V2: fix F2 seasonality + sweep thresholds to find 100% WR.

Original F2 (wrong): bullish zone 6-18 months post-halving.
Reality: BTC tops are 12-18 mo post-halving, bears 18-30, bottom 30-42.

Fixed F2:
  0-6 mo:   +0.3 (accumulation)
  6-12 mo:  +1.0 (early bull)
  12-15 mo: +0.5 (peak watch)
  15-18 mo:  0.0 (top zone — risky to enter)
  18-30 mo: -0.5 (bear)
  30-42 mo: +0.3 (bottom forming)
  42+ mo:   +0.5 (next cycle setup)
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


def compute_F1_trend(daily):
    log_c = np.log(daily['close'].values)
    sm = atrous_haar(log_c, J=6)
    sl5 = slope_a(sm[5], 5)
    sl6 = slope_a(sm[6], 5)
    f1 = np.zeros(len(daily))
    f1[(sl5 > 0) & (sl6 > 0)] = 1.0
    f1[(sl5 < 0) & (sl6 < 0)] = -1.0
    return pd.Series(f1, index=daily.index)


def compute_F2_seasonality_v2(daily):
    """Fixed: accounts for top/bottom timing in halving cycle."""
    halvings = [pd.Timestamp(2012, 11, 28), pd.Timestamp(2016, 7, 9),
                 pd.Timestamp(2020, 5, 11), pd.Timestamp(2024, 4, 19)]
    f2 = np.zeros(len(daily))
    for i, d in enumerate(daily.index):
        prev_halvings = [h for h in halvings if h <= d]
        if not prev_halvings:
            f2[i] = 0; continue
        last_h = max(prev_halvings)
        months_since = (d - last_h).days / 30
        if months_since < 6:
            f2[i] = 0.3
        elif months_since <= 12:
            f2[i] = 1.0  # early bull
        elif months_since <= 15:
            f2[i] = 0.5  # peak watch
        elif months_since <= 18:
            f2[i] = 0.0  # top zone — DON'T enter here
        elif months_since <= 30:
            f2[i] = -0.5  # bear
        elif months_since <= 42:
            f2[i] = 0.3  # bottom forming
        else:
            f2[i] = 0.5  # next cycle setup
    return pd.Series(f2, index=daily.index)


def compute_F4_flow(daily, cm):
    cm_aligned = cm.reindex(daily.index, method='ffill')
    net_flow = (cm_aligned['FlowOutExUSD'] - cm_aligned['FlowInExUSD'])
    nf_ma = net_flow.rolling(30).mean()
    nf_std = net_flow.rolling(30).std()
    z = ((net_flow - nf_ma) / nf_std).clip(-3, 3) / 3
    return z.fillna(0)


def compute_F6_volatility(daily):
    ret = np.log(daily['close'] / daily['close'].shift(1))
    rv30 = ret.rolling(30).std() * np.sqrt(365)
    rv_ma = rv30.rolling(180).mean()
    rv_std = rv30.rolling(180).std()
    z = -((rv30 - rv_ma) / rv_std).clip(-3, 3) / 3
    return z.fillna(0)


def backtest_composite(daily, factors_df, weights, threshold, exit_threshold):
    w = pd.Series(weights, index=factors_df.columns)
    score = (factors_df * w).sum(axis=1) / w.abs().sum()

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

    return pd.DataFrame(trades), cash, pd.Series(eq, index=dates)


def report(label, tr, fe, eq):
    if len(tr) == 0:
        print(f"  {label:50}  no trades")
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
    pf_str = f"{pf:>5.2f}" if pf != float('inf') else "  inf"
    print(f"  {label:50}  N={len(tr):>3}  WR={wr:>4.1f}%  PF={pf_str}  total={total:>+7.0f}%  DD={dd:>4.1f}%  2026={y26:>+6.1f}%  worstMAE={mae:>+5.1f}%")


def main():
    daily = pd.read_csv(ROOT/'btc_usd_daily_coinbase.csv', parse_dates=['date']).set_index('date').sort_index()
    daily = daily[daily['close']>0].dropna(subset=['close'])
    cm = pd.read_csv(ROOT/'btc_daily_coinmetrics.csv', parse_dates=['date']).set_index('date').sort_index()

    f1 = compute_F1_trend(daily)
    f2_old = pd.Series(np.where((np.array([(d - max(h for h in [pd.Timestamp(2012,11,28), pd.Timestamp(2016,7,9), pd.Timestamp(2020,5,11), pd.Timestamp(2024,4,19)] if h <= d)).days/30 for d in daily.index]) >= 6) &
                                  (np.array([(d - max(h for h in [pd.Timestamp(2012,11,28), pd.Timestamp(2016,7,9), pd.Timestamp(2020,5,11), pd.Timestamp(2024,4,19)] if h <= d)).days/30 for d in daily.index]) <= 18), 1.0, 0.3), index=daily.index)
    f2_new = compute_F2_seasonality_v2(daily)
    f4 = compute_F4_flow(daily, cm)
    f6 = compute_F6_volatility(daily)

    # Build factor df
    factors = pd.DataFrame({'F1': f1, 'F2': f2_new, 'F4': f4, 'F6': f6}).fillna(0)

    print("=" * 130)
    print("V2 Multi-factor with FIXED F2 seasonality — threshold sweep for 100% WR")
    print("=" * 130)

    # Try various combinations and thresholds
    weights_list = [
        ('All equal (1,1,1,1)',     [1, 1, 1, 1]),
        ('F1 heavy (2,1,1,1)',      [2, 1, 1, 1]),
        ('F4 heavy (1,1,2,1)',      [1, 1, 2, 1]),
        ('F2 heavy (1,2,1,1)',      [1, 2, 1, 1]),
        ('No F2 (1,0,1,1)',         [1, 0, 1, 1]),
        ('No F6 (1,1,1,0)',         [1, 1, 1, 0]),
        ('F1+F4 only (1,0,1,0)',    [1, 0, 1, 0]),
        ('F1+F2 only (1,1,0,0)',    [1, 1, 0, 0]),
    ]

    for label, w in weights_list:
        print(f"\n  {label}:")
        for thr in [0.5, 0.6, 0.7, 0.8]:
            for ex_thr in [-0.1, 0, 0.1, 0.2]:
                tr, fe, eq = backtest_composite(daily, factors, w, thr, ex_thr)
                report(f"    thr_in={thr} thr_out={ex_thr}", tr, fe, eq)


if __name__ == "__main__":
    main()
