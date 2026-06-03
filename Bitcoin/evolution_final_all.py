"""All-in-one final test:
  1. V2 (VIX 50) hybrid variations with Wavelet V4 (different splits)
  2. Apply V2 + V5 hybrid to ETH
  3. Leverage feasibility (1x, 2x, 3x) for V2 and V5 hybrid
  4. Cross-asset hybrid: V2-BTC + Wavelet-ETH
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

def atrous_haar(x, J=6):
    sm = [x.astype(float).copy()]
    for j in range(1, J+1):
        gap = 2**(j-1); prev = sm[-1]; s = prev.copy()
        if len(x) > gap: s[gap:] = 0.5*(prev[gap:] + prev[:-gap])
        sm.append(s)
    return sm


def load_daily(asset):
    name = "btc_usd_daily_coinbase.csv" if asset == 'BTC' else "eth_usd_daily_coinbase.csv"
    df = pd.read_csv(ROOT/name, parse_dates=['date']).set_index('date').sort_index()
    return df[df['close']>0].dropna(subset=['close'])

def load_vix():
    vix = pd.read_csv(ROOT / 'vix_daily.csv', skiprows=3,
                       names=['Date','close','high','low','open','volume'])
    vix['Date'] = pd.to_datetime(vix['Date'])
    return vix.set_index('Date')


def backtest_original(df, vix_df, vix_threshold=50.0, use_vix=True, leverage=1.0):
    """Original strategy (weekly logic). Optional leverage."""
    closes = df['close'].values
    dates = df.index
    n = len(closes)
    weekly_close = df['close'].resample('W-MON', label='left', closed='left').last().dropna()
    w_ema200 = pd.Series(ema(weekly_close.values, 200), index=weekly_close.index)
    w_ema20  = pd.Series(ema(weekly_close.values, 20),  index=weekly_close.index)
    w_ema5   = pd.Series(ema(weekly_close.values, 5),   index=weekly_close.index)
    w_rsi    = pd.Series(rsi_a(weekly_close.values, 14), index=weekly_close.index)
    w_rsi_sma = w_rsi.rolling(14).mean()

    cross_under_ema200 = ((weekly_close < w_ema200) & (weekly_close.shift(1) >= w_ema200.shift(1)))
    cross_ema20_over_5 = ((w_ema20 > w_ema5) & (w_ema20.shift(1) <= w_ema5.shift(1)))
    exit_rsi = ((w_rsi_sma < 70) & (w_rsi_sma.shift(1) >= 70))

    cross_under_d = cross_under_ema200.shift(1).reindex(dates, method='ffill').fillna(False).astype(bool)
    cross_ema_d   = cross_ema20_over_5.shift(1).reindex(dates, method='ffill').fillna(False).astype(bool)
    exit_rsi_d    = exit_rsi.shift(1).reindex(dates, method='ffill').fillna(False).astype(bool)
    sig_strict = {}
    for name, s in [('xu', cross_under_d), ('xc', cross_ema_d), ('xe', exit_rsi_d)]:
        prev = s.shift(1).fillna(False)
        sig_strict[name] = (s & (~prev)).values

    if use_vix and vix_df is not None:
        vix_high = vix_df['high'].reindex(dates, method='ffill').values
        vix_trigger = vix_high >= vix_threshold
    else:
        vix_trigger = np.zeros(n, dtype=bool)

    cash = INITIAL; units = 0.0
    eq = np.zeros(n); trades = []
    entry_value = 0.0; entry_date = None
    entries_taken = 0
    intra_min = 0
    liquidations = 0
    for i in range(n):
        c = closes[i]
        if units > 0:
            mtm = (c - entry_value/units if units > 0 else 0) * units
            equity = cash + mtm
            low = df['low'].values[i]
            if intra_min == 0 or low < intra_min: intra_min = low
        else:
            equity = cash
        eq[i] = max(equity, 0.001)

        # Liquidation
        if units > 0 and leverage > 1:
            avg_entry = entry_value/units
            liq_px = avg_entry * (1 - 1.0/leverage + 0.005)
            if df['low'].values[i] <= liq_px:
                pnl = (liq_px - avg_entry) * units
                cash = max(cash + pnl, 0.001)
                mae = (intra_min/avg_entry - 1) * 100 * leverage
                trades.append({'entry_date': entry_date, 'exit_date': dates[i],
                              'pnl_pct': (liq_px/avg_entry - 1)*100*leverage,
                              'mae_pct': mae, 'reason':'LIQ',
                              'hold_d': (dates[i] - entry_date).total_seconds()/86400})
                liquidations += 1
                units = 0; entry_value = 0; entry_date = None
                entries_taken = 0; intra_min = 0
                continue

        # Exit on RSI cross
        if units > 0 and sig_strict['xe'][i]:
            eff = c * (1 - SLIP)
            avg_entry = entry_value/units
            pnl_dollars = (eff - avg_entry) * units - units * eff * FEE
            cash += pnl_dollars
            pnl_pct = (eff/avg_entry - 1) * 100 * leverage
            mae = (intra_min/avg_entry - 1) * 100 * leverage
            trades.append({'entry_date': entry_date, 'exit_date': dates[i],
                          'pnl_pct': pnl_pct, 'mae_pct': mae, 'reason':'rsi',
                          'hold_d': (dates[i] - entry_date).total_seconds()/86400})
            units = 0; entry_value = 0; entry_date = None
            entries_taken = 0; intra_min = 0
            continue

        # VIX 100%
        if units == 0 and vix_trigger[i]:
            eff = c * (1 + SLIP)
            spend = cash * 0.99
            bq = (spend * leverage) / eff
            fee = bq * eff * FEE
            cash -= fee
            units = bq; entry_value = units * eff; entry_date = dates[i]
            entries_taken = 2
            intra_min = df['low'].values[i]
            continue

        # 33% on EMA200 cross-under
        if units == 0 and sig_strict['xu'][i] and not vix_trigger[i]:
            eff = c * (1 + SLIP)
            spend = cash * 0.33
            bq = (spend * leverage) / eff
            fee = bq * eff * FEE
            cash -= fee
            units += bq; entry_value = units * eff; entry_date = dates[i]
            entries_taken = 1
            intra_min = df['low'].values[i]
            continue

        # +67% via VIX
        if units > 0 and entries_taken == 1 and vix_trigger[i] and not sig_strict['xc'][i]:
            eff = c * (1 + SLIP)
            spend = cash * 0.99
            bq = (spend * leverage) / eff
            fee = bq * eff * FEE
            cash -= fee
            units += bq; entry_value += bq * eff
            entries_taken = 2

        # +67% via EMA20>EMA5
        if units > 0 and entries_taken == 1 and sig_strict['xc'][i] and not vix_trigger[i]:
            eff = c * (1 + SLIP)
            spend = cash * 0.99
            bq = (spend * leverage) / eff
            fee = bq * eff * FEE
            cash -= fee
            units += bq; entry_value += bq * eff
            entries_taken = 2

    if units > 0:
        c = closes[-1]; eff = c * (1 - SLIP)
        avg_entry = entry_value/units
        pnl = (eff - avg_entry) * units - units * eff * FEE
        cash += pnl
        pnl_pct = (eff/avg_entry - 1) * 100 * leverage
        mae = (intra_min/avg_entry - 1) * 100 * leverage if units > 0 else 0
        trades.append({'entry_date': entry_date, 'exit_date': dates[-1],
                      'pnl_pct': pnl_pct, 'mae_pct': mae, 'reason': 'EOH',
                      'hold_d': (dates[-1] - entry_date).total_seconds()/86400})

    return pd.DataFrame(trades), cash, pd.Series(eq, index=dates), liquidations


def backtest_wavelet_v4(df, leverage=1.0):
    closes = df['close'].values
    log_c = np.log(closes)
    sm = atrous_haar(log_c, J=6)
    s5 = sm[5]; s6 = sm[6]
    sl6 = np.r_[np.full(5, np.nan), sm[6][5:] - sm[6][:-5]]
    upper = s5 + 2 * (s5 - s6)
    n = len(closes)
    entry = np.zeros(n, dtype=bool)
    for i in range(1, n):
        if not (np.isnan(upper[i-1]) or np.isnan(upper[i])):
            if log_c[i-1] <= upper[i-1] and log_c[i] > upper[i]:
                entry[i] = True
    exit_cond = (log_c < s5) | (sl6 < 0)

    cash = INITIAL; units = 0.0; entry_px = 0
    eq = np.zeros(n); trades = []
    ed = None; intra_min = 0
    dates = df.index
    liquidations = 0
    state = 0
    for i in range(n):
        c = closes[i]
        if units > 0:
            mtm = (c - entry_px) * units
            equity = cash + mtm
            low = df['low'].values[i]
            if intra_min == 0 or low < intra_min: intra_min = low
        else:
            equity = cash
        eq[i] = max(equity, 0.001)

        # Liquidation
        if units > 0 and leverage > 1:
            liq_px = entry_px * (1 - 1.0/leverage + 0.005)
            if df['low'].values[i] <= liq_px:
                pnl = (liq_px - entry_px) * units
                cash = max(cash + pnl, 0.001)
                trades.append({'entry_date': ed, 'exit_date': dates[i],
                              'pnl_pct': (liq_px/entry_px - 1)*100*leverage,
                              'mae_pct': (intra_min/entry_px - 1)*100*leverage,
                              'reason':'LIQ', 'hold_d': (dates[i] - ed).total_seconds()/86400})
                liquidations += 1
                units = 0; entry_px = 0; intra_min = 0; state = 0
                continue

        if state == 1 and exit_cond[i]:
            eff = c*(1-SLIP)
            pnl = (eff - entry_px) * units - units * eff * FEE
            cash += pnl
            pnl_pct = (eff/entry_px - 1) * 100 * leverage
            mae = (intra_min/entry_px - 1)*100*leverage if entry_px > 0 else 0
            trades.append({'entry_date': ed, 'exit_date': dates[i],
                          'pnl_pct': pnl_pct, 'mae_pct': mae, 'reason':'sig',
                          'hold_d': (dates[i] - ed).total_seconds()/86400})
            units = 0; entry_px = 0; intra_min = 0; state = 0
        elif state == 0 and entry[i]:
            eff = c*(1+SLIP)
            units = (cash*0.99*leverage)/eff
            fee = units*eff*FEE
            cash -= fee
            entry_px = eff; ed = dates[i]
            intra_min = df['low'].values[i]; state = 1

    if units > 0:
        c = closes[-1]; eff = c*(1-SLIP)
        pnl = (eff - entry_px) * units - units * eff * FEE
        cash += pnl
        pnl_pct = (eff/entry_px - 1)*100*leverage
        mae = (intra_min/entry_px - 1)*100*leverage if entry_px > 0 else 0
        trades.append({'entry_date': ed, 'exit_date': dates[-1],
                      'pnl_pct': pnl_pct, 'mae_pct': mae, 'reason':'EOH',
                      'hold_d': (dates[-1] - ed).total_seconds()/86400})

    return pd.DataFrame(trades), cash, pd.Series(eq, index=dates), liquidations


def report(name, tr, fe, eq, dates, liq=0):
    if len(tr) == 0:
        return None
    total = (fe/INITIAL - 1) * 100
    dd = ((eq.cummax() - eq)/eq.cummax()).max() * 100
    wr = (tr['pnl_pct']>0).sum()/len(tr) * 100
    pos = tr[tr['pnl_pct']>0]['pnl_pct'].sum()
    neg = abs(tr[tr['pnl_pct']<0]['pnl_pct'].sum())
    pf = pos/neg if neg > 0 else float('inf')
    mae = tr['mae_pct'].min()
    eq_26 = eq[eq.index >= pd.Timestamp(2026,1,1)]
    y26 = (eq_26.iloc[-1]/eq_26.iloc[0] - 1)*100 if len(eq_26)>1 else 0
    eq_recent = eq[eq.index >= pd.Timestamp(2022,1,1)]
    rec = (eq_recent.iloc[-1]/eq_recent.iloc[0] - 1)*100 if len(eq_recent)>1 else 0
    worst_y = 0; best_y = 0
    for year in dates.year.unique():
        ym = dates.year == year
        sub = eq[ym]
        if len(sub) < 2: continue
        r = (sub.iloc[-1]/sub.iloc[0] - 1)*100
        if r < worst_y: worst_y = r
        if r > best_y: best_y = r
    pf_str = f"{pf:>5.2f}" if pf != float('inf') else "  inf"
    print(f"    N={len(tr):>3}  WR={wr:>4.1f}%  PF={pf_str}  total={total:>+7.0f}%  DD={dd:>4.1f}%  "
          f"mMAE={mae:>+5.1f}%  recent4y={rec:>+5.0f}%  2026={y26:>+6.1f}%  "
          f"worstY={worst_y:>+5.0f}%  bestY={best_y:>+5.0f}%  LIQ={liq}")


def hybrid(eq_a, tr_a, dates_a, eq_b, tr_b, dates_b, split=0.5, name=""):
    """Run hybrid: split of A and B. Combine equity curves."""
    # Re-base both equity curves to start at INITIAL*split / INITIAL*(1-split)
    eq_combined = (eq_a/INITIAL)*INITIAL*split + (eq_b/INITIAL)*INITIAL*(1-split)
    final = eq_combined.iloc[-1]
    combined_tr = pd.concat([tr_a, tr_b]).sort_values('entry_date').reset_index(drop=True)
    print(f"  {name}:")
    report(name, combined_tr, final, eq_combined, dates_a)


def main():
    btc = load_daily('BTC')
    eth = load_daily('ETH')
    vix = load_vix()

    print("=" * 175)
    print("PART 1 — BTC: V2 (VIX 50) hybrid variations with Wavelet V4")
    print("=" * 175)

    # V2 alone
    tr_v2, fe_v2, eq_v2, _ = backtest_original(btc, vix, vix_threshold=50.0)
    print(f"\n  V2 BTC (Original VIX≥50) standalone:")
    report('V2', tr_v2, fe_v2, eq_v2, btc.index)

    # Wavelet V4 alone
    tr_w4, fe_w4, eq_w4, _ = backtest_wavelet_v4(btc)
    print(f"\n  Wavelet V4 BTC standalone:")
    report('W4', tr_w4, fe_w4, eq_w4, btc.index)

    # Different splits
    for split in [0.7, 0.5, 0.3]:
        hybrid(eq_v2, tr_v2, btc.index, eq_w4, tr_w4, btc.index, split=split,
               name=f"V2/W4 split {int(split*100)}/{int((1-split)*100)} (V2={int(split*100)}%)")

    print("\n\n" + "=" * 175)
    print("PART 2 — ETH: V2 and V5 hybrid applied")
    print("=" * 175)

    tr_v2e, fe_v2e, eq_v2e, _ = backtest_original(eth, vix, vix_threshold=50.0)
    print(f"\n  V2 ETH (Original VIX≥50) standalone:")
    report('V2-ETH', tr_v2e, fe_v2e, eq_v2e, eth.index)

    tr_w4e, fe_w4e, eq_w4e, _ = backtest_wavelet_v4(eth)
    print(f"\n  Wavelet V4 ETH standalone:")
    report('W4-ETH', tr_w4e, fe_w4e, eq_w4e, eth.index)

    for split in [0.5]:
        hybrid(eq_v2e, tr_v2e, eth.index, eq_w4e, tr_w4e, eth.index, split=split,
               name=f"V2/W4 ETH split 50/50")

    # CROSS-ASSET HYBRID: V2 BTC + Wavelet ETH (might be powerful)
    print("\n\n  CROSS-ASSET HYBRID — V2 BTC + Wavelet ETH (50/50 split):")
    # Align both to same date range
    common_dates = btc.index.intersection(eth.index)
    eq_v2_aligned = eq_v2.reindex(common_dates, method='ffill')
    eq_w4e_aligned = eq_w4e.reindex(common_dates, method='ffill')
    eq_cross = (eq_v2_aligned/INITIAL)*INITIAL*0.5 + (eq_w4e_aligned/INITIAL)*INITIAL*0.5
    combined_tr = pd.concat([tr_v2, tr_w4e]).sort_values('entry_date').reset_index(drop=True)
    report('cross', combined_tr, eq_cross.iloc[-1], eq_cross, common_dates)

    print("\n\n" + "=" * 175)
    print("PART 3 — LEVERAGE feasibility (V2 BTC and V2 ETH)")
    print("=" * 175)

    for asset, df in [('BTC', btc), ('ETH', eth)]:
        print(f"\n  {asset} V2 (Original VIX≥50) — leverage scan:")
        for lev in [1.0, 2.0, 3.0]:
            tr, fe, eq, liq = backtest_original(df, vix, vix_threshold=50.0, leverage=lev)
            print(f"    Leverage {lev:.0f}x:")
            report('V2', tr, fe, eq, df.index, liq=liq)

        print(f"\n  {asset} Wavelet V4 — leverage scan:")
        for lev in [1.0, 2.0, 3.0]:
            tr, fe, eq, liq = backtest_wavelet_v4(df, leverage=lev)
            print(f"    Leverage {lev:.0f}x:")
            report('W4', tr, fe, eq, df.index, liq=liq)

    print("\n\n" + "=" * 175)
    print("YEAR-BY-YEAR for top hybrid candidates")
    print("=" * 175)
    for split in [0.7, 0.5, 0.3]:
        eq_h = (eq_v2/INITIAL)*INITIAL*split + (eq_w4/INITIAL)*INITIAL*(1-split)
        print(f"\n  Hybrid V2/W4 split {int(split*100)}/{int((1-split)*100)}:")
        for year in sorted(btc.index.year.unique()):
            ym = btc.index.year == year
            sub = eq_h[ym]
            if len(sub) < 2: continue
            r = (sub.iloc[-1]/sub.iloc[0] - 1) * 100
            print(f"    {year}: {r:+.1f}%")


if __name__ == "__main__":
    main()
