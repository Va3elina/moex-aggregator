"""Comprehensive evolution test of user's original strategy + variants.

Variants tested on BTC daily (and 4H for V6):
  V0: Buy & Hold (baseline)
  V1: Original (weekly logic, VIX>=76, pyramid 33/67/100)
  V2: Evolution 1a — VIX threshold lowered to 50
  V3: Evolution 1b — Daily logic (no weekly resample)
  V4: Evolution 1c — Original without VIX
  V5: Evolution 2 — Hybrid Original + Wavelet V4 (50/50 portfolio)
  V6: Evolution 4 — Original ported to 4H
  V7: Wavelet V4 alone (for comparison)

Reports: total/DD, recent 4y, 2026, year-by-year.
"""
import warnings; warnings.filterwarnings("ignore")
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(__file__).parent
FEE, SLIP, INITIAL = 0.0005, 0.0003, 100.0


# ============ Helpers ============
def ema(arr, n):
    if hasattr(arr, 'values'): arr = arr.values
    out = np.full(len(arr), np.nan)
    if len(arr) < n: return out
    out[n-1] = arr[:n].mean()
    k = 2.0/(n+1.0)
    for i in range(n, len(arr)):
        out[i] = arr[i]*k + out[i-1]*(1-k)
    return out

def rsi_a(close, n=14):
    s = pd.Series(close)
    delta = s.diff()
    gain = delta.clip(lower=0); loss = -delta.clip(upper=0)
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

def load_hourly(asset):
    name = "btc_usd_hourly_coinbase.csv" if asset == 'BTC' else "eth_usd_hourly_coinbase.csv"
    df = pd.read_csv(ROOT/name, parse_dates=['datetime']).set_index('datetime').sort_index()
    df.index = df.index.tz_localize(None)
    return df[df['close']>0].dropna(subset=['close'])

def resample_4h(hourly):
    return hourly.resample('4h').agg({'open':'first','high':'max','low':'min','close':'last','volume':'sum'}).dropna()


def load_vix():
    """Load VIX daily high, forward-filled to BTC index later."""
    vix = pd.read_csv(ROOT / 'vix_daily.csv', skiprows=3,
                       names=['Date','close','high','low','open','volume'])
    vix['Date'] = pd.to_datetime(vix['Date'])
    return vix.set_index('Date')


# ============ Original strategy backtest (weekly logic on daily engine) ============
def backtest_original(df, vix_df, vix_threshold=76.0, use_vix=True, use_weekly_logic=True):
    """User's original Pine strategy as Python backtest."""
    closes = df['close'].values
    dates = df.index
    n = len(closes)

    # === Weekly indicators (Pine: request.security("W", ...))
    if use_weekly_logic:
        weekly_close = df['close'].resample('W-MON', label='left', closed='left').last().dropna()
    else:
        weekly_close = df['close'].copy()

    w_ema200 = pd.Series(ema(weekly_close.values, 200), index=weekly_close.index)
    w_ema20 = pd.Series(ema(weekly_close.values, 20), index=weekly_close.index)
    w_ema5 = pd.Series(ema(weekly_close.values, 5), index=weekly_close.index)
    w_rsi = pd.Series(rsi_a(weekly_close.values, 14), index=weekly_close.index)
    w_rsi_sma = w_rsi.rolling(14).mean()

    # Cross detection (Pine ta.crossunder/crossover)
    cross_under_ema200 = ((weekly_close < w_ema200) & (weekly_close.shift(1) >= w_ema200.shift(1)))
    cross_ema20_over_5 = ((w_ema20 > w_ema5) & (w_ema20.shift(1) <= w_ema5.shift(1)))
    exit_rsi = ((w_rsi_sma < 70) & (w_rsi_sma.shift(1) >= 70))

    # Forward-fill (with 1-bar lag to avoid look-ahead) into daily index
    cross_under_d = cross_under_ema200.shift(1).reindex(dates, method='ffill').fillna(False).astype(bool)
    cross_ema_d = cross_ema20_over_5.shift(1).reindex(dates, method='ffill').fillna(False).astype(bool)
    exit_rsi_d = exit_rsi.shift(1).reindex(dates, method='ffill').fillna(False).astype(bool)

    # Detect "first day signal is True" (Pine fires once per cross)
    sig_strict = {}
    for name, s in [('xu', cross_under_d), ('xc', cross_ema_d), ('xe', exit_rsi_d)]:
        prev = s.shift(1).fillna(False)
        sig_strict[name] = (s & (~prev)).values

    # VIX trigger
    if use_vix and vix_df is not None:
        vix_high_d = vix_df['high'].reindex(dates, method='ffill').values
        vix_trigger = vix_high_d >= vix_threshold
    else:
        vix_trigger = np.zeros(n, dtype=bool)

    # === Backtest with pyramid 33/67/100
    cash = INITIAL; qty = 0.0
    eq = np.zeros(n); trades = []
    entry_value = 0.0; entry_date = None
    entries_taken = 0  # 0, 1, 2 — number of pyramid steps done
    intra_min = 0.0
    for i in range(n):
        c = closes[i]
        eq[i] = cash + qty*c
        if qty > 0:
            low = df['low'].values[i]
            if intra_min == 0 or low < intra_min: intra_min = low

        # Exit on RSI cross
        if qty > 0 and sig_strict['xe'][i]:
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

        # VIX 100% (no position)
        if qty == 0 and vix_trigger[i]:
            eff = c * (1 + SLIP)
            spend = cash * 0.99
            bq = spend/eff; cost = bq*eff*(1+FEE)
            cash -= cost; qty = bq
            entry_value = cost; entry_date = dates[i]
            entries_taken = 2  # max'd out
            intra_min = df['low'].values[i]
            continue

        # 33% on EMA200 cross-under (no VIX, no position)
        if qty == 0 and sig_strict['xu'][i] and not vix_trigger[i]:
            eff = c * (1 + SLIP)
            spend = cash * 0.33
            bq = spend/eff; cost = bq*eff*(1+FEE)
            cash -= cost; qty += bq
            entry_value = cost; entry_date = dates[i]
            entries_taken = 1
            intra_min = df['low'].values[i]
            continue

        # +67% via VIX while at 33%
        if qty > 0 and entries_taken == 1 and vix_trigger[i] and not sig_strict['xc'][i]:
            eff = c * (1 + SLIP)
            spend = cash * 0.99  # remaining
            bq = spend/eff; cost = bq*eff*(1+FEE)
            if cost <= cash:
                cash -= cost; qty += bq
                entry_value += cost
                entries_taken = 2

        # +67% via EMA20>EMA5 while at 33%
        if qty > 0 and entries_taken == 1 and sig_strict['xc'][i] and not vix_trigger[i]:
            eff = c * (1 + SLIP)
            spend = cash * 0.99
            bq = spend/eff; cost = bq*eff*(1+FEE)
            if cost <= cash:
                cash -= cost; qty += bq
                entry_value += cost
                entries_taken = 2

    # Final liquidation
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


# ============ Wavelet V4 backtest (extreme overextension only) ============
def backtest_wavelet_v4(df):
    closes = df['close'].values
    log_c = np.log(closes)
    sm = atrous_haar(log_c, J=6)
    s5 = sm[5]; s6 = sm[6]
    sl6 = np.r_[np.full(5, np.nan), sm[6][5:] - sm[6][:-5]]
    upper = s5 + 2 * (s5 - s6)
    n = len(closes)

    # Entry: price crosses above upper
    entry = np.zeros(n, dtype=bool)
    for i in range(1, n):
        if not (np.isnan(upper[i-1]) or np.isnan(upper[i])):
            if log_c[i-1] <= upper[i-1] and log_c[i] > upper[i]:
                entry[i] = True
    # Exit: price below s5 OR slope6 < 0
    exit_cond = (log_c < s5) | (sl6 < 0)

    # State machine
    state = np.zeros(n)
    s = 0
    for i in range(n):
        if s == 1 and exit_cond[i]: s = 0
        elif s == 0 and entry[i]: s = 1
        state[i] = float(s)

    # Backtest
    cash = INITIAL; qty = 0.0
    eq = np.zeros(n); trades = []
    ev = 0; ed = None; intra_min = 0
    dates = df.index
    for i in range(n):
        c = closes[i]
        eq[i] = cash + qty*c
        if qty > 0:
            low = df['low'].values[i]
            if intra_min == 0 or low < intra_min: intra_min = low
        if state[i] == 1 and qty == 0:
            eff = c*(1+SLIP); spend = cash*0.99
            bq = spend/eff; cost = bq*eff*(1+FEE)
            cash -= cost; qty = bq; ev = cost; ed = dates[i]
            intra_min = df['low'].values[i]
        elif state[i] == 0 and qty > 0:
            eff = c*(1-SLIP); proceeds = qty*eff*(1-FEE)
            cash += proceeds
            pnl = (proceeds/ev - 1) * 100
            mae = (intra_min/(ev/qty) - 1) * 100 if qty > 0 else 0
            trades.append({'entry_date': ed, 'exit_date': dates[i], 'pnl_pct': pnl, 'mae_pct': mae,
                          'hold_d': (dates[i] - ed).total_seconds()/86400})
            qty = 0; ev = 0; ed = None; intra_min = 0
    if qty > 0:
        c = closes[-1]; eff = c*(1-SLIP); proceeds = qty*eff*(1-FEE)
        cash += proceeds
        pnl = (proceeds/ev - 1) * 100
        mae = (intra_min/(ev/qty) - 1) * 100 if qty > 0 else 0
        trades.append({'entry_date': ed, 'exit_date': dates[-1], 'pnl_pct': pnl, 'mae_pct': mae,
                      'hold_d': (dates[-1] - ed).total_seconds()/86400})
    return pd.DataFrame(trades), cash, pd.Series(eq, index=dates)


# ============ Metrics ============
def metrics(trades, final_eq, eq, dates):
    if len(trades) == 0:
        return {'N': 0, 'WR': 0, 'PF': 0, 'total_ret': (final_eq/INITIAL-1)*100,
                'total_dd': 0, 'mae_worst': 0, 'year_2026': 0,
                'recent_4y': 0, 'worst_year': 0, 'best_year': 0}
    total_ret = (final_eq/INITIAL - 1) * 100
    total_dd = ((eq.cummax() - eq)/eq.cummax()).max() * 100
    wr = (trades['pnl_pct']>0).sum() / len(trades) * 100
    pos = trades[trades['pnl_pct']>0]['pnl_pct'].sum()
    neg = abs(trades[trades['pnl_pct']<0]['pnl_pct'].sum())
    pf = pos/neg if neg > 0 else float('inf')
    mae_worst = trades['mae_pct'].min()

    eq_2026 = eq[eq.index >= pd.Timestamp(2026, 1, 1)]
    year_2026 = (eq_2026.iloc[-1]/eq_2026.iloc[0] - 1) * 100 if len(eq_2026) > 1 else 0
    eq_recent = eq[eq.index >= pd.Timestamp(2022, 1, 1)]
    recent_4y = (eq_recent.iloc[-1]/eq_recent.iloc[0] - 1) * 100 if len(eq_recent) > 1 else 0

    worst_year = 0; best_year = 0
    for year in dates.year.unique():
        ym = dates.year == year
        sub = eq[ym]
        if len(sub) < 2: continue
        ret = (sub.iloc[-1]/sub.iloc[0] - 1) * 100
        if ret < worst_year: worst_year = ret
        if ret > best_year: best_year = ret

    return {'N': len(trades), 'WR': wr, 'PF': pf,
            'total_ret': total_ret, 'total_dd': total_dd, 'mae_worst': mae_worst,
            'year_2026': year_2026, 'recent_4y': recent_4y,
            'worst_year': worst_year, 'best_year': best_year}


def fmt(m):
    pf_str = f"{m['PF']:>5.2f}" if m['PF'] != float('inf') else "  inf"
    return (f"N={m['N']:>3}  WR={m['WR']:>4.1f}%  PF={pf_str}  total={m['total_ret']:>+7.0f}%  "
            f"DD={m['total_dd']:>4.1f}%  recent4y={m['recent_4y']:>+5.0f}%  2026={m['year_2026']:>+6.1f}%  "
            f"worstY={m['worst_year']:>+5.0f}%  bestY={m['best_year']:>+5.0f}%")


def main():
    btc = load_daily('BTC')
    vix = load_vix()
    btc_hourly = load_hourly('BTC')
    btc_4h = resample_4h(btc_hourly)

    print("=" * 175)
    print("ALL EVOLUTION VARIANTS — BTC")
    print("=" * 175)

    # V0: Buy & Hold
    bh = INITIAL / btc['close'].iloc[0] * btc['close']
    bh_total = (bh.iloc[-1]/INITIAL - 1) * 100
    bh_dd = ((bh.cummax() - bh)/bh.cummax()).max() * 100
    bh_2026 = bh[bh.index >= pd.Timestamp(2026, 1, 1)]
    bh_2026_ret = (bh_2026.iloc[-1]/bh_2026.iloc[0] - 1)*100 if len(bh_2026)>1 else 0
    bh_recent = bh[bh.index >= pd.Timestamp(2022, 1, 1)]
    bh_recent_ret = (bh_recent.iloc[-1]/bh_recent.iloc[0] - 1)*100 if len(bh_recent)>1 else 0
    print(f"  V0  Buy & Hold:                              total={bh_total:+.0f}%  DD={bh_dd:.0f}%  recent4y={bh_recent_ret:+.0f}%  2026={bh_2026_ret:+.1f}%")

    # V1: Original (weekly logic, VIX 76)
    tr1, fe1, eq1 = backtest_original(btc, vix, vix_threshold=76.0, use_vix=True, use_weekly_logic=True)
    m1 = metrics(tr1, fe1, eq1, btc.index)
    print(f"\n  V1  Original (weekly + VIX≥76):")
    print(f"      {fmt(m1)}")

    # V2: VIX threshold 50
    tr2, fe2, eq2 = backtest_original(btc, vix, vix_threshold=50.0, use_vix=True, use_weekly_logic=True)
    m2 = metrics(tr2, fe2, eq2, btc.index)
    print(f"\n  V2  Evolution 1a — VIX threshold 50:")
    print(f"      {fmt(m2)}")

    # V3: Daily logic (no weekly resample)
    tr3, fe3, eq3 = backtest_original(btc, vix, vix_threshold=76.0, use_vix=True, use_weekly_logic=False)
    m3 = metrics(tr3, fe3, eq3, btc.index)
    print(f"\n  V3  Evolution 1b — Daily logic (no weekly resample):")
    print(f"      {fmt(m3)}")

    # V4: Original without VIX
    tr4, fe4, eq4 = backtest_original(btc, vix, use_vix=False, use_weekly_logic=True)
    m4 = metrics(tr4, fe4, eq4, btc.index)
    print(f"\n  V4  Evolution 1c — No VIX (pure EMA logic):")
    print(f"      {fmt(m4)}")

    # V7: Wavelet V4 alone (for comparison)
    tr7, fe7, eq7 = backtest_wavelet_v4(btc)
    m7 = metrics(tr7, fe7, eq7, btc.index)
    print(f"\n  V7  Wavelet V4 alone (extreme overextension):")
    print(f"      {fmt(m7)}")

    # V5: HYBRID — 50/50 split of Original (no VIX) + Wavelet V4
    # Equity = 0.5 * eq_orig (rescaled to 50 init) + 0.5 * eq_wavelet
    eq5 = (eq4 / INITIAL * 50) + (eq7 / INITIAL * 50)
    final5 = eq5.iloc[-1]
    # Trades come from both
    tr5_orig = tr4.copy(); tr5_orig['source'] = 'original'
    tr5_wav = tr7.copy(); tr5_wav['source'] = 'wavelet'
    tr5 = pd.concat([tr5_orig, tr5_wav]).sort_values('entry_date').reset_index(drop=True)
    # PnL on hybrid is approximately each side's pnl_pct * 50% of total
    # For aggregate metrics use combined trades
    total_ret5 = (final5/INITIAL - 1) * 100
    dd5 = ((eq5.cummax() - eq5)/eq5.cummax()).max() * 100
    wr5 = (tr5['pnl_pct']>0).sum() / max(len(tr5),1) * 100
    eq5_2026 = eq5[eq5.index >= pd.Timestamp(2026, 1, 1)]
    y26_5 = (eq5_2026.iloc[-1]/eq5_2026.iloc[0] - 1) * 100 if len(eq5_2026)>1 else 0
    eq5_recent = eq5[eq5.index >= pd.Timestamp(2022, 1, 1)]
    rec_5 = (eq5_recent.iloc[-1]/eq5_recent.iloc[0] - 1) * 100 if len(eq5_recent)>1 else 0
    worst_y5 = 0; best_y5 = 0
    for year in btc.index.year.unique():
        ym = btc.index.year == year
        sub = eq5[ym]
        if len(sub) < 2: continue
        ret = (sub.iloc[-1]/sub.iloc[0] - 1) * 100
        if ret < worst_y5: worst_y5 = ret
        if ret > best_y5: best_y5 = ret
    print(f"\n  V5  Evolution 2 — HYBRID Original(no VIX) + Wavelet V4 [50/50 split]:")
    print(f"      Trades (orig+wavelet): N={len(tr5)} ({len(tr4)}+{len(tr7)})  "
          f"WR={wr5:.1f}%  total={total_ret5:+.0f}%  DD={dd5:.1f}%  recent4y={rec_5:+.0f}%  "
          f"2026={y26_5:+.1f}%  worstY={worst_y5:+.0f}%  bestY={best_y5:+.0f}%")

    # V6: Original on 4H
    tr6, fe6, eq6 = backtest_original(btc_4h, vix, vix_threshold=76.0, use_vix=True, use_weekly_logic=True)
    m6 = metrics(tr6, fe6, eq6, btc_4h.index)
    print(f"\n  V6  Evolution 4 — Original on 4H:")
    print(f"      {fmt(m6)}")

    # === Year by year for top candidates ===
    print("\n" + "=" * 175)
    print("YEAR-BY-YEAR — top candidates")
    print("=" * 175)
    candidates = [('V1 Original', eq1, tr1),
                  ('V4 No VIX', eq4, tr4),
                  ('V5 Hybrid', eq5, tr5),
                  ('V7 Wavelet V4', eq7, tr7)]
    years = sorted(btc.index.year.unique())
    print(f"  {'year':>4} | " + " | ".join(f"{name:>15}" for name, _, _ in candidates))
    for year in years:
        line = f"  {year:>4} | "
        for name, eq, tr in candidates:
            ym = btc.index.year == year
            sub = eq[ym] if hasattr(eq, 'index') else None
            if sub is None or len(sub) < 2:
                line += f"{'—':>15} | "
                continue
            ret = (sub.iloc[-1]/sub.iloc[0] - 1) * 100
            year_start = pd.Timestamp(year, 1, 1); year_end = pd.Timestamp(year, 12, 31)
            n_t = ((tr['exit_date'] >= year_start) & (tr['exit_date'] <= year_end)).sum() if len(tr) else 0
            line += f"{ret:>+7.1f}% (N={n_t:>2}) | "
        print(line)


if __name__ == "__main__":
    main()
