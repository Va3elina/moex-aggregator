"""Search for portfolio combination + hard stops + multi-asset extension.

Target: DD ≤ 35%, WR ≥ 70%, beats B&H, smooth equity growth.

Tests:
  PART 1: Multi-asset application of best wavelet strategy (BTC/ETH/LTC/SOL/BCH)
  PART 2: Portfolio combinations of best individual strategies (equal weights)
  PART 3: Hard stop loss on best strategies (-15%, -20% per trade)
  PART 4: Risk-parity portfolio (inverse volatility weighting)
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

def atrous_haar(x, J=8):
    sm = [x.astype(float).copy()]
    for j in range(1, J+1):
        gap = 2**(j-1); prev = sm[-1]; s = prev.copy()
        if len(x) > gap: s[gap:] = 0.5*(prev[gap:] + prev[:-gap])
        sm.append(s)
    return sm


def load_daily(asset):
    files = {'BTC': 'btc_usd_daily_coinbase.csv', 'ETH': 'eth_usd_daily_coinbase.csv',
             'LTC': 'ltc_usd_daily_coinbase.csv', 'SOL': 'sol_usd_daily_coinbase.csv',
             'BCH': 'bch_usd_daily_coinbase.csv'}
    df = pd.read_csv(ROOT / files[asset], parse_dates=['date']).set_index('date').sort_index()
    return df[df['close']>0].dropna(subset=['close'])


def strategy_8a_std30(df, k=1.5, hard_stop=None):
    """BTC champion: 8a (k=1.5, std30) — extreme overextension entry."""
    closes = df['close'].values
    log_c = np.log(closes)
    sm = atrous_haar(log_c, J=6)
    s5 = sm[5]
    std30 = pd.Series(log_c - s5).rolling(30).std().values
    upper = s5 + k * std30
    n = len(closes)
    entry = (log_c > upper) & (np.roll(log_c, 1) <= np.roll(upper, 1))
    entry[0] = False
    exit_cond = log_c < s5

    cash = INITIAL; qty = 0.0
    eq = np.zeros(n); trades = []
    ev = 0; ed = None; intra_min = 0
    state = 0
    dates = df.index
    for i in range(n):
        c = closes[i]
        eq[i] = cash + qty*c
        if qty > 0:
            low = df['low'].values[i]
            if intra_min == 0 or low < intra_min: intra_min = low
            if hard_stop is not None and qty > 0:
                stop_px = (ev/qty) * (1 + hard_stop)
                if low <= stop_px:
                    eff = stop_px * (1 - SLIP)
                    proceeds = qty * eff * (1 - FEE)
                    cash += proceeds
                    pnl = (proceeds/ev - 1) * 100
                    mae = (intra_min/(ev/qty) - 1) * 100
                    trades.append({'entry_date': ed, 'exit_date': dates[i],
                                  'pnl_pct': pnl, 'mae_pct': mae, 'reason': 'HARD_STOP',
                                  'hold_d': (dates[i] - ed).total_seconds()/86400})
                    qty = 0; ev = 0; ed = None; intra_min = 0; state = 0
                    continue

        if state == 1 and exit_cond[i]:
            eff = c * (1 - SLIP)
            proceeds = qty * eff * (1 - FEE)
            cash += proceeds
            pnl = (proceeds/ev - 1) * 100
            mae = (intra_min/(ev/qty) - 1) * 100 if qty > 0 else 0
            trades.append({'entry_date': ed, 'exit_date': dates[i],
                          'pnl_pct': pnl, 'mae_pct': mae, 'reason': 'sig',
                          'hold_d': (dates[i] - ed).total_seconds()/86400})
            qty = 0; ev = 0; ed = None; intra_min = 0; state = 0
        elif state == 0 and entry[i] and not np.isnan(std30[i]):
            eff = c * (1 + SLIP)
            spend = cash * 0.99
            bq = spend/eff; cost = bq*eff*(1+FEE)
            cash -= cost; qty = bq
            ev = cost; ed = dates[i]
            intra_min = df['low'].values[i]; state = 1

    if qty > 0:
        c = closes[-1]; eff = c * (1 - SLIP)
        proceeds = qty * eff * (1 - FEE)
        cash += proceeds
        pnl = (proceeds/ev - 1) * 100
        mae = (intra_min/(ev/qty) - 1) * 100 if qty > 0 else 0
        trades.append({'entry_date': ed, 'exit_date': dates[-1],
                      'pnl_pct': pnl, 'mae_pct': mae, 'reason': 'EOH',
                      'hold_d': (dates[-1] - ed).total_seconds()/86400})

    return pd.DataFrame(trades), cash, pd.Series(eq, index=dates)


def strategy_wavelet_v1_weekly(daily, hard_stop=None):
    """BTC V1 wavelet replacement: s7/s4/s2 + RSI exit, pyramid 33/67."""
    weekly = daily.resample('W-MON', label='left', closed='left').agg(
        {'open':'first','high':'max','low':'min','close':'last','volume':'sum'}).dropna()
    wc = weekly['close']
    log_wc = np.log(wc.values)
    sm = atrous_haar(log_wc, J=7)
    s7 = pd.Series(sm[7], index=wc.index)
    s4 = pd.Series(sm[4], index=wc.index)
    s2 = pd.Series(sm[2], index=wc.index)
    log_wc_s = pd.Series(log_wc, index=wc.index)

    # RSI weekly
    delta = wc.diff()
    gain = delta.clip(lower=0); loss = -delta.clip(upper=0)
    avg_g = gain.ewm(alpha=1/14, adjust=False).mean()
    avg_l = loss.ewm(alpha=1/14, adjust=False).mean()
    rsi = 100 - 100/(1 + avg_g/avg_l)
    rsi_sma = rsi.rolling(14).mean()

    entry1 = (log_wc_s < s7) & (log_wc_s.shift(1) >= s7.shift(1))
    entry2 = (s4 > s2) & (s4.shift(1) <= s2.shift(1))
    exit_w = (rsi_sma < 70) & (rsi_sma.shift(1) >= 70)

    dates = daily.index
    e1 = entry1.shift(1).reindex(dates, method='ffill').fillna(False).astype(bool)
    e2 = entry2.shift(1).reindex(dates, method='ffill').fillna(False).astype(bool)
    ex = exit_w.shift(1).reindex(dates, method='ffill').fillna(False).astype(bool)
    e1s = (e1 & ~e1.shift(1).fillna(False)).values
    e2s = (e2 & ~e2.shift(1).fillna(False)).values
    exs = (ex & ~ex.shift(1).fillna(False)).values

    closes = daily['close'].values; n = len(closes)
    cash = INITIAL; qty = 0.0
    eq = np.zeros(n); trades = []
    ev = 0; ed = None; intra_min = 0
    entries_taken = 0
    for i in range(n):
        c = closes[i]
        eq[i] = cash + qty*c
        if qty > 0:
            low = daily['low'].values[i]
            if intra_min == 0 or low < intra_min: intra_min = low
            if hard_stop is not None:
                stop_px = (ev/qty) * (1 + hard_stop)
                if low <= stop_px:
                    eff = stop_px * (1 - SLIP)
                    proceeds = qty * eff * (1 - FEE)
                    cash += proceeds
                    pnl = (proceeds/ev - 1) * 100
                    mae = (intra_min/(ev/qty) - 1) * 100
                    trades.append({'entry_date': ed, 'exit_date': dates[i],
                                  'pnl_pct': pnl, 'mae_pct': mae, 'reason': 'HARD_STOP',
                                  'hold_d': (dates[i] - ed).total_seconds()/86400})
                    qty = 0; ev = 0; ed = None; intra_min = 0; entries_taken = 0
                    continue

        if qty > 0 and exs[i]:
            eff = c * (1 - SLIP)
            proceeds = qty * eff * (1 - FEE)
            cash += proceeds
            pnl = (proceeds/ev - 1) * 100
            mae = (intra_min/(ev/qty) - 1) * 100 if qty > 0 else 0
            trades.append({'entry_date': ed, 'exit_date': dates[i],
                          'pnl_pct': pnl, 'mae_pct': mae, 'reason': 'rsi',
                          'hold_d': (dates[i] - ed).total_seconds()/86400})
            qty = 0; ev = 0; ed = None; intra_min = 0; entries_taken = 0
            continue

        if qty == 0 and e1s[i]:
            eff = c * (1 + SLIP)
            spend = cash * 0.33
            bq = spend/eff; cost = bq*eff*(1+FEE)
            cash -= cost; qty = bq
            ev = cost; ed = dates[i]; entries_taken = 1
            intra_min = daily['low'].values[i]
            continue

        if qty > 0 and entries_taken == 1 and e2s[i]:
            eff = c * (1 + SLIP)
            spend = cash * 0.99
            bq = spend/eff; cost = bq*eff*(1+FEE)
            if cost <= cash:
                cash -= cost; qty += bq
                ev += cost; entries_taken = 2

    if qty > 0:
        c = closes[-1]; eff = c * (1 - SLIP)
        proceeds = qty * eff * (1 - FEE)
        cash += proceeds
        pnl = (proceeds/ev - 1) * 100
        mae = (intra_min/(ev/qty) - 1) * 100 if qty > 0 else 0
        trades.append({'entry_date': ed, 'exit_date': dates[-1],
                      'pnl_pct': pnl, 'mae_pct': mae, 'reason': 'EOH',
                      'hold_d': (dates[-1] - ed).total_seconds()/86400})

    return pd.DataFrame(trades), cash, pd.Series(eq, index=dates)


def metrics(tr, fe, eq, dates):
    if len(tr) == 0:
        return None
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
    # smoothness: ratio of positive months
    monthly = eq.resample('ME').last().pct_change().dropna()
    pct_pos_months = (monthly > 0).sum() / len(monthly) * 100 if len(monthly) else 0
    return {'N': len(tr), 'WR': wr, 'PF': pf, 'total_ret': total, 'total_dd': dd,
            'mae_worst': mae, 'recent_4y': rec, 'year_2026': y26,
            'pct_pos_months': pct_pos_months}


def fmt_row(m):
    if m is None: return "(no trades)"
    pf_str = f"{m['PF']:>5.2f}" if m['PF'] != float('inf') else "  inf"
    return (f"N={m['N']:>3}  WR={m['WR']:>4.1f}%  PF={pf_str}  total={m['total_ret']:>+7.0f}%  "
            f"DD={m['total_dd']:>4.1f}%  recent4y={m['recent_4y']:>+5.0f}%  2026={m['year_2026']:>+6.1f}%  "
            f"posMo={m['pct_pos_months']:>4.1f}%")


def main():
    assets = ['BTC', 'ETH', 'LTC', 'SOL', 'BCH']
    dfs = {a: load_daily(a) for a in assets}

    print("=" * 150)
    print("PART 1 — Best wavelet strategy (8a std30) applied to all crypto")
    print("=" * 150)
    eqs_8a = {}
    trs_8a = {}
    for asset in assets:
        df = dfs[asset]
        tr, fe, eq = strategy_8a_std30(df)
        eqs_8a[asset] = eq; trs_8a[asset] = tr
        m = metrics(tr, fe, eq, df.index)
        print(f"  {asset}: {fmt_row(m)}")

    print("\n" + "=" * 150)
    print("PART 2 — Wavelet V1 (s7/s4/s2 + RSI) on all crypto")
    print("=" * 150)
    eqs_w1 = {}
    trs_w1 = {}
    for asset in assets:
        df = dfs[asset]
        tr, fe, eq = strategy_wavelet_v1_weekly(df)
        eqs_w1[asset] = eq; trs_w1[asset] = tr
        m = metrics(tr, fe, eq, df.index)
        print(f"  {asset}: {fmt_row(m)}")

    # ===== Portfolio combinations =====
    print("\n" + "=" * 150)
    print("PART 3 — Equal-weight portfolios")
    print("=" * 150)

    def portfolio(components):
        """components: list of (eq_series, weight). Returns aligned portfolio equity."""
        common = None
        for eq, _ in components:
            common = eq.index if common is None else common.intersection(eq.index)
        port_eq = pd.Series(0.0, index=common)
        for eq, w in components:
            eq_a = eq.reindex(common, method='ffill').fillna(method='bfill')
            port_eq += (eq_a / INITIAL) * INITIAL * w
        return port_eq

    portfolios = [
        ("BTC 8a + ETH 8a (50/50)",
         [(eqs_8a['BTC'], 0.5), (eqs_8a['ETH'], 0.5)],
         [trs_8a['BTC'], trs_8a['ETH']]),
        ("BTC 8a + ETH V1 (50/50)",
         [(eqs_8a['BTC'], 0.5), (eqs_w1['ETH'], 0.5)],
         [trs_8a['BTC'], trs_w1['ETH']]),
        ("BTC V1 + ETH V1 (50/50)",
         [(eqs_w1['BTC'], 0.5), (eqs_w1['ETH'], 0.5)],
         [trs_w1['BTC'], trs_w1['ETH']]),
        ("BTC 8a + BTC V1 + ETH 8a + ETH V1 (25/25/25/25)",
         [(eqs_8a['BTC'], 0.25), (eqs_w1['BTC'], 0.25), (eqs_8a['ETH'], 0.25), (eqs_w1['ETH'], 0.25)],
         [trs_8a['BTC'], trs_w1['BTC'], trs_8a['ETH'], trs_w1['ETH']]),
        ("All 8a: BTC+ETH+LTC+SOL (25/25/25/25)",
         [(eqs_8a['BTC'], 0.25), (eqs_8a['ETH'], 0.25),
          (eqs_8a['LTC'], 0.25), (eqs_8a['SOL'], 0.25)],
         [trs_8a['BTC'], trs_8a['ETH'], trs_8a['LTC'], trs_8a['SOL']]),
        ("All 8a 5-asset: BTC+ETH+LTC+SOL+BCH (20/20/20/20/20)",
         [(eq_8a, 0.2) for eq_8a in eqs_8a.values()],
         list(trs_8a.values())),
    ]
    for label, comps, tr_list in portfolios:
        port_eq = portfolio(comps)
        port_dd = ((port_eq.cummax() - port_eq)/port_eq.cummax()).max() * 100
        port_total = (port_eq.iloc[-1]/INITIAL - 1) * 100
        port_2026 = port_eq[port_eq.index >= pd.Timestamp(2026,1,1)]
        port_y26 = (port_2026.iloc[-1]/port_2026.iloc[0] - 1)*100 if len(port_2026)>1 else 0
        port_rec = port_eq[port_eq.index >= pd.Timestamp(2022,1,1)]
        port_recent = (port_rec.iloc[-1]/port_rec.iloc[0] - 1)*100 if len(port_rec)>1 else 0
        monthly = port_eq.resample('ME').last().pct_change().dropna()
        pct_pos_months = (monthly > 0).sum() / len(monthly) * 100 if len(monthly) else 0
        all_tr = pd.concat(tr_list)
        wr = (all_tr['pnl_pct'] > 0).sum() / len(all_tr) * 100 if len(all_tr) else 0
        print(f"\n  {label}:")
        print(f"    total={port_total:>+7.0f}%  DD={port_dd:>4.1f}%  recent4y={port_recent:>+5.0f}%  "
              f"2026={port_y26:>+6.1f}%  combined_N={len(all_tr)}  combined_WR={wr:.1f}%  posMo={pct_pos_months:.1f}%")

    # ===== Hard stop on best variants =====
    print("\n" + "=" * 150)
    print("PART 4 — Hard MAE stop applied to best strategies")
    print("=" * 150)

    for asset in ['BTC', 'ETH']:
        df = dfs[asset]
        for stop in [None, -0.15, -0.20, -0.25]:
            tr, fe, eq = strategy_8a_std30(df, hard_stop=stop)
            m = metrics(tr, fe, eq, df.index)
            label = f"{asset} 8a, stop={stop*100:.0f}%" if stop else f"{asset} 8a, no stop"
            print(f"  {label:30}  {fmt_row(m)}")
        print()
        for stop in [None, -0.15, -0.20, -0.25]:
            tr, fe, eq = strategy_wavelet_v1_weekly(df, hard_stop=stop)
            m = metrics(tr, fe, eq, df.index)
            label = f"{asset} V1, stop={stop*100:.0f}%" if stop else f"{asset} V1, no stop"
            print(f"  {label:30}  {fmt_row(m)}")
        print()


if __name__ == "__main__":
    main()
