"""Adaptation of 'Трендовушка-Универсалка' (Trend+Seasonality) for crypto.

Original logic (Russian futures):
  At 17:00 check price vs 10:30 (morning price)
  - LONG if current > morning
  - SHORT if current < morning by 2%+
  - Hold until 11:00 next day

Crypto adaptation: BTC/ETH hourly. Various session-time combinations.
"""
import warnings; warnings.filterwarnings("ignore")
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(__file__).parent
FEE, SLIP, INITIAL = 0.0005, 0.0003, 100.0


def load_hourly(asset):
    name = f"{asset.lower()}_usd_hourly_coinbase.csv"
    df = pd.read_csv(ROOT/name, parse_dates=['datetime']).set_index('datetime').sort_index()
    df.index = df.index.tz_localize(None)
    return df[df['close']>0].dropna(subset=['close'])


def backtest_session_strategy(df, ref_hour, decision_hour, exit_hour,
                                short_threshold_pct=2.0, allow_short=True,
                                long_threshold_pct=0.0):
    """At decision_hour each day, compare close to ref_hour close.
    If price > ref by long_threshold: LONG.
    If price < ref by short_threshold: SHORT.
    Exit at exit_hour next day."""
    closes = df['close'].values
    highs = df['high'].values
    lows = df['low'].values
    dates = df.index
    hours_arr = df.index.hour.values
    dates_arr = df.index.date

    # Pre-build {date → ref_price} O(N) lookup
    ref_mask = hours_arr == ref_hour
    ref_dates = dates_arr[ref_mask]
    ref_prices = closes[ref_mask]
    ref_lookup = dict(zip(ref_dates, ref_prices))

    n = len(closes)
    cash = INITIAL
    qty = 0.0  # positive = long, negative = short
    eq = np.zeros(n)
    trades = []
    entry_px = 0.0
    entry_date = None
    intra_min = 0
    intra_max = 0

    for i in range(n):
        c = closes[i]; h = highs[i]; l = lows[i]
        # Mark to market
        if qty > 0:
            eq[i] = cash + qty*c
            if intra_min == 0 or l < intra_min: intra_min = l
            if h > intra_max: intra_max = h
        elif qty < 0:
            eq[i] = cash + abs(qty) * (entry_px - c) + abs(qty) * entry_px  # mtm: cash + (entry-current)*shares
            if intra_min == 0 or l < intra_min: intra_min = l
            if h > intra_max: intra_max = h
        else:
            eq[i] = cash

        cur_hour = hours_arr[i]

        # EXIT signal: at exit_hour AND we have position
        if cur_hour == exit_hour and qty != 0:
            if qty > 0:
                eff = c * (1 - SLIP)
                proceeds = qty * eff * (1 - FEE)
                cash += proceeds
                pnl_pct = (eff/entry_px - 1) * 100
                mae = (intra_min/entry_px - 1) * 100
            else:
                eff = c * (1 + SLIP)
                # short: profit = (entry - exit) per share
                profit = abs(qty) * (entry_px - eff) - abs(qty) * eff * FEE
                cash += profit + abs(qty) * entry_px  # release principal
                pnl_pct = (entry_px/eff - 1) * 100
                mae = -(intra_max/entry_px - 1) * 100
            trades.append({'entry_date': entry_date, 'exit_date': dates[i],
                          'side': 'long' if qty > 0 else 'short',
                          'pnl_pct': pnl_pct, 'mae_pct': mae,
                          'hold_h': (dates[i] - entry_date).total_seconds()/3600})
            qty = 0; entry_px = 0; entry_date = None; intra_min = 0; intra_max = 0

        # ENTRY signal: at decision_hour AND we are flat
        if cur_hour == decision_hour and qty == 0:
            today = dates_arr[i]
            if today not in ref_lookup:
                continue
            ref_price = ref_lookup[today]
            price_change_pct = (c / ref_price - 1) * 100

            if price_change_pct > long_threshold_pct:
                # LONG
                eff = c * (1 + SLIP)
                spend = cash * 0.99
                qty = spend / eff
                cash -= spend + spend * FEE
                cash = max(cash, 0)
                entry_px = eff
                entry_date = dates[i]
                intra_min = l; intra_max = h
            elif allow_short and price_change_pct < -short_threshold_pct:
                # SHORT
                eff = c * (1 - SLIP)
                spend = cash * 0.99
                shares = spend / eff
                qty = -shares
                cash -= spend * FEE  # keep cash; principal locked
                entry_px = eff
                entry_date = dates[i]
                intra_min = l; intra_max = h

    # Close any open position
    if qty != 0:
        c = closes[-1]
        if qty > 0:
            eff = c * (1 - SLIP)
            proceeds = qty * eff * (1 - FEE)
            cash += proceeds
            pnl_pct = (eff/entry_px - 1) * 100
        else:
            eff = c * (1 + SLIP)
            profit = abs(qty) * (entry_px - eff) - abs(qty) * eff * FEE
            cash += profit + abs(qty) * entry_px
            pnl_pct = (entry_px/eff - 1) * 100
        trades.append({'entry_date': entry_date, 'exit_date': dates[-1],
                      'side': 'long' if qty > 0 else 'short',
                      'pnl_pct': pnl_pct, 'mae_pct': 0,
                      'hold_h': (dates[-1] - entry_date).total_seconds()/3600})

    return pd.DataFrame(trades), cash, pd.Series(eq, index=dates)


def metrics(tr, fe, eq):
    if len(tr) == 0:
        return None
    total = (fe/INITIAL - 1) * 100
    dd = ((eq.cummax() - eq)/eq.cummax()).max() * 100
    wr = (tr['pnl_pct']>0).sum()/len(tr) * 100
    pos = tr[tr['pnl_pct']>0]['pnl_pct'].sum()
    neg = abs(tr[tr['pnl_pct']<0]['pnl_pct'].sum())
    pf = pos/neg if neg > 0 else float('inf')
    eq_26 = eq[eq.index >= pd.Timestamp(2026,1,1)]
    y26 = (eq_26.iloc[-1]/eq_26.iloc[0] - 1)*100 if len(eq_26)>1 else 0
    eq_rec = eq[eq.index >= pd.Timestamp(2022,1,1)]
    rec = (eq_rec.iloc[-1]/eq_rec.iloc[0] - 1)*100 if len(eq_rec)>1 else 0
    n_long = (tr['side'] == 'long').sum() if 'side' in tr.columns else 0
    n_short = (tr['side'] == 'short').sum() if 'side' in tr.columns else 0
    return {'N': len(tr), 'WR': wr, 'PF': pf, 'total': total, 'dd': dd,
            'rec4y': rec, 'y26': y26, 'n_long': n_long, 'n_short': n_short}


def report(label, m):
    if m is None:
        print(f"  {label:60}  (no trades)"); return
    pf_str = f"{m['PF']:>5.2f}" if m['PF'] != float('inf') else "  inf"
    print(f"  {label:60}  N={m['N']:>4} (L={m['n_long']:>3}/S={m['n_short']:>3})  WR={m['WR']:>4.1f}%  PF={pf_str}  total={m['total']:>+7.0f}%  DD={m['dd']:>4.1f}%  rec4y={m['rec4y']:>+5.0f}%  2026={m['y26']:>+6.1f}%")


def main():
    btc = load_hourly('BTC')
    eth = load_hourly('ETH')

    for asset_name, df in [('BTC', btc), ('ETH', eth)]:
        bh = INITIAL / df['close'].iloc[0] * df['close']
        bh_total = (bh.iloc[-1]/INITIAL - 1) * 100
        bh_dd = ((bh.cummax() - bh)/bh.cummax()).max() * 100
        bh_26 = bh[bh.index >= pd.Timestamp(2026,1,1)]
        bh_26_ret = (bh_26.iloc[-1]/bh_26.iloc[0] - 1)*100 if len(bh_26)>1 else 0

        print(f"\n{'='*180}")
        print(f"{asset_name} hourly — Трендовушка-Универсалка adapted")
        print(f"  Buy & Hold: total={bh_total:+.0f}%, DD={bh_dd:.0f}%, 2026={bh_26_ret:+.1f}%")
        print(f"{'='*180}")

        # Test various session combinations
        # (ref_hour, decision_hour, exit_hour, label)
        configs = [
            (0,  16, 8,  "Daily 00→16→next 08 (full day)"),
            (8,  16, 8,  "Asian→US→next morning (8→16→next 8)"),
            (13, 20, 8,  "US open→US close→Asian next (13→20→8)"),
            (0,  20, 8,  "Daily start→US close→next morning (0→20→8)"),
            (8,  20, 8,  "Asian end→US end→Asian next (8→20→8)"),
            (12, 16, 0,  "Noon UTC→US peak→midnight (12→16→0)"),
            (10, 17, 11, "Match original: 10→17→11 next"),
        ]

        for ref_h, dec_h, exit_h, label in configs:
            for short_thr in [2.0, 3.0, 5.0]:
                for allow_s in [False, True]:
                    if allow_s:
                        cfg_label = f"{label}  shortIf<-{short_thr}%"
                    else:
                        cfg_label = f"{label}  LONG-ONLY"
                    tr, fe, eq = backtest_session_strategy(
                        df, ref_h, dec_h, exit_h,
                        short_threshold_pct=short_thr,
                        allow_short=allow_s)
                    m = metrics(tr, fe, eq)
                    if m and m['N'] > 10:
                        report(cfg_label, m)
                if not allow_s:
                    break  # don't repeat long-only for different short_thr


if __name__ == "__main__":
    main()
