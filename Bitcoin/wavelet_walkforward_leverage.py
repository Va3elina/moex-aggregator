"""Walk-forward + leverage analysis for V4 (BTC) and V6 (ETH) pure-signal champions.

Walk-forward methodology:
  - Run V4/V6 across full history with no re-optimization (rule-based)
  - Slice into rolling 12-month windows
  - Compute per-window: return, DD, N trades, WR
  - Report: % of windows positive, worst window, best window, time underwater
  - Per-year and per-quarter breakdowns

Leverage tests:
  - Same V4/V6 with leverage 1x, 2x, 3x, 5x
  - Check: liquidations, max DD per trade, account DD
"""
import warnings; warnings.filterwarnings("ignore")
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(__file__).parent
FEE, SLIP, INITIAL = 0.0005, 0.0003, 100.0


def ema(arr, n):
    out = np.full(len(arr), np.nan)
    if len(arr) < n: return out
    out[n-1] = arr[:n].mean()
    k = 2.0/(n+1.0)
    for i in range(n, len(arr)):
        out[i] = arr[i]*k + out[i-1]*(1-k)
    return out

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

def load_daily(asset):
    name = "btc_usd_daily_coinbase.csv" if asset == 'BTC' else "eth_usd_daily_coinbase.csv"
    df = pd.read_csv(ROOT/name, parse_dates=['date']).set_index('date').sort_index()
    return df[df['close']>0].dropna(subset=['close'])


def precompute(df, btc_daily=None):
    log_c = np.log(df['close'].values)
    sm = atrous_haar(log_c, J=6)
    p = {
        'log_c': log_c, 'close': df['close'].values,
        'high': df['high'].values, 'low': df['low'].values,
        'index': df.index,
        's4': sm[4], 's5': sm[5], 's6': sm[6],
        'sl4': slope_a(sm[4], 5), 'sl5': slope_a(sm[5], 5), 'sl6': slope_a(sm[6], 5),
    }
    if btc_daily is not None:
        btc_close = btc_daily['close']
        btc_ema200 = pd.Series(ema(btc_close.values, 200), index=btc_daily.index).shift(1)
        btc_close_shift = btc_close.shift(1)
        p['btc_close'] = btc_close_shift.reindex(df.index, method='ffill').values
        p['btc_ema200'] = btc_ema200.reindex(df.index, method='ffill').values
    return p


def build_signals(p):
    log_c, s4, s5, s6 = p['log_c'], p['s4'], p['s5'], p['s6']
    sl5, sl6 = p['sl5'], p['sl6']
    n = len(log_c)

    e1 = np.zeros(n, dtype=bool)
    for i in range(1, n):
        if not (np.isnan(log_c[i-1]) or np.isnan(s5[i-1])):
            if log_c[i-1] <= s5[i-1] and log_c[i] > s5[i]:
                e1[i] = True

    upper = s5 + 2 * (s5 - s6)
    e2 = np.zeros(n, dtype=bool)
    for i in range(1, n):
        if not (np.isnan(upper[i-1]) or np.isnan(upper[i])):
            if log_c[i-1] <= upper[i-1] and log_c[i] > upper[i]:
                e2[i] = True

    e3 = np.zeros(n, dtype=bool)
    prev_sl5 = np.r_[np.nan, sl5[:-1]]
    for i in range(1, n):
        if not (np.isnan(sl5[i]) or np.isnan(sl6[i]) or np.isnan(prev_sl5[i])):
            if sl6[i] > 0 and prev_sl5[i] <= 0 and sl5[i] > 0:
                e3[i] = True

    x1 = log_c < s5
    x2 = sl6 < 0

    fdawn = np.zeros(n, dtype=bool)
    for i in range(1, n):
        if not (np.isnan(sl6[i]) or np.isnan(log_c[i]) or np.isnan(s5[i])):
            if sl6[i-1] <= 0 and sl6[i] > 0 and log_c[i] < s5[i]:
                fdawn[i] = True

    bearish = (s5 < s6) & (sl6 < 0)
    es1 = np.zeros(n, dtype=bool)
    for i in range(1, n):
        if bearish[i] and not bearish[i-1]:
            es1[i] = True

    return {
        'E1_price_above_s5': e1,
        'E2_overextension': e2,
        'E3_pullback': e3,
        'X1_price_below_s5': x1,
        'X2_slope6_neg': x2,
        'F_false_dawn': fdawn,
        'ES1_bearish_align': es1,
        'ES2_false_dawn_short': fdawn,
    }


def state_machine_long_short(entry_long, exit_long, entry_short, exit_short, block_long=None, allow_short=True):
    n = len(entry_long)
    state = np.zeros(n); s = 0
    for i in range(n):
        if s == 1:
            if exit_long[i]: s = 0
        elif s == -1:
            if exit_short[i]: s = 0
        if s == 0:
            blocked = block_long[i] if block_long is not None else False
            if entry_long[i] and not blocked:
                s = 1
            elif allow_short and entry_short[i]:
                s = -1
        state[i] = float(s)
    return state


def v4_btc_target(sigs, p):
    """V4: E2 only (extreme overextension) long-only for BTC."""
    entry = sigs['E2_overextension']
    exit_ = sigs['X1_price_below_s5'] | sigs['X2_slope6_neg']
    no_short = np.zeros(len(entry), dtype=bool)
    return state_machine_long_short(entry, exit_, no_short, no_short, allow_short=False)


def v6_eth_target(sigs, p):
    """V6: ETH long+short with BTC macro filter."""
    entry_l = sigs['E1_price_above_s5'] | sigs['E2_overextension'] | sigs['E3_pullback']
    exit_l = sigs['X1_price_below_s5'] | sigs['X2_slope6_neg']
    entry_s = sigs['ES1_bearish_align'] | sigs['ES2_false_dawn_short']
    exit_s = sigs['E1_price_above_s5'] | sigs['E2_overextension']
    block_l = sigs['F_false_dawn']
    btc_bull = (p['btc_close'] > p['btc_ema200'])
    btc_bear = ~btc_bull
    entry_l = entry_l & btc_bull
    entry_s = entry_s & btc_bear
    return state_machine_long_short(entry_l, exit_l, entry_s, exit_s, block_long=block_l, allow_short=True)


def backtest(p, target, leverage=1.0):
    closes = p['close']; highs = p['high']; lows = p['low']; dates = p['index']
    n = len(closes)
    cash = INITIAL; units = 0.0; side = 0
    eq = np.zeros(n); trades = []
    entry_px = 0.0; entry_date = None
    intra_min = 0; intra_max = 0
    liquidations = 0

    for i in range(n):
        c = closes[i]; h = highs[i]; l = lows[i]
        if side == 1:
            mtm = (c - entry_px) * units
            equity = cash + mtm
            if intra_min == 0 or l < intra_min: intra_min = l
            if h > intra_max: intra_max = h
        elif side == -1:
            mtm = (entry_px - c) * units
            equity = cash + mtm
            if intra_min == 0 or l < intra_min: intra_min = l
            if h > intra_max: intra_max = h
        else:
            equity = cash
        eq[i] = max(equity, 0.001)

        # Liquidation check
        if side == 1 and leverage > 1:
            liq_px = entry_px * (1 - 1.0/leverage + 0.005)
            if l <= liq_px:
                eff = liq_px
                pnl = (eff - entry_px) * units
                cash += pnl
                cash = max(cash, 0.001)
                pnl_pct = -100.0 / leverage * leverage  # =-100% from leverage's perspective
                pnl_pct = (eff/entry_px - 1) * 100 * leverage
                mae = (intra_min/entry_px - 1) * 100 * leverage
                trades.append({'side':'long_LIQ', 'entry_date': entry_date, 'exit_date': dates[i],
                              'entry_px': entry_px, 'exit_px': eff,
                              'pnl_pct': pnl_pct, 'mae_pct': mae,
                              'hold_d': (dates[i] - entry_date).total_seconds()/86400})
                liquidations += 1
                side = 0; units = 0; entry_px = 0; intra_min = 0; intra_max = 0
                continue
        elif side == -1 and leverage > 1:
            liq_px = entry_px * (1 + 1.0/leverage - 0.005)
            if h >= liq_px:
                eff = liq_px
                pnl = (entry_px - eff) * units
                cash += pnl
                cash = max(cash, 0.001)
                pnl_pct = (entry_px/eff - 1) * 100 * leverage
                mae = -(intra_max/entry_px - 1) * 100 * leverage
                trades.append({'side':'short_LIQ', 'entry_date': entry_date, 'exit_date': dates[i],
                              'entry_px': entry_px, 'exit_px': eff,
                              'pnl_pct': pnl_pct, 'mae_pct': mae,
                              'hold_d': (dates[i] - entry_date).total_seconds()/86400})
                liquidations += 1
                side = 0; units = 0; entry_px = 0; intra_min = 0; intra_max = 0
                continue

        tgt = target[i]
        if side == 1 and tgt != 1:
            eff = c * (1 - SLIP)
            pnl = (eff - entry_px) * units - units * eff * FEE
            cash += pnl
            pnl_pct = (eff/entry_px - 1) * 100 * leverage
            mae = (intra_min/entry_px - 1) * 100 * leverage
            trades.append({'side':'long', 'entry_date': entry_date, 'exit_date': dates[i],
                          'entry_px': entry_px, 'exit_px': eff,
                          'pnl_pct': pnl_pct, 'mae_pct': mae,
                          'hold_d': (dates[i] - entry_date).total_seconds()/86400})
            side = 0; units = 0; entry_px = 0; intra_min = 0; intra_max = 0
        elif side == -1 and tgt != -1:
            eff = c * (1 + SLIP)
            pnl = (entry_px - eff) * units - units * eff * FEE
            cash += pnl
            pnl_pct = (entry_px/eff - 1) * 100 * leverage
            mae = -(intra_max/entry_px - 1) * 100 * leverage
            trades.append({'side':'short', 'entry_date': entry_date, 'exit_date': dates[i],
                          'entry_px': entry_px, 'exit_px': eff,
                          'pnl_pct': pnl_pct, 'mae_pct': mae,
                          'hold_d': (dates[i] - entry_date).total_seconds()/86400})
            side = 0; units = 0; entry_px = 0; intra_min = 0; intra_max = 0

        if tgt == 1 and side == 0:
            eff = c * (1 + SLIP)
            units = (cash * 0.99 * leverage) / eff
            fee = units * eff * FEE
            cash -= fee
            entry_px = eff; entry_date = dates[i]; side = 1
            intra_min = l; intra_max = h
        elif tgt == -1 and side == 0:
            eff = c * (1 - SLIP)
            units = (cash * 0.99 * leverage) / eff
            fee = units * eff * FEE
            cash -= fee
            entry_px = eff; entry_date = dates[i]; side = -1
            intra_min = l; intra_max = h

    if side != 0:
        c = closes[-1]
        if side == 1:
            eff = c * (1 - SLIP)
            pnl = (eff - entry_px) * units - units * eff * FEE
        else:
            eff = c * (1 + SLIP)
            pnl = (entry_px - eff) * units - units * eff * FEE
        cash += pnl
        pnl_pct = ((eff/entry_px - 1) if side == 1 else (entry_px/eff - 1)) * 100 * leverage
        mae = (intra_min/entry_px - 1) * 100 * leverage if side == 1 else -(intra_max/entry_px - 1) * 100 * leverage
        trades.append({'side': 'long' if side == 1 else 'short',
                       'entry_date': entry_date, 'exit_date': dates[-1],
                       'entry_px': entry_px, 'exit_px': eff,
                       'pnl_pct': pnl_pct, 'mae_pct': mae,
                       'hold_d': (dates[-1] - entry_date).total_seconds()/86400})

    return pd.DataFrame(trades), cash, pd.Series(eq, index=dates), liquidations


def rolling_returns(eq, window_days=365):
    """Compute rolling N-day returns for each bar."""
    rets = []
    for i in range(len(eq)):
        target_date = eq.index[i] - pd.Timedelta(days=window_days)
        past = eq[eq.index <= target_date]
        if len(past) > 0:
            r = (eq.iloc[i] / past.iloc[-1] - 1) * 100
            rets.append((eq.index[i], r))
    return pd.Series([r[1] for r in rets], index=[r[0] for r in rets])


def time_underwater(eq):
    """Longest streak below the previous peak."""
    peak = eq.iloc[0]
    longest_streak = 0; current_streak = 0
    longest_start = eq.index[0]
    cur_start = eq.index[0]
    for i in range(len(eq)):
        if eq.iloc[i] >= peak:
            peak = eq.iloc[i]
            current_streak = 0
            cur_start = eq.index[i]
        else:
            current_streak += 1
            if current_streak > longest_streak:
                longest_streak = current_streak
                longest_start = cur_start
    return longest_streak, longest_start


def walk_forward_report(name, p, target_fn, leverages=(1.0, 2.0, 3.0, 5.0)):
    sigs = build_signals(p)
    target = target_fn(sigs, p)
    dates = p['index']

    print(f"\n{'='*150}")
    print(f"{name} — Walk-forward + leverage analysis")
    print(f"{'='*150}")

    for lev in leverages:
        tr, fe, eq, liq = backtest(p, target, leverage=lev)
        total_ret = (fe/INITIAL - 1) * 100
        total_dd = ((eq.cummax() - eq) / eq.cummax()).max() * 100
        wr = (tr['pnl_pct'] > 0).sum() / max(len(tr), 1) * 100
        pos = tr[tr['pnl_pct']>0]['pnl_pct'].sum()
        neg = abs(tr[tr['pnl_pct']<0]['pnl_pct'].sum())
        pf = pos/neg if neg > 0 else float('inf')
        mae_worst = tr['mae_pct'].min() if len(tr) > 0 else 0
        n_long = len(tr[tr['side'].str.startswith('long')]) if len(tr) > 0 else 0
        n_short = len(tr[tr['side'].str.startswith('short')]) if len(tr) > 0 else 0

        # Rolling 1-year returns
        roll_1y = rolling_returns(eq, 365)
        if len(roll_1y) > 0:
            pct_pos_1y = (roll_1y > 0).sum() / len(roll_1y) * 100
            worst_1y = roll_1y.min(); best_1y = roll_1y.max(); median_1y = roll_1y.median()
        else:
            pct_pos_1y = 0; worst_1y = 0; best_1y = 0; median_1y = 0

        # Time underwater
        underwater_days, underwater_start = time_underwater(eq)

        print(f"\n  Leverage {lev}x:")
        print(f"    Trades: N={len(tr)} ({n_long} long / {n_short} short)  |  WR={wr:.1f}%  PF={pf:.2f}  |  LIQUIDATIONS: {liq}")
        print(f"    Total: ret={total_ret:+.0f}%  DD={total_dd:.1f}%  mae_worst={mae_worst:+.1f}%")
        print(f"    Rolling 1Y: %positive={pct_pos_1y:.1f}%  worst1y={worst_1y:+.1f}%  median1y={median_1y:+.1f}%  best1y={best_1y:+.1f}%")
        print(f"    Time underwater: {underwater_days} bars (started {underwater_start.date()})")

    # Detailed year-by-year for 1x and 2x
    for lev in [1.0, 2.0]:
        tr, fe, eq, liq = backtest(p, target, leverage=lev)
        print(f"\n  {name} @ leverage {lev}x — per year:")
        print(f"    {'year':>4}  {'ret%':>8}  {'DD%':>5}  {'N':>3}  {'L/S':>5}  {'worstT%':>8}  {'worstMAE%':>9}  {'WR%':>5}")
        for year in dates.year.unique():
            year_mask = (dates.year == year)
            if year_mask.sum() < 5: continue
            sub_eq = eq[year_mask]
            if len(sub_eq) < 2: continue
            ret = (sub_eq.iloc[-1]/sub_eq.iloc[0] - 1) * 100
            dd = ((sub_eq.cummax() - sub_eq)/sub_eq.cummax()).max() * 100
            year_start = pd.Timestamp(year, 1, 1); year_end = pd.Timestamp(year, 12, 31)
            year_tr = tr[(tr['exit_date'] >= year_start) & (tr['exit_date'] <= year_end)] if len(tr) > 0 else pd.DataFrame()
            n_t = len(year_tr)
            n_l = year_tr['side'].str.startswith('long').sum() if n_t else 0
            n_s = year_tr['side'].str.startswith('short').sum() if n_t else 0
            worst_t = year_tr['pnl_pct'].min() if n_t else 0
            worst_mae = year_tr['mae_pct'].min() if n_t else 0
            wr_y = (year_tr['pnl_pct'] > 0).sum() / max(n_t, 1) * 100
            print(f"    {year}  {ret:>+7.1f}%  {dd:>4.1f}%  {n_t:>3}  {n_l}/{n_s:<3}  {worst_t:>+7.1f}%  {worst_mae:>+8.1f}%  {wr_y:>4.0f}")

    # Rolling 2-year windows — walk-forward style
    print(f"\n  {name} @ leverage 1x — rolling 2-year window returns:")
    print(f"    {'start':>10}  {'end':>10}  {'ret%':>8}  {'DD%':>5}  {'N':>3}  {'WR%':>5}")
    tr, fe, eq, liq = backtest(p, target, leverage=1.0)
    start_year = dates.year.min(); end_year = dates.year.max()
    for sy in range(start_year, end_year-1):
        ey = sy + 2
        ws = pd.Timestamp(sy, 1, 1); we = pd.Timestamp(ey, 12, 31)
        eq_w = eq[(eq.index >= ws) & (eq.index <= we)]
        if len(eq_w) < 30: continue
        ret = (eq_w.iloc[-1]/eq_w.iloc[0] - 1) * 100
        dd = ((eq_w.cummax() - eq_w)/eq_w.cummax()).max() * 100
        tr_w = tr[(tr['exit_date'] >= ws) & (tr['exit_date'] <= we)] if len(tr) > 0 else pd.DataFrame()
        n_t = len(tr_w)
        wr_w = (tr_w['pnl_pct'] > 0).sum() / max(n_t, 1) * 100
        print(f"    {ws.date()}  {we.date()}  {ret:>+7.0f}%  {dd:>4.1f}%  {n_t:>3}  {wr_w:>4.0f}")


def main():
    btc = load_daily('BTC')
    eth = load_daily('ETH')

    p_btc = precompute(btc, btc_daily=btc)
    p_eth = precompute(eth, btc_daily=btc)

    walk_forward_report('BTC V4 (extreme overextension)', p_btc, v4_btc_target)
    walk_forward_report('ETH V6 (long+short + BTC macro)', p_eth, v6_eth_target)


if __name__ == "__main__":
    main()
