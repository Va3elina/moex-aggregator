"""Wavelet-based strategies on BTC daily — four different concepts.

Compared to:
- A) Buy & Hold
- B) Original weekly-logic strategy (no VIX, no wavelets)

Wavelet strategies:
- W1) Trend follower: s5 AND s6 slope > 0 → long; either turns down → exit
- W2) Pullback-in-uptrend: s6 up AND s4 just flipped up → long; s4 flips down → exit
- W3) Multi-scale scoring: position size proportional to count of up-slopes across s3-s6
- W4) Original strategy + early exit when s5 slope flips down

Causal à trous Haar wavelet (no look-ahead).
Realistic costs: 0.05% fee + 0.03% slippage per side.
"""
import warnings; warnings.filterwarnings("ignore")
from pathlib import Path
import numpy as np
import pandas as pd

CSV = Path(__file__).parent / "btc_usd_daily_coinbase.csv"
FEE = 0.0005
SLIP = 0.0003
INITIAL = 100.0


# ------------------ Indicators ------------------
def ema(series, n):
    s = series.values
    out = np.full(len(s), np.nan)
    if len(s) < n: return pd.Series(out, index=series.index)
    out[n-1] = s[:n].mean()
    k = 2.0 / (n + 1.0)
    for i in range(n, len(s)):
        out[i] = s[i] * k + out[i-1] * (1 - k)
    return pd.Series(out, index=series.index)


def rsi(series, n=14):
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_g = gain.ewm(alpha=1/n, adjust=False).mean()
    avg_l = loss.ewm(alpha=1/n, adjust=False).mean()
    rs = avg_g / avg_l
    return 100 - 100 / (1 + rs)


def atrous_haar(x, J=6):
    n = len(x)
    smooths = [np.asarray(x, dtype=float).copy()]
    for j in range(1, J + 1):
        gap = 2 ** (j - 1)
        prev = smooths[-1]
        s = np.full(n, np.nan)
        for i in range(n):
            if i - gap < 0:
                s[i] = prev[i]
            else:
                s[i] = 0.5 * (prev[i] + prev[i - gap])
        smooths.append(s)
    return smooths


def slope(arr, lookback=5):
    out = np.full(len(arr), np.nan)
    for i in range(lookback, len(arr)):
        if not np.isnan(arr[i]) and not np.isnan(arr[i - lookback]):
            out[i] = arr[i] - arr[i - lookback]
    return out


# ------------------ Generic backtest engine ------------------
def backtest_signals(closes, dates, position_target, max_position=1.0):
    """position_target: array of target position fractions [0..1].
    Rebalances when target changes. Returns trades + final equity + curve."""
    cash = INITIAL
    qty = 0.0
    eq_curve = []
    trades = []
    open_entry_value = 0.0  # cash invested in current position
    open_entry_date = None
    prev_target = 0.0

    for i in range(len(closes)):
        c = closes[i]
        date = dates[i]
        equity = cash + qty * c
        eq_curve.append(equity)

        tgt = position_target[i]
        if np.isnan(tgt): continue

        # Compute desired qty
        desired_value = equity * tgt
        desired_qty = desired_value / c if c > 0 else 0

        diff_qty = desired_qty - qty
        if abs(diff_qty * c) < equity * 0.01:  # less than 1% rebalance — skip
            prev_target = tgt
            continue

        if diff_qty > 0:
            # BUY
            eff = c * (1 + SLIP)
            cost = diff_qty * eff * (1 + FEE)
            if cost > cash:
                # adjust
                diff_qty = (cash * 0.99) / (eff * (1 + FEE))
                cost = diff_qty * eff * (1 + FEE)
            cash -= cost
            if qty == 0:
                open_entry_value = cost
                open_entry_date = date
            else:
                open_entry_value += cost
            qty += diff_qty
        else:
            # SELL (partial or full)
            eff = c * (1 - SLIP)
            sell_qty = -diff_qty
            proceeds = sell_qty * eff * (1 - FEE)
            cash += proceeds
            if qty > 0:
                # Track pnl on portion sold
                cost_portion = open_entry_value * (sell_qty / qty)
                pnl_pct = (proceeds / cost_portion - 1) * 100 if cost_portion > 0 else 0
                trades.append({'entry_date': open_entry_date, 'exit_date': date,
                              'pnl_pct': pnl_pct, 'qty_frac': sell_qty / qty,
                              'hold_days': (date - open_entry_date).days})
                open_entry_value -= cost_portion
            qty -= sell_qty
            if qty < 1e-9:
                qty = 0; open_entry_value = 0; open_entry_date = None

        prev_target = tgt

    # final liquidation for stats
    if qty > 0:
        eff = closes[-1] * (1 - SLIP)
        proceeds = qty * eff * (1 - FEE)
        cash += proceeds
        pnl_pct = (proceeds / open_entry_value - 1) * 100 if open_entry_value > 0 else 0
        trades.append({'entry_date': open_entry_date, 'exit_date': dates[-1],
                      'pnl_pct': pnl_pct, 'qty_frac': 1.0,
                      'hold_days': (dates[-1] - open_entry_date).days})

    return pd.DataFrame(trades), cash, pd.Series(eq_curve, index=dates)


# ------------------ Original strategy (B) ------------------
def compute_weekly_signals(daily):
    w = daily['close'].resample('W-MON', label='left', closed='left').last().dropna()
    weekly = pd.DataFrame({'close': w})
    weekly['ema200'] = ema(weekly['close'], 200)
    weekly['ema20']  = ema(weekly['close'], 20)
    weekly['ema5']   = ema(weekly['close'], 5)
    weekly['rsi']    = rsi(weekly['close'], 14)
    weekly['rsi_sma'] = weekly['rsi'].rolling(14).mean()
    weekly['cross_below_ema200'] = (weekly['close'] < weekly['ema200']) & (weekly['close'].shift(1) >= weekly['ema200'].shift(1))
    weekly['cross_ema20_over_5'] = (weekly['ema20'] > weekly['ema5']) & (weekly['ema20'].shift(1) <= weekly['ema5'].shift(1))
    weekly['exit_rsi'] = (weekly['rsi_sma'] < 70) & (weekly['rsi_sma'].shift(1) >= 70)
    return weekly


def backtest_original(daily, early_exit_slope_neg=None):
    """Original strategy. Optional: early exit when wavelet slope turns negative."""
    weekly = compute_weekly_signals(daily)
    sig_d = weekly[['cross_below_ema200', 'cross_ema20_over_5', 'exit_rsi']].shift(1).reindex(daily.index, method='ffill')
    sig_strict = pd.DataFrame(index=daily.index)
    for col in sig_d.columns:
        prev = sig_d[col].shift(1).fillna(False)
        sig_strict[col] = sig_d[col] & (~prev)

    closes = daily['close'].values
    dates = daily.index
    cash = INITIAL
    qty = 0.0
    entries_taken = 0
    entry_value = 0.0
    entry_date = None
    eq_curve = []
    trades = []
    prev_slope_pos = False

    for i in range(len(daily)):
        c = closes[i]
        date = dates[i]
        eq = cash + qty * c
        eq_curve.append(eq)
        s = sig_strict.iloc[i]

        # Early exit on slope flip (only if positive→negative)
        early_exit_trig = False
        if early_exit_slope_neg is not None and qty > 0:
            cur_pos = early_exit_slope_neg[i]  # True if slope still positive
            if prev_slope_pos and not cur_pos:
                early_exit_trig = True
            prev_slope_pos = cur_pos if not np.isnan(cur_pos) else prev_slope_pos
        elif early_exit_slope_neg is not None:
            cur_pos = early_exit_slope_neg[i]
            prev_slope_pos = bool(cur_pos) if not np.isnan(cur_pos) else False

        if qty > 0 and (bool(s.get('exit_rsi', False)) or early_exit_trig):
            eff = c * (1 - SLIP)
            proceeds = qty * eff * (1 - FEE)
            cash += proceeds
            pnl_pct = (proceeds / entry_value - 1) * 100 if entry_value > 0 else 0
            reason = 'early_exit' if early_exit_trig else 'rsi_exit'
            trades.append({'entry_date': entry_date, 'exit_date': date,
                          'pnl_pct': pnl_pct, 'hold_days': (date - entry_date).days,
                          'reason': reason})
            qty = 0; entries_taken = 0; entry_value = 0; entry_date = None
            continue

        if qty == 0 and bool(s.get('cross_below_ema200', False)):
            spend = cash * 0.33
            eff = c * (1 + SLIP)
            buy_qty = spend / eff
            cost = buy_qty * eff * (1 + FEE)
            cash -= cost
            qty += buy_qty
            entry_value = cost
            entry_date = date
            entries_taken = 1
            continue

        if qty > 0 and entries_taken < 2 and bool(s.get('cross_ema20_over_5', False)):
            spend = cash * 0.99
            eff = c * (1 + SLIP)
            buy_qty = spend / eff
            cost = buy_qty * eff * (1 + FEE)
            if cost <= cash:
                cash -= cost
                qty += buy_qty
                entry_value += cost
                entries_taken = 2

    if qty > 0:
        eff = closes[-1] * (1 - SLIP)
        proceeds = qty * eff * (1 - FEE)
        cash += proceeds
        pnl_pct = (proceeds / entry_value - 1) * 100 if entry_value > 0 else 0
        trades.append({'entry_date': entry_date, 'exit_date': dates[-1],
                      'pnl_pct': pnl_pct, 'hold_days': (dates[-1] - entry_date).days,
                      'reason': 'final'})

    return pd.DataFrame(trades), cash, pd.Series(eq_curve, index=dates)


# ------------------ Reporting ------------------
def report(name, trades, final_eq, eq_curve):
    if len(trades) == 0:
        print(f"  {name:42}  no trades"); return None
    wr = (trades['pnl_pct'] > 0).sum() / max(len(trades), 1) * 100
    avg = trades['pnl_pct'].mean()
    max_dd = ((eq_curve.cummax() - eq_curve) / eq_curve.cummax()).max() * 100
    pos = trades[trades['pnl_pct'] > 0]['pnl_pct'].sum()
    neg = abs(trades[trades['pnl_pct'] < 0]['pnl_pct'].sum())
    pf = pos / neg if neg > 0 else float('inf')
    ret = (final_eq / INITIAL - 1) * 100
    pf_str = f"{pf:>4.2f}" if pf != float('inf') else " inf"
    print(f"  {name:42}  N={len(trades):>3}  WR={wr:>5.1f}%  PF={pf_str}  "
          f"avg={avg:>+6.2f}%  ret={ret:>+8.1f}%  DD={max_dd:>4.1f}%")
    return {'n': len(trades), 'wr': wr, 'pf': pf, 'avg': avg, 'ret': ret, 'dd': max_dd}


def main():
    print("Loading BTC daily...")
    df = pd.read_csv(CSV, parse_dates=['date']).set_index('date').sort_index()
    print(f"  {len(df)} days  {df.index[0].date()} → {df.index[-1].date()}")

    # Wavelet decomposition on log-close
    smooths = atrous_haar(np.log(df['close']).values, J=6)
    s3 = smooths[3]; s4 = smooths[4]; s5 = smooths[5]; s6 = smooths[6]
    sl3 = slope(s3, 5)
    sl4 = slope(s4, 5)
    sl5 = slope(s5, 5)
    sl6 = slope(s6, 5)
    closes = df['close'].values
    dates = df.index

    # Slope-positive arrays
    pos3 = (sl3 > 0).astype(float); pos3[np.isnan(sl3)] = np.nan
    pos4 = (sl4 > 0).astype(float); pos4[np.isnan(sl4)] = np.nan
    pos5 = (sl5 > 0).astype(float); pos5[np.isnan(sl5)] = np.nan
    pos6 = (sl6 > 0).astype(float); pos6[np.isnan(sl6)] = np.nan

    print("\n" + "=" * 90)
    print("Backtest comparison — BTC daily 2015-2026 (≈11 years)")
    print("=" * 90)

    # ---- A) Buy & Hold ----
    bh_eq_curve = (INITIAL / closes[0]) * df['close']
    bh_final = bh_eq_curve.iloc[-1]
    bh_dd = ((bh_eq_curve.cummax() - bh_eq_curve) / bh_eq_curve.cummax()).max() * 100
    bh_ret = (bh_final / INITIAL - 1) * 100
    print(f"  {'A) Buy & Hold':42}  N=  1  WR=100.0%  PF= inf  "
          f"avg={bh_ret:+.1f}%  ret={bh_ret:>+8.1f}%  DD={bh_dd:>4.1f}%")

    # ---- B) Original strategy (no VIX, no wavelet) ----
    tr_b, eq_b, eqc_b = backtest_original(df, early_exit_slope_neg=None)
    report("B) Original (weekly logic, no VIX)", tr_b, eq_b, eqc_b)

    # ---- W1) Wavelet trend follower: s5 up AND s6 up ----
    target_w1 = np.where((pos5 == 1) & (pos6 == 1), 1.0, 0.0).astype(float)
    target_w1[np.isnan(sl5) | np.isnan(sl6)] = np.nan
    tr_w1, eq_w1, eqc_w1 = backtest_signals(closes, dates, target_w1)
    report("W1) Trend follower (s5∧s6 up)", tr_w1, eq_w1, eqc_w1)

    # ---- W2) Pullback in uptrend: s6 up, s4 just flipped up ----
    # detect s4 flip from down to up while s6 up; exit when s4 flips down or s6 flips down
    target_w2 = np.zeros(len(closes), dtype=float)
    target_w2[:] = np.nan
    state = 0
    for i in range(len(closes)):
        if np.isnan(pos4[i]) or np.isnan(pos6[i]):
            target_w2[i] = 0.0; continue
        if state == 0:
            # waiting for pullback to end inside uptrend
            if pos6[i] == 1 and pos4[i] == 1 and i > 0 and pos4[i-1] == 0:
                state = 1
        elif state == 1:
            if pos4[i] == 0 or pos6[i] == 0:
                state = 0
        target_w2[i] = float(state)
    tr_w2, eq_w2, eqc_w2 = backtest_signals(closes, dates, target_w2)
    report("W2) Pullback-in-uptrend", tr_w2, eq_w2, eqc_w2)

    # ---- W3) Multi-scale scoring ----
    # Count up-slopes across s3,s4,s5,s6 → target fraction
    score = np.full(len(closes), np.nan)
    for i in range(len(closes)):
        vals = [pos3[i], pos4[i], pos5[i], pos6[i]]
        if any(np.isnan(v) for v in vals): continue
        score[i] = sum(vals)
    # Map score to position: 4→1.0, 3→0.75, 2→0.50, ≤1→0
    target_w3 = np.where(score >= 2, score / 4.0, 0.0)
    target_w3[np.isnan(score)] = np.nan
    tr_w3, eq_w3, eqc_w3 = backtest_signals(closes, dates, target_w3)
    report("W3) Multi-scale scoring (s3-s6)", tr_w3, eq_w3, eqc_w3)

    # ---- W4) Original + early exit on s5 slope flip ----
    s5_up = (sl5 > 0).astype(float)
    s5_up[np.isnan(sl5)] = np.nan
    tr_w4, eq_w4, eqc_w4 = backtest_original(df, early_exit_slope_neg=s5_up)
    report("W4) Original + early-exit s5 down", tr_w4, eq_w4, eqc_w4)

    # ---- W5) Multi-scale scoring with even higher granularity (4 scales as bins of 0/0.25/0.5/0.75/1)
    target_w5 = score / 4.0
    target_w5[np.isnan(score)] = np.nan
    tr_w5, eq_w5, eqc_w5 = backtest_signals(closes, dates, target_w5)
    report("W5) Scoring linear 0/0.25/0.5/0.75/1", tr_w5, eq_w5, eqc_w5)

    # ---- W6) Trend follower with s6 only (less strict) ----
    target_w6 = np.where(pos6 == 1, 1.0, 0.0).astype(float)
    target_w6[np.isnan(sl6)] = np.nan
    tr_w6, eq_w6, eqc_w6 = backtest_signals(closes, dates, target_w6)
    report("W6) Trend follower s6 only (64d)", tr_w6, eq_w6, eqc_w6)

    # ---- W7) Trend follower with s5 only ----
    target_w7 = np.where(pos5 == 1, 1.0, 0.0).astype(float)
    target_w7[np.isnan(sl5)] = np.nan
    tr_w7, eq_w7, eqc_w7 = backtest_signals(closes, dates, target_w7)
    report("W7) Trend follower s5 only (32d)", tr_w7, eq_w7, eqc_w7)

    # ===== Trade detail summary =====
    print("\n" + "=" * 90)
    print("Trade detail comparison")
    print("=" * 90)
    print(f"  {'variant':40}  {'N':>4}  {'avgHold':>8}  {'bigWin':>8}  {'bigLoss':>8}")
    for name, tr in [('B) Original', tr_b),
                      ('W1) s5∧s6', tr_w1),
                      ('W2) Pullback', tr_w2),
                      ('W3) Scoring 2/3/4', tr_w3),
                      ('W4) Original+early', tr_w4),
                      ('W5) Linear scoring', tr_w5),
                      ('W6) s6 only', tr_w6),
                      ('W7) s5 only', tr_w7)]:
        if len(tr) == 0: continue
        ah = tr['hold_days'].mean()
        bw = tr['pnl_pct'].max(); bl = tr['pnl_pct'].min()
        print(f"  {name:40}  {len(tr):>4}  {ah:>6.0f}d  {bw:>+7.1f}%  {bl:>+7.1f}%")


if __name__ == "__main__":
    main()
