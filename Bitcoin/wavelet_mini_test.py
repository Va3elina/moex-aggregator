"""Mini-test: causal à trous Haar wavelet decomposition on BTC daily.

Goal: see whether adding a multi-scale wavelet filter to the user's strategy
(weekly logic on daily engine: EMA200 cross + EMA20>EMA5 + RSI exit) improves
performance.

Variants compared:
  A) Buy & Hold
  B) Original strategy (no VIX, pure EMA + RSI exit)
  C) Original + filter "slow wavelet scale slope > 0"
  D) Original + filter "all three scales agree (slow + medium up)"

Also: FFT spectrum of BTC daily log-returns to see dominant cycles.
"""
import warnings; warnings.filterwarnings("ignore")
from pathlib import Path
import numpy as np
import pandas as pd

CSV = Path(__file__).parent / "btc_usd_daily_coinbase.csv"
FEE = 0.0005      # 0.05% per side
SLIP = 0.0003     # 0.03% per side
INITIAL = 100.0


# ------------------ Indicators ------------------
def ema(series, n):
    """TV-faithful EMA: SMA seed at n-1, then EMA from n."""
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


# ------------------ À trous Haar wavelet (causal) ------------------
def atrous_haar(x, J=6):
    """Causal à trous decomposition with Haar filter.
    Returns: smooths[0..J] where smooths[0]=x, smooths[J] is smoothest.
    Each level j has characteristic scale ~2^j bars (looking only at past).
    """
    n = len(x)
    smooths = [np.asarray(x, dtype=float).copy()]
    for j in range(1, J + 1):
        gap = 2 ** (j - 1)
        prev = smooths[-1]
        s = np.full(n, np.nan)
        for i in range(n):
            if i - gap < 0:
                s[i] = prev[i]  # left padding: hold value
            else:
                s[i] = 0.5 * (prev[i] + prev[i - gap])
        smooths.append(s)
    return smooths


def slope(arr, lookback=5):
    """Simple slope: arr[i] - arr[i - lookback], over last `lookback` bars."""
    out = np.full(len(arr), np.nan)
    for i in range(lookback, len(arr)):
        if not np.isnan(arr[i]) and not np.isnan(arr[i - lookback]):
            out[i] = arr[i] - arr[i - lookback]
    return out


# ------------------ FFT spectrum ------------------
def fft_spectrum(returns, top_n=10):
    """Return top N dominant periods (in days) from FFT of returns."""
    r = returns.dropna().values
    r = r - r.mean()
    fft = np.fft.rfft(r)
    freqs = np.fft.rfftfreq(len(r), d=1.0)  # cycles per day
    power = np.abs(fft) ** 2
    # exclude DC and very long periods (low resolution)
    mask = (freqs > 1 / 365)  # period < 1 year
    idx = np.argsort(power[mask])[::-1][:top_n]
    sub_freqs = freqs[mask][idx]
    sub_pow = power[mask][idx]
    periods = 1.0 / sub_freqs
    return periods, sub_pow


# ------------------ Weekly logic (Pine equivalent) ------------------
def compute_weekly_signals(daily):
    """Resample daily → weekly. Compute EMA200, EMA20, EMA5, RSI, SMA(RSI).
    Return weekly DF + cross signals."""
    w = daily['close'].resample('W-MON', label='left', closed='left').last().dropna()
    weekly = pd.DataFrame({'close': w})
    weekly['ema200'] = ema(weekly['close'], 200)
    weekly['ema20']  = ema(weekly['close'], 20)
    weekly['ema5']   = ema(weekly['close'], 5)
    weekly['rsi']    = rsi(weekly['close'], 14)
    weekly['rsi_sma'] = weekly['rsi'].rolling(14).mean()
    # crosses (Pine ta.crossunder/over)
    weekly['cross_below_ema200'] = (weekly['close'] < weekly['ema200']) & (weekly['close'].shift(1) >= weekly['ema200'].shift(1))
    weekly['cross_ema20_over_5'] = (weekly['ema20'] > weekly['ema5']) & (weekly['ema20'].shift(1) <= weekly['ema5'].shift(1))
    weekly['exit_rsi'] = (weekly['rsi_sma'] < 70) & (weekly['rsi_sma'].shift(1) >= 70)
    return weekly


def forward_fill_weekly_to_daily(weekly, daily):
    """For each daily bar, get the most recent COMPLETED weekly value."""
    # shift weekly by 1 to avoid look-ahead: signal fires on day after week closes
    w_shifted = weekly.shift(1)
    aligned = w_shifted.reindex(daily.index, method='ffill')
    return aligned


# ------------------ Backtest ------------------
def backtest(daily, signals, wave_filter=None):
    """LONG-only. Entry: cross_below_ema200 (33%) or cross_ema20_over_5 (67% add).
    Exit: exit_rsi.
    wave_filter: optional boolean series (daily-aligned) — block entries when False.
    Simplification: ignore VIX (we don't have intraday VIX in CSV).
    """
    closes = daily['close'].values
    dates = daily.index
    sig = signals

    cash = INITIAL
    pos = 0.0  # units of BTC
    entries_taken = 0  # 0, 1, 2 (third would be VIX which we skip)
    eq_curve = []
    trades = []
    entry_price = 0.0
    entry_date = None

    for i in range(len(daily)):
        c = closes[i]
        date = dates[i]
        # Compute current equity
        equity = cash + pos * c
        eq_curve.append(equity)

        s = sig.iloc[i]
        filt_ok = True if wave_filter is None else bool(wave_filter.iloc[i])

        # Exit
        if pos > 0 and bool(s.get('exit_rsi', False)):
            proceeds = pos * c * (1 - SLIP) * (1 - FEE)
            cash += proceeds
            pnl_pct = (proceeds / (entry_price * pos) - 1) * 100 if entry_price > 0 else 0
            trades.append({'entry_date': entry_date, 'exit_date': date, 'pnl_pct': pnl_pct,
                           'hold_days': (date - entry_date).days, 'reason': 'rsi_exit'})
            pos = 0.0; entries_taken = 0; entry_price = 0.0; entry_date = None
            continue

        # Entry 1: cross_below_ema200 (33%)
        if pos == 0 and bool(s.get('cross_below_ema200', False)) and filt_ok:
            spend = cash * 0.33
            eff_price = c * (1 + SLIP)
            qty = spend / eff_price
            cost = qty * eff_price * (1 + FEE)
            cash -= cost
            pos += qty
            # weighted avg entry
            entry_price = eff_price
            entry_date = date
            entries_taken = 1
            continue

        # Entry 2: cross_ema20_over_5 (67% add)
        if pos > 0 and entries_taken < 2 and bool(s.get('cross_ema20_over_5', False)) and filt_ok:
            spend = cash * (0.67 / (1 - 0.33))  # spend the remaining 67% of initial capital portion
            eff_price = c * (1 + SLIP)
            qty = spend / eff_price
            cost = qty * eff_price * (1 + FEE)
            if cost <= cash:
                cash -= cost
                # weighted avg entry
                total_value = entry_price * pos + eff_price * qty
                pos += qty
                entry_price = total_value / pos
                entries_taken = 2

    # final equity
    if pos > 0:
        proceeds = pos * closes[-1] * (1 - SLIP) * (1 - FEE)
        cash += proceeds
        pnl_pct = (closes[-1] / entry_price - 1) * 100
        trades.append({'entry_date': entry_date, 'exit_date': dates[-1], 'pnl_pct': pnl_pct,
                       'hold_days': (dates[-1] - entry_date).days, 'reason': 'final'})

    final_equity = cash
    return pd.DataFrame(trades), final_equity, pd.Series(eq_curve, index=dates)


# ------------------ Reporting ------------------
def report(name, trades, final_eq, eq_curve, bh_final):
    if len(trades) == 0:
        print(f"  {name:40}  no trades"); return
    wr = (trades['pnl_pct'] > 0).sum() / len(trades) * 100
    avg = trades['pnl_pct'].mean()
    max_dd = ((eq_curve.cummax() - eq_curve) / eq_curve.cummax()).max() * 100
    pos = trades[trades['pnl_pct'] > 0]['pnl_pct'].sum()
    neg = abs(trades[trades['pnl_pct'] < 0]['pnl_pct'].sum())
    pf = pos / neg if neg > 0 else float('inf')
    ret = (final_eq / INITIAL - 1) * 100
    print(f"  {name:40}  N={len(trades):>3}  WR={wr:>4.1f}%  PF={pf:>4.2f}  "
          f"avg={avg:>+5.2f}%  ret={ret:>+7.1f}%  DD={max_dd:>4.1f}%")


def main():
    print("Loading BTC daily...")
    df = pd.read_csv(CSV, parse_dates=['date']).set_index('date').sort_index()
    print(f"  {len(df)} days  {df.index[0].date()} → {df.index[-1].date()}")

    # ===== FFT spectrum =====
    print("\n" + "=" * 80)
    print("FFT spectrum — dominant cycles in BTC daily log-returns")
    print("=" * 80)
    log_ret = np.log(df['close']).diff()
    periods, powers = fft_spectrum(log_ret, top_n=12)
    print(f"  {'rank':>4}  {'period (days)':>14}  {'period (months)':>16}  {'relative power':>15}")
    max_p = powers.max()
    for i, (p, pw) in enumerate(zip(periods, powers), 1):
        print(f"  {i:>4}  {p:>13.1f}  {p/30:>15.2f}  {pw/max_p*100:>14.1f}%")
    print(f"\n  Interpretation: BTC has no single dominant cycle. Top peaks are scattered →")
    print(f"  series is non-stationary (FFT extrapolation pointless, as expected).")

    # ===== Wavelet decomposition =====
    print("\n" + "=" * 80)
    print("Wavelet decomposition (à trous Haar, causal, 6 levels)")
    print("=" * 80)
    smooths = atrous_haar(np.log(df['close']).values, J=6)
    # Map level → characteristic scale: 2,4,8,16,32,64 days
    df_w = df.copy()
    for j in range(1, 7):
        df_w[f's{j}'] = smooths[j]
        df_w[f's{j}_slope'] = slope(smooths[j], lookback=5)
    print("  Levels: s1=2d, s2=4d, s3=8d, s4=16d, s5=32d, s6=64d")
    # Distribution of slope signs at level 5 and 6
    s5_up = (df_w['s5_slope'] > 0).sum() / df_w['s5_slope'].notna().sum() * 100
    s6_up = (df_w['s6_slope'] > 0).sum() / df_w['s6_slope'].notna().sum() * 100
    print(f"  s5 (32d) slope >0:  {s5_up:.1f}% of bars")
    print(f"  s6 (64d) slope >0:  {s6_up:.1f}% of bars")

    # ===== Weekly signals =====
    weekly = compute_weekly_signals(df)
    signals_daily = forward_fill_weekly_to_daily(weekly[['cross_below_ema200',
                                                          'cross_ema20_over_5',
                                                          'exit_rsi']], df)
    # only the day where the weekly signal first appears (don't repeat ffill)
    # Mark signal True only on the daily bar where weekly bar closed (= first daily of new week)
    weekly_signal_dates = weekly.index + pd.Timedelta(days=7)  # next Monday
    # Easier: detect transitions from False → True in the forward-filled series
    sig_strict = pd.DataFrame(index=df.index)
    for col in ['cross_below_ema200', 'cross_ema20_over_5', 'exit_rsi']:
        prev = signals_daily[col].shift(1).fillna(False)
        sig_strict[col] = signals_daily[col] & (~prev)

    n_sig = {c: int(sig_strict[c].sum()) for c in sig_strict.columns}
    print(f"\n  Weekly signals over {len(df)} days:")
    print(f"    cross_below_ema200: {n_sig['cross_below_ema200']}  (LONG 33% triggers)")
    print(f"    cross_ema20_over_5: {n_sig['cross_ema20_over_5']}  (+67% triggers)")
    print(f"    exit_rsi:           {n_sig['exit_rsi']}  (exit triggers)")

    # ===== Backtests =====
    print("\n" + "=" * 80)
    print("Backtest comparison")
    print("=" * 80)

    # A) Buy & Hold
    bh_qty = INITIAL / df['close'].iloc[0]
    bh_final = bh_qty * df['close'].iloc[-1]
    bh_eq = bh_qty * df['close']
    bh_dd = ((bh_eq.cummax() - bh_eq) / bh_eq.cummax()).max() * 100
    bh_ret = (bh_final / INITIAL - 1) * 100
    print(f"  {'A) Buy & Hold':40}  N=  1  WR=100.0%  PF= inf  "
          f"avg={bh_ret:+.2f}%  ret={bh_ret:>+7.1f}%  DD={bh_dd:>4.1f}%")

    # B) Original strategy (no VIX)
    tr_b, eq_b, eqc_b = backtest(df, sig_strict, wave_filter=None)
    report("B) Original (no VIX)", tr_b, eq_b, eqc_b, bh_final)

    # C) + wavelet filter: s5 slope > 0
    filt_s5 = pd.Series(df_w['s5_slope'].values > 0, index=df.index)
    tr_c, eq_c, eqc_c = backtest(df, sig_strict, wave_filter=filt_s5)
    report("C) + filter s5 slope>0 (32d up)", tr_c, eq_c, eqc_c, bh_final)

    # D) + wavelet filter: s6 slope > 0
    filt_s6 = pd.Series(df_w['s6_slope'].values > 0, index=df.index)
    tr_d, eq_d, eqc_d = backtest(df, sig_strict, wave_filter=filt_s6)
    report("D) + filter s6 slope>0 (64d up)", tr_d, eq_d, eqc_d, bh_final)

    # E) + filter: BOTH s5 AND s6 slope > 0 (consensus)
    filt_both = filt_s5 & filt_s6
    tr_e, eq_e, eqc_e = backtest(df, sig_strict, wave_filter=filt_both)
    report("E) + filter s5 AND s6 up", tr_e, eq_e, eqc_e, bh_final)

    # F) + filter: at least one of s4,s5,s6 going up (more lenient)
    filt_s4 = pd.Series(df_w['s4_slope'].values > 0, index=df.index)
    filt_any = filt_s4 | filt_s5 | filt_s6
    tr_f, eq_f, eqc_f = backtest(df, sig_strict, wave_filter=filt_any)
    report("F) + filter any of s4/s5/s6 up", tr_f, eq_f, eqc_f, bh_final)

    # Trade breakdown for B and best of C/D/E
    print("\n" + "=" * 80)
    print("Trade-level comparison: B (no filter) vs C (s5 slope filter)")
    print("=" * 80)
    print(f"  {'variant':6}  {'N':>3}  {'wins':>5}  {'losses':>7}  {'biggest win':>12}  {'biggest loss':>13}  {'avg hold':>9}")
    for name, tr in [('B', tr_b), ('C', tr_c), ('D', tr_d), ('E', tr_e), ('F', tr_f)]:
        if len(tr) == 0: continue
        wins = (tr['pnl_pct'] > 0).sum()
        losses = (tr['pnl_pct'] < 0).sum()
        bw = tr['pnl_pct'].max()
        bl = tr['pnl_pct'].min()
        ah = tr['hold_days'].mean()
        print(f"  {name:6}  {len(tr):>3}  {wins:>5}  {losses:>7}  {bw:>+11.1f}%  {bl:>+12.1f}%  {ah:>7.0f}d")


if __name__ == "__main__":
    main()
