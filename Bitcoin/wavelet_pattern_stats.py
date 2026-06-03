"""Wavelet pattern discovery + forward return probabilities for BTC and ETH.

For each pattern (cross, slope turn, alignment, etc.) we measure:
  - N (number of occurrences over history)
  - Forward returns at +5, +10, +30, +60, +120 bars
  - For each horizon: median, mean, std, % positive (probability up)
  - Confidence: std / sqrt(N) → significance of edge

Daily timeframe for both BTC and ETH.
"""
import warnings; warnings.filterwarnings("ignore")
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(__file__).parent


def atrous_haar(x, J=6):
    sm = [x.astype(float).copy()]
    for j in range(1, J+1):
        gap = 2**(j-1); prev = sm[-1]; s = prev.copy()
        if len(x) > gap: s[gap:] = 0.5*(prev[gap:] + prev[:-gap])
        sm.append(s)
    return sm

def slope(arr, lb=5):
    out = np.full(len(arr), np.nan)
    if len(arr) > lb: out[lb:] = arr[lb:] - arr[:-lb]
    return out


def load_daily(asset):
    name = "btc_usd_daily_coinbase.csv" if asset == 'BTC' else "eth_usd_daily_coinbase.csv"
    df = pd.read_csv(ROOT/name, parse_dates=['date']).set_index('date').sort_index()
    return df[df['close']>0].dropna(subset=['close'])


def precompute(df):
    log_c = np.log(df['close'].values)
    sm = atrous_haar(log_c, J=6)
    return {
        'log_c': log_c, 'close': df['close'].values, 'index': df.index,
        's3': sm[3], 's4': sm[4], 's5': sm[5], 's6': sm[6],
        'sl3': slope(sm[3], 5), 'sl4': slope(sm[4], 5),
        'sl5': slope(sm[5], 5), 'sl6': slope(sm[6], 5),
    }


# ============ Event detectors (return bool array, True at event bar) ============
def detect_cross_above(a, b):
    """True at bar where a crosses above b."""
    out = np.zeros(len(a), dtype=bool)
    for i in range(1, len(a)):
        if not (np.isnan(a[i-1]) or np.isnan(b[i-1]) or np.isnan(a[i]) or np.isnan(b[i])):
            if a[i-1] <= b[i-1] and a[i] > b[i]:
                out[i] = True
    return out

def detect_cross_below(a, b):
    out = np.zeros(len(a), dtype=bool)
    for i in range(1, len(a)):
        if not (np.isnan(a[i-1]) or np.isnan(b[i-1]) or np.isnan(a[i]) or np.isnan(b[i])):
            if a[i-1] >= b[i-1] and a[i] < b[i]:
                out[i] = True
    return out

def detect_state_transition(cond, target=True):
    """True at bar where cond becomes target (False→True for target=True)."""
    out = np.zeros(len(cond), dtype=bool)
    for i in range(1, len(cond)):
        if cond[i-1] != target and cond[i] == target:
            out[i] = True
    return out


# Build all events for an asset
def build_events(p):
    s3, s4, s5, s6 = p['s3'], p['s4'], p['s5'], p['s6']
    sl3, sl4, sl5, sl6 = p['sl3'], p['sl4'], p['sl5'], p['sl6']
    log_c = p['log_c']

    events = {}

    # 1. Crosses
    events['s5_cross_above_s6'] = detect_cross_above(s5, s6)
    events['s5_cross_below_s6'] = detect_cross_below(s5, s6)
    events['s4_cross_above_s5'] = detect_cross_above(s4, s5)
    events['s4_cross_below_s5'] = detect_cross_below(s4, s5)

    # 2. Slope sign transitions
    events['sl5_turns_positive'] = detect_state_transition(sl5 > 0, True)
    events['sl5_turns_negative'] = detect_state_transition(sl5 > 0, False)
    events['sl6_turns_positive'] = detect_state_transition(sl6 > 0, True)
    events['sl6_turns_negative'] = detect_state_transition(sl6 > 0, False)
    events['sl4_turns_positive'] = detect_state_transition(sl4 > 0, True)
    events['sl4_turns_negative'] = detect_state_transition(sl4 > 0, False)

    # 3. Multi-slope alignment transitions
    all_up = (sl4 > 0) & (sl5 > 0) & (sl6 > 0)
    all_down = (sl4 < 0) & (sl5 < 0) & (sl6 < 0)
    events['all3_slopes_become_up'] = detect_state_transition(all_up, True)
    events['all3_slopes_become_down'] = detect_state_transition(all_down, True)

    # 4. Slope of slope (acceleration) — slope5 starts increasing strongly
    sl5_change = np.r_[np.full(5, np.nan), sl5[5:] - sl5[:-5]]
    events['sl5_accelerating'] = detect_state_transition((sl5 > 0) & (sl5_change > 0), True)
    events['sl5_decelerating'] = detect_state_transition((sl5 > 0) & (sl5_change < 0), True)

    # 5. Price vs s5 cross
    events['price_cross_above_s5'] = detect_cross_above(log_c, s5)
    events['price_cross_below_s5'] = detect_cross_below(log_c, s5)

    # 6. Combined: bullish wavelet golden cross WHILE slope6 up
    bullish_aligned = (s5 > s6) & (sl6 > 0)
    events['bullish_alignment_starts'] = detect_state_transition(bullish_aligned, True)

    bearish_aligned = (s5 < s6) & (sl6 < 0)
    events['bearish_alignment_starts'] = detect_state_transition(bearish_aligned, True)

    # 7. Pullback finished: slope6 > 0 throughout, slope5 transitions from neg to positive
    pullback_finished = np.zeros(len(sl5), dtype=bool)
    prev_sl5 = np.r_[np.nan, sl5[:-1]]
    for i in range(1, len(sl5)):
        if not np.isnan(sl5[i]) and not np.isnan(sl6[i]) and not np.isnan(prev_sl5[i]):
            if sl6[i] > 0 and prev_sl5[i] <= 0 and sl5[i] > 0:
                pullback_finished[i] = True
    events['pullback_within_uptrend'] = pullback_finished

    # 8. Distress: slope6 turns negative while price was above s5 (top tagged)
    top_tag = np.zeros(len(sl6), dtype=bool)
    for i in range(1, len(sl6)):
        if not np.isnan(sl6[i]) and not np.isnan(log_c[i]) and not np.isnan(s5[i]):
            if sl6[i-1] >= 0 and sl6[i] < 0 and log_c[i] > s5[i]:
                top_tag[i] = True
    events['slope6_turns_neg_while_above_s5'] = top_tag

    # 9. Bottom signal: slope6 turns positive while price below s5
    bottom_tag = np.zeros(len(sl6), dtype=bool)
    for i in range(1, len(sl6)):
        if not np.isnan(sl6[i]) and not np.isnan(log_c[i]) and not np.isnan(s5[i]):
            if sl6[i-1] <= 0 and sl6[i] > 0 and log_c[i] < s5[i]:
                bottom_tag[i] = True
    events['slope6_turns_pos_while_below_s5'] = bottom_tag

    # 10. Spread widening: |s5-s6| growing while both slopes positive (momentum strengthening)
    spread = s5 - s6
    spread_change = np.r_[np.full(5, np.nan), spread[5:] - spread[:-5]]
    momentum_strength = (spread > 0) & (spread_change > 0) & (sl5 > 0) & (sl6 > 0)
    events['momentum_strengthening'] = detect_state_transition(momentum_strength, True)

    # 11. Trend exhaustion: spread shrinking while slopes still positive
    momentum_weakening = (spread > 0) & (spread_change < 0) & (sl5 > 0) & (sl6 > 0)
    events['momentum_weakening'] = detect_state_transition(momentum_weakening, True)

    # 12. Extreme: price > s5 + 2*(s5-s6) (strong breakout above slow trend)
    overextended = log_c > (s5 + 2 * (s5 - s6))
    events['extreme_overextension_up'] = detect_state_transition(overextended, True)

    # 13. Capitulation: price < s5 - 2*(s6-s5) (deep below s5 by spread × 2)
    capitulation = log_c < (s5 - 2 * abs(s5 - s6))
    events['capitulation_down'] = detect_state_transition(capitulation, True)

    return events


# ============ Forward return analysis ============
def forward_returns(close, event_mask, horizons=(5, 10, 30, 60, 120)):
    """For each event bar, compute forward return % at each horizon."""
    idx = np.where(event_mask)[0]
    results = {}
    for h in horizons:
        rets = []
        for i in idx:
            if i + h < len(close):
                r = (close[i+h] / close[i] - 1) * 100
                rets.append(r)
        results[h] = np.array(rets)
    return results


def event_summary(close, event_mask, name, asset):
    """Return summary dict for one event."""
    n_events = int(event_mask.sum())
    if n_events == 0:
        return None
    fwd = forward_returns(close, event_mask)
    row = {'asset': asset, 'event': name, 'N': n_events}
    for h, rets in fwd.items():
        if len(rets) == 0:
            row[f'{h}d_med'] = None; row[f'{h}d_mean'] = None
            row[f'{h}d_pct_up'] = None; row[f'{h}d_t'] = None
        else:
            row[f'{h}d_med'] = np.median(rets)
            row[f'{h}d_mean'] = np.mean(rets)
            row[f'{h}d_pct_up'] = (rets > 0).sum() / len(rets) * 100
            # t-statistic: mean / (std/sqrt(N)) — how far from zero
            std_rets = np.std(rets)
            row[f'{h}d_t'] = (np.mean(rets) / (std_rets / np.sqrt(len(rets)))) if std_rets > 0 else 0
    return row


def main():
    btc = load_daily('BTC')
    eth = load_daily('ETH')

    for asset, df in [('BTC', btc), ('ETH', eth)]:
        p = precompute(df)
        events = build_events(p)
        close = df['close'].values

        # Build summary table for this asset
        rows = []
        for name, mask in events.items():
            row = event_summary(close, mask, name, asset)
            if row: rows.append(row)
        results = pd.DataFrame(rows)

        # Print
        print("\n" + "=" * 175)
        print(f"{asset} wavelet pattern statistics ({len(df)} bars, {df.index[0].date()} → {df.index[-1].date()})")
        print("=" * 175)
        print(f"  {'event':36}  {'N':>4}  |  "
              f"{'5d med%':>8}{'mean%':>8}{'P(up)':>6}{'t':>6}  |  "
              f"{'10d med%':>9}{'mean%':>8}{'P(up)':>6}{'t':>6}  |  "
              f"{'30d med%':>9}{'mean%':>8}{'P(up)':>6}{'t':>6}  |  "
              f"{'60d med%':>9}{'mean%':>8}{'P(up)':>6}{'t':>6}  |  "
              f"{'120d med%':>10}{'mean%':>8}{'P(up)':>6}{'t':>6}")
        print("  " + "-" * 170)
        for _, r in results.iterrows():
            line = f"  {r['event']:36}  {int(r['N']):>4}  |  "
            for h in (5, 10, 30, 60, 120):
                med = r[f'{h}d_med']; mean = r[f'{h}d_mean']
                pup = r[f'{h}d_pct_up']; t = r[f'{h}d_t']
                if med is None:
                    line += f"{'—':>8}{'—':>8}{'—':>6}{'—':>6}  |  "
                else:
                    sig = '***' if abs(t) > 2.5 else ('**' if abs(t) > 1.96 else ('*' if abs(t) > 1.65 else ' '))
                    line += f"{med:>+7.1f}%{mean:>+7.1f}%{pup:>5.0f}%{t:>+5.1f}{sig}|  "
            print(line)

        # Sort by strongest 60d edge (|t| value)
        print(f"\n{asset} — strongest 60-day directional signals (|t| ranked)")
        results['abs_t_60'] = results['60d_t'].abs()
        sorted_r = results.sort_values('abs_t_60', ascending=False).head(8)
        for _, r in sorted_r.iterrows():
            direction = "BULLISH" if r['60d_mean'] > 0 else "BEARISH"
            print(f"  {r['event']:38}  N={int(r['N']):>3}  60d mean={r['60d_mean']:>+6.1f}%  med={r['60d_med']:>+6.1f}%  P(up)={r['60d_pct_up']:>4.0f}%  t={r['60d_t']:>+5.1f}  → {direction}")


if __name__ == "__main__":
    main()
