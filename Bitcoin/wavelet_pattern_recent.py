"""Wavelet pattern statistics — RECENT periods comparison.

Same patterns as before, but split by period:
  - Full history (2015/16 → 2026)
  - Last 4 years (2022 → 2026): post-FTX era
  - Last 2 years (2024 → 2026): most recent regime

Goal: identify which patterns are still actionable NOW vs which were
historical artifacts.
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
    """Compute on FULL series — wavelet needs history. Then slice."""
    log_c = np.log(df['close'].values)
    sm = atrous_haar(log_c, J=6)
    return {
        'log_c': log_c, 'close': df['close'].values, 'index': df.index,
        's3': sm[3], 's4': sm[4], 's5': sm[5], 's6': sm[6],
        'sl3': slope(sm[3], 5), 'sl4': slope(sm[4], 5),
        'sl5': slope(sm[5], 5), 'sl6': slope(sm[6], 5),
    }


# Events
def cross_above(a, b):
    out = np.zeros(len(a), dtype=bool)
    for i in range(1, len(a)):
        if not (np.isnan(a[i-1]) or np.isnan(b[i-1]) or np.isnan(a[i]) or np.isnan(b[i])):
            if a[i-1] <= b[i-1] and a[i] > b[i]: out[i] = True
    return out

def cross_below(a, b):
    out = np.zeros(len(a), dtype=bool)
    for i in range(1, len(a)):
        if not (np.isnan(a[i-1]) or np.isnan(b[i-1]) or np.isnan(a[i]) or np.isnan(b[i])):
            if a[i-1] >= b[i-1] and a[i] < b[i]: out[i] = True
    return out

def transition(cond, target=True):
    out = np.zeros(len(cond), dtype=bool)
    for i in range(1, len(cond)):
        if cond[i-1] != target and cond[i] == target:
            out[i] = True
    return out


def build_events(p):
    s4, s5, s6 = p['s4'], p['s5'], p['s6']
    sl4, sl5, sl6 = p['sl4'], p['sl5'], p['sl6']
    log_c = p['log_c']

    events = {}
    events['s5 пересекла s6 ВВЕРХ (бычий крест)'] = cross_above(s5, s6)
    events['s5 пересекла s6 ВНИЗ (медв. крест)'] = cross_below(s5, s6)
    events['s4 пересекла s5 ВВЕРХ'] = cross_above(s4, s5)
    events['s4 пересекла s5 ВНИЗ'] = cross_below(s4, s5)
    events['slope5 повернулся ВВЕРХ'] = transition(sl5 > 0, True)
    events['slope5 повернулся ВНИЗ'] = transition(sl5 > 0, False)
    events['slope6 повернулся ВВЕРХ'] = transition(sl6 > 0, True)
    events['slope6 повернулся ВНИЗ'] = transition(sl6 > 0, False)
    events['slope4 повернулся ВВЕРХ'] = transition(sl4 > 0, True)
    events['slope4 повернулся ВНИЗ'] = transition(sl4 > 0, False)

    all_up = (sl4 > 0) & (sl5 > 0) & (sl6 > 0)
    all_down = (sl4 < 0) & (sl5 < 0) & (sl6 < 0)
    events['ВСЕ 3 шкалы стали ВВЕРХ'] = transition(all_up, True)
    events['ВСЕ 3 шкалы стали ВНИЗ'] = transition(all_down, True)

    sl5_change = np.r_[np.full(5, np.nan), sl5[5:] - sl5[:-5]]
    events['тренд УСКОРЯЕТСЯ (slope5 растёт)'] = transition((sl5 > 0) & (sl5_change > 0), True)
    events['тренд ТОРМОЗИТ (slope5 падает)'] = transition((sl5 > 0) & (sl5_change < 0), True)

    events['цена ВЫШЛА выше s5'] = cross_above(log_c, s5)
    events['цена УПАЛА ниже s5'] = cross_below(log_c, s5)

    bullish_aligned = (s5 > s6) & (sl6 > 0)
    bearish_aligned = (s5 < s6) & (sl6 < 0)
    events['БЫЧЬЕ выравнивание начинается'] = transition(bullish_aligned, True)
    events['МЕДВЕЖЬЕ выравнивание начинается'] = transition(bearish_aligned, True)

    # Pullback finished
    pullback = np.zeros(len(sl5), dtype=bool)
    prev_sl5 = np.r_[np.nan, sl5[:-1]]
    for i in range(1, len(sl5)):
        if not (np.isnan(sl5[i]) or np.isnan(sl6[i]) or np.isnan(prev_sl5[i])):
            if sl6[i] > 0 and prev_sl5[i] <= 0 and sl5[i] > 0:
                pullback[i] = True
    events['КОНЕЦ отката в аптренде'] = pullback

    # Top tag
    top = np.zeros(len(sl6), dtype=bool)
    for i in range(1, len(sl6)):
        if not (np.isnan(sl6[i]) or np.isnan(log_c[i]) or np.isnan(s5[i])):
            if sl6[i-1] >= 0 and sl6[i] < 0 and log_c[i] > s5[i]:
                top[i] = True
    events['slope6 ВНИЗ пока цена ВЫШЕ s5'] = top

    # FALSE DAWN — key bearish signal
    fdawn = np.zeros(len(sl6), dtype=bool)
    for i in range(1, len(sl6)):
        if not (np.isnan(sl6[i]) or np.isnan(log_c[i]) or np.isnan(s5[i])):
            if sl6[i-1] <= 0 and sl6[i] > 0 and log_c[i] < s5[i]:
                fdawn[i] = True
    events['🪤 slope6 ВВЕРХ пока цена ПОД s5'] = fdawn

    spread = s5 - s6
    spread_change = np.r_[np.full(5, np.nan), spread[5:] - spread[:-5]]
    events['СПРЕД РАСТЁТ (тренд набирает)'] = transition((spread > 0) & (spread_change > 0) & (sl5 > 0) & (sl6 > 0), True)
    events['СПРЕД СУЖАЕТСЯ (тренд устал)'] = transition((spread > 0) & (spread_change < 0) & (sl5 > 0) & (sl6 > 0), True)
    events['ЦЕНА ОТОРВАЛАСЬ ВВЕРХ'] = transition(log_c > (s5 + 2 * (s5 - s6)), True)
    events['КАПИТУЛЯЦИЯ вниз'] = transition(log_c < (s5 - 2 * abs(s5 - s6)), True)
    return events


def forward_returns(close, event_mask, horizons=(30, 60, 120)):
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


def event_stats_periods(close, dates, event_mask, periods):
    """Returns dict period_name → {N, mean, pct_up, t} at 60-day horizon."""
    res = {}
    for period_name, start_date in periods.items():
        period_mask = (dates >= start_date) & event_mask
        n = int(period_mask.sum())
        if n == 0:
            res[period_name] = {'N': 0, 'mean': 0, 'pct_up': 0, 't': 0, 'med': 0}
            continue
        fwd = forward_returns(close, period_mask)
        rets = fwd[60]
        if len(rets) == 0:
            res[period_name] = {'N': n, 'mean': 0, 'pct_up': 0, 't': 0, 'med': 0}
            continue
        std = np.std(rets)
        t = np.mean(rets) / (std/np.sqrt(len(rets))) if std > 0 else 0
        res[period_name] = {
            'N': len(rets), 'mean': np.mean(rets), 'med': np.median(rets),
            'pct_up': (rets > 0).sum() / len(rets) * 100, 't': t,
        }
    return res


def main():
    btc = load_daily('BTC')
    eth = load_daily('ETH')

    periods = {
        'ALL':       pd.Timestamp(2010, 1, 1),
        '2022-2026': pd.Timestamp(2022, 1, 1),
        '2024-2026': pd.Timestamp(2024, 1, 1),
    }

    for asset, df in [('BTC', btc), ('ETH', eth)]:
        p = precompute(df)
        events = build_events(p)
        close = df['close'].values
        dates = df.index

        print(f"\n{'='*180}")
        print(f"{asset} — паттерны и их статистика 60-дн форвард по периодам")
        print(f"{'='*180}")
        print(f"  {'паттерн':50}  |  {'ALL 2015→2026':>42}  |  {'Последние 4 года (2022→2026)':>42}  |  {'Последние 2 года (2024→2026)':>42}")
        print(f"  {'':50}  |  {'N':>3} {'mean%':>7} {'med%':>7} {'P↑':>5} {'t':>6} {'verdict':>9}  |  "
              f"{'N':>3} {'mean%':>7} {'med%':>7} {'P↑':>5} {'t':>6} {'verdict':>9}  |  "
              f"{'N':>3} {'mean%':>7} {'med%':>7} {'P↑':>5} {'t':>6} {'verdict':>9}")
        print("  " + "-" * 200)

        results = []
        for name, mask in events.items():
            stats = event_stats_periods(close, dates, mask, periods)
            line = f"  {name:50}  |  "
            for pname in periods:
                s = stats[pname]
                if s['N'] == 0:
                    line += f"{'—':>52}  |  "
                    continue
                # Verdict
                if abs(s['t']) > 1.96:
                    verdict = "STRONG" + ("↑" if s['mean'] > 0 else "↓")
                elif abs(s['t']) > 1.0:
                    verdict = "weak" + ("↑" if s['mean'] > 0 else "↓")
                else:
                    verdict = "noise"
                line += f"{s['N']:>3} {s['mean']:>+6.1f}% {s['med']:>+6.1f}% {s['pct_up']:>4.0f}% {s['t']:>+5.1f} {verdict:>10}  |  "
            print(line)
            results.append((name, stats))

        # Highlight patterns that REVERSED
        print(f"\n  🔥 Паттерны где направление СМЕНИЛОСЬ или ослабло на недавних данных:")
        for name, stats in results:
            all_t = stats['ALL']['t']; recent_t = stats['2024-2026']['t']
            if stats['2024-2026']['N'] < 3: continue
            if all_t > 1.5 and recent_t < 0:
                print(f"     ⚠ {name}: было +t={all_t:.1f} (вверх), стало t={recent_t:.1f} (вниз) — paradigm shift")
            elif all_t > 2 and recent_t < 0.5:
                print(f"     ↓ {name}: было +t={all_t:.1f}, стало t={recent_t:.1f} — сигнал ослаб")
            elif all_t < -1.5 and recent_t > 0:
                print(f"     ⚡ {name}: было -t={all_t:.1f} (медв), стало t={recent_t:.1f} (бычий)")


if __name__ == "__main__":
    main()
