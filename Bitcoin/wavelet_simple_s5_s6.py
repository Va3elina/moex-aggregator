"""Простая стратегия на s5/s6 + цене.

Изучаем 4 события:
  1. Цена пересекает s5 ВВЕРХ (price crosses above s5)
  2. Цена пересекает s5 ВНИЗ
  3. s5 пересекает s6 ВВЕРХ (golden cross)
  4. s5 пересекает s6 ВНИЗ (death cross)

И 4 СОСТОЯНИЯ (alignment):
  A. price > s5 > s6  (full bull)
  B. price < s5 < s6  (full bear)
  C. s5 > s6, price < s5 (pullback в аптренде)
  D. s5 < s6, price > s5 (bounce в даунтренде)

Для каждого:
  - Forward returns 5/30/60/120 дней
  - Probability up
  - Mean / median

Потом строим стратегию из самых сильных сигналов и тестируем.
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


def load_daily(asset):
    name = "btc_usd_daily_coinbase.csv" if asset == 'BTC' else "eth_usd_daily_coinbase.csv"
    df = pd.read_csv(ROOT/name, parse_dates=['date']).set_index('date').sort_index()
    return df[df['close']>0].dropna(subset=['close'])


def detect_cross_above(a, b):
    out = np.zeros(len(a), dtype=bool)
    for i in range(1, len(a)):
        if not (np.isnan(a[i-1]) or np.isnan(b[i-1])):
            if a[i-1] <= b[i-1] and a[i] > b[i]: out[i] = True
    return out

def detect_cross_below(a, b):
    out = np.zeros(len(a), dtype=bool)
    for i in range(1, len(a)):
        if not (np.isnan(a[i-1]) or np.isnan(b[i-1])):
            if a[i-1] >= b[i-1] and a[i] < b[i]: out[i] = True
    return out


def fwd_returns(close, mask, horizons=(5, 30, 60, 120)):
    idx = np.where(mask)[0]
    result = {}
    for h in horizons:
        rets = []
        for i in idx:
            if i + h < len(close):
                r = (close[i+h]/close[i] - 1) * 100
                rets.append(r)
        result[h] = np.array(rets)
    return result


def print_event_stats(asset, events, close, periods):
    """For each event, print N + forward returns at multiple horizons."""
    print(f"\n  {asset} — событие × forward return (период 2022-01-01 → present):")
    print(f"  {'event':40}  {'N':>4}  |  "
          f"{'5d mean':>8}{'P↑':>5}  |  "
          f"{'30d mean':>9}{'P↑':>5}  |  "
          f"{'60d mean':>9}{'P↑':>5}  |  "
          f"{'120d mean':>10}{'P↑':>5}")
    print("  " + "-" * 110)
    for name, mask in events.items():
        # Filter recent only (after 2022)
        period_mask = mask & (periods >= pd.Timestamp(2022, 1, 1))
        n = int(period_mask.sum())
        if n == 0:
            print(f"  {name:40}  {n:>4}  | (no events)")
            continue
        fwd = fwd_returns(close, period_mask)
        line = f"  {name:40}  {n:>4}  |  "
        for h in (5, 30, 60, 120):
            rets = fwd[h]
            if len(rets) == 0:
                line += f"{'—':>8}{'—':>5}  |  "
            else:
                m = np.mean(rets); pup = (rets > 0).sum()/len(rets)*100
                line += f"{m:>+7.1f}%{pup:>4.0f}%  |  "
        print(line)


def state_classification(log_c, s5, s6):
    """Classify each bar into 4 states."""
    states = np.zeros(len(log_c), dtype=int)
    # 0=full_bull, 1=pullback_uptrend, 2=bounce_downtrend, 3=full_bear, -1=invalid
    for i in range(len(log_c)):
        if np.isnan(s5[i]) or np.isnan(s6[i]):
            states[i] = -1; continue
        if s5[i] > s6[i]:  # уптренд по wavelet
            if log_c[i] > s5[i]: states[i] = 0  # full bull
            else: states[i] = 1                # pullback
        else:  # даунтренд
            if log_c[i] > s5[i]: states[i] = 2  # bounce
            else: states[i] = 3                # full bear
    return states


def state_stats(close, states, dates, period_start):
    """Per-state: how often it occurs and avg daily return WITHIN the state."""
    period_mask = dates >= period_start
    rets_daily = np.log(close[1:]) - np.log(close[:-1])  # daily log return
    rets_daily = np.r_[0, rets_daily]
    results = {}
    state_names = {0: 'price>s5>s6 (FULL BULL)', 1: 's5>s6, price<s5 (pullback)',
                    2: 's5<s6, price>s5 (bounce)', 3: 'price<s5<s6 (FULL BEAR)'}
    for st in [0, 1, 2, 3]:
        sm = (states == st) & period_mask
        n = int(sm.sum())
        if n == 0: continue
        r = rets_daily[sm]
        mean_daily = np.mean(r) * 100
        annualized = (np.exp(mean_daily/100 * 365) - 1) * 100
        results[state_names[st]] = {'N': n, 'pct_time': n/period_mask.sum()*100,
                                     'mean_daily': mean_daily, 'annualized': annualized}
    return results


# ============ Build different simple strategies ============
def make_target(log_c, s5, s6, rule):
    """Generate target (0/1) array based on rule."""
    n = len(log_c)
    target = np.zeros(n)
    state = 0  # 0=flat, 1=long
    for i in range(n):
        if np.isnan(s5[i]) or np.isnan(s6[i]):
            target[i] = state; continue

        if rule == 'A_s5gts6':
            # Long while s5 > s6
            if state == 0 and s5[i] > s6[i] and (i==0 or s5[i-1] <= s6[i-1]):
                state = 1
            elif state == 1 and s5[i] < s6[i]:
                state = 0
        elif rule == 'B_price_gt_s5':
            # Long while price > s5
            if state == 0 and log_c[i] > s5[i] and (i==0 or log_c[i-1] <= s5[i-1]):
                state = 1
            elif state == 1 and log_c[i] < s5[i]:
                state = 0
        elif rule == 'C_both':
            # Long when price > s5 AND s5 > s6
            # Entry: price crosses above s5 while s5 > s6
            cond = log_c[i] > s5[i] and s5[i] > s6[i]
            if state == 0 and cond:
                state = 1
            elif state == 1 and (log_c[i] < s5[i] or s5[i] < s6[i]):
                state = 0
        elif rule == 'D_full_bull_state':
            # Long ONLY while in "full bull" state (price > s5 > s6)
            full_bull = log_c[i] > s5[i] and s5[i] > s6[i]
            if state == 0 and full_bull and not (i>0 and log_c[i-1] > s5[i-1] and s5[i-1] > s6[i-1]):
                state = 1
            elif state == 1 and not full_bull:
                state = 0
        elif rule == 'E_either_cross_long':
            # Long on EITHER cross up (price×s5 or s5×s6), exit on EITHER cross down
            cross_up_price = log_c[i] > s5[i] and (i==0 or log_c[i-1] <= s5[i-1])
            cross_up_wave  = s5[i] > s6[i]  and (i==0 or s5[i-1]  <= s6[i-1])
            cross_dn_price = log_c[i] < s5[i] and (i==0 or log_c[i-1] >= s5[i-1])
            cross_dn_wave  = s5[i] < s6[i]  and (i==0 or s5[i-1]  >= s6[i-1])
            if state == 0 and (cross_up_price or cross_up_wave):
                state = 1
            elif state == 1 and (cross_dn_price or cross_dn_wave):
                state = 0
        elif rule == 'F_aggressive':
            # Long on price > s5, exit on s5 < s6 (slow exit)
            if state == 0 and log_c[i] > s5[i] and (i==0 or log_c[i-1] <= s5[i-1]):
                state = 1
            elif state == 1 and s5[i] < s6[i]:
                state = 0
        target[i] = float(state)
    return target


def backtest(df, target):
    closes = df['close'].values
    dates = df.index
    n = len(closes)
    cash = INITIAL; qty = 0.0
    eq = np.zeros(n); trades = []
    ev = 0; ed = None; intra_min = 0
    for i in range(n):
        c = closes[i]
        eq[i] = cash + qty*c
        if qty > 0:
            low = df['low'].values[i]
            if intra_min == 0 or low < intra_min: intra_min = low
        if target[i] == 1 and qty == 0:
            eff = c*(1+SLIP); spend = cash*0.99
            bq = spend/eff; cost = bq*eff*(1+FEE)
            cash -= cost; qty = bq; ev = cost; ed = dates[i]
            intra_min = df['low'].values[i]
        elif target[i] == 0 and qty > 0:
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


def report(name, tr, fe, eq, dates):
    if len(tr) == 0:
        print(f"    {name}: no trades"); return
    total = (fe/INITIAL - 1) * 100
    dd = ((eq.cummax() - eq)/eq.cummax()).max() * 100
    wr = (tr['pnl_pct']>0).sum()/len(tr) * 100
    pos = tr[tr['pnl_pct']>0]['pnl_pct'].sum()
    neg = abs(tr[tr['pnl_pct']<0]['pnl_pct'].sum())
    pf = pos/neg if neg > 0 else float('inf')
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
    avg_hold = tr['hold_d'].mean()
    pf_str = f"{pf:>5.2f}" if pf != float('inf') else "  inf"
    print(f"    {name:30}  N={len(tr):>3}  WR={wr:>4.1f}%  PF={pf_str}  total={total:>+7.0f}%  DD={dd:>4.1f}%  recent4y={rec:>+5.0f}%  2026={y26:>+6.1f}%  worstY={worst_y:>+5.0f}%  bestY={best_y:>+5.0f}%  avgHold={avg_hold:>4.0f}d")


def main():
    btc = load_daily('BTC')
    eth = load_daily('ETH')

    for asset, df in [('BTC', btc), ('ETH', eth)]:
        log_c = np.log(df['close'].values)
        sm = atrous_haar(log_c, J=6)
        s5 = sm[5]; s6 = sm[6]
        close = df['close'].values
        dates = df.index

        print("=" * 130)
        print(f"{asset}")
        print("=" * 130)

        # === PART 1: Event statistics ===
        events = {
            '1. price crosses ABOVE s5':  detect_cross_above(log_c, s5),
            '2. price crosses BELOW s5':  detect_cross_below(log_c, s5),
            '3. s5 crosses ABOVE s6 (golden)': detect_cross_above(s5, s6),
            '4. s5 crosses BELOW s6 (death)':  detect_cross_below(s5, s6),
            '5. price crosses ABOVE s6':  detect_cross_above(log_c, s6),
            '6. price crosses BELOW s6':  detect_cross_below(log_c, s6),
        }
        print_event_stats(asset, events, close, dates)

        # === PART 2: State classification ===
        states = state_classification(log_c, s5, s6)
        print(f"\n  {asset} — состояния (alignment) и средняя доходность ВНУТРИ состояния (2022+):")
        state_results = state_stats(close, states, dates, pd.Timestamp(2022,1,1))
        for state_name, stats in state_results.items():
            print(f"    {state_name:30}  N={stats['N']:>4}  {stats['pct_time']:>4.1f}% времени  "
                  f"mean_daily={stats['mean_daily']:>+5.3f}%  annualized={stats['annualized']:>+6.0f}%")

        # === PART 3: Backtest 6 simple strategies ===
        print(f"\n  {asset} — backtest простых стратегий:")
        for rule in ['A_s5gts6', 'B_price_gt_s5', 'C_both', 'D_full_bull_state',
                     'E_either_cross_long', 'F_aggressive']:
            target = make_target(log_c, s5, s6, rule)
            tr, fe, eq = backtest(df, target)
            label = {'A_s5gts6': 'A: long while s5 > s6',
                     'B_price_gt_s5': 'B: long while price > s5',
                     'C_both': 'C: long if price>s5 AND s5>s6',
                     'D_full_bull_state': 'D: long ONLY in FULL BULL',
                     'E_either_cross_long': 'E: long on ANY cross up',
                     'F_aggressive': 'F: price>s5 in, s5<s6 out'}[rule]
            report(label, tr, fe, eq, dates)

        # B&H comparison
        bh = INITIAL / df['close'].iloc[0] * df['close']
        bh_total = (bh.iloc[-1]/INITIAL - 1) * 100
        bh_dd = ((bh.cummax() - bh)/bh.cummax()).max() * 100
        bh_26 = bh[bh.index >= pd.Timestamp(2026,1,1)]
        bh_26_ret = (bh_26.iloc[-1]/bh_26.iloc[0] - 1)*100 if len(bh_26)>1 else 0
        bh_recent = bh[bh.index >= pd.Timestamp(2022,1,1)]
        bh_recent_ret = (bh_recent.iloc[-1]/bh_recent.iloc[0] - 1)*100 if len(bh_recent)>1 else 0
        print(f"    Buy & Hold:                     total={bh_total:>+7.0f}%  DD={bh_dd:>4.1f}%  recent4y={bh_recent_ret:>+5.0f}%  2026={bh_26_ret:>+6.1f}%")


if __name__ == "__main__":
    main()
