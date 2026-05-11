"""Post-mortem analysis of every trade — figure out WHY winners win and losers lose."""
import warnings; warnings.filterwarnings("ignore")
from pathlib import Path
import numpy as np
import pandas as pd
from scipy import stats as sp
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler

DIR = Path(__file__).parent / "moex_daily"
PAIRS = ['AFKS','SMLT','POSI','ASTR','VKCO','SOFL','WUSH','DELI']


def wilder_ma(s, n):
    out = np.full(len(s), np.nan)
    if len(s) < n: return pd.Series(out, index=s.index)
    v = s.values; out[n-1] = np.nanmean(v[:n])
    for i in range(n, len(s)): out[i] = (out[i-1]*(n-1) + v[i]) / n
    return pd.Series(out, index=s.index)


def features(bars):
    d = bars.copy(); c,h,l,o,v = d['close'],d['high'],d['low'],d['open'],d['volume']
    delta = c.diff()
    gain = delta.clip(lower=0); loss = -delta.clip(upper=0)
    avg_g = wilder_ma(gain, 14); avg_l = wilder_ma(loss, 14)
    d['rsi_14'] = 100 - 100/(1 + avg_g/avg_l)
    pc = c.shift(1); tr = pd.concat([h-l, (h-pc).abs(), (l-pc).abs()], axis=1).max(axis=1)
    d['atr'] = wilder_ma(tr, 14)
    d['atr_pct'] = d['atr'] / c * 100
    up_move = h - h.shift(1); dn_move = l.shift(1) - l
    plus_dm = pd.Series(np.where((up_move > dn_move) & (up_move > 0), up_move, 0), index=d.index)
    minus_dm = pd.Series(np.where((dn_move > up_move) & (dn_move > 0), dn_move, 0), index=d.index)
    plus_di = 100 * wilder_ma(plus_dm, 14) / d['atr']
    minus_di = 100 * wilder_ma(minus_dm, 14) / d['atr']
    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di)
    d['adx'] = wilder_ma(dx, 14)
    ma20 = c.rolling(20).mean(); sd20 = c.rolling(20).std()
    d['bb_pct_b'] = (c - (ma20 - 2*sd20)) / (4*sd20)
    d['ema_20'] = c.ewm(span=20, adjust=False).mean()
    d['ema_50'] = c.ewm(span=50, adjust=False).mean()
    d['ema_200'] = c.ewm(span=200, adjust=False).mean()
    d['trend_up'] = (d['ema_20'] > d['ema_50']) & (d['ema_50'] > d['ema_200'])
    d['trend_dn'] = (d['ema_20'] < d['ema_50']) & (d['ema_50'] < d['ema_200'])
    d['vol_ratio'] = v / v.rolling(20).mean()
    d['vol_tier'] = 0
    d.loc[d['vol_ratio'] >= 1.5, 'vol_tier'] = 1
    d.loc[d['vol_ratio'] >= 3.25, 'vol_tier'] = 2
    d.loc[d['vol_ratio'] >= 6.0, 'vol_tier'] = 3
    d['ret_5'] = c.pct_change(5)
    d['ret_20'] = c.pct_change(20)
    d['ret_60'] = c.pct_change(60)
    d['pos_60'] = (c - l.rolling(60).min()) / (h.rolling(60).max() - l.rolling(60).min())
    d['pos_20'] = (c - l.rolling(20).min()) / (h.rolling(20).max() - l.rolling(20).min())
    d['dist_from_20d_high'] = (c / h.rolling(20).max() - 1) * 100
    d['dist_from_20d_low'] = (c / l.rolling(20).min() - 1) * 100
    return d


def load(name):
    p = DIR / f'{name.lower()}_d.csv'
    df = pd.read_csv(p, parse_dates=['date']).set_index('date').sort_index()
    for c in ['open','high','low','close','volume']: df[c] = pd.to_numeric(df[c], errors='coerce')
    return df


def collect_trades(pairs, side='long'):
    trades = []
    for p in pairs:
        bars = features(load(p))
        if side == 'long':
            sig = ((bars['rsi_14'] > 70) & (bars['pos_60'] > 0.7) & (bars['ret_20'] > 0.10) &
                    (bars['adx'] > 25) & bars['trend_up'] &
                    bars['vol_tier'].isin([1, 2])).fillna(False)
        else:
            sig = ((bars['rsi_14'] < 30) & (bars['pos_60'] < 0.1) & (bars['ret_20'] < -0.10) &
                    (bars['adx'] > 25) & bars['trend_dn'] &
                    bars['vol_tier'].isin([1, 2])).fillna(False)
        i = 0; max_bars = 40
        while i < len(bars) - max_bars:
            if not sig.iloc[i]:
                i += 1; continue
            ep = bars['close'].iloc[i]
            window = bars.iloc[i+1:i+max_bars+1]
            if side == 'long':
                peak = window['high'].cummax()
                trough = window['low'].cummin()
                max_pnl_pct = (peak.iloc[-1] / ep - 1) * 100
                worst_pnl_pct = (trough.iloc[-1] / ep - 1) * 100
                peak_bar = window['high'].idxmax()
                peak_bar_n = list(window.index).index(peak_bar) + 1
            else:
                trough = window['low'].cummin(); peak = window['high'].cummax()
                max_pnl_pct = (ep / trough.iloc[-1] - 1) * 100
                worst_pnl_pct = (ep / peak.iloc[-1] - 1) * 100
                trough_bar = window['low'].idxmin()
                peak_bar_n = list(window.index).index(trough_bar) + 1

            tp_active = False; exit_idx = None; exit_pnl = 0
            running_peak = ep
            for j_off, (idx, row) in enumerate(window.iterrows(), start=1):
                h_b = row['high']; l_b = row['low']; c_b = row['close']
                if side == 'long':
                    running_peak = max(running_peak, h_b)
                    peak_pnl = (running_peak / ep - 1) * 100
                    if not tp_active and peak_pnl >= 5.0: tp_active = True
                    gb_level = ep + (running_peak - ep) * 0.75
                    if tp_active and l_b <= gb_level:
                        exit_idx = j_off; exit_pnl = (gb_level/ep - 1)*100; break
                    if not tp_active and c_b < row['ema_20']:
                        exit_idx = j_off; exit_pnl = (c_b/ep - 1)*100; break
                else:
                    running_peak = min(running_peak, l_b)
                    peak_pnl = (ep / running_peak - 1) * 100
                    if not tp_active and peak_pnl >= 5.0: tp_active = True
                    cb_level = running_peak + (ep - running_peak) * 0.25
                    if tp_active and h_b >= cb_level:
                        exit_idx = j_off; exit_pnl = (ep/cb_level - 1)*100; break
                    if not tp_active and c_b > row['ema_20']:
                        exit_idx = j_off; exit_pnl = (ep/c_b - 1)*100; break
            if exit_idx is None:
                exit_idx = max_bars
                last_close = window['close'].iloc[-1]
                exit_pnl = (last_close/ep - 1)*100 if side == 'long' else (ep/last_close - 1)*100

            trades.append({
                'pair': p, 'side': side, 'entry_date': bars.index[i], 'entry_price': ep,
                'rsi_at_entry': bars['rsi_14'].iloc[i],
                'pos_60': bars['pos_60'].iloc[i], 'pos_20': bars['pos_20'].iloc[i],
                'ret_20': bars['ret_20'].iloc[i] * 100, 'ret_60': bars['ret_60'].iloc[i] * 100,
                'adx': bars['adx'].iloc[i], 'atr_pct': bars['atr_pct'].iloc[i],
                'vol_ratio': bars['vol_ratio'].iloc[i], 'vol_tier': bars['vol_tier'].iloc[i],
                'bb_pct_b': bars['bb_pct_b'].iloc[i],
                'dist_from_20d_high': bars['dist_from_20d_high'].iloc[i],
                'dist_from_20d_low': bars['dist_from_20d_low'].iloc[i],
                'final_pnl': exit_pnl, 'max_pnl': max_pnl_pct, 'worst_pnl': worst_pnl_pct,
                'peak_bar': peak_bar_n, 'exit_bar': exit_idx,
                'is_winner': exit_pnl > 0, 'give_back': max_pnl_pct - exit_pnl,
            })
            i += exit_idx + 1
    return pd.DataFrame(trades)


def main():
    print("=" * 100)
    print("TRADE POST-MORTEM — what differentiates winners from losers?")
    print("=" * 100)
    df_l = collect_trades(PAIRS, 'long')
    df_s = collect_trades(PAIRS, 'short')
    df = pd.concat([df_l, df_s], ignore_index=True)
    print(f"\nTotal: {len(df)} ({len(df_l)} long + {len(df_s)} short)")
    wins = df[df['is_winner']]; losses = df[~df['is_winner']]
    print(f"  Winners: {len(wins)} ({len(wins)/len(df)*100:.1f}%) | Losers: {len(losses)}")

    print("\n" + "=" * 100)
    print("PART 1: WINNER vs LOSER — distribution differences (t-test)")
    print("=" * 100)
    feats = ['rsi_at_entry','pos_60','pos_20','ret_20','ret_60','adx','atr_pct',
             'vol_ratio','vol_tier','bb_pct_b','dist_from_20d_high','dist_from_20d_low']
    print(f"  {'feature':22}  {'WIN':>9}  {'LOSS':>9}  {'diff':>8}  {'t':>6}  {'p':>8}")
    print("  " + "-"*78)
    for f in feats:
        w = wins[f].dropna(); l = losses[f].dropna()
        if len(w) < 5 or len(l) < 5: continue
        t, p = sp.ttest_ind(w, l, equal_var=False)
        sig = "***" if p < 0.01 else ("**" if p < 0.05 else ("*" if p < 0.1 else ""))
        print(f"  {f:22}  {w.mean():>+8.3f}  {l.mean():>+8.3f}  "
              f"{w.mean() - l.mean():>+7.3f}  {t:>+5.2f}  {p:>7.4f} {sig}")

    print("\n" + "=" * 100)
    print("PART 2: LOGISTIC REGRESSION — learn weights automatically")
    print("=" * 100)
    df_clean = df.dropna(subset=feats + ['is_winner'])
    X = df_clean[feats].values; y = df_clean['is_winner'].astype(int).values
    scaler = StandardScaler(); Xs = scaler.fit_transform(X)
    lr = LogisticRegression(max_iter=1000, C=1.0)
    lr.fit(Xs, y)
    weights = pd.DataFrame({'feature': feats, 'weight': lr.coef_[0]}).sort_values(
        'weight', key=abs, ascending=False)
    print(f"\n  Standardized weights (bigger |w| = more impactful):")
    for _, r in weights.iterrows():
        bar = "█" * int(abs(r['weight']) * 15) if abs(r['weight']) > 0.05 else ""
        sign = "+" if r['weight'] > 0 else "−"
        meaning = "→ win" if r['weight'] > 0 else "→ loss"
        print(f"    {r['feature']:22}  {sign}{abs(r['weight']):.3f}  {bar:<25}  ({meaning})")

    proba = lr.predict_proba(Xs)[:, 1]
    df_clean = df_clean.copy(); df_clean['p_win'] = proba
    print(f"\n  Filtering by predicted-win probability:")
    print(f"  {'threshold':>10}  {'trades':>7}  {'WR%':>6}  {'avg_pnl%':>9}")
    for T in [0.5, 0.6, 0.7, 0.8, 0.9, 0.95]:
        sub = df_clean[df_clean['p_win'] > T]
        if len(sub) == 0: continue
        sub_wr = sub['is_winner'].mean() * 100
        sub_avg = sub['final_pnl'].mean()
        print(f"     p>{T:.2f}  {len(sub):>7}  {sub_wr:>5.1f}%  {sub_avg:>+8.2f}%")

    print("\n" + "=" * 100)
    print("PART 3: BIGGEST LOSERS — анатомия катастрофы")
    print("=" * 100)
    print(f"  {'pair':6}  {'side':>5}  {'entry':>11}  {'final':>7}  {'max_pnl':>7}  "
          f"{'rsi':>5}  {'adx':>5}  {'vol_t':>5}  {'pos':>5}  {'dist_hi':>7}")
    biggest_l = df.nsmallest(10, 'final_pnl')
    for _, r in biggest_l.iterrows():
        print(f"  {r['pair']:6}  {r['side']:>5}  {str(r['entry_date'].date()):>11}  "
              f"{r['final_pnl']:>+6.2f}%  {r['max_pnl']:>+6.2f}%  "
              f"{r['rsi_at_entry']:>5.1f}  {r['adx']:>5.1f}  {int(r['vol_tier']):>5}  "
              f"{r['pos_60']:>5.2f}  {r['dist_from_20d_high']:>+6.2f}%")

    print("\n" + "=" * 100)
    print("PART 4: PEAK timing")
    print("=" * 100)
    print(f"  Winners avg peak_bar: {wins['peak_bar'].mean():.1f}d, median: {wins['peak_bar'].median():.0f}d")
    print(f"  Losers  avg peak_bar: {losses['peak_bar'].mean():.1f}d, median: {losses['peak_bar'].median():.0f}d")
    print(f"  Winner exit_bar avg : {wins['exit_bar'].mean():.1f}d")
    print(f"  Loser  exit_bar avg : {losses['exit_bar'].mean():.1f}d")
    for label, sub in [("Winners", wins), ("Losers", losses)]:
        print(f"\n  {label} peak-bar distribution:")
        for lo, hi in [(1, 3), (4, 7), (8, 14), (15, 40)]:
            cnt = ((sub['peak_bar'] >= lo) & (sub['peak_bar'] <= hi)).sum()
            print(f"    {lo}-{hi}d: {cnt} ({cnt/len(sub)*100:.0f}%)")


if __name__ == "__main__":
    main()
