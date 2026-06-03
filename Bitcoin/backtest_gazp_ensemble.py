"""Ensemble backtest: combine independent GAZP signals via voting.

Independent signal families (low cross-correlation):
  1. Positioning : fiz_lsr <= 1.6  (retail panic short → contrarian long)
  2. Term struct : term_ratio_30_90 >= 1.05 (near-term IV spike = acute stress)
  3. Vol premium : vrp_vs >= 8  (variance-swap IV >> realized = bullish on RU)
  4. Skew        : rr25 >= -5  (reduced put skew = fear receding)
  5. Flow        : pc_vol_ratio >= 0.6 (elevated put hedging = contrarian)

Each TRUE = +1 vote. Enter LONG when votes >= threshold. Exit on hold or
when votes collapse.
"""
import warnings; warnings.filterwarnings("ignore")
import numpy as np
import pandas as pd
from pathlib import Path

ROOT = Path(__file__).parent
INITIAL = 100.0
FEE = 0.0003
SLIP = 0.0001


def load():
    df = pd.read_csv(ROOT/'gazp_master_features.csv', parse_dates=['date']).set_index('date')
    return df


def signals(df):
    s = pd.DataFrame(index=df.index)
    s['pos']  = (df['fiz_lsr'] <= 1.6)
    s['term'] = (df['term_ratio_30_90'] >= 1.05)
    s['volp'] = (df['vrp_vs'] >= 8) if 'vrp_vs' in df.columns else (df['vrp'] >= 8)
    s['skew'] = (df['rr25'] >= -5)
    s['flow'] = (df['pc_vol_ratio'] >= 0.6)
    s = s.fillna(False)
    s['votes'] = s[['pos','term','volp','skew','flow']].sum(axis=1)
    return s


def backtest(df, entry_mask, exit_mask, min_hold=10, max_hold=60):
    closes = df['close'].values; dates = df.index; n = len(df)
    em = entry_mask.values; xm = exit_mask.values
    cash = INITIAL; eq = np.full(n, INITIAL); in_pos=False; ei=0; ep=0; trades=[]
    for i in range(n):
        c = closes[i]
        if in_pos:
            eq[i] = cash*(c/ep); hold=i-ei; do=False
            if hold>=max_hold: do=True
            elif hold>=min_hold and xm[i]: do=True
            if do:
                xp=c*(1-SLIP); r=xp/ep-1-2*FEE; cash*=(1+r)
                trades.append({'entry':dates[ei],'exit':dates[i],'hold':hold,'pnl':r*100}); in_pos=False; eq[i]=cash
        else: eq[i]=cash
        if not in_pos and em[i]:
            ei=i; ep=c*(1+SLIP); in_pos=True
    return pd.Series(eq,index=dates), pd.DataFrame(trades)


def summ(eq,tr,label):
    total=(eq.iloc[-1]/INITIAL-1)*100; dd=((eq.cummax()-eq)/eq.cummax()).max()*100
    n=len(tr); wr=(tr['pnl']>0).sum()/max(n,1)*100; avg=tr['pnl'].mean() if n else 0
    # exposure
    return f"{label:<42} total={total:>+7.0f}% DD={dd:>4.0f}% N={n:>3} WR={wr:>3.0f}% avg={avg:>+5.1f}%"


def main():
    df = load()
    df = df[df['atm_iv'].notna()].copy()
    print(f"Period: {df.index[0].date()} → {df.index[-1].date()}  ({len(df)} days)")
    bh=(df['close'].iloc[-1]/df['close'].iloc[0]-1)*100
    print(f"GAZP B&H: {bh:+.0f}%\n")

    s = signals(df)
    print("Signal hit rates (% of days TRUE):")
    for col in ['pos','term','volp','skew','flow']:
        print(f"  {col}: {s[col].mean()*100:.0f}%")
    print(f"Votes distribution: {s['votes'].value_counts().sort_index().to_dict()}\n")

    print("="*95)
    print("ENSEMBLE: LONG when votes >= N, exit when votes drop below N-1 or maxhold")
    print("="*95)
    for vth in [1, 2, 3, 4]:
        em = (s['votes'] >= vth)
        xm = (s['votes'] < vth-1) if vth > 1 else pd.Series(False, index=df.index)
        eq, tr = backtest(df, em, xm, 10, 60)
        print("  " + summ(eq, tr, f"votes >= {vth}"))

    print("\n" + "="*95)
    print("INDIVIDUAL signals (LONG when TRUE, exit when FALSE / maxhold):")
    print("="*95)
    for col in ['pos','term','volp','skew','flow']:
        em = s[col]; xm = ~s[col]
        eq, tr = backtest(df, em, xm, 10, 60)
        print("  " + summ(eq, tr, col))

    print("\n" + "="*95)
    print("BEST PAIRS (2-signal AND):")
    print("="*95)
    import itertools
    pairs = list(itertools.combinations(['pos','term','volp','skew','flow'], 2))
    pair_results = []
    for a, b in pairs:
        em = s[a] & s[b]; xm = ~(s[a] & s[b])
        eq, tr = backtest(df, em, xm, 10, 60)
        total=(eq.iloc[-1]/INITIAL-1)*100; dd=((eq.cummax()-eq)/eq.cummax()).max()*100
        n=len(tr); wr=(tr['pnl']>0).sum()/max(n,1)*100
        pair_results.append((f"{a}+{b}", total, dd, n, wr, eq, tr))
    pair_results.sort(key=lambda x: x[1], reverse=True)
    for name, total, dd, n, wr, eq, tr in pair_results:
        print(f"  {name:<42} total={total:>+7.0f}% DD={dd:>4.0f}% N={n:>3} WR={wr:>3.0f}%")

    # Best overall detail
    print("\n" + "="*95)
    print("CHAMPION DETAIL")
    print("="*95)
    best = pair_results[0]
    print(f"Best pair: {best[0]} → {best[1]:+.0f}% DD {best[2]:.0f}% WR {best[4]:.0f}%")
    for _, t in best[6].iterrows():
        print(f"  {t['entry'].date()} → {t['exit'].date()} ({int(t['hold']):>3}d)  PnL {t['pnl']:>+6.1f}%")


if __name__ == "__main__":
    main()
