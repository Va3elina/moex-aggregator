"""Test VRP strategy on Russian market using REAL options data (MOEX RVI).

Apply same logic as BTC/ETH:
  - Entry: VRP <= threshold (RVI - Realized Vol)
  - Exit: hold N days or RVI spike

Test on:
  - IMOEX index
  - Individual stocks: AFKS, ASTR, DELI, POSI, SMLT, SOFL, VKCO, WUSH
"""
import warnings; warnings.filterwarnings("ignore")
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(__file__).parent
INITIAL = 100.0
FEE = 0.0005
SLIP = 0.0003


def load_rvi():
    return pd.read_csv(ROOT/'rvi_daily.csv', parse_dates=['TRADEDATE']).set_index('TRADEDATE').sort_index()


def load_moex_security(ticker):
    path = ROOT.parent / 'CopyTrading' / 'moex_daily' / f'{ticker.lower()}_d.csv'
    if not path.exists():
        return None
    df = pd.read_csv(path, parse_dates=['date']).set_index('date').sort_index()
    return df[df['close']>0]


def build_features(price, rvi):
    df = pd.DataFrame(index=price.index)
    df['close'] = price['close']
    df['high'] = price.get('high', price['close'])
    df['low'] = price.get('low', price['close'])
    log_ret = np.log(df['close'] / df['close'].shift(1))
    df['rv_30d'] = log_ret.rolling(30).std() * np.sqrt(365) * 100
    df['rvi'] = rvi['close'].reindex(df.index, method='ffill')
    df['vrp'] = df['rvi'] - df['rv_30d']
    return df


def backtest_long(df, entry_mask, min_hold, dvol_exit, max_hold):
    closes = df['close'].values
    dates = df.index
    dvol = df['rvi'].values
    n = len(closes)
    cash = INITIAL
    eq = np.full(n, INITIAL, dtype=float)
    in_pos = False
    entry_idx = 0; entry_px = 0
    trades = []
    for i in range(n):
        c = closes[i]
        if in_pos: eq[i] = cash * (c / entry_px)
        else: eq[i] = cash
        if in_pos:
            hold = i - entry_idx
            do_exit = False
            if hold >= max_hold: do_exit = True
            elif hold >= min_hold and not np.isnan(dvol[i]) and dvol[i] >= dvol_exit: do_exit = True
            if do_exit:
                exit_p = c*(1-SLIP)
                ret = (exit_p/entry_px - 1 - 2*FEE)
                cash *= (1+ret)
                trades.append({'pnl_pct': ret*100, 'hold_d': hold,
                              'entry_date': dates[entry_idx], 'exit_date': dates[i]})
                in_pos = False
                eq[i] = cash
        if not in_pos and entry_mask[i]:
            entry_idx = i; entry_px = c*(1+SLIP); in_pos = True
    return pd.Series(eq, index=dates), pd.DataFrame(trades)


def backtest_always(df, entry_mask, min_hold, dvol_exit, max_hold, short_cost=0.10):
    """Always in market: LONG + SHORT between, with annual carry cost."""
    closes = df['close'].values
    dates = df.index
    dvol = df['rvi'].values
    n = len(closes)
    cash = INITIAL
    eq = np.full(n, INITIAL, dtype=float)
    pos = 0  # 0=flat, 1=long, -1=short
    entry_idx = 0; entry_px = 0
    daily_cost = short_cost / 365
    trades = []
    for i in range(n):
        c = closes[i]
        if pos == 1: eq[i] = cash * (c/entry_px)
        elif pos == -1:
            days = i - entry_idx
            eq[i] = cash * (1 + (entry_px-c)/entry_px - daily_cost*days)
        else: eq[i] = cash
        if pos == 1:
            hold = i - entry_idx
            do_exit = False
            if hold >= max_hold: do_exit = True
            elif hold >= min_hold and not np.isnan(dvol[i]) and dvol[i] >= dvol_exit: do_exit = True
            if do_exit:
                exit_p = c*(1-SLIP)
                ret = (exit_p/entry_px - 1 - 2*FEE)
                cash *= (1+ret)
                trades.append({'side':'long','pnl_pct':ret*100,'hold_d':hold,
                              'entry_date':dates[entry_idx],'exit_date':dates[i]})
                entry_idx = i; entry_px = c*(1-SLIP); pos = -1
                eq[i] = cash
                continue
        if entry_mask[i]:
            if pos == -1:
                exit_p = c*(1+SLIP)
                days = i - entry_idx
                gross = (entry_px-exit_p)/entry_px - 2*FEE
                net = gross - daily_cost*days
                cash *= (1+net)
                trades.append({'side':'short','pnl_pct':net*100,'hold_d':days,
                              'entry_date':dates[entry_idx],'exit_date':dates[i]})
                pos = 0
                eq[i] = cash
            if pos == 0:
                entry_idx = i; entry_px = c*(1+SLIP); pos = 1
                eq[i] = cash
    if pos != 0:
        c = closes[-1]
        if pos == 1:
            exit_p = c*(1-SLIP); ret = (exit_p/entry_px - 1 - 2*FEE)
        else:
            exit_p = c*(1+SLIP); days = (n-1)-entry_idx
            gross = (entry_px-exit_p)/entry_px - 2*FEE
            ret = gross - daily_cost*days
        cash *= (1+ret)
        trades.append({'side':'long' if pos==1 else 'short',
                      'pnl_pct':ret*100,'hold_d':(n-1)-entry_idx,
                      'entry_date':dates[entry_idx],'exit_date':dates[-1]})
    return pd.Series(eq, index=dates), pd.DataFrame(trades)


def main():
    rvi = load_rvi()
    print(f"MOEX RVI loaded: {rvi.index[0].date()} → {rvi.index[-1].date()} ({len(rvi)} days)")
    print(f"  RVI range: {rvi['close'].min():.0f} - {rvi['close'].max():.0f}, median {rvi['close'].median():.0f}\n")

    # Test on IMOEX + all RU stocks we have
    tickers = ['IMOEX', 'AFKS', 'ASTR', 'DELI', 'POSI', 'SMLT', 'SOFL', 'VKCO', 'WUSH']

    print("="*150)
    print("Event analysis: when RVI ≥ X, what happens to each RU asset?")
    print("="*150)
    print(f"  {'Ticker':<8}  {'Period':>22}  {'BH%':>6}  {'N (RVI≥40)':>11}  {'30d ret':>8}  {'P(up)':>6}  {'60d ret':>8}  {'P(up)':>6}")

    for ticker in tickers:
        price = load_moex_security(ticker)
        if price is None:
            print(f"  {ticker:<8}  data not found")
            continue
        if 'close' not in price.columns: continue

        df = build_features(price, rvi)
        df = df.dropna(subset=['rvi', 'close']).copy()
        if len(df) < 100: continue

        bh = (df['close'].iloc[-1]/df['close'].iloc[0] - 1) * 100

        for h in [30, 60]:
            df[f'fwd_{h}d'] = df['close'].pct_change(h).shift(-h) * 100

        # When RVI ≥ 40 (similar to VIX≥30 since RVI median is 30 vs VIX median 17)
        mask = (df['rvi'] >= 40).fillna(False)
        n = mask.sum()
        if n == 0:
            print(f"  {ticker:<8}  {df.index[0].date()} → {df.index[-1].date()}  {bh:>+5.0f}%  no signals")
            continue
        r30 = df.loc[mask, 'fwd_30d'].dropna()
        r60 = df.loc[mask, 'fwd_60d'].dropna()
        period = f"{df.index[0].date()} → {df.index[-1].date()}"
        if len(r60) > 0:
            print(f"  {ticker:<8}  {period:>22}  {bh:>+5.0f}%  {len(r60):>11}  "
                  f"{r30.mean():>+6.1f}%  {(r30>0).mean()*100:>4.0f}%  "
                  f"{r60.mean():>+6.1f}%  {(r60>0).mean()*100:>4.0f}%")

    # Detailed backtest on IMOEX with different RVI thresholds
    print("\n" + "="*150)
    print("Backtest on IMOEX — RVI signal strategy")
    print("="*150)

    imoex = load_moex_security('IMOEX')
    df = build_features(imoex, rvi).dropna(subset=['rvi']).copy()
    bh_imoex = (df['close'].iloc[-1]/df['close'].iloc[0] - 1) * 100
    print(f"  IMOEX Buy & Hold: {bh_imoex:+.0f}%\n")

    print(f"  RVI threshold sweep — LONG only (hold 30-90d, exit on RVI≥60):")
    for rvi_th in [35, 40, 45, 50, 60, 70, 80]:
        entry = (df['rvi'] >= rvi_th).fillna(False).values
        eq, tr = backtest_long(df, entry, 14, 60, 90)
        total = (eq.iloc[-1]/INITIAL - 1) * 100
        dd = ((eq.cummax() - eq)/eq.cummax()).max() * 100
        wr = (tr['pnl_pct']>0).sum()/max(len(tr),1)*100
        print(f"    RVI ≥ {rvi_th:>2}:  N={len(tr):>3}  WR={wr:>3.0f}%  total={total:>+6.0f}%  DD={dd:>4.0f}%")

    print(f"\n  VRP threshold sweep — LONG only:")
    for vrp_th in [10, 20, 30]:
        entry = (df['vrp'] >= vrp_th).fillna(False).values
        eq, tr = backtest_long(df, entry, 14, 60, 90)
        total = (eq.iloc[-1]/INITIAL - 1) * 100
        dd = ((eq.cummax() - eq)/eq.cummax()).max() * 100
        wr = (tr['pnl_pct']>0).sum()/max(len(tr),1)*100
        print(f"    VRP ≥ +{vrp_th:>2}:  N={len(tr):>3}  WR={wr:>3.0f}%  total={total:>+6.0f}%  DD={dd:>4.0f}%")

    # Always-in-market on IMOEX (best signal)
    print(f"\n  IMOEX always-in-market (RVI ≥ 40):")
    entry = (df['rvi'] >= 40).fillna(False).values
    for short_cost in [0, 5, 10, 20]:
        eq, tr = backtest_always(df, entry, 14, 60, 90, short_cost/100)
        total = (eq.iloc[-1]/INITIAL - 1) * 100
        dd = ((eq.cummax() - eq)/eq.cummax()).max() * 100
        n_l = (tr['side']=='long').sum() if len(tr) else 0
        n_s = (tr['side']=='short').sum() if len(tr) else 0
        print(f"    short carry {short_cost:>2}%/yr:  total={total:>+6.0f}%  DD={dd:>4.0f}%  L={n_l} S={n_s}")

    # Same on individual stocks
    print(f"\n  Backtest on individual RU stocks — RVI ≥ 40 LONG hold 30-90d:")
    print(f"  {'Ticker':<8}  {'Period':>22}  {'BH%':>6}  {'Strategy%':>10}  {'DD%':>5}  {'N':>3}  {'WR%':>4}")
    for ticker in ['AFKS', 'ASTR', 'DELI', 'POSI', 'SMLT', 'SOFL', 'VKCO', 'WUSH']:
        price = load_moex_security(ticker)
        if price is None: continue
        df_s = build_features(price, rvi).dropna(subset=['rvi']).copy()
        if len(df_s) < 100: continue
        bh = (df_s['close'].iloc[-1]/df_s['close'].iloc[0] - 1) * 100
        entry = (df_s['rvi'] >= 40).fillna(False).values
        eq, tr = backtest_long(df_s, entry, 14, 60, 90)
        total = (eq.iloc[-1]/INITIAL - 1) * 100
        dd = ((eq.cummax() - eq)/eq.cummax()).max() * 100
        wr = (tr['pnl_pct']>0).sum()/max(len(tr),1)*100
        period = f"{df_s.index[0].date()} → {df_s.index[-1].date()}"
        print(f"  {ticker:<8}  {period:>22}  {bh:>+5.0f}%  {total:>+9.0f}%  {dd:>4.0f}%  {len(tr):>3}  {wr:>3.0f}%")


if __name__ == "__main__":
    main()
