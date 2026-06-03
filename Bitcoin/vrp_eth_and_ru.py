"""Test VRP strategy on ETH (with always-in-market) and RU market (IMOEX).

For ETH: same logic as BTC but with ETH DVOL.
For RU: no DVOL available, use VIX + realized vol of IMOEX.
"""
import warnings; warnings.filterwarnings("ignore")
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(__file__).parent
INITIAL = 100.0
FEE = 0.0005
SLIP = 0.0003


def load_btc():
    price = pd.read_csv(ROOT/'btc_usd_daily_coinbase.csv', parse_dates=['date']).set_index('date').sort_index()
    return price[price['close']>0].dropna()

def load_eth():
    price = pd.read_csv(ROOT/'eth_usd_daily_coinbase.csv', parse_dates=['date']).set_index('date').sort_index()
    return price[price['close']>0].dropna()

def load_dvol(asset):
    return pd.read_csv(ROOT / f'{asset.lower()}_dvol_daily.csv', parse_dates=['date']).set_index('date').sort_index()

def load_vix():
    vix = pd.read_csv(ROOT/'vix_daily.csv', skiprows=3,
                       names=['Date','close','high','low','open','volume'])
    vix['Date'] = pd.to_datetime(vix['Date'])
    return vix.set_index('Date')

def load_imoex():
    """Load MOEX IMOEX index."""
    imoex_path = ROOT.parent / 'CopyTrading' / 'moex_daily' / 'imoex_d.csv'
    if not imoex_path.exists():
        return None
    df = pd.read_csv(imoex_path, parse_dates=['date']).set_index('date').sort_index()
    return df[df['close']>0]


def build_features(price, dvol):
    df = pd.DataFrame(index=price.index)
    df['close'] = price['close']
    df['high'] = price.get('high', price['close'])
    df['low'] = price.get('low', price['close'])
    log_ret = np.log(df['close'] / df['close'].shift(1))
    df['rv_30d'] = log_ret.rolling(30).std() * np.sqrt(365) * 100
    df['dvol'] = dvol['close'].reindex(df.index, method='ffill')
    df['vrp'] = df['dvol'] - df['rv_30d']
    return df


def backtest_always_in_market(df, long_entry, long_exit_min_hold, long_exit_dvol_level,
                                 long_max_hold, short_annual_cost_pct=10.0):
    closes = df['close'].values
    dates = df.index
    dvol = df['dvol'].values
    n = len(closes)
    cash = INITIAL
    eq = np.full(n, INITIAL, dtype=float)
    pos = 0
    entry_idx = 0
    entry_px = 0
    daily_cost = short_annual_cost_pct / 365 / 100
    trades = []
    for i in range(n):
        c = closes[i]
        if pos == 1:
            eq[i] = cash * (c / entry_px)
        elif pos == -1:
            days = i - entry_idx
            eq[i] = cash * (1 + (entry_px - c)/entry_px - daily_cost*days)
        else:
            eq[i] = cash
        # Exit LONG
        if pos == 1:
            hold = i - entry_idx
            do_exit = False
            if hold >= long_max_hold: do_exit = True
            elif hold >= long_exit_min_hold and not np.isnan(dvol[i]) and dvol[i] >= long_exit_dvol_level:
                do_exit = True
            if do_exit:
                exit_p = c * (1 - SLIP)
                ret = (exit_p/entry_px - 1 - 2*FEE)
                cash *= (1 + ret)
                trades.append({'side': 'long', 'pnl_pct': ret*100, 'hold_d': hold,
                              'entry_date': dates[entry_idx], 'exit_date': dates[i]})
                entry_idx = i
                entry_px = c * (1 - SLIP)
                pos = -1
                eq[i] = cash
                continue
        # LONG entry
        if long_entry[i]:
            if pos == -1:
                exit_p = c * (1 + SLIP)
                days = i - entry_idx
                gross = (entry_px - exit_p)/entry_px - 2*FEE
                cost = daily_cost*days
                net = gross - cost
                cash *= (1 + net)
                trades.append({'side': 'short', 'pnl_pct': net*100, 'hold_d': days,
                              'entry_date': dates[entry_idx], 'exit_date': dates[i]})
                pos = 0
                eq[i] = cash
            if pos == 0:
                entry_idx = i
                entry_px = c * (1 + SLIP)
                pos = 1
                eq[i] = cash
    if pos != 0:
        c = closes[-1]
        if pos == 1:
            exit_p = c*(1-SLIP)
            ret = (exit_p/entry_px - 1 - 2*FEE)
        else:
            exit_p = c*(1+SLIP)
            days = (n-1) - entry_idx
            gross = (entry_px - exit_p)/entry_px - 2*FEE
            ret = gross - daily_cost*days
        cash *= (1 + ret)
        trades.append({'side': 'long' if pos==1 else 'short', 'pnl_pct': ret*100,
                      'hold_d': (n-1)-entry_idx,
                      'entry_date': dates[entry_idx], 'exit_date': dates[-1]})
    return pd.Series(eq, index=dates), pd.DataFrame(trades)


def backtest_long_only(df, long_entry, min_hold, dvol_exit, max_hold):
    closes = df['close'].values
    dates = df.index
    dvol = df['dvol'].values
    n = len(closes)
    cash = INITIAL
    eq = np.full(n, INITIAL, dtype=float)
    in_pos = False
    entry_idx = 0
    entry_px = 0
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
                trades.append({'pnl_pct': ret*100, 'hold_d': hold})
                in_pos = False
                eq[i] = cash
        if not in_pos and long_entry[i]:
            entry_idx = i
            entry_px = c*(1+SLIP)
            in_pos = True
    return pd.Series(eq, index=dates), pd.DataFrame(trades)


def main():
    # ============ ETH ============
    print("="*150)
    print("PART 1: ETH (VRP strategy with always-in-market)")
    print("="*150)
    eth_price = load_eth()
    eth_dvol = load_dvol('ETH')
    eth = build_features(eth_price, eth_dvol)
    eth = eth.dropna(subset=['dvol']).copy()
    print(f"  ETH data: {eth.index[0].date()} → {eth.index[-1].date()}")
    bh_eth = (eth['close'].iloc[-1]/eth['close'].iloc[0] - 1) * 100
    print(f"  Buy & Hold ETH: {bh_eth:+.0f}%\n")

    for vrp_thr, label in [(-10, "VRP ≤ -10"), (-15, "VRP ≤ -15"), (-20, "VRP ≤ -20"), (-25, "VRP ≤ -25")]:
        entry = (eth['vrp'] <= vrp_thr).fillna(False).values
        # LONG only
        eq, tr = backtest_long_only(eth, entry, 60, 70, 180)
        lo_total = (eq.iloc[-1]/INITIAL - 1)*100
        lo_dd = ((eq.cummax() - eq)/eq.cummax()).max()*100
        # Always in market
        for short_cost in [0, 10, 20]:
            eq_aim, tr_aim = backtest_always_in_market(eth, entry, 60, 70, 180, short_cost)
            aim_total = (eq_aim.iloc[-1]/INITIAL - 1)*100
            aim_dd = ((eq_aim.cummax() - eq_aim)/eq_aim.cummax()).max()*100
            n_l = (tr_aim['side']=='long').sum() if len(tr_aim) else 0
            n_s = (tr_aim['side']=='short').sum() if len(tr_aim) else 0
            print(f"  ETH {label} hold 60-180d  short_cost={short_cost:>2.0f}%:  "
                  f"LONG-only={lo_total:+.0f}% (DD {lo_dd:.0f}%)  "
                  f"Always-in={aim_total:+.0f}% (DD {aim_dd:.0f}%)  "
                  f"L={n_l} S={n_s}")
        print()

    # ============ RU market (IMOEX) ============
    print("="*150)
    print("PART 2: Russian market (IMOEX) — VIX as IV proxy")
    print("="*150)

    imoex = load_imoex()
    if imoex is None:
        print("  IMOEX data not found, skipping")
        return

    print(f"  IMOEX data: {imoex.index[0].date()} → {imoex.index[-1].date()} ({len(imoex)} days)")

    vix = load_vix()
    # IMOEX features
    df = pd.DataFrame(index=imoex.index)
    df['close'] = imoex['close']
    df['high'] = imoex.get('high', imoex['close'])
    df['low'] = imoex.get('low', imoex['close'])
    log_ret = np.log(df['close'] / df['close'].shift(1))
    df['rv_30d'] = log_ret.rolling(30).std() * np.sqrt(365) * 100

    # Use VIX as IV proxy
    df['vix'] = vix['close'].reindex(df.index, method='ffill')
    df['vrp_vix'] = df['vix'] - df['rv_30d']

    df = df.dropna(subset=['vix']).copy()
    bh_imoex = (df['close'].iloc[-1]/df['close'].iloc[0] - 1) * 100
    print(f"  Buy & Hold IMOEX: {bh_imoex:+.0f}%")

    print(f"\n  VIX statistics:")
    print(f"    Range: {df['vix'].min():.0f} - {df['vix'].max():.0f}")
    print(f"    Median: {df['vix'].median():.0f}")
    print(f"    Above 30: {(df['vix']>=30).sum()} days")
    print(f"    Above 40: {(df['vix']>=40).sum()} days")

    print(f"\n  IMOEX VRP_vix statistics:")
    print(f"    Range: {df['vrp_vix'].min():.0f} - {df['vrp_vix'].max():.0f}")
    print(f"    Median: {df['vrp_vix'].median():.0f}")

    # Test signals on IMOEX
    print(f"\n  Event analysis — what happens after signal (30d forward returns):")
    print(f"  {'Signal':<35}  {'N':>4}  {'30d mean':>10}  {'P(up)':>6}  {'60d mean':>10}  {'P(up)':>6}")

    for h in [30, 60, 90]:
        df[f'fwd_{h}d'] = df['close'].pct_change(h).shift(-h) * 100

    signals = {
        'VIX ≥ 30':                df['vix'] >= 30,
        'VIX ≥ 40':                df['vix'] >= 40,
        'VIX ≥ 50':                df['vix'] >= 50,
        'VIX ≥ 76':                df['vix'] >= 76,
        'IMOEX VRP_vix ≥ +10':     df['vrp_vix'] >= 10,
        'IMOEX VRP_vix ≥ +20':     df['vrp_vix'] >= 20,
        'IMOEX RV ≥ 30':           df['rv_30d'] >= 30,
        'IMOEX RV ≥ 50':           df['rv_30d'] >= 50,
    }
    for name, mask in signals.items():
        m = mask.fillna(False)
        n = m.sum()
        if n < 5: continue
        r30 = df.loc[m, 'fwd_30d'].dropna()
        r60 = df.loc[m, 'fwd_60d'].dropna()
        if len(r60) == 0: continue
        print(f"  {name:<35}  {len(r60):>4}  "
              f"{r30.mean():>+8.1f}%  {(r30>0).mean()*100:>4.0f}%  "
              f"{r60.mean():>+8.1f}%  {(r60>0).mean()*100:>4.0f}%")

    # Backtest VIX-based long
    print(f"\n  Backtest: IMOEX LONG when VIX ≥ X, hold 60 days:")
    for vix_th in [30, 40, 50, 76]:
        entry = (df['vix'] >= vix_th).fillna(False).values
        # Simple LONG strategy
        df_for_bt = df[['close','high','low']].copy()
        df_for_bt['dvol'] = df['vix']
        df_for_bt['rv_30d'] = df['rv_30d']
        df_for_bt['vrp'] = df['vrp_vix']
        eq, tr = backtest_long_only(df_for_bt, entry, 30, 100, 90)  # min hold 30, max 90, no early exit
        total = (eq.iloc[-1]/INITIAL - 1) * 100
        dd = ((eq.cummax() - eq)/eq.cummax()).max() * 100
        wr = (tr['pnl_pct']>0).sum()/max(len(tr),1)*100
        print(f"    VIX ≥ {vix_th}: total={total:+.0f}%  DD={dd:.0f}%  N={len(tr)}  WR={wr:.0f}%")


if __name__ == "__main__":
    main()
