"""Portfolio FIZ strategy across SBER + GAZP + Si + RTS.

Hypothesis: stricter signals (lower FIZ threshold = rarer but higher conviction)
compensated by diversification across 4 assets.

Each asset gets 25% allocation. When asset's signal fires -> LONG that asset
with full 25% allocation. When signal cleared -> back to cash.

Compare:
  - Strict signal (FIZ <= 20th-pct of trailing window, rare/high-conviction)
  - Loose signal (FIZ <= 1.6 fixed, our baseline)
  - On 1, 2, 3, 4 assets
"""
import warnings; warnings.filterwarnings("ignore")
import numpy as np, pandas as pd
from pathlib import Path

ROOT = Path(__file__).parent
INIT=100.0; FEE=0.0003; SLIP=0.0001


def load_asset(name, ind_csv, px_csv, futoi_csv, px_col='close'):
    if not (ROOT/ind_csv).exists() or not (ROOT/futoi_csv).exists():
        print(f"{name}: missing files"); return None
    ind = pd.read_csv(ROOT/ind_csv, parse_dates=['date']).set_index('date').sort_index()
    if px_csv and (ROOT/px_csv).exists():
        px=pd.read_csv(ROOT/px_csv, parse_dates=['date']).set_index('date').sort_index()
        close = px[px_col] if px_col in px.columns else px.iloc[:,0]
    else:
        alt = ROOT.parent/'CopyTrading'/'moex_daily'/'imoex_d.csv'
        close = pd.read_csv(alt, parse_dates=['date']).set_index('date')['close'] if alt.exists() else ind['F']
    df = pd.DataFrame(index=ind.index)
    df['close']=close.reindex(ind.index).ffill()
    df['atm_iv']=ind['atm_iv']
    df['ret']=df['close'].pct_change()
    df['rv30']=df['ret'].rolling(30).std()*np.sqrt(252)*100
    df['vrp']=df['atm_iv']-df['rv30']
    ft=pd.read_csv(ROOT/futoi_csv, parse_dates=['date']).set_index('date').sort_index()
    df['fiz_lsr']=(ft['fiz_long']/ft['fiz_short'].replace(0,1)).reindex(df.index)
    # rolling 252-day rank: 0 = lowest of window (panic), 1 = highest
    win=252
    df['fiz_rank']=df['fiz_lsr'].rolling(win, min_periods=60).apply(
        lambda x: (x < x.iloc[-1]).mean(), raw=False)
    return df[df['atm_iv'].notna()].copy()


def signal(df, mode, params):
    """Return boolean Series: True when LONG."""
    if mode == 'strict_rank':
        # FIZ in bottom N% of trailing year = rare panic
        return df['fiz_rank'] <= params['rank_max']
    if mode == 'loose_fixed':
        return df['fiz_lsr'] <= params['fiz_max']
    if mode == 'strict_fixed':
        return df['fiz_lsr'] <= params['fiz_max']  # caller picks tighter threshold
    if mode == 'fiz_and_vrp':
        return (df['fiz_lsr'] <= params['fiz_max']) & (df['vrp'] >= params.get('vrp_min', 0))
    return df['fiz_lsr'] <= 1.4


def exit_mask(df, params):
    """Return boolean: True when exit."""
    return (df['fiz_lsr'] >= params.get('fiz_exit', 3.0)) | (df['vrp'] <= params.get('vrp_exit', -5))


def asset_position(df, em, xm, minh=10, maxh=90, stop=None):
    """Return Series of position: 1 when in long, 0 when flat."""
    closes=df['close'].values; em_v=em.values; xm_v=xm.values; n=len(df)
    pos=np.zeros(n); inp=False; ei=0; ep=0
    for i in range(n):
        if inp:
            pos[i]=1; h=i-ei; chg=closes[i]/ep-1; do=False
            if stop is not None and chg<=-stop: do=True
            elif h>=maxh: do=True
            elif h>=minh and xm_v[i]: do=True
            if do:
                pos[i]=0; inp=False
        if not inp and em_v[i]: ei=i; ep=closes[i]*(1+SLIP); inp=True; pos[i]=1
    return pd.Series(pos, index=df.index)


def portfolio_bt(assets, signals_by_asset, exits_by_asset, minh=10, maxh=90, stop=None):
    """
    Each asset gets equal alloc. When asset signals -> LONG full alloc.
    No leverage; if multiple signals, split capital among active positions.

    Returns aggregate equity curve.
    """
    n_assets=len(assets)
    alloc = 1.0/n_assets
    # collect per-asset position and per-bar return
    asset_data={}
    common_idx=None
    for name, df in assets.items():
        pos = asset_position(df, signals_by_asset[name], exits_by_asset[name], minh, maxh, stop)
        ret_when_in = df['ret'].copy()  # daily return
        asset_data[name] = pd.DataFrame({'pos': pos, 'ret': ret_when_in})
        idx = df.index
        common_idx = idx if common_idx is None else common_idx.union(idx)
    common_idx = sorted(common_idx)

    # Build portfolio
    cash = INIT
    # Track per-asset notional
    notional = {n: 0.0 for n in assets}
    prev_pos = {n: 0 for n in assets}
    prev_px = {n: None for n in assets}
    eq_curve = []
    trades = []
    trade_starts = {n: None for n in assets}

    for d in common_idx:
        # For each asset: check position change
        total_alloc_used = 0
        for name, df in assets.items():
            if d not in asset_data[name].index: continue
            row = asset_data[name].loc[d]
            cur_pos = int(row['pos'])

            # close prev day -> mark to market notional
            if prev_pos[name]==1 and prev_px[name] is not None:
                px_now = df.loc[d,'close'] if d in df.index else prev_px[name]
                notional[name] *= (px_now / prev_px[name])
                prev_px[name] = px_now

            # transition flat -> long
            if prev_pos[name]==0 and cur_pos==1:
                # allocate cash * alloc to this asset
                amt = cash * alloc
                if d in df.index:
                    px_entry = df.loc[d,'close']*(1+SLIP)
                    notional[name] = amt * (1-FEE)
                    prev_px[name] = px_entry
                    cash -= amt
                    trade_starts[name] = (d, amt)
            # transition long -> flat
            elif prev_pos[name]==1 and cur_pos==0:
                # close: realize notional back to cash with exit slip
                if d in df.index:
                    px_exit = df.loc[d,'close']*(1-SLIP)
                    realized = notional[name]*(1-FEE)
                    cash += realized
                    if trade_starts[name]:
                        amt0 = trade_starts[name][1]
                        pnl_pct = (realized/amt0 - 1)*100
                        trades.append({
                            'asset': name,
                            'entry': trade_starts[name][0],
                            'exit': d,
                            'pnl_pct': pnl_pct,
                            'amt': amt0,
                            'realized': realized,
                        })
                    notional[name]=0
                    prev_px[name]=None
            prev_pos[name] = cur_pos

        total_eq = cash + sum(notional.values())
        eq_curve.append({'date': d, 'equity': total_eq, 'cash': cash, **{f'n_{n}': notional[n] for n in assets}})

    eq_df = pd.DataFrame(eq_curve).set_index('date')
    return eq_df, pd.DataFrame(trades)


def stats(eq_df, tr_df):
    eq = eq_df['equity']
    tot = (eq.iloc[-1]/INIT-1)*100
    dd = ((eq.cummax()-eq)/eq.cummax()).max()*100
    nt = len(tr_df)
    wr = (tr_df['pnl_pct']>0).mean()*100 if nt else 0
    pos = tr_df[tr_df['pnl_pct']>0]['pnl_pct'].sum() if nt else 0
    neg = -tr_df[tr_df['pnl_pct']<0]['pnl_pct'].sum() if nt else 0
    pf = pos/neg if neg>0 else 9.99
    avg = tr_df['pnl_pct'].mean() if nt else 0
    # per year
    yr={}
    for y in range(2020, 2027):
        seg = eq[eq.index.year==y]
        if len(seg)>1:
            yr[y] = (seg.iloc[-1]/seg.iloc[0]-1)*100
    return tot, dd, nt, wr, pf, avg, yr


def main():
    print("Loading assets...")
    assets = {}
    for name, ind, px, futoi, pcol in [
        ('SBER', 'sber_option_indicators_daily.csv', 'sber_daily.csv', 'sber_futoi_history.csv', 'close'),
        ('GAZP', 'gazp_option_indicators_daily.csv', 'gazp_daily.csv', 'gazp_futoi_history.csv', 'close'),
        ('Si',   'si_option_indicators_daily.csv',   'si_daily.csv',   'si_futoi_history.csv',   'close'),
        ('RTS',  'rts_option_indicators_daily.csv',  'rts_daily.csv',  'rts_futoi_history.csv',  'close'),
    ]:
        d = load_asset(name, ind, px, futoi, pcol)
        if d is not None:
            assets[name] = d
            print(f"  {name}: {d.index[0].date()}→{d.index[-1].date()} {len(d)}d, B&H {(d['close'].iloc[-1]/d['close'].iloc[0]-1)*100:+.0f}%")
    print()

    # Strategies to test
    strategies = [
        ("LOOSE: FIZ<=1.6 each, exit FIZ>=3 OR VRP<-5", 'loose_fixed', {'fiz_max':1.6, 'fiz_exit':3.0, 'vrp_exit':-5}),
        ("MEDIUM: FIZ<=1.4 each", 'strict_fixed', {'fiz_max':1.4, 'fiz_exit':3.0, 'vrp_exit':-5}),
        ("STRICT: FIZ<=1.2 each (rare/high-conv)", 'strict_fixed', {'fiz_max':1.2, 'fiz_exit':3.0, 'vrp_exit':-5}),
        ("RANK-STRICT: FIZ in bottom 10% of trailing year", 'strict_rank', {'rank_max':0.10, 'fiz_exit':3.0, 'vrp_exit':-5}),
        ("RANK-MEDIUM: FIZ in bottom 20% of trailing year", 'strict_rank', {'rank_max':0.20, 'fiz_exit':3.0, 'vrp_exit':-5}),
    ]

    for label, mode, params in strategies:
        print(f"=== {label} ===")
        sigs={}; exits={}
        for name, df in assets.items():
            sigs[name] = signal(df, mode, params).fillna(False)
            exits[name] = exit_mask(df, params).fillna(False)

        eq, tr = portfolio_bt(assets, sigs, exits, minh=10, maxh=90, stop=0.10)
        tot, dd, nt, wr, pf, avg, yr = stats(eq, tr)
        print(f"  total={tot:+.0f}% DD={dd:.0f}% trades={nt} WR={wr:.0f}% PF={pf:.2f} avg={avg:+.1f}%")
        ys=" ".join(f"{y%100}:{v:+.0f}" for y,v in yr.items())
        print(f"  per-year: {ys}")
        # CAGR
        n_yrs = (eq.index[-1]-eq.index[0]).days/365.25
        cagr = ((eq['equity'].iloc[-1]/INIT)**(1/n_yrs)-1)*100 if n_yrs>0 else 0
        print(f"  CAGR ≈ {cagr:+.1f}%")
        # by asset trades
        if nt:
            print("  trades by asset:", dict(tr['asset'].value_counts()))
        print()


if __name__ == "__main__":
    main()
