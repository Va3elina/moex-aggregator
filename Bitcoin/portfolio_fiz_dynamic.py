"""Portfolio FIZ with DYNAMIC allocation.

When only 1 asset signals -> 100% capital into it.
When 2 signal -> 50/50. Etc.
This keeps capital working instead of sitting idle.

Plus leverage option (2x notional via futures).
"""
import warnings; warnings.filterwarnings("ignore")
import numpy as np, pandas as pd
from pathlib import Path
import portfolio_fiz_strict as base

ROOT = Path(__file__).parent
INIT=100.0; FEE=0.0003; SLIP=0.0001


def portfolio_bt_dynamic(assets, sigs, exits, minh=10, maxh=90, stop=None, lev=1.0):
    """Dynamic alloc: each asset gets equal share of currently ACTIVE positions.

    1 active -> 100% to it. 2 active -> 50/50. Etc.
    Rebalance: only on entry/exit (not every day, to avoid churn).
    """
    # positions per asset
    pos_by_a = {}
    for name, df in assets.items():
        pos_by_a[name] = base.asset_position(df, sigs[name], exits[name], minh, maxh, stop)

    common_idx = sorted(set().union(*[df.index for df in assets.values()]))
    cash = INIT
    # per-asset: units (notional / entry_px), entry_px
    units = {n:0.0 for n in assets}
    entry_px = {n: None for n in assets}
    prev_pos = {n:0 for n in assets}
    eq_curve = []
    trades = []
    trade_open = {n: None for n in assets}

    for d in common_idx:
        # Determine target positions today
        targets = {}
        for name, pos in pos_by_a.items():
            if d in pos.index:
                targets[name] = int(pos.loc[d])

        # Total active count today (after transitions)
        active_now = sum(targets.get(n,0) for n in assets)

        # Process exits FIRST
        for name in list(assets.keys()):
            if prev_pos.get(name)==1 and targets.get(name,0)==0:
                # close
                df = assets[name]
                if d in df.index and entry_px[name] is not None:
                    px = df.loc[d,'close']*(1-SLIP)
                    realized = units[name]*px*(1-FEE)
                    cash += realized
                    if trade_open[name]:
                        amt0 = trade_open[name][1]
                        pnl_pct=(realized/amt0-1)*100
                        trades.append({
                            'asset':name,'entry':trade_open[name][0],'exit':d,
                            'pnl_pct':pnl_pct,'amt':amt0,'realized':realized
                        })
                    units[name]=0; entry_px[name]=None; trade_open[name]=None
                prev_pos[name]=0

        # Process entries
        new_entries = [n for n in assets if prev_pos[n]==0 and targets.get(n,0)==1]
        if new_entries:
            # Rebalance: every active position gets equal share of total equity
            # Compute current notional of existing positions
            existing_notional = {n: units[n]*(assets[n].loc[d,'close'] if d in assets[n].index else (entry_px[n] or 0)) for n in assets if prev_pos[n]==1}
            total_eq = cash + sum(existing_notional.values())

            # Target alloc per active position
            total_active = sum(1 for n in assets if prev_pos[n]==1) + len(new_entries)
            target_per = (total_eq * lev) / total_active

            # For new entries: allocate target_per from cash (or proportionally if not enough)
            for name in new_entries:
                df = assets[name]
                if d in df.index:
                    px_e = df.loc[d,'close']*(1+SLIP)
                    amt = min(target_per, cash)
                    if amt > 0:
                        units[name] = amt*(1-FEE)/px_e
                        entry_px[name] = px_e
                        cash -= amt
                        trade_open[name]=(d, amt)
                        prev_pos[name]=1

        # Mark to market & record
        total_notional=0
        for name, df in assets.items():
            if units[name]>0 and d in df.index:
                total_notional += units[name]*df.loc[d,'close']
        total_eq = cash + total_notional
        eq_curve.append({'date':d, 'equity':total_eq, 'cash':cash})

    eq_df = pd.DataFrame(eq_curve).set_index('date')
    return eq_df, pd.DataFrame(trades)


def main():
    print("Loading assets...")
    assets = {}
    for name, ind, px, futoi, pcol in [
        ('SBER', 'sber_option_indicators_daily.csv', 'sber_daily.csv', 'sber_futoi_history.csv', 'close'),
        ('GAZP', 'gazp_option_indicators_daily.csv', 'gazp_daily.csv', 'gazp_futoi_history.csv', 'close'),
        ('Si',   'si_option_indicators_daily.csv',   'si_daily.csv',   'si_futoi_history.csv',   'close'),
        ('RTS',  'rts_option_indicators_daily.csv',  'rts_daily.csv',  'rts_futoi_history.csv',  'close'),
    ]:
        d = base.load_asset(name, ind, px, futoi, pcol)
        if d is not None: assets[name]=d
    print(f"{len(assets)} assets loaded\n")

    strategies = [
        ("STRICT FIZ<=1.2 dynamic alloc 1x", 'strict_fixed', {'fiz_max':1.2,'fiz_exit':3.0,'vrp_exit':-5}, 1.0),
        ("STRICT FIZ<=1.2 dynamic alloc 1.5x", 'strict_fixed', {'fiz_max':1.2,'fiz_exit':3.0,'vrp_exit':-5}, 1.5),
        ("STRICT FIZ<=1.2 dynamic alloc 2x", 'strict_fixed', {'fiz_max':1.2,'fiz_exit':3.0,'vrp_exit':-5}, 2.0),
        ("MEDIUM FIZ<=1.4 dynamic 1x", 'strict_fixed', {'fiz_max':1.4,'fiz_exit':3.0,'vrp_exit':-5}, 1.0),
        ("MEDIUM FIZ<=1.4 dynamic 2x", 'strict_fixed', {'fiz_max':1.4,'fiz_exit':3.0,'vrp_exit':-5}, 2.0),
        ("LOOSE FIZ<=1.6 dynamic 2x", 'loose_fixed', {'fiz_max':1.6,'fiz_exit':3.0,'vrp_exit':-5}, 2.0),
    ]

    for label, mode, params, lev in strategies:
        print(f"=== {label} ===")
        sigs={n: base.signal(df,mode,params).fillna(False) for n,df in assets.items()}
        exs={n: base.exit_mask(df,params).fillna(False) for n,df in assets.items()}
        eq, tr = portfolio_bt_dynamic(assets, sigs, exs, minh=10, maxh=90, stop=0.10, lev=lev)
        tot, dd, nt, wr, pf, avg, yr = base.stats(eq, tr)
        ys=" ".join(f"{y%100}:{v:+.0f}" for y,v in yr.items())
        n_yrs=(eq.index[-1]-eq.index[0]).days/365.25
        cagr=((eq['equity'].iloc[-1]/INIT)**(1/n_yrs)-1)*100 if n_yrs>0 else 0
        print(f"  total={tot:+.0f}% DD={dd:.0f}% N={nt} WR={wr:.0f}% PF={pf:.2f} CAGR={cagr:+.1f}%")
        print(f"  per-year: {ys}")
        if nt: print(f"  by asset: {dict(tr['asset'].value_counts())}")
        print()


if __name__ == "__main__":
    main()
