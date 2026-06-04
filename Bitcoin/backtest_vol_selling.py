"""Volatility selling backtest on SBER options chain.

Strategy: Iron Condor (short strangle + long wings) when VRP elevated.
  - Pick monthly expiry ~30 DTE
  - Sell 15-delta call + 15-delta put (collect premium)
  - Buy 5-delta wings as tail protection (cap loss)
  - Hold until: 50% of max profit captured, OR <7 DTE, OR breach
  - Re-enter on next signal

Uses REAL settlement prices from option chain.
Per-day P&L = MTM short premium (negative when IV/price moves against).
"""
import warnings; warnings.filterwarnings("ignore")
import numpy as np, pandas as pd
from pathlib import Path
from parse_sber_secid import parse_secid
from black76_iv import black76_price, black76_iv

ROOT = Path(__file__).parent


def load():
    chain = pd.read_csv(ROOT/'sber_options_history_merged.csv')
    chain = chain[chain['SETTLEPRICE'].notna() & (chain['SETTLEPRICE'] > 0)].copy()
    parsed = chain['SECID'].apply(parse_secid)
    chain = chain[parsed.notna()].copy()
    p = pd.DataFrame(list(parsed[parsed.notna()])); p.index = chain.index
    chain['strike']=p['strike']; chain['otype']=p['option_type']
    chain['expiry_code']=p['expiry_code']
    chain['settle_group']=(p['series']+p['exp_month'].astype(str).str.zfill(2)+p['exp_year_digit'].astype(str)+p['week'])
    chain['series']=p['series']
    chain['TRADEDATE']=pd.to_datetime(chain['TRADEDATE'])
    em = pd.read_csv(ROOT/'sber_expiry_map.csv')
    exp_map=dict(zip(em['expiry_code'], pd.to_datetime(em['expiry_date'])))

    ind=pd.read_csv(ROOT/'sber_option_indicators_daily.csv', parse_dates=['date']).set_index('date')
    px=pd.read_csv(ROOT/'sber_daily.csv', parse_dates=['date']).set_index('date')
    rusfar=pd.read_csv(ROOT/'rusfar_daily.csv', parse_dates=['date']).set_index('date')['rate_pct']/100
    return chain, exp_map, ind, px, rusfar


def derive_forward(K, C, P, disc):
    cp=C-P; v=~np.isnan(cp)
    if v.sum()<2: return None
    b,a=np.polyfit(K[v], cp[v], 1)
    return a/disc if abs(disc)>1e-9 else None


def pick_delta_strike(K, ivs, F, T, r, target_delta, side):
    """Find strike with call-delta closest to target. side='call' or 'put'.
    For put 15-delta: call-delta ~ 0.15 (deep OTM put has call-delta near 1; we use abs put-delta=0.15 -> call-delta=0.85)
    Actually: call delta range 0..1; put delta = call delta - exp(-rT). At ATM call delta~0.5.
    OTM call: delta < 0.5. OTM put: call delta > 0.5 (since put is ITM equivalent).
    Use Black-76 delta: call_delta = exp(-rT)*N(d1).
    """
    from scipy.stats import norm
    disc=np.exp(-r*T)
    deltas=[]
    for k,iv in zip(K,ivs):
        if iv<=0 or np.isnan(iv):
            deltas.append(np.nan); continue
        d1=(np.log(F/k)+0.5*iv*iv*T)/(iv*np.sqrt(T))
        deltas.append(disc*norm.cdf(d1))
    deltas=np.array(deltas)
    if side=='call':
        # OTM call ~ delta target (e.g. 0.15)
        valid=~np.isnan(deltas) & (K>F)
        if valid.sum()==0: return None
        idx=np.argmin(np.abs(deltas[valid]-target_delta))
        return float(K[valid][idx])
    else:
        # OTM put: equivalent call-delta = 1-|put_delta|. For 15-delta put: call_delta ~ 0.85
        # Easier: find K<F where call_delta ~ 1-target
        valid=~np.isnan(deltas) & (K<F)
        if valid.sum()==0: return None
        idx=np.argmin(np.abs(deltas[valid]-(1-target_delta)))
        return float(K[valid][idx])


def main():
    chain, exp_map, ind, px, rusfar = load()
    print(f"Chain loaded: {len(chain)} rows, {chain['TRADEDATE'].nunique()} days")

    # Find good signal days (VRP elevated)
    ind=ind.copy()
    ind['ret']=px['close'].reindex(ind.index).pct_change()
    ind['rv30']=ind['ret'].rolling(30).std()*np.sqrt(252)*100
    ind['vrp']=ind['atm_iv']-ind['rv30']

    SIGNAL_DAYS = ind[ind['vrp'] >= 5].index  # enter on VRP >= 5
    print(f"VRP>=5 signal days: {len(SIGNAL_DAYS)}")
    print(f"VRP>=10: {(ind['vrp']>=10).sum()}, VRP>=15: {(ind['vrp']>=15).sum()}")

    # === Iron Condor backtest ===
    INIT_CAPITAL = 100000  # ₽
    MARGIN_PER_CONDOR = 5000  # rough; FORTS margin will tune
    POSITION_SIZE = 0.1  # 10% of capital per trade
    SHORT_DELTA = 0.20  # 20-delta short strangle
    WING_DELTA = 0.05   # 5-delta long wings
    TARGET_PROFIT = 0.5  # exit at 50% of max profit
    MIN_DTE_TO_HOLD = 7  # close if <7 days left
    TARGET_DTE = 30      # pick expiry near 30 days

    cash = INIT_CAPITAL
    equity_curve = []
    open_pos = None  # dict if active position
    trades = []

    all_dates = sorted(ind.index.unique())
    for d in all_dates:
        # === MTM open position ===
        if open_pos:
            # find settlement prices for our 4 legs today
            day = chain[chain['TRADEDATE']==d]
            mtm = open_pos['credit_received']  # initial premium collected
            for leg in ['sc','sp','lc','lp']:
                secid=open_pos[leg]['secid']
                row=day[day['SECID']==secid]
                if len(row):
                    px_now=float(row['SETTLEPRICE'].iloc[0])
                else:
                    px_now=open_pos[leg]['last_px']
                open_pos[leg]['last_px']=px_now
                # short legs: positive if px dropped (we sold high, can buy back low)
                # long legs: positive if px rose
                if leg.startswith('s'):
                    mtm += (open_pos[leg]['open_px'] - px_now)
                else:
                    mtm += (px_now - open_pos[leg]['open_px'])
            open_pos['current_pnl'] = mtm * open_pos['contracts']
            dte = (open_pos['expiry'] - d).days

            # Exit conditions
            do_exit = False; reason=''
            max_profit = open_pos['credit_received'] * open_pos['contracts']
            if open_pos['current_pnl'] >= max_profit * TARGET_PROFIT:
                do_exit=True; reason='50% profit'
            elif dte < MIN_DTE_TO_HOLD:
                do_exit=True; reason='near expiry'
            # stop on breach
            elif open_pos['current_pnl'] <= -max_profit * 2:
                do_exit=True; reason='stop'

            if do_exit:
                cash += open_pos['current_pnl']
                # subtract commissions
                comm = 0.5 * 4 * open_pos['contracts']  # ₽0.5 per leg per contract round trip
                cash -= comm
                trades.append({'entry':open_pos['entry_date'],'exit':d,
                               'pnl':open_pos['current_pnl']-comm,
                               'pnl_pct':(open_pos['current_pnl']-comm)/(open_pos['margin'])*100,
                               'reason':reason})
                open_pos=None

        # === Open new position on signal ===
        if open_pos is None and d in SIGNAL_DAYS:
            day = chain[chain['TRADEDATE']==d]
            # find expiry near 30 DTE
            day_codes = day.groupby('settle_group').first().reset_index()
            cands=[]
            for _, r in day_codes.iterrows():
                ed = exp_map.get(r['expiry_code'])
                if ed is None: continue
                dte=(ed-d).days
                if 20<=dte<=45: cands.append((dte, r['settle_group'], ed))
            if not cands:
                equity_curve.append((d, cash)); continue
            dte, sg, exp = min(cands, key=lambda x:abs(x[0]-TARGET_DTE))
            grp = day[day['settle_group']==sg]
            calls = grp[grp['otype']=='C'].set_index('strike')['SETTLEPRICE'].sort_index()
            puts = grp[grp['otype']=='P'].set_index('strike')['SETTLEPRICE'].sort_index()
            common = sorted(set(calls.index) & set(puts.index))
            if len(common)<6:
                equity_curve.append((d, cash)); continue
            K=np.array(common,float); C=calls.reindex(common).values; P=puts.reindex(common).values
            T=dte/365.0
            r=rusfar.asof(d) if d>=rusfar.index[0] else 0.12
            if pd.isna(r): r=0.12
            disc=np.exp(-r*T)
            F=derive_forward(K,C,P,disc)
            if F is None or F<=0:
                equity_curve.append((d, cash)); continue
            # invert IV per strike
            ivs=[]
            for k,c_p,p_p in zip(K,C,P):
                iv = black76_iv(c_p, F, k, T, r, 'call') if k>=F else black76_iv(p_p, F, k, T, r, 'put')
                ivs.append(iv if iv else np.nan)
            ivs=np.array(ivs)

            # pick strikes
            sc_K = pick_delta_strike(K, ivs, F, T, r, SHORT_DELTA, 'call')
            sp_K = pick_delta_strike(K, ivs, F, T, r, SHORT_DELTA, 'put')
            lc_K = pick_delta_strike(K, ivs, F, T, r, WING_DELTA, 'call')
            lp_K = pick_delta_strike(K, ivs, F, T, r, WING_DELTA, 'put')
            if None in (sc_K, sp_K, lc_K, lp_K):
                equity_curve.append((d, cash)); continue

            sc_px=float(calls[sc_K]); sp_px=float(puts[sp_K])
            lc_px=float(calls[lc_K]); lp_px=float(puts[lp_K])
            credit = (sc_px + sp_px) - (lc_px + lp_px)  # per contract
            if credit<=0:
                equity_curve.append((d, cash)); continue

            # contracts sizing: max loss per condor = max(call_wing_width, put_wing_width) - credit
            call_wing = lc_K - sc_K
            put_wing = sp_K - lp_K
            max_loss_per = max(call_wing, put_wing) - credit
            target_margin = cash * POSITION_SIZE
            contracts = max(1, int(target_margin / max(max_loss_per, 1)))

            # find SECIDs
            def secid_for(strike, otype):
                row=grp[(grp['strike']==strike)&(grp['otype']==otype)]
                return row['SECID'].iloc[0] if len(row) else None
            sc_id = secid_for(sc_K,'C'); sp_id=secid_for(sp_K,'P')
            lc_id = secid_for(lc_K,'C'); lp_id=secid_for(lp_K,'P')
            if None in (sc_id,sp_id,lc_id,lp_id):
                equity_curve.append((d, cash)); continue

            open_pos = {
                'entry_date': d, 'expiry': exp, 'contracts': contracts,
                'sc':{'secid':sc_id,'open_px':sc_px,'last_px':sc_px,'strike':sc_K},
                'sp':{'secid':sp_id,'open_px':sp_px,'last_px':sp_px,'strike':sp_K},
                'lc':{'secid':lc_id,'open_px':lc_px,'last_px':lc_px,'strike':lc_K},
                'lp':{'secid':lp_id,'open_px':lp_px,'last_px':lp_px,'strike':lp_K},
                'credit_received': credit,
                'margin': max_loss_per * contracts,
                'current_pnl': 0,
            }

        # record equity (cash + MTM if position open)
        eq = cash + (open_pos['current_pnl'] if open_pos else 0)
        equity_curve.append((d, eq))

    # === Results ===
    eq=pd.DataFrame(equity_curve, columns=['date','equity']).set_index('date')
    tr=pd.DataFrame(trades)
    tot=(eq['equity'].iloc[-1]/INIT_CAPITAL-1)*100
    dd=((eq['equity'].cummax()-eq['equity'])/eq['equity'].cummax()).max()*100
    wr=(tr['pnl']>0).mean()*100 if len(tr) else 0
    print(f"\n=== Iron Condor backtest (sell {SHORT_DELTA*100:.0f}d strangle + buy {WING_DELTA*100:.0f}d wings) ===")
    print(f"VRP>=5 entry, exit 50% profit or <7DTE or 2x loss stop")
    print(f"Total: {tot:+.0f}%, MaxDD: {dd:.0f}%, Trades: {len(tr)}, WR: {wr:.0f}%")
    if len(tr):
        print(f"Avg trade PnL: {tr['pnl'].mean():+.0f} ₽")
        print(f"Avg trade ROI on margin: {tr['pnl_pct'].mean():+.1f}%")
        # per year
        tr['year']=pd.to_datetime(tr['exit']).dt.year
        for y, g in tr.groupby('year'):
            print(f"  {y}: N={len(g)} WR={(g['pnl']>0).mean()*100:.0f}% PnL={g['pnl'].sum():+.0f} ₽")
    eq.to_csv(ROOT/'sber_vol_selling_equity.csv')


if __name__ == "__main__":
    main()
