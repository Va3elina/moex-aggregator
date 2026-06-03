"""Backtest SHORT strategies based on options-derived bearish signals.

Bearish signals found in research:
  1. BTC DVOL ≤ 35 (extreme calm before storm) — P(up)=25%, -7.2%/60d
  2. ETH VRP z30 ≤ -2 (rapid VRP collapse on ETH) — P(up)=32%, -6.9%/60d
  3. ETH VRP ≥ +20 (ETH options overpriced) — P(up)=35%, -4.3%/60d
  4. BTC DVOL slope 7d ≤ -10 (DVOL collapsing) — P(up)=41%, -8.4%/60d
  5. Composite: BTC DVOL low + DVOL slope down (super calm + getting calmer)

Tests:
  - Different hold periods (30, 60, 90, 120)
  - Different exit rules (fixed days, DVOL spike, price stop)
  - Combined with macro filter (crypto in downtrend on weekly)
  - Long-short hedge combinations
"""
import warnings; warnings.filterwarnings("ignore")
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(__file__).parent
INITIAL = 100.0
FEE = 0.0005
SLIP = 0.0003


def ema(arr, n):
    if hasattr(arr, 'values'): arr = arr.values
    out = np.full(len(arr), np.nan)
    if len(arr) < n: return out
    out[n-1] = arr[:n].mean(); k = 2.0/(n+1.0)
    for i in range(n, len(arr)):
        out[i] = arr[i]*k + out[i-1]*(1-k)
    return out


def load_data(asset='BTC'):
    if asset == 'BTC':
        price = pd.read_csv(ROOT/'btc_usd_daily_coinbase.csv', parse_dates=['date']).set_index('date').sort_index()
    else:
        price = pd.read_csv(ROOT/'eth_usd_daily_coinbase.csv', parse_dates=['date']).set_index('date').sort_index()
    price = price[price['close']>0].dropna()
    dvol = pd.read_csv(ROOT / f'{asset.lower()}_dvol_daily.csv', parse_dates=['date']).set_index('date').sort_index()
    return price, dvol


def build_features(price, dvol):
    df = pd.DataFrame(index=price.index)
    df['close'] = price['close']
    df['high'] = price['high']
    df['low'] = price['low']
    log_ret = np.log(df['close'] / df['close'].shift(1))
    df['rv_30d'] = log_ret.rolling(30).std() * np.sqrt(365) * 100
    df['dvol'] = dvol['close'].reindex(df.index, method='ffill')
    df['vrp'] = df['dvol'] - df['rv_30d']
    df['dvol_ma30'] = df['dvol'].rolling(30).mean()
    df['dvol_z30'] = (df['dvol'] - df['dvol_ma30']) / df['dvol'].rolling(30).std()
    df['vrp_ma30'] = df['vrp'].rolling(30).mean()
    df['vrp_z30'] = (df['vrp'] - df['vrp_ma30']) / df['vrp'].rolling(30).std()
    df['dvol_slope_7d'] = df['dvol'] - df['dvol'].shift(7)
    df['vrp_slope_7d'] = df['vrp'] - df['vrp'].shift(7)
    df['ema50'] = ema(df['close'].values, 50)
    df['ema200'] = ema(df['close'].values, 200)
    df['ret_30d'] = df['close'].pct_change(30) * 100
    df['ret_90d'] = df['close'].pct_change(90) * 100
    return df


def backtest_short(df, entry_mask, hold_days, exit_dvol_normalize=None):
    """SHORT strategy backtest.
    Entry: signal triggers
    Exit: hold_days OR optionally when DVOL drops below normalize_threshold (rally fully calmed)
    """
    closes = df['close'].values
    highs = df['high'].values
    lows = df['low'].values
    dvol = df['dvol'].values
    dates = df.index
    n = len(closes)

    cash = INITIAL
    eq = np.full(n, INITIAL, dtype=float)
    in_pos = False
    entry_idx = 0
    entry_px = 0
    intra_max = 0  # for short, MAE is highest price reached
    trades = []

    for i in range(n):
        c = closes[i]
        # Mark to market for short
        if in_pos:
            if highs[i] > intra_max: intra_max = highs[i]
            # Short PnL: when price drops, we profit
            mtm_ret = (entry_px - c) / entry_px
            eq[i] = cash * (1 + mtm_ret)
        else:
            eq[i] = cash

        # Exit check
        if in_pos:
            hold = i - entry_idx
            do_exit = False
            reason = 'hold'
            if hold >= hold_days:
                do_exit = True; reason = 'maxhold'
            elif exit_dvol_normalize is not None and not np.isnan(dvol[i]) and dvol[i] >= exit_dvol_normalize:
                do_exit = True; reason = 'dvol_spike'

            if do_exit:
                exit_p = c * (1 + SLIP)  # buying back to cover short
                ret = (entry_px - exit_p) / entry_px - 2*FEE  # short return
                mae = (entry_px - intra_max) / entry_px - 2*FEE  # worst intra-trade
                cash *= (1 + ret)
                trades.append({'entry_date': dates[entry_idx], 'exit_date': dates[i],
                              'entry_px': entry_px, 'exit_px': exit_p,
                              'pnl_pct': ret*100, 'mae_pct': mae*100,
                              'hold_d': hold, 'reason': reason})
                in_pos = False
                eq[i] = cash

        # Entry SHORT
        if not in_pos and entry_mask[i]:
            entry_idx = i
            entry_px = c * (1 - SLIP)
            in_pos = True
            intra_max = highs[i]

    return pd.Series(eq, index=dates), pd.DataFrame(trades)


def metrics(eq, trades):
    if len(trades) == 0:
        return {'N': 0, 'WR': 0, 'PF': 0, 'total': 0, 'DD': 0, 'MAE': 0, 'avg_hold': 0}
    total = (eq.iloc[-1]/INITIAL - 1) * 100
    dd = ((eq.cummax() - eq)/eq.cummax()).max() * 100
    wr = (trades['pnl_pct']>0).sum()/len(trades) * 100
    pos = trades[trades['pnl_pct']>0]['pnl_pct'].sum()
    neg = abs(trades[trades['pnl_pct']<0]['pnl_pct'].sum())
    pf = pos/neg if neg > 0 else float('inf')
    mae = trades['mae_pct'].min()
    avg_hold = trades['hold_d'].mean()
    return {'N': len(trades), 'WR': wr, 'PF': pf, 'total': total, 'DD': dd, 'MAE': mae, 'avg_hold': avg_hold}


def fmt(m):
    pf_s = f"{m['PF']:>5.2f}" if m['PF'] != float('inf') else "  inf"
    return (f"N={m['N']:>3}  WR={m['WR']:>4.0f}%  PF={pf_s}  total={m['total']:>+7.0f}%  "
            f"DD={m['DD']:>4.1f}%  MAE={m['MAE']:>+5.1f}%  hold={m['avg_hold']:>3.0f}d")


def main():
    btc_price, btc_dvol = load_data('BTC')
    eth_price, eth_dvol = load_data('ETH')
    btc = build_features(btc_price, btc_dvol)
    eth = build_features(eth_price, eth_dvol)
    btc = btc.dropna(subset=['dvol']).copy()
    eth = eth.dropna(subset=['dvol']).copy()

    print(f"BTC data: {btc.index[0].date()} → {btc.index[-1].date()} ({len(btc)} days)")
    print(f"ETH data: {eth.index[0].date()} → {eth.index[-1].date()} ({len(eth)} days)")

    bh_btc = (btc['close'].iloc[-1]/btc['close'].iloc[0] - 1) * 100
    bh_eth = (eth['close'].iloc[-1]/eth['close'].iloc[0] - 1) * 100
    print(f"  Buy & Hold BTC: {bh_btc:+.0f}%, ETH: {bh_eth:+.0f}%\n")

    # === STUDY 1: BTC short signals
    print("="*150)
    print(f"STUDY 1: BTC SHORT signals — various entry conditions, hold 60 days")
    print("="*150)

    btc_signals = [
        ('BTC DVOL ≤ 35 (extreme calm)',           btc['dvol'] <= 35),
        ('BTC DVOL ≤ 40',                            btc['dvol'] <= 40),
        ('BTC DVOL slope 7d ≤ -10',                  btc['dvol_slope_7d'] <= -10),
        ('BTC DVOL slope 7d ≤ -5',                   btc['dvol_slope_7d'] <= -5),
        ('BTC VRP ≥ +20',                            btc['vrp'] >= 20),
        ('BTC VRP ≥ +30',                            btc['vrp'] >= 30),
        ('BTC VRP slope 7d ≥ +10',                   btc['vrp_slope_7d'] >= 10),
        ('Composite: DVOL ≤ 40 AND VRP ≥ +10',       (btc['dvol'] <= 40) & (btc['vrp'] >= 10)),
        ('Composite: DVOL ≤ 40 AND price up 30d',    (btc['dvol'] <= 40) & (btc['ret_30d'] > 10)),
        ('Composite: DVOL ≤ 35 AND price up 30d',    (btc['dvol'] <= 35) & (btc['ret_30d'] > 5)),
        ('Composite: VRP ≥ +20 AND price up 90d',    (btc['vrp'] >= 20) & (btc['ret_90d'] > 30)),
    ]

    for label, mask in btc_signals:
        m_arr = mask.fillna(False).values
        eq, tr = backtest_short(btc, m_arr, 60)
        m = metrics(eq, tr)
        print(f"  {label:55}  {fmt(m)}")

    # === STUDY 2: Hold period sweep for best BTC short
    print("\n" + "="*150)
    print(f"STUDY 2: BTC DVOL ≤ 35 SHORT — hold period sweep")
    print("="*150)

    best_short = (btc['dvol'] <= 35).fillna(False).values
    for hd in [14, 21, 30, 45, 60, 90, 120]:
        eq, tr = backtest_short(btc, best_short, hd)
        m = metrics(eq, tr)
        print(f"  hold {hd:>3}d:  {fmt(m)}")

    # === STUDY 3: Exit rule for short
    print("\n" + "="*150)
    print(f"STUDY 3: BTC DVOL ≤ 35 SHORT — exit on DVOL spike vs fixed hold")
    print("="*150)
    for hd in [60, 90, 120]:
        for exit_dvol in [None, 50, 60, 70]:
            eq, tr = backtest_short(btc, best_short, hd, exit_dvol_normalize=exit_dvol)
            m = metrics(eq, tr)
            label = f"hold≤{hd}d" + (f", exit if DVOL≥{exit_dvol}" if exit_dvol else "")
            print(f"  {label:35}  {fmt(m)}")

    # === STUDY 4: ETH SHORT signals
    print("\n" + "="*150)
    print(f"STUDY 4: ETH SHORT signals")
    print("="*150)

    eth_signals = [
        ('ETH DVOL ≤ 40',                            eth['dvol'] <= 40),
        ('ETH DVOL ≤ 50',                            eth['dvol'] <= 50),
        ('ETH VRP ≥ +20',                            eth['vrp'] >= 20),
        ('ETH VRP z30 ≤ -2 (rapid drop)',            eth['vrp_z30'] <= -2),
        ('ETH VRP z30 ≤ -1.5',                       eth['vrp_z30'] <= -1.5),
        ('ETH DVOL slope 7d ≤ -10',                  eth['dvol_slope_7d'] <= -10),
        ('Composite: ETH DVOL ≤ 50 AND VRP z30 ≤ -2', (eth['dvol'] <= 50) & (eth['vrp_z30'] <= -2)),
        ('Composite: ETH price up 30d AND DVOL ≤ 50', (eth['ret_30d'] > 10) & (eth['dvol'] <= 50)),
    ]
    for label, mask in eth_signals:
        m_arr = mask.fillna(False).values
        eq, tr = backtest_short(eth, m_arr, 60)
        m = metrics(eq, tr)
        print(f"  {label:55}  {fmt(m)}")

    # === STUDY 5: Trade-by-trade detail for best short
    print("\n" + "="*150)
    print(f"STUDY 5: Trade detail for top SHORT — BTC DVOL ≤ 35 hold 60d")
    print("="*150)

    best_short = (btc['dvol'] <= 35).fillna(False).values
    eq, tr = backtest_short(btc, best_short, 60)
    if len(tr) > 0:
        print(f"  {'#':>3}  {'entry':>11}  {'exit':>11}  {'days':>5}  {'entry_px':>10}  {'exit_px':>10}  {'PnL':>8}  {'MAE':>7}  {'reason':>15}")
        for i, t in enumerate(tr.itertuples(), 1):
            print(f"  {i:>3}  {t.entry_date.date()}  {t.exit_date.date()}  "
                  f"{int(t.hold_d):>4}d  ${t.entry_px:>9,.0f}  ${t.exit_px:>9,.0f}  "
                  f"{t.pnl_pct:>+7.1f}%  {t.mae_pct:>+6.1f}%  {t.reason:>15}")

    # === STUDY 6: Long+short combined portfolio
    print("\n" + "="*150)
    print(f"STUDY 6: COMBINED PORTFOLIO — VRP long + DVOL short on BTC")
    print("="*150)

    # Run long and short separately, combine via equity curves
    # LONG: VRP ≤ -10, hold 90d
    long_mask = (btc['vrp'] <= -10).fillna(False).values

    def backtest_long(df, entry_mask, hold_days):
        closes = df['close'].values
        n = len(closes); cash = INITIAL
        eq = np.full(n, INITIAL, dtype=float)
        in_pos = False; entry_idx = 0; entry_px = 0; intra_min = 0
        trades = []
        for i in range(n):
            c = closes[i]
            if in_pos:
                if df['low'].values[i] < intra_min or intra_min == 0:
                    intra_min = df['low'].values[i]
                eq[i] = cash * (c/entry_px)
            else:
                eq[i] = cash
            if in_pos and (i - entry_idx >= hold_days):
                exit_p = c * (1-SLIP)
                ret = (exit_p/entry_px - 1 - 2*FEE)
                cash *= (1+ret)
                trades.append({'pnl_pct': ret*100})
                in_pos = False
                eq[i] = cash
            if not in_pos and entry_mask[i]:
                entry_idx = i; entry_px = c*(1+SLIP); in_pos = True
                intra_min = df['low'].values[i]
        return pd.Series(eq, index=df.index), pd.DataFrame(trades)

    eq_long, tr_long = backtest_long(btc, long_mask, 90)
    short_mask = (btc['dvol'] <= 35).fillna(False).values
    eq_short, tr_short = backtest_short(btc, short_mask, 60)

    # Portfolio: 50% to each
    eq_port = 0.5 * eq_long + 0.5 * eq_short
    p_total = (eq_port.iloc[-1]/INITIAL - 1) * 100
    p_dd = ((eq_port.cummax() - eq_port)/eq_port.cummax()).max() * 100
    l_total = (eq_long.iloc[-1]/INITIAL - 1) * 100
    l_dd = ((eq_long.cummax() - eq_long)/eq_long.cummax()).max() * 100
    s_total = (eq_short.iloc[-1]/INITIAL - 1) * 100
    s_dd = ((eq_short.cummax() - eq_short)/eq_short.cummax()).max() * 100

    print(f"\n  LONG (VRP ≤ -10, 90d):        total={l_total:>+8.0f}%  DD={l_dd:>4.1f}%  N={len(tr_long)}")
    print(f"  SHORT (DVOL ≤ 35, 60d):       total={s_total:>+8.0f}%  DD={s_dd:>4.1f}%  N={len(tr_short)}")
    print(f"  50/50 portfolio:               total={p_total:>+8.0f}%  DD={p_dd:>4.1f}%")
    print(f"  Buy & Hold BTC:                total={bh_btc:>+8.0f}%")


if __name__ == "__main__":
    main()
