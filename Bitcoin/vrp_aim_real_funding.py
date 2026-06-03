"""Backtest BTC AIM strategy with REAL funding rates from Deribit (interest_8h).

Bybit fees: 0.02% maker / 0.055% taker. Market entry = taker = 0.055%.
On limit exit could be maker = 0.02%, but we conservatively use taker both sides.

Funding rate is applied to NOTIONAL position (qty * current_price), not equity.
So with leverage, funding cost scales linearly with leverage.

Deribit rates are LOWER than Bybit by ~1.5x on average (institutional vs retail).
We add a "bybit_multiplier" to scale Deribit rates up to Bybit-equivalent.
"""
import warnings; warnings.filterwarnings("ignore")
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(__file__).parent
INITIAL = 100.0
FEE_PCT = 0.00055      # Bybit taker 0.055%
SLIP_PCT = 0.0003       # 3bp


def load_data():
    price = pd.read_csv(ROOT/'btc_usd_daily_coinbase.csv', parse_dates=['date']).set_index('date').sort_index()
    price = price[price['close']>0].dropna()
    dvol = pd.read_csv(ROOT / 'btc_dvol_daily.csv', parse_dates=['date']).set_index('date').sort_index()

    funding = pd.read_csv(ROOT/'btc_funding.csv', parse_dates=['ts'])
    funding['ts'] = funding['ts'].dt.tz_localize(None)
    # interest_8h is the 8h-projected rate from current hourly rate
    # for daily backtest, sum hourly rates per day, expressed as daily decimal
    funding['date'] = funding['ts'].dt.normalize()
    funding['hourly'] = funding['rate'] / 8  # convert interest_8h back to hourly rate
    daily_funding = funding.groupby('date')['hourly'].sum().rename('daily_funding')  # = sum of 24 hourly rates per day
    return price, dvol, daily_funding


def build_features(price, dvol, daily_funding):
    df = pd.DataFrame(index=price.index)
    df['close'] = price['close']
    log_ret = np.log(df['close'] / df['close'].shift(1))
    df['rv_30d'] = log_ret.rolling(30).std() * np.sqrt(365) * 100
    df['dvol'] = dvol['close'].reindex(df.index, method='ffill')
    df['vrp'] = df['dvol'] - df['rv_30d']
    df['funding_d'] = daily_funding.reindex(df.index).fillna(0)
    return df


def backtest_real(df, vrp_thr=-10, min_hold=60, dvol_exit=70, max_hold=180,
                    leverage=1.0, always_in_market=True, bybit_mult=1.5):
    """bybit_mult: multiplier on Deribit funding to approximate retail venue."""
    closes = df['close'].values
    dates = df.index
    dvol = df['dvol'].values
    vrp = df['vrp'].values
    fund_d = df['funding_d'].values * bybit_mult

    n = len(closes)
    cash = INITIAL
    eq = np.full(n, INITIAL, dtype=float)
    pos = 0
    entry_idx = 0
    entry_px = 0
    qty = 0
    trades = []
    funding_accumulated = 0  # in dollars

    for i in range(n):
        c = closes[i]

        # Daily mark to market + funding accrual
        if pos == 1:
            unrealized = qty * (c - entry_px)
            # LONG pays positive funding → cost
            funding_accumulated += qty * c * fund_d[i]
            eq[i] = cash + unrealized - funding_accumulated
        elif pos == -1:
            unrealized = qty * (entry_px - c)
            # SHORT receives positive funding → income (subtract → add to PnL)
            funding_accumulated += abs(qty) * c * (-fund_d[i])  # negative = income
            eq[i] = cash + unrealized - funding_accumulated
        else:
            eq[i] = cash

        # LONG exit
        if pos == 1:
            hold = i - entry_idx
            do_exit = False
            if hold >= max_hold: do_exit = True
            elif hold >= min_hold and not np.isnan(dvol[i]) and dvol[i] >= dvol_exit: do_exit = True
            if do_exit:
                exit_p = c * (1 - SLIP_PCT)
                gross = qty * (exit_p - entry_px)
                fees = qty * (entry_px + exit_p) * FEE_PCT
                net = gross - fees - funding_accumulated
                cash += net
                trades.append({'side':'long', 'entry_d':dates[entry_idx], 'exit_d':dates[i],
                              'entry_px':entry_px, 'exit_px':exit_p, 'qty':qty, 'hold_d':hold,
                              'gross':gross, 'fees':fees, 'funding':funding_accumulated, 'net':net,
                              'net_pct': net / (cash - net) * 100 if (cash - net) > 0 else 0})

                if always_in_market:
                    entry_idx = i
                    entry_px = c * (1 - SLIP_PCT)
                    qty = leverage * cash * 0.99 / entry_px
                    pos = -1
                    funding_accumulated = 0
                else:
                    pos = 0
                eq[i] = cash
                continue

        # LONG entry signal
        if not np.isnan(vrp[i]) and vrp[i] <= vrp_thr:
            if pos == -1:
                exit_p = c * (1 + SLIP_PCT)
                gross = qty * (entry_px - exit_p)
                fees = qty * (entry_px + exit_p) * FEE_PCT
                net = gross - fees - funding_accumulated
                cash += net
                trades.append({'side':'short', 'entry_d':dates[entry_idx], 'exit_d':dates[i],
                              'entry_px':entry_px, 'exit_px':exit_p, 'qty':qty, 'hold_d':i-entry_idx,
                              'gross':gross, 'fees':fees, 'funding':funding_accumulated, 'net':net,
                              'net_pct': net / (cash - net) * 100 if (cash - net) > 0 else 0})
                pos = 0
                funding_accumulated = 0

            if pos == 0:
                entry_idx = i
                entry_px = c * (1 + SLIP_PCT)
                qty = leverage * cash * 0.99 / entry_px
                pos = 1
                funding_accumulated = 0
                eq[i] = cash

    # Close at end
    if pos != 0:
        c = closes[-1]
        if pos == 1:
            exit_p = c * (1 - SLIP_PCT); gross = qty * (exit_p - entry_px)
        else:
            exit_p = c * (1 + SLIP_PCT); gross = qty * (entry_px - exit_p)
        fees = qty * (entry_px + exit_p) * FEE_PCT
        net = gross - fees - funding_accumulated
        cash += net
        trades.append({'side':'long' if pos==1 else 'short', 'entry_d':dates[entry_idx], 'exit_d':dates[-1],
                      'entry_px':entry_px, 'exit_px':exit_p, 'qty':qty, 'hold_d':(n-1)-entry_idx,
                      'gross':gross, 'fees':fees, 'funding':funding_accumulated, 'net':net,
                      'net_pct': net / (cash - net) * 100 if (cash - net) > 0 else 0})
        eq[-1] = cash

    return pd.Series(eq, index=dates), pd.DataFrame(trades)


def main():
    price, dvol, daily_funding = load_data()
    df = build_features(price, dvol, daily_funding).dropna(subset=['dvol']).copy()
    print(f"\nBTC data: {df.index[0].date()} → {df.index[-1].date()}  ({len(df)}d)")
    bh = (df['close'].iloc[-1]/df['close'].iloc[0] - 1) * 100
    print(f"Buy & Hold: {bh:+.0f}%\n")

    # Show funding stats
    yearly_fund = df['funding_d'] * 365 * 100
    print(f"Funding stats (Deribit annualized):")
    print(f"  Mean: {yearly_fund.mean():+.1f}%/yr  Median: {yearly_fund.median():+.1f}%/yr")
    print(f"  p10: {yearly_fund.quantile(0.1):+.0f}%  p90: {yearly_fund.quantile(0.9):+.0f}%  max: {yearly_fund.max():+.0f}%")

    print("\n" + "="*180)
    print("BACKTEST WITH REAL FUNDING (Deribit data, scaled by 'bybit_mult' to approximate Bybit retail venue)")
    print("Bybit fees: 0.055% taker per side (× 2 = 0.11% per round trip), 3bp slippage")
    print("="*180)

    for bybit_mult, label in [(1.0, "Deribit (institutional)"), (1.5, "Bybit estimate (1.5x Deribit)"), (2.0, "Bybit FOMO worst (2x Deribit)")]:
        print(f"\n>>> {label}:")
        print(f"  {'Lev':>4}  {'Total %':>9}  {'DD %':>5}  {'N L':>3}  {'N S':>3}  "
              f"{'Fees $':>7}  {'Fund $':>9}  {'Avg L%':>7}  {'Avg S%':>7}")
        for lev in [1.0, 2.0, 3.0, 5.0]:
            eq, tr = backtest_real(df, leverage=lev, always_in_market=True, bybit_mult=bybit_mult)
            total = (eq.iloc[-1]/INITIAL - 1) * 100
            dd = ((eq.cummax() - eq)/eq.cummax()).max() * 100
            n_l = (tr['side']=='long').sum()
            n_s = (tr['side']=='short').sum()
            avg_l = tr[tr['side']=='long']['net_pct'].mean() if n_l else 0
            avg_s = tr[tr['side']=='short']['net_pct'].mean() if n_s else 0
            print(f"  {lev:>3.0f}x  {total:>+8.0f}%  {dd:>4.0f}%  {n_l:>3}  {n_s:>3}  "
                  f"{tr['fees'].sum():>6.0f}  {tr['funding'].sum():>+8.0f}  "
                  f"{avg_l:>+6.1f}%  {avg_s:>+6.1f}%")

    # Long-only comparison
    print("\n" + "="*180)
    print("LONG-ONLY (no SHORT between) vs AIM — at 2x leverage with Bybit-like funding (1.5x Deribit)")
    print("="*180)

    for lev in [1.0, 2.0, 3.0]:
        print(f"\n  Leverage {lev:.0f}x:")
        eq_aim, tr_aim = backtest_real(df, leverage=lev, always_in_market=True, bybit_mult=1.5)
        eq_long, tr_long = backtest_real(df, leverage=lev, always_in_market=False, bybit_mult=1.5)
        t_aim = (eq_aim.iloc[-1]/INITIAL - 1) * 100
        t_long = (eq_long.iloc[-1]/INITIAL - 1) * 100
        dd_aim = ((eq_aim.cummax() - eq_aim)/eq_aim.cummax()).max() * 100
        dd_long = ((eq_long.cummax() - eq_long)/eq_long.cummax()).max() * 100
        print(f"    AIM:       {t_aim:>+8.0f}%  DD {dd_aim:>4.0f}%  (extra from SHORTs: {t_aim - t_long:+.0f}pp)")
        print(f"    LONG only: {t_long:>+8.0f}%  DD {dd_long:>4.0f}%")

    # Per-trade detail
    print("\n" + "="*180)
    print("DETAIL: 2x AIM with Bybit-like funding (1.5x Deribit)")
    print("="*180)
    eq, tr = backtest_real(df, leverage=2.0, always_in_market=True, bybit_mult=1.5)
    for _, t in tr.iterrows():
        print(f"  {t['side'].upper():<5}  {t['entry_d'].date()} → {t['exit_d'].date()} ({int(t['hold_d']):>3}d)  "
              f"qty={t['qty']:>7.5f}  gross={t['gross']:>+9.2f}$  fees={t['fees']:>5.2f}$  "
              f"fund={t['funding']:>+8.2f}$  net={t['net']:>+9.2f}$ ({t['net_pct']:>+6.1f}%)")
    total = (eq.iloc[-1]/INITIAL - 1) * 100
    print(f"\n  Final: ${eq.iloc[-1]:.0f}  ({total:+.0f}%)")
    print(f"  Total funding paid: ${tr['funding'].sum():.0f}  ({tr['funding'].sum()/INITIAL*100:.1f}% of initial)")
    print(f"  Total fees paid:    ${tr['fees'].sum():.0f}  ({tr['fees'].sum()/INITIAL*100:.1f}% of initial)")


if __name__ == "__main__":
    main()
