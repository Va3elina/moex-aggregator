"""Realistic backtest with funding cost + commission + slippage at various leverage.

Replicates Pine AIM logic exactly:
  LONG: VRP <= -10 → exit on DVOL >= 70 (after min 60d) or max 180d
  SHORT (AIM): opens right after LONG exit, held until next VRP signal
  Loop

Costs modeled:
  - Commission: 0.05% per side (Binance perps maker)
  - Slippage: 3bp per side
  - Funding rate (perps): payable on NOTIONAL position (scales with leverage)
      * LONG funding: typically NEGATIVE for shorts (you pay in bull market)
      * SHORT funding: typically POSITIVE in bull market (you RECEIVE), NEGATIVE in bear
"""
import warnings; warnings.filterwarnings("ignore")
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(__file__).parent
INITIAL = 100.0
FEE_PCT = 0.0005          # 0.05% per side
SLIP_PCT = 0.0003          # 0.03% per side


def load_btc():
    price = pd.read_csv(ROOT/'btc_usd_daily_coinbase.csv', parse_dates=['date']).set_index('date').sort_index()
    price = price[price['close']>0].dropna()
    dvol = pd.read_csv(ROOT / 'btc_dvol_daily.csv', parse_dates=['date']).set_index('date').sort_index()
    return price, dvol


def build_features(price, dvol):
    df = pd.DataFrame(index=price.index)
    df['close'] = price['close']
    log_ret = np.log(df['close'] / df['close'].shift(1))
    df['rv_30d'] = log_ret.rolling(30).std() * np.sqrt(365) * 100
    df['dvol'] = dvol['close'].reindex(df.index, method='ffill')
    df['vrp'] = df['dvol'] - df['rv_30d']
    return df


def backtest_aim_realistic(df, vrp_thr=-10, min_hold=60, dvol_exit=70, max_hold=180,
                             leverage=1.0,
                             long_funding_annual=0.10,   # LONG pays funding in bull market
                             short_funding_annual=-0.10, # SHORT receives funding in bull market (negative = income for short)
                             always_in_market=True):
    """
    funding_annual sign convention:
      Positive = cost (you PAY funding)
      Negative = income (you RECEIVE funding)

    For BTC perps in bull market:
      - LONGs pay funding (long_funding_annual = +0.10 to +0.30)
      - SHORTs receive funding (short_funding_annual = -0.10 to -0.30)
    For bear market: signs flip
    """
    closes = df['close'].values
    dates = df.index
    dvol = df['dvol'].values
    vrp = df['vrp'].values
    n = len(closes)

    cash = INITIAL
    eq = np.full(n, INITIAL, dtype=float)
    pos = 0  # 0=flat, 1=long, -1=short
    entry_idx = 0
    entry_px = 0
    qty = 0  # contracts (BTC)
    daily_long_fund = long_funding_annual / 365
    daily_short_fund = short_funding_annual / 365
    trades = []
    funding_accumulated = 0  # per-position funding tracker

    for i in range(n):
        c = closes[i]

        if pos == 1:
            # LONG MTM + accumulate funding
            unrealized = qty * (c - entry_px)
            funding_accumulated += qty * c * daily_long_fund  # cost (positive = pay)
            eq[i] = cash + unrealized - funding_accumulated
        elif pos == -1:
            unrealized = qty * (entry_px - c)  # short: profit when price falls
            funding_accumulated += abs(qty) * c * daily_short_fund  # cost (positive = pay)
            eq[i] = cash + unrealized - funding_accumulated
        else:
            eq[i] = cash

        # Check LONG exit
        if pos == 1:
            hold = i - entry_idx
            do_exit = False
            if hold >= max_hold: do_exit = True
            elif hold >= min_hold and not np.isnan(dvol[i]) and dvol[i] >= dvol_exit: do_exit = True
            if do_exit:
                exit_p = c * (1 - SLIP_PCT)
                gross_pnl = qty * (exit_p - entry_px)
                fees = qty * (entry_px + exit_p) * FEE_PCT  # entry+exit fees
                net_pnl = gross_pnl - fees - funding_accumulated
                cash += net_pnl
                trades.append({'side':'long', 'entry_d':dates[entry_idx], 'exit_d':dates[i],
                              'entry_px':entry_px, 'exit_px':exit_p, 'qty':qty, 'hold_d':hold,
                              'gross_pnl':gross_pnl, 'fees':fees, 'funding':funding_accumulated,
                              'net_pnl':net_pnl, 'net_pct':net_pnl/cash*100 if cash>0 else 0})

                # Open SHORT if AIM enabled
                if always_in_market:
                    entry_idx = i
                    entry_px = c * (1 - SLIP_PCT)
                    qty = leverage * cash * 0.99 / entry_px  # note: positive qty, side stored in pos
                    pos = -1
                    funding_accumulated = 0
                else:
                    pos = 0
                eq[i] = cash
                continue

        # Check LONG entry
        if not np.isnan(vrp[i]) and vrp[i] <= vrp_thr:
            if pos == -1:
                # close short
                exit_p = c * (1 + SLIP_PCT)
                gross_pnl = qty * (entry_px - exit_p)
                fees = qty * (entry_px + exit_p) * FEE_PCT
                net_pnl = gross_pnl - fees - funding_accumulated
                cash += net_pnl
                trades.append({'side':'short', 'entry_d':dates[entry_idx], 'exit_d':dates[i],
                              'entry_px':entry_px, 'exit_px':exit_p, 'qty':qty, 'hold_d':i-entry_idx,
                              'gross_pnl':gross_pnl, 'fees':fees, 'funding':funding_accumulated,
                              'net_pnl':net_pnl, 'net_pct':net_pnl/cash*100 if cash>0 else 0})
                pos = 0
                funding_accumulated = 0

            if pos == 0:
                entry_idx = i
                entry_px = c * (1 + SLIP_PCT)
                qty = leverage * cash * 0.99 / entry_px
                pos = 1
                funding_accumulated = 0
                eq[i] = cash

    # Close any open position at end
    if pos != 0:
        c = closes[-1]
        if pos == 1:
            exit_p = c * (1 - SLIP_PCT)
            gross_pnl = qty * (exit_p - entry_px)
        else:
            exit_p = c * (1 + SLIP_PCT)
            gross_pnl = qty * (entry_px - exit_p)
        fees = qty * (entry_px + exit_p) * FEE_PCT
        net_pnl = gross_pnl - fees - funding_accumulated
        cash += net_pnl
        trades.append({'side':'long' if pos==1 else 'short', 'entry_d':dates[entry_idx], 'exit_d':dates[-1],
                      'entry_px':entry_px, 'exit_px':exit_p, 'qty':qty, 'hold_d':(n-1)-entry_idx,
                      'gross_pnl':gross_pnl, 'fees':fees, 'funding':funding_accumulated,
                      'net_pnl':net_pnl, 'net_pct':net_pnl/cash*100 if cash>0 else 0})
        eq[-1] = cash

    return pd.Series(eq, index=dates), pd.DataFrame(trades)


def summarize(eq, tr, label):
    total = (eq.iloc[-1]/INITIAL - 1) * 100
    dd = ((eq.cummax() - eq)/eq.cummax()).max() * 100
    n_l = (tr['side']=='long').sum() if len(tr) else 0
    n_s = (tr['side']=='short').sum() if len(tr) else 0
    fund_total = tr['funding'].sum() if 'funding' in tr.columns and len(tr) else 0
    fees_total = tr['fees'].sum() if 'fees' in tr.columns and len(tr) else 0
    avg_l_pnl = tr[tr['side']=='long']['net_pct'].mean() if n_l > 0 else 0
    avg_s_pnl = tr[tr['side']=='short']['net_pct'].mean() if n_s > 0 else 0
    return {
        'label': label,
        'total_pct': total,
        'dd_pct': dd,
        'n_long': n_l,
        'n_short': n_s,
        'fund_cost': fund_total,
        'fees': fees_total,
        'avg_long_pnl': avg_l_pnl,
        'avg_short_pnl': avg_s_pnl,
    }


def main():
    price, dvol = load_btc()
    df = build_features(price, dvol).dropna(subset=['dvol']).copy()
    print(f"\nBTC data: {df.index[0].date()} → {df.index[-1].date()} ({len(df)}d)")
    bh = (df['close'].iloc[-1]/df['close'].iloc[0] - 1) * 100
    print(f"Buy & Hold BTC: {bh:+.0f}%\n")

    print("="*180)
    print("STRATEGY: VRP <= -10 LONG → exit DVOL>=70 → AlwaysIn SHORT → next VRP signal closes")
    print("Costs: 0.05% fee per side, 0.03% slippage per side, funding on NOTIONAL position (scales with leverage)")
    print("="*180)

    # Funding scenarios for BTC perps:
    # bull market (historical avg): LONG pays ~10%/yr, SHORT receives ~10%/yr → short_fund = -0.10 (income)
    # neutral: LONG pays ~5%, SHORT receives ~5% → short_fund = -0.05
    # bear market: LONG receives, SHORT pays → short_fund = +0.10
    # extreme bull (FOMO): LONG pays 30-50%, SHORT receives same → short_fund = -0.30

    scenarios = [
        ("No funding (current Pine)",    0.00, 0.00),
        ("Neutral (avg of cycles)",      0.05,-0.05),
        ("Bull market (typical)",        0.10,-0.10),
        ("Strong bull (FOMO periods)",   0.20,-0.20),
        ("Mixed: LONG pays, SHORT pays", 0.10, 0.10),  # worst case
    ]
    leverages = [1.0, 2.0, 3.0, 5.0]

    for scen_name, lf, sf in scenarios:
        print(f"\n>>> Scenario: {scen_name}  (LONG funding={lf*100:+.0f}%/yr, SHORT funding={sf*100:+.0f}%/yr)")
        print(f"  {'Leverage':>8}  {'Total %':>9}  {'DD %':>5}  {'N L':>3}  {'N S':>3}  "
              f"{'Avg L %':>9}  {'Avg S %':>9}  {'Fund $':>10}  {'Fees $':>8}")
        for lev in leverages:
            eq, tr = backtest_aim_realistic(df, leverage=lev,
                                             long_funding_annual=lf,
                                             short_funding_annual=sf,
                                             always_in_market=True)
            s = summarize(eq, tr, f"{lev}x")
            print(f"  {s['label']:>8}  {s['total_pct']:>+8.0f}%  {s['dd_pct']:>4.0f}%  "
                  f"{s['n_long']:>3}  {s['n_short']:>3}  "
                  f"{s['avg_long_pnl']:>+8.1f}%  {s['avg_short_pnl']:>+8.1f}%  "
                  f"{s['fund_cost']:>+9.0f}  {s['fees']:>7.0f}")

    # LONG-only comparison
    print("\n" + "="*180)
    print("LONG-ONLY (no SHORT between) — comparison")
    print("="*180)
    print(f"  {'Leverage':>8}  {'Total %':>9}  {'DD %':>5}  {'N L':>3}  "
          f"{'Avg L %':>9}  {'Fund $':>10}  {'Fees $':>8}")
    for lf in [0.00, 0.10, 0.20]:
        print(f"\n>>> LONG funding {lf*100:.0f}%/yr:")
        for lev in leverages:
            eq, tr = backtest_aim_realistic(df, leverage=lev,
                                             long_funding_annual=lf, short_funding_annual=0,
                                             always_in_market=False)
            s = summarize(eq, tr, f"{lev}x")
            print(f"  {s['label']:>8}  {s['total_pct']:>+8.0f}%  {s['dd_pct']:>4.0f}%  "
                  f"{s['n_long']:>3}  "
                  f"{s['avg_long_pnl']:>+8.1f}%  "
                  f"{s['fund_cost']:>+9.0f}  {s['fees']:>7.0f}")

    # Detail: per-trade for 2x AIM with realistic funding
    print("\n" + "="*180)
    print("DETAIL: 2x AIM with realistic funding (LONG pays 10%/yr, SHORT receives 10%/yr)")
    print("="*180)
    eq, tr = backtest_aim_realistic(df, leverage=2.0,
                                     long_funding_annual=0.10, short_funding_annual=-0.10,
                                     always_in_market=True)
    for _, t in tr.iterrows():
        print(f"  {t['side'].upper():<5}  {str(t['entry_d'].date())} → {str(t['exit_d'].date())} "
              f"({int(t['hold_d']):>3}d)  qty={t['qty']:>7.5f}  "
              f"gross={t['gross_pnl']:>+8.1f}$  fees={t['fees']:>5.2f}$  fund={t['funding']:>+7.2f}$  "
              f"net={t['net_pnl']:>+8.1f}$ ({t['net_pct']:>+6.1f}%)")
    total = (eq.iloc[-1]/INITIAL - 1) * 100
    print(f"\n  Final equity: ${eq.iloc[-1]:.0f}  ({total:+.0f}%)")


if __name__ == "__main__":
    main()
