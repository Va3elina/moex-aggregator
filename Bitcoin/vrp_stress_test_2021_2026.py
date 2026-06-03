"""Full-history backtest 2021-03 → 2026-05 covering crypto winter.

Major events in this period:
  2021-03  DVOL starts publishing (~85% during bull mania)
  2021-04  Coinbase IPO peak ($64k BTC)
  2021-05  China mining ban — BTC -50% in 30 days, DVOL spike to 130
  2021-11  BTC ATH $69k
  2022-01  Start of bear (-20%)
  2022-05  LUNA/UST collapse — BTC -35% in week, DVOL 110+
  2022-06  3AC/Celsius collapse — BTC -40% from peak
  2022-11  FTX collapse — BTC -25% in week, DVOL 90+
  2022-12  Bottom $15.5k (-77% from ATH)
  2023-03  SVB bank crisis, BTC bounces
  2024-04  Halving
  2024-11  Trump win → bull
  2025+    Latest bull cycle

Strategy: VRP <= -10 LONG → AlwaysIn SHORT → next VRP signal
With real funding (Deribit) where available, $10/yr LONG cost as proxy where missing.
"""
import warnings; warnings.filterwarnings("ignore")
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(__file__).parent
INITIAL = 100.0
FEE_PCT = 0.00055
SLIP_PCT = 0.0003


def load_data():
    price = pd.read_csv(ROOT/'btc_usd_daily_coinbase.csv', parse_dates=['date']).set_index('date').sort_index()
    price = price[price['close']>0].dropna()
    dvol = pd.read_csv(ROOT/'btc_dvol_daily.csv', parse_dates=['date']).set_index('date').sort_index()

    # Funding: real where we have it, fallback to historical mean +10%/yr LONG-pay (typical bull regime)
    funding = pd.read_csv(ROOT/'btc_funding.csv', parse_dates=['ts'])
    funding['ts'] = funding['ts'].dt.tz_localize(None)
    funding['date'] = funding['ts'].dt.normalize()
    funding['hourly'] = funding['rate'] / 8
    daily_funding = funding.groupby('date')['hourly'].sum().rename('daily_funding')
    return price, dvol, daily_funding


def build_features(price, dvol, daily_funding, fallback_annual_funding=0.10):
    df = pd.DataFrame(index=price.index)
    df['close'] = price['close']
    log_ret = np.log(df['close'] / df['close'].shift(1))
    df['rv_30d'] = log_ret.rolling(30).std() * np.sqrt(365) * 100
    df['dvol'] = dvol['close'].reindex(df.index, method='ffill')
    df['vrp'] = df['dvol'] - df['rv_30d']
    funding_reidx = daily_funding.reindex(df.index)
    # Fill missing funding with historical mean
    df['funding_d'] = funding_reidx.fillna(fallback_annual_funding / 365)
    return df


def backtest(df, vrp_thr=-10, min_hold=60, dvol_exit=70, max_hold=180,
              leverage=1.0, always_in_market=True, bybit_mult=1.5):
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
    funding_accumulated = 0
    liquidated = False

    for i in range(n):
        c = closes[i]

        if pos == 1:
            unrealized = qty * (c - entry_px)
            funding_accumulated += qty * c * fund_d[i]
            eq[i] = cash + unrealized - funding_accumulated
        elif pos == -1:
            unrealized = qty * (entry_px - c)
            funding_accumulated += abs(qty) * c * (-fund_d[i])
            eq[i] = cash + unrealized - funding_accumulated
        else:
            eq[i] = cash

        # === Liquidation check (margin call) ===
        # Liquidation triggers when equity drops below ~5% of initial margin
        # Initial margin = position notional / leverage. With our sizing, notional = leverage * 0.99 * cash_at_entry.
        # Required margin at given leverage = notional / leverage = 0.99 * cash_at_entry
        # If equity falls below maintenance margin (~50% of init margin → 0.5 * 0.99 * cash_at_entry),
        # we get liquidated and lose entry margin.
        # Simplified: liquidate if equity falls to (1 - 1/leverage * 0.9) of pre-trade cash
        # For leverage L: ~(1 - 0.9/L) drop threshold
        # 1x: -90% (basically never)
        # 2x: -45%
        # 3x: -30%
        # 5x: -18%
        if pos != 0 and leverage > 1:
            liq_pct = 0.9 / leverage  # adverse move that wipes margin
            if pos == 1:
                adverse_move = (entry_px - c) / entry_px
            else:
                adverse_move = (c - entry_px) / entry_px
            if adverse_move >= liq_pct:
                trades.append({'side':'LIQUIDATED-'+('long' if pos==1 else 'short'),
                              'entry_d':dates[entry_idx], 'exit_d':dates[i],
                              'entry_px':entry_px, 'exit_px':c, 'qty':qty, 'hold_d':i-entry_idx,
                              'gross':-qty*entry_px*0.99, 'fees':0, 'funding':funding_accumulated,
                              'net':-cash*0.99, 'net_pct':-99})
                cash = cash * 0.01  # 1% remains
                pos = 0
                liquidated = True
                eq[i] = cash
                continue

        # Long exit
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

                if always_in_market and cash > 1:
                    entry_idx = i
                    entry_px = c * (1 - SLIP_PCT)
                    qty = leverage * cash * 0.99 / entry_px
                    pos = -1
                    funding_accumulated = 0
                else:
                    pos = 0
                eq[i] = cash
                continue

        # Long entry signal
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

            if pos == 0 and cash > 1:
                entry_idx = i
                entry_px = c * (1 + SLIP_PCT)
                qty = leverage * cash * 0.99 / entry_px
                pos = 1
                funding_accumulated = 0
                eq[i] = cash

    return pd.Series(eq, index=dates), pd.DataFrame(trades), liquidated


def main():
    price, dvol, daily_funding = load_data()
    df = build_features(price, dvol, daily_funding).dropna(subset=['dvol']).copy()
    print(f"\nBTC data: {df.index[0].date()} → {df.index[-1].date()}  ({len(df)}d)")
    bh = (df['close'].iloc[-1]/df['close'].iloc[0] - 1) * 100
    print(f"Buy & Hold: {bh:+.0f}%\n")

    # DVOL stats per regime
    print("DVOL/VRP behavior across this period:")
    print(f"  DVOL: min={df['dvol'].min():.0f}  max={df['dvol'].max():.0f}  mean={df['dvol'].mean():.0f}")
    print(f"  VRP <= -10 events (LONG signals): {(df['vrp'] <= -10).sum()}")
    print(f"  DVOL >= 70 events (LONG exits): {(df['dvol'] >= 70).sum()}")

    # Major crashes — what did BTC and signals look like?
    crash_periods = [
        ("China mining ban", '2021-05-15', '2021-07-15'),
        ("LUNA/UST collapse", '2022-05-01', '2022-05-31'),
        ("3AC/Celsius",       '2022-06-01', '2022-07-15'),
        ("FTX collapse",      '2022-11-01', '2022-12-15'),
        ("Crypto winter low", '2022-12-15', '2023-01-31'),
    ]
    print("\nMajor crash events:")
    for name, s, e in crash_periods:
        sub = df.loc[s:e]
        if len(sub) > 0:
            btc_move = (sub['close'].iloc[-1] / sub['close'].iloc[0] - 1) * 100
            dvol_peak = sub['dvol'].max()
            vrp_min = sub['vrp'].min()
            print(f"  {name:<22}  {s} → {e}  BTC {btc_move:+.0f}%  DVOL peak {dvol_peak:.0f}  VRP min {vrp_min:+.0f}")

    print("\n" + "="*180)
    print("STRESS TEST: VRP AIM strategy across crypto winter (2021-2026)")
    print(f"With Bybit-like funding (1.5x Deribit), fees 0.055% × 2, slippage 3bp × 2, liquidation modeled")
    print("="*180)

    for lev in [1.0, 2.0, 3.0, 5.0]:
        eq, tr, liq = backtest(df, leverage=lev, always_in_market=True, bybit_mult=1.5)
        total = (eq.iloc[-1]/INITIAL - 1) * 100
        dd = ((eq.cummax() - eq)/eq.cummax()).max() * 100
        n_l = (tr['side']=='long').sum()
        n_s = (tr['side']=='short').sum()
        n_liq = tr['side'].str.startswith('LIQUIDATED').sum()
        wr = (tr['net']>0).sum()/max(len(tr),1)*100
        print(f"\n  Leverage {lev:.0f}x:  total={total:>+8.0f}%  DD={dd:>4.0f}%  "
              f"trades L={n_l} S={n_s} LIQ={n_liq}  WR={wr:.0f}%  "
              f"{'LIQUIDATED!' if liq else 'survived'}")

    # Per-trade detail at 2x
    print("\n" + "="*180)
    print("DETAIL: 2x AIM full history (2021-03 → 2026-05)")
    print("="*180)
    eq, tr, _ = backtest(df, leverage=2.0, always_in_market=True, bybit_mult=1.5)
    cash_running = INITIAL
    for _, t in tr.iterrows():
        side_label = t['side'].upper()
        marker = ""
        if 'LIQ' in side_label:
            marker = " ⚠️ LIQUIDATED"
        elif t['net'] < 0:
            marker = " ❌"
        else:
            marker = " ✓"
        print(f"  {side_label:<20}  {t['entry_d'].date()} → {t['exit_d'].date()} ({int(t['hold_d']):>3}d)  "
              f"qty={t['qty']:>8.5f}  net={t['net']:>+10.2f}$ ({t['net_pct']:>+6.1f}%){marker}")
    total = (eq.iloc[-1]/INITIAL - 1) * 100
    print(f"\n  Final: ${eq.iloc[-1]:,.0f}  ({total:+,.0f}%)")
    print(f"  Buy & Hold same period: {bh:+.0f}%")
    print(f"  Strategy alpha: {total - bh:+,.0f}pp")

    # Check what happened during specific crashes
    print("\n" + "="*180)
    print("HOW STRATEGY BEHAVED DURING EACH MAJOR CRASH (at 2x)")
    print("="*180)
    for name, s, e in crash_periods:
        eq_sub = eq.loc[s:e]
        if len(eq_sub) > 0:
            btc_sub = df.loc[s:e, 'close']
            btc_move = (btc_sub.iloc[-1]/btc_sub.iloc[0] - 1) * 100
            strat_move = (eq_sub.iloc[-1]/eq_sub.iloc[0] - 1) * 100
            print(f"  {name:<22}  BTC {btc_move:>+6.1f}%  →  Strategy {strat_move:>+7.1f}%  ({'BEAT' if strat_move > btc_move else 'LOST'} BH by {abs(strat_move - btc_move):.0f}pp)")


if __name__ == "__main__":
    main()
