"""Improved AIM strategy with safety filters tested on 2021-2026.

Filters tested:
  A. Stop loss on SHORT (close if BTC +20%/+30% against)
  B. Short max hold = 60d (not 365)
  C. Disable SHORT in bull regime (weekly EMA200)
  D. Combo: B + C + stop loss
  E. LONG-only (no SHORT)
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
    funding = pd.read_csv(ROOT/'btc_funding.csv', parse_dates=['ts'])
    funding['ts'] = funding['ts'].dt.tz_localize(None)
    funding['date'] = funding['ts'].dt.normalize()
    funding['hourly'] = funding['rate'] / 8
    daily_funding = funding.groupby('date')['hourly'].sum().rename('daily_funding')
    return price, dvol, daily_funding


def build_features(price, dvol, daily_funding):
    df = pd.DataFrame(index=price.index)
    df['close'] = price['close']
    log_ret = np.log(df['close'] / df['close'].shift(1))
    df['rv_30d'] = log_ret.rolling(30).std() * np.sqrt(365) * 100
    df['dvol'] = dvol['close'].reindex(df.index, method='ffill')
    df['vrp'] = df['dvol'] - df['rv_30d']
    # Weekly EMA200 (simplified: 200-day EMA on daily)
    df['ema200_d'] = df['close'].ewm(span=200, adjust=False).mean()
    df['bull_regime'] = df['close'] > df['ema200_d']
    df['funding_d'] = daily_funding.reindex(df.index).fillna(0.10/365)
    return df


def backtest(df, vrp_thr=-10, min_hold=60, dvol_exit=70, max_hold_long=180,
              max_hold_short=365, leverage=1.0, always_in_market=True,
              short_in_bull_only=False,    # if True: SHORT only when BTC > EMA200
              short_in_bear_only=False,    # if True: SHORT only when BTC < EMA200
              short_stop_pct=None,         # close SHORT if BTC moves +X% against entry
              bybit_mult=1.5):
    closes = df['close'].values
    dates = df.index
    dvol = df['dvol'].values
    vrp = df['vrp'].values
    fund_d = df['funding_d'].values * bybit_mult
    bull = df['bull_regime'].values
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

        # Liquidation
        if pos != 0 and leverage > 1:
            liq_pct = 0.9 / leverage
            if pos == 1:
                adv = (entry_px - c) / entry_px
            else:
                adv = (c - entry_px) / entry_px
            if adv >= liq_pct:
                trades.append({'side':'LIQ-'+('long' if pos==1 else 'short'),
                              'entry_d':dates[entry_idx], 'exit_d':dates[i], 'qty':qty,
                              'hold_d':i-entry_idx, 'net':-cash*0.99, 'net_pct':-99})
                cash *= 0.01
                pos = 0
                liquidated = True
                eq[i] = cash
                continue

        # SHORT stop loss
        if pos == -1 and short_stop_pct is not None:
            adv_pct = (c - entry_px) / entry_px
            if adv_pct >= short_stop_pct:
                exit_p = c * (1 + SLIP_PCT)
                gross = qty * (entry_px - exit_p)
                fees = qty * (entry_px + exit_p) * FEE_PCT
                net = gross - fees - funding_accumulated
                cash += net
                trades.append({'side':'short-stopped', 'entry_d':dates[entry_idx], 'exit_d':dates[i],
                              'qty':qty, 'hold_d':i-entry_idx, 'net':net,
                              'net_pct': net / (cash - net) * 100 if (cash - net) > 0 else 0})
                pos = 0
                funding_accumulated = 0
                eq[i] = cash
                continue

        # SHORT max hold
        if pos == -1 and (i - entry_idx) >= max_hold_short:
            exit_p = c * (1 + SLIP_PCT)
            gross = qty * (entry_px - exit_p)
            fees = qty * (entry_px + exit_p) * FEE_PCT
            net = gross - fees - funding_accumulated
            cash += net
            trades.append({'side':'short-maxhold', 'entry_d':dates[entry_idx], 'exit_d':dates[i],
                          'qty':qty, 'hold_d':i-entry_idx, 'net':net,
                          'net_pct': net / (cash - net) * 100 if (cash - net) > 0 else 0})
            pos = 0
            funding_accumulated = 0
            eq[i] = cash
            continue

        # LONG exit
        if pos == 1:
            hold = i - entry_idx
            do_exit = False
            if hold >= max_hold_long: do_exit = True
            elif hold >= min_hold and not np.isnan(dvol[i]) and dvol[i] >= dvol_exit: do_exit = True
            if do_exit:
                exit_p = c * (1 - SLIP_PCT)
                gross = qty * (exit_p - entry_px)
                fees = qty * (entry_px + exit_p) * FEE_PCT
                net = gross - fees - funding_accumulated
                cash += net
                trades.append({'side':'long', 'entry_d':dates[entry_idx], 'exit_d':dates[i],
                              'qty':qty, 'hold_d':hold, 'net':net,
                              'net_pct': net / (cash - net) * 100 if (cash - net) > 0 else 0})

                # Open SHORT?
                open_short = always_in_market and cash > 1
                if short_in_bull_only and not bull[i]: open_short = False
                if short_in_bear_only and bull[i]: open_short = False

                if open_short:
                    entry_idx = i
                    entry_px = c * (1 - SLIP_PCT)
                    qty = leverage * cash * 0.99 / entry_px
                    pos = -1
                    funding_accumulated = 0
                else:
                    pos = 0
                eq[i] = cash
                continue

        # LONG entry
        if not np.isnan(vrp[i]) and vrp[i] <= vrp_thr:
            if pos == -1:
                exit_p = c * (1 + SLIP_PCT)
                gross = qty * (entry_px - exit_p)
                fees = qty * (entry_px + exit_p) * FEE_PCT
                net = gross - fees - funding_accumulated
                cash += net
                trades.append({'side':'short', 'entry_d':dates[entry_idx], 'exit_d':dates[i],
                              'qty':qty, 'hold_d':i-entry_idx, 'net':net,
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


def summarize(eq, tr, label, liq):
    total = (eq.iloc[-1]/INITIAL - 1) * 100
    dd = ((eq.cummax() - eq)/eq.cummax()).max() * 100
    wr = (tr['net']>0).sum()/max(len(tr),1)*100 if len(tr) else 0
    n_liq = tr['side'].str.startswith('LIQ').sum() if len(tr) else 0
    return f"{label:<60}  total={total:>+8.0f}%  DD={dd:>4.0f}%  N={len(tr):>3}  WR={wr:>3.0f}%  LIQ={n_liq}  {'⚠️' if liq else 'OK'}"


def main():
    price, dvol, daily_funding = load_data()
    df = build_features(price, dvol, daily_funding).dropna(subset=['dvol']).copy()
    print(f"\nBTC 2021-03 → 2026-05  ({len(df)}d)")
    bh = (df['close'].iloc[-1]/df['close'].iloc[0] - 1) * 100
    print(f"Buy & Hold: {bh:+.0f}%\n")

    print("="*180)
    print("STRATEGY VARIANTS TESTED ON FULL 2021-2026 HISTORY (with real funding + fees + liquidation)")
    print("="*180)

    variants = [
        # (label, kwargs)
        ("BASELINE: AIM hold-forever",                    dict(always_in_market=True, max_hold_short=365)),
        ("AIM + SHORT max hold 60d",                       dict(always_in_market=True, max_hold_short=60)),
        ("AIM + SHORT max hold 30d",                       dict(always_in_market=True, max_hold_short=30)),
        ("AIM + SHORT only in BEAR regime (EMA200)",       dict(always_in_market=True, short_in_bear_only=True, max_hold_short=180)),
        ("AIM + SHORT stop at +20% adverse",               dict(always_in_market=True, short_stop_pct=0.20, max_hold_short=365)),
        ("AIM + SHORT stop +20% + bear-only + max 60d",    dict(always_in_market=True, short_in_bear_only=True, short_stop_pct=0.20, max_hold_short=60)),
        ("LONG ONLY (no SHORT)",                           dict(always_in_market=False)),
    ]

    for label, kwargs in variants:
        print(f"\n  {label}")
        for lev in [1.0, 2.0, 3.0]:
            eq, tr, liq = backtest(df, leverage=lev, **kwargs)
            print(f"    {lev:.0f}x  " + summarize(eq, tr, "", liq))


if __name__ == "__main__":
    main()
