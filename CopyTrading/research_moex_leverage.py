"""Leveraged MOEX Cluster 0 — test 1x/2x/3x/5x/10x with liquidation modeling."""
import warnings
warnings.filterwarnings("ignore")
from pathlib import Path
import numpy as np
import pandas as pd

DIR = Path(__file__).parent / "moex_1h"
INITIAL = 100.0
FEE = 0.0005
SLIPPAGE = 0.0003
MAINT_MARGIN = 0.005  # 0.5% maintenance — liquidation if equity drops below this


def wilder_ma(s, n):
    out = np.full(len(s), np.nan)
    if len(s) < n: return pd.Series(out, index=s.index)
    v = s.values; out[n-1] = np.nanmean(v[:n])
    for i in range(n, len(s)): out[i] = (out[i-1]*(n-1) + v[i]) / n
    return pd.Series(out, index=s.index)


def features(bars):
    d = bars.copy(); c,h,l,o = d['close'],d['high'],d['low'],d['open']
    rng = (h-l).replace(0, np.nan)
    d['lower_wick'] = (pd.concat([c,o], axis=1).min(axis=1) - l) / rng
    pc = c.shift(1); tr = pd.concat([h-l, (h-pc).abs(), (l-pc).abs()], axis=1).max(axis=1)
    atr14 = wilder_ma(tr, 14); d['atr_pct'] = atr14 / c * 100
    e50 = c.ewm(span=50, adjust=False).mean(); e200 = c.ewm(span=200, adjust=False).mean()
    d['ema_50_200'] = (e50 - e200) / c * 100
    d['ret_4h'] = c.pct_change(4); d['ret_72h'] = c.pct_change(72)
    return d


def signal(d, R):
    j = d.join(R, how='left', rsuffix='_R')
    return ((j['ema_50_200_R']>2.0) & (j['ret_72h_R']>0.02) &
            (j['ema_50_200']>1.0) & (j['ret_4h']<-0.01) &
            (j['atr_pct']>0.5) & (j['lower_wick']>0.25)).fillna(False)


def backtest_leveraged(bars, sig, hold=48, leverage=1.0):
    """Realistic leverage model:
    - Notional exposure = equity * leverage * 0.95 (95% to avoid margin call on entry)
    - Loss multiplied by leverage
    - If equity drops below maint_margin% of notional → LIQUIDATED to 0
    """
    closes = bars['close'].values; highs = bars['high'].values; lows = bars['low'].values
    cash, equity_history = INITIAL, [INITIAL]
    pos_qty, entry_price, held = 0.0, 0, 0
    notional = 0
    trades = []
    liquidated = False

    for i in range(len(bars)):
        c = closes[i]

        if pos_qty > 0:
            held += 1
            # Check liquidation intrabar
            worst_loss_per_unit = entry_price - lows[i]
            unrealized_loss = worst_loss_per_unit * pos_qty
            equity_at_low = cash + (lows[i] * pos_qty) - (entry_price * pos_qty)  # MTM
            # Margin call if equity goes below 5% of notional (typical)
            if equity_at_low < notional * MAINT_MARGIN:
                # Force liquidation at the worst point
                liq_price = entry_price - (cash - notional * MAINT_MARGIN) / pos_qty
                liq_price = max(liq_price, 0.01)
                eff = liq_price * (1 - SLIPPAGE)
                proceeds = pos_qty * eff
                cash = max(0, cash + proceeds - pos_qty * entry_price - pos_qty * eff * FEE)
                trades.append({'ret_pct': (eff/entry_price - 1)*100 * leverage,
                                 'exit': 'LIQ', 'bars': held})
                pos_qty = 0; held = 0
                if cash < INITIAL * 0.05:
                    liquidated = True
                    break

            elif held >= hold:
                eff = c * (1 - SLIPPAGE)
                # Close position: receive proceeds, subtract entry cost paid earlier
                proceeds = pos_qty * eff
                cash = cash + proceeds - pos_qty * entry_price - pos_qty * eff * FEE
                pnl_pct = (eff/entry_price - 1)*100 * leverage  # leverage scales return on equity
                trades.append({'ret_pct': pnl_pct, 'exit': 'TIME', 'bars': held})
                pos_qty = 0; held = 0

        if pos_qty == 0 and sig.iloc[i] and not liquidated:
            # Enter long with leverage * cash exposure
            eff = c * (1 + SLIPPAGE)
            notional = cash * leverage * 0.95
            pos_qty = notional / eff
            # Cash deducted (margin / borrow happens conceptually)
            # We track P&L on the notional in the equity calculation
            entry_price = eff
            held = 0
            # Pay entry fee on notional
            cash -= notional * FEE

        # Equity = cash + open position MTM
        if pos_qty > 0:
            eq = cash + pos_qty * (c - entry_price)
        else:
            eq = cash
        equity_history.append(max(0, eq))

    final_eq = equity_history[-1]
    return pd.DataFrame(trades), final_eq, equity_history, liquidated


def load(name):
    p = DIR / f'{name.lower()}_1h.csv'
    df = pd.read_csv(p, parse_dates=['datetime']).set_index('datetime').sort_index()
    for c in ['open','high','low','close','volume']: df[c] = pd.to_numeric(df[c], errors='coerce')
    return df


def compute_metrics(equity_curve, n_years):
    eq = np.array(equity_curve)
    final = eq[-1]
    total_ret = (final / INITIAL - 1) * 100
    cagr = ((final / INITIAL) ** (1/n_years) - 1) * 100 if n_years > 0 and final > 0 else -100
    peak = np.maximum.accumulate(eq)
    dd = (eq / peak - 1) * 100
    max_dd = dd.min()
    # daily returns
    return total_ret, cagr, max_dd


def main():
    imoex_f = features(load('imoex'))[['ema_50_200','ret_4h','ret_72h','atr_pct']]
    pairs = ['AFKS','SMLT','POSI','ASTR','VKCO','SOFL','WUSH','DELI']

    print("=" * 100)
    print("LEVERAGE TEST — Cluster 0 v2 on MOEX")
    print("=" * 100)
    print(f"\n{'leverage':>8}  {'pair':6}  {'trades':>6}  {'WR%':>5}  {'CAGR%':>7}  "
          f"{'TotalRet%':>10}  {'MaxDD%':>8}  {'liq?':>5}")
    print("-" * 80)

    for lev in [1.0, 2.0, 3.0, 5.0, 10.0]:
        total_eq = 0; n_pairs = 0; all_t = []; total_liq = 0
        ret_strat = []
        for p in pairs:
            try: bars = features(load(p))
            except FileNotFoundError: continue
            sig = signal(bars, imoex_f)
            if sig.sum() == 0: continue
            tr, eq, ec, liq = backtest_leveraged(bars, sig, hold=48, leverage=lev)
            if len(tr) == 0: continue
            n_years = (bars.index[-1] - bars.index[0]).days / 365.25
            total_ret, cagr, mdd = compute_metrics(ec, n_years)
            wr = (tr['ret_pct']>0).mean()*100
            n_liq = (tr['exit']=='LIQ').sum()
            print(f"  {lev:>6.1f}x  {p:6}  {len(tr):>6}  {wr:>4.1f}%  "
                  f"{cagr:>+6.1f}%  {total_ret:>+9.1f}%  {mdd:>+7.1f}%  "
                  f"{'LIQ' if liq else f'{n_liq}':>5}")
            total_eq += eq; n_pairs += 1
            all_t.append(tr); ret_strat.append(cagr)
            if liq: total_liq += 1
        if n_pairs:
            avg_eq = total_eq / n_pairs
            print(f"  {lev:>6.1f}x  {'AVG':6}  {'':>6}  {'':>5}  "
                  f"{np.mean(ret_strat):>+6.1f}%  {(avg_eq/INITIAL-1)*100:>+9.1f}%  "
                  f"{'':>7}  liq={total_liq}/{n_pairs}")
        print()


if __name__ == "__main__":
    main()
