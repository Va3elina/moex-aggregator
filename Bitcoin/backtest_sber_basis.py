"""Phase 1: backtest SBER strategies on FUTURES BASIS signals.

Signals tested:
  1. Quarterly basis annualized (front-month) — backwardation = panic
  2. Carry residual = basis_ann - RUSFAR — pure positioning premium
  3. Inverted basis term structure (back-month < front-month) — rare stress
  4. Calendar spread (front vs next quarter)

Underlying: SBRF series (SRH/M/U/Z = Mar/Jun/Sep/Dec quarterlies).

SBRF futures price is in kopecks × 100 of SBER stock (1 contract = 100 shares).
So: stock_equivalent_futures = futures_price / 100.
"""
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).parent
INITIAL = 100.0
FEE = 0.0003     # MOEX broker typical 0.03% per side
SLIP = 0.0001


def parse_sbrf_secid(secid):
    """SRH0 → Mar 2020, SRM0 → Jun 2020, SRU3 → Sep 2023, SRZ4 → Dec 2024.
    Format: SR + {month_letter}{year_digit}.
    Quarterly: H=Mar, M=Jun, U=Sep, Z=Dec.
    Year digit (single) maps to nearest year — MOEX rolls 0-9 cycle.
    """
    if not secid.startswith('SR') or len(secid) < 4:
        return None, None
    month_map = {'H': 3, 'M': 6, 'U': 9, 'Z': 12}
    rest = secid[2:]
    # Skip composite (e.g. SRZ2SRH3 = calendar spread synthetic)
    if len(rest) > 2:
        return None, None
    if rest[0] not in month_map:
        return None, None  # not quarterly
    if not rest[1].isdigit():
        return None, None
    month = month_map[rest[0]]
    return month, int(rest[1])


def expiry_date(month, year_digit, ref_year):
    """Map single-digit year to full year nearest to ref_year."""
    # 2020-2029 cycle: year_digit 0..9 maps to 2020..2029
    candidates = [2010 + year_digit, 2020 + year_digit, 2030 + year_digit]
    full_year = min(candidates, key=lambda y: abs(y - ref_year))
    # Quarterly futures expire 3rd Friday or thereabouts — use 15th of month as proxy
    return datetime(full_year, month, 15).date()


def load_data():
    fut = pd.read_csv(ROOT/'sber_futures_history.csv', parse_dates=['TRADEDATE'])
    fut = fut[fut['SETTLEPRICE'].notna()]
    # Parse SECID
    parsed = fut['SECID'].apply(parse_sbrf_secid)
    fut['exp_month'] = parsed.apply(lambda x: x[0])
    fut['exp_year_digit'] = parsed.apply(lambda x: x[1])
    fut = fut[fut['exp_month'].notna()].copy()
    fut['exp_year_digit'] = fut['exp_year_digit'].astype(int)
    fut['ref_year'] = fut['TRADEDATE'].dt.year
    fut['expiry'] = fut.apply(lambda r: expiry_date(int(r['exp_month']), r['exp_year_digit'], r['ref_year']), axis=1)
    fut['days_to_exp'] = (pd.to_datetime(fut['expiry']) - fut['TRADEDATE']).dt.days
    fut = fut[fut['days_to_exp'] > 0].copy()
    fut['fut_px_stock_eq'] = fut['SETTLEPRICE'] / 100  # convert kopecks per contract to RUB per share

    sber = pd.read_csv(ROOT/'sber_daily.csv', parse_dates=['date']).set_index('date').sort_index()
    rusfar = pd.read_csv(ROOT/'rusfar_daily.csv', parse_dates=['date']).set_index('date').sort_index()
    futoi = pd.read_csv(ROOT/'sber_futoi_signals.csv', parse_dates=['date']).set_index('date').sort_index()
    return fut, sber, rusfar, futoi


def build_basis_features(fut, sber, rusfar):
    """For each trading day pick front-month and next-quarter SBRF futures."""
    rows = []
    for d, day_data in fut.groupby('TRADEDATE'):
        if d not in sber.index:
            continue
        spot = sber.loc[d, 'close']
        # sort by days_to_exp ascending
        day_data = day_data.sort_values('days_to_exp')
        # Filter to front-month (min DTE) and next quarter
        if len(day_data) < 1:
            continue
        front = day_data.iloc[0]
        next_q = day_data.iloc[1] if len(day_data) >= 2 else None

        front_basis = (front['fut_px_stock_eq'] - spot) / spot
        front_basis_ann = front_basis * 365 / max(front['days_to_exp'], 1)
        # carry residual
        rf = rusfar.loc[d, 'rate_pct'] / 100 if d in rusfar.index else 0.10
        carry_res = front_basis_ann - rf

        row = {
            'date': d, 'spot': spot,
            'front_secid': front['SECID'],
            'front_px': front['fut_px_stock_eq'],
            'front_dte': front['days_to_exp'],
            'front_basis_pct': front_basis * 100,
            'front_basis_ann_pct': front_basis_ann * 100,
            'rusfar': rf * 100,
            'carry_residual_pct': carry_res * 100,
        }
        if next_q is not None:
            next_basis = (next_q['fut_px_stock_eq'] - spot) / spot
            next_basis_ann = next_basis * 365 / max(next_q['days_to_exp'], 1)
            row.update({
                'next_secid': next_q['SECID'],
                'next_px': next_q['fut_px_stock_eq'],
                'next_dte': next_q['days_to_exp'],
                'next_basis_ann_pct': next_basis_ann * 100,
                'inverted': next_basis_ann < front_basis_ann,  # back < front = inversion
                'cal_spread_pct': (next_q['fut_px_stock_eq'] - front['fut_px_stock_eq']) / front['fut_px_stock_eq'] * 100,
            })
        rows.append(row)
    return pd.DataFrame(rows).set_index('date').sort_index()


def backtest_long(df, signal_col, threshold, op='lt', min_hold=10, max_hold=60, exit_thr=None, exit_op='gt'):
    closes = df['spot'].values
    sig = df[signal_col].values
    dates = df.index
    n = len(df)
    cash = INITIAL
    eq = np.full(n, INITIAL, dtype=float)
    in_pos = False
    entry_idx = 0
    entry_px = 0
    trades = []
    for i in range(n):
        c = closes[i]
        if in_pos:
            eq[i] = cash * (c / entry_px)
            hold = i - entry_idx
            do_exit = False
            if hold >= max_hold:
                do_exit = True; reason = "max"
            elif hold >= min_hold and exit_thr is not None:
                cond = (sig[i] > exit_thr) if exit_op == 'gt' else (sig[i] < exit_thr)
                if cond:
                    do_exit = True; reason = "signal"
            if do_exit:
                exit_p = c * (1 - SLIP)
                ret = exit_p/entry_px - 1 - 2*FEE
                cash *= (1 + ret)
                trades.append({'entry': dates[entry_idx], 'exit': dates[i], 'hold': hold,
                              'entry_sig': sig[entry_idx], 'exit_sig': sig[i],
                              'pnl_pct': ret * 100, 'reason': reason})
                in_pos = False
                eq[i] = cash
        else:
            eq[i] = cash
        if not in_pos and not np.isnan(sig[i]):
            cond = (sig[i] < threshold) if op == 'lt' else (sig[i] > threshold)
            if cond:
                entry_idx = i
                entry_px = c * (1 + SLIP)
                in_pos = True
    return pd.Series(eq, index=dates), pd.DataFrame(trades)


def summ(eq, tr, label):
    total = (eq.iloc[-1]/INITIAL - 1) * 100
    dd = ((eq.cummax() - eq)/eq.cummax()).max() * 100
    n = len(tr)
    wr = (tr['pnl_pct']>0).sum()/max(n, 1)*100
    avg = tr['pnl_pct'].mean() if n else 0
    return f"{label:<55}  total={total:>+8.0f}%  DD={dd:>4.0f}%  N={n:>3}  WR={wr:>3.0f}%  avg={avg:>+5.1f}%"


def main():
    fut, sber, rusfar, futoi = load_data()
    print(f"Futures: {len(fut)} rows, {fut['TRADEDATE'].nunique()} days, {fut['SECID'].nunique()} unique SECIDs")
    print(f"SBER: {sber.index[0].date()} → {sber.index[-1].date()}")
    print(f"RUSFAR: {rusfar.index[0].date()} → {rusfar.index[-1].date()}")
    print(f"futoi: {futoi.index[0].date()} → {futoi.index[-1].date()}\n")

    print("Building basis features...")
    df = build_basis_features(fut, sber, rusfar)
    print(f"Basis features: {len(df)} days, {df.index[0].date()} → {df.index[-1].date()}\n")

    bh = (df['spot'].iloc[-1]/df['spot'].iloc[0] - 1) * 100
    print(f"SBER B&H over basis period: {bh:+.0f}%\n")

    # Show basis distribution
    print("Front basis annualized distribution (%):")
    for q in [0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95]:
        print(f"  p{int(q*100)}: {df['front_basis_ann_pct'].quantile(q):+.1f}%")
    print()
    print("Carry residual distribution (basis_ann - RUSFAR) (%):")
    for q in [0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95]:
        print(f"  p{int(q*100)}: {df['carry_residual_pct'].quantile(q):+.1f}%")
    print()

    # SIGNAL 1: Front basis backwardation = LONG
    print("="*100)
    print("SIGNAL 1: LONG when front basis is in DEEP BACKWARDATION (panic)")
    print("="*100)
    for thr in [-30, -20, -10, -5, 0]:
        eq, tr = backtest_long(df, 'front_basis_ann_pct', thr, op='lt',
                                min_hold=10, max_hold=60, exit_thr=10, exit_op='gt')
        print(summ(eq, tr, f"basis_ann <= {thr}%"))

    # SIGNAL 2: Carry residual deep negative (basis MUCH below RUSFAR = panic)
    print("\n" + "="*100)
    print("SIGNAL 2: LONG when carry residual (basis - RUSFAR) is deeply negative")
    print("="*100)
    for thr in [-30, -20, -15, -10, -5, 0]:
        eq, tr = backtest_long(df, 'carry_residual_pct', thr, op='lt',
                                min_hold=10, max_hold=60, exit_thr=5, exit_op='gt')
        print(summ(eq, tr, f"carry_residual <= {thr}%"))

    # SIGNAL 3: Inverted term structure
    if 'inverted' in df.columns:
        print("\n" + "="*100)
        print("SIGNAL 3: LONG when basis term structure is INVERTED (back < front)")
        print("="*100)
        # binary signal: enter when inverted, exit when normalizes
        # Use carry_residual_diff as proxy
        df['inverted_int'] = df['inverted'].astype(int).fillna(0)
        eq, tr = backtest_long(df, 'inverted_int', 0.5, op='gt',
                                min_hold=5, max_hold=60, exit_thr=0.5, exit_op='lt')
        print(summ(eq, tr, "LONG when inverted=True (back basis < front basis)"))

    # SIGNAL 4: Combined with futoi
    print("\n" + "="*100)
    print("COMBO: FIZ extreme short AND basis backwardation (double confirmation)")
    print("="*100)
    df_combo = df.join(futoi[['fiz_long', 'fiz_short']], how='inner')
    df_combo['fiz_lsr'] = df_combo['fiz_long'] / df_combo['fiz_short'].replace(0, 1)
    df_combo['combo_signal'] = ((df_combo['fiz_lsr'] < 1.4) &
                                  (df_combo['carry_residual_pct'] < 0)).astype(int)
    df_combo_clean = df_combo.dropna(subset=['combo_signal'])
    if len(df_combo_clean):
        eq, tr = backtest_long(df_combo_clean, 'combo_signal', 0.5, op='gt',
                                min_hold=10, max_hold=60, exit_thr=0.5, exit_op='lt')
        print(summ(eq, tr, "FIZ < 1.4 + carry_residual < 0"))

    # Forward-return analysis on basis signals
    print("\n" + "="*100)
    print("Forward returns by basis annualized quintile (informativeness check):")
    print("="*100)
    df_fwd = df.join(sber[['close']].rename(columns={'close': 'sber_close'}), how='inner')
    for h in [20, 60]:
        df_fwd[f'fwd_{h}d'] = df_fwd['sber_close'].pct_change(h).shift(-h) * 100
    df_fwd['basis_q'] = pd.qcut(df_fwd['front_basis_ann_pct'], 5,
                                  labels=['Q1_deep_back', 'Q2', 'Q3', 'Q4', 'Q5_contango'])
    print(f"\n  {'Bucket':<14}  N    "
          f"{'mean 20d %':>10}  {'P(up) 20d':>10}  "
          f"{'mean 60d %':>10}  {'P(up) 60d':>10}")
    for b in ['Q1_deep_back', 'Q2', 'Q3', 'Q4', 'Q5_contango']:
        sub = df_fwd[df_fwd['basis_q'] == b]
        r20 = sub['fwd_20d'].dropna()
        r60 = sub['fwd_60d'].dropna()
        print(f"  {b:<14}  {len(sub):>3}  "
              f"{r20.mean():>+9.2f}%  {(r20>0).mean()*100:>8.0f}%  "
              f"{r60.mean():>+9.2f}%  {(r60>0).mean()*100:>8.0f}%")

    out = ROOT / 'sber_basis_features.csv'
    df.to_csv(out)
    print(f"\nSaved features to {out.name}")


if __name__ == "__main__":
    main()
