"""Test ALL known options-derived indicators on BTC.

Indicators tested:
  Volatility-based:
    1. DVOL absolute level
    2. DVOL z-score (vs rolling MA)
    3. DVOL percentile rank
    4. DVOL slope (rate of change)
    5. DVOL momentum (MA cross)

  VRP-based (IV - Realized):
    6. VRP absolute level
    7. VRP z-score
    8. VRP slope (changing fast → market re-pricing)
    9. VRP percentile rank

  Term Structure:
    10. Short DVOL vs long DVOL spread (BTC has only single DVOL but proxy via realized vol windows)

  Cross-asset:
    11. BTC DVOL → ETH returns
    12. ETH DVOL → BTC returns
    13. DVOL spread BTC vs ETH

  Composite:
    14. DVOL high + VRP low (both extreme)
    15. DVOL z-score + price momentum

Score each by forward returns (30d, 60d, 90d).
"""
import warnings; warnings.filterwarnings("ignore")
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(__file__).parent
INITIAL = 100.0


def load_data(asset='BTC'):
    if asset == 'BTC':
        price = pd.read_csv(ROOT/'btc_usd_daily_coinbase.csv', parse_dates=['date']).set_index('date').sort_index()
    else:
        price = pd.read_csv(ROOT/'eth_usd_daily_coinbase.csv', parse_dates=['date']).set_index('date').sort_index()
    price = price[price['close']>0].dropna()
    dvol = pd.read_csv(ROOT / f'{asset.lower()}_dvol_daily.csv', parse_dates=['date']).set_index('date').sort_index()
    return price, dvol


def build_features(price, dvol, asset):
    df = pd.DataFrame(index=price.index)
    df['close'] = price['close']
    log_ret = np.log(df['close'] / df['close'].shift(1))

    # Realized vol windows
    df['rv_7d'] = log_ret.rolling(7).std() * np.sqrt(365) * 100
    df['rv_30d'] = log_ret.rolling(30).std() * np.sqrt(365) * 100
    df['rv_90d'] = log_ret.rolling(90).std() * np.sqrt(365) * 100

    # DVOL
    df['dvol'] = dvol['close'].reindex(df.index, method='ffill')

    # DVOL transformations
    df['dvol_ma30'] = df['dvol'].rolling(30).mean()
    df['dvol_std30'] = df['dvol'].rolling(30).std()
    df['dvol_z30'] = (df['dvol'] - df['dvol_ma30']) / df['dvol_std30']
    df['dvol_pct_rank180'] = df['dvol'].rolling(180).rank(pct=True) * 100  # percentile 0-100
    df['dvol_slope_7d'] = df['dvol'] - df['dvol'].shift(7)
    df['dvol_ma_cross'] = df['dvol'].rolling(20).mean() - df['dvol'].rolling(60).mean()  # short MA - long MA

    # VRP
    df['vrp'] = df['dvol'] - df['rv_30d']
    df['vrp_ma30'] = df['vrp'].rolling(30).mean()
    df['vrp_z30'] = (df['vrp'] - df['vrp_ma30']) / df['vrp'].rolling(30).std()
    df['vrp_slope_7d'] = df['vrp'] - df['vrp'].shift(7)
    df['vrp_pct_rank180'] = df['vrp'].rolling(180).rank(pct=True) * 100

    # Term structure proxy: 7d vol vs 30d vol
    df['vol_term_ratio'] = df['rv_7d'] / df['rv_30d']
    df['vol_term_z30'] = (df['vol_term_ratio'] - df['vol_term_ratio'].rolling(30).mean()) / df['vol_term_ratio'].rolling(30).std()

    # Forward returns
    for h in [7, 14, 30, 60, 90, 120]:
        df[f'fwd_{h}d'] = df['close'].pct_change(h).shift(-h) * 100

    # Price momentum
    df['ret_30d'] = df['close'].pct_change(30) * 100

    return df


def event_table(df, signals, name):
    """For each signal, compute forward return stats."""
    print(f"\n  {'='*150}")
    print(f"  {name}")
    print(f"  {'='*150}")
    print(f"  {'Signal':<55}  {'N':>4}  {'30d mean':>10}  {'P(up)':>6}  {'60d mean':>10}  {'P(up)':>6}  {'90d mean':>10}  {'P(up)':>6}  {'t-stat 60d':>11}")
    print(f"  {'-'*150}")
    for sig_name, mask in signals.items():
        mask = mask.fillna(False)
        n = mask.sum()
        if n == 0:
            print(f"  {sig_name:<55}  no events")
            continue
        r30 = df.loc[mask, 'fwd_30d'].dropna()
        r60 = df.loc[mask, 'fwd_60d'].dropna()
        r90 = df.loc[mask, 'fwd_90d'].dropna()
        if len(r60) == 0:
            print(f"  {sig_name:<55}  N={n:>3}  no forward data")
            continue
        t60 = r60.mean() / (r60.std()/np.sqrt(len(r60))) if r60.std() > 0 else 0
        print(f"  {sig_name:<55}  {len(r60):>4}  "
              f"{r30.mean():>+8.1f}%  {(r30>0).mean()*100:>4.0f}%  "
              f"{r60.mean():>+8.1f}%  {(r60>0).mean()*100:>4.0f}%  "
              f"{r90.mean():>+8.1f}%  {(r90>0).mean()*100:>4.0f}%  "
              f"{t60:>+8.1f}")


def main():
    btc_price, btc_dvol = load_data('BTC')
    eth_price, eth_dvol = load_data('ETH')
    btc = build_features(btc_price, btc_dvol, 'BTC')
    eth = build_features(eth_price, eth_dvol, 'ETH')
    btc = btc.dropna(subset=['dvol']).copy()
    eth = eth.dropna(subset=['dvol']).copy()

    print(f"BTC data: {btc.index[0].date()} → {btc.index[-1].date()} ({len(btc)} days)")
    print(f"ETH data: {eth.index[0].date()} → {eth.index[-1].date()} ({len(eth)} days)")

    # === Group 1: DVOL absolute levels ===
    btc_signals_g1 = {
        'DVOL ≤ 35 (very calm)':       btc['dvol'] <= 35,
        'DVOL ≤ 40':                   btc['dvol'] <= 40,
        'DVOL between 40-50':          (btc['dvol'] > 40) & (btc['dvol'] <= 50),
        'DVOL between 50-60':          (btc['dvol'] > 50) & (btc['dvol'] <= 60),
        'DVOL between 60-70':          (btc['dvol'] > 60) & (btc['dvol'] <= 70),
        'DVOL ≥ 70 (high)':            btc['dvol'] >= 70,
        'DVOL ≥ 80 (very high)':       btc['dvol'] >= 80,
    }
    event_table(btc, btc_signals_g1, "BTC: DVOL ABSOLUTE LEVELS")

    # === Group 2: DVOL relative/z-score ===
    btc_signals_g2 = {
        'DVOL z30 ≤ -2 (anomaly low)':  btc['dvol_z30'] <= -2,
        'DVOL z30 ≤ -1.5':              btc['dvol_z30'] <= -1.5,
        'DVOL z30 ≥ +2 (anomaly high)': btc['dvol_z30'] >= 2,
        'DVOL z30 ≥ +1.5':              btc['dvol_z30'] >= 1.5,
        'DVOL pct rank ≤ 10 (180d)':    btc['dvol_pct_rank180'] <= 10,
        'DVOL pct rank ≥ 90':           btc['dvol_pct_rank180'] >= 90,
        'DVOL slope 7d ≥ +10':          btc['dvol_slope_7d'] >= 10,
        'DVOL slope 7d ≤ -10':          btc['dvol_slope_7d'] <= -10,
        'DVOL MA cross > 0 (rising)':   btc['dvol_ma_cross'] > 0,
        'DVOL MA cross < -5 (falling)': btc['dvol_ma_cross'] < -5,
    }
    event_table(btc, btc_signals_g2, "BTC: DVOL RELATIVE / Z-SCORE")

    # === Group 3: VRP-based ===
    btc_signals_g3 = {
        'VRP ≤ -25':                    btc['vrp'] <= -25,
        'VRP ≤ -20':                    btc['vrp'] <= -20,
        'VRP ≤ -10':                    btc['vrp'] <= -10,
        'VRP ≤ -5':                     btc['vrp'] <= -5,
        'VRP between -10 and 0':        (btc['vrp'] > -10) & (btc['vrp'] <= 0),
        'VRP between 0 and +10':        (btc['vrp'] > 0) & (btc['vrp'] <= 10),
        'VRP ≥ +20':                    btc['vrp'] >= 20,
        'VRP ≥ +30':                    btc['vrp'] >= 30,
        'VRP z30 ≤ -2':                 btc['vrp_z30'] <= -2,
        'VRP z30 ≥ +2':                 btc['vrp_z30'] >= 2,
        'VRP slope 7d ≤ -10 (collapsing)': btc['vrp_slope_7d'] <= -10,
        'VRP slope 7d ≥ +10 (expanding)':  btc['vrp_slope_7d'] >= 10,
        'VRP pct rank ≤ 10':            btc['vrp_pct_rank180'] <= 10,
        'VRP pct rank ≥ 90':            btc['vrp_pct_rank180'] >= 90,
    }
    event_table(btc, btc_signals_g3, "BTC: VRP-BASED")

    # === Group 4: Term structure proxy ===
    btc_signals_g4 = {
        'Vol term ratio ≥ 1.5 (short>long)': btc['vol_term_ratio'] >= 1.5,
        'Vol term ratio ≤ 0.7 (short<long)': btc['vol_term_ratio'] <= 0.7,
        'Vol term z ≥ 2 (term inverted)':   btc['vol_term_z30'] >= 2,
        'Vol term z ≤ -2':                   btc['vol_term_z30'] <= -2,
    }
    event_table(btc, btc_signals_g4, "BTC: TERM STRUCTURE PROXY (7d vs 30d realized vol)")

    # === Group 5: Cross-asset (BTC DVOL → ETH returns) ===
    # Align BTC dvol to ETH dates
    btc_dvol_on_eth = btc['dvol'].reindex(eth.index, method='ffill')
    eth['btc_dvol'] = btc_dvol_on_eth
    eth['dvol_ratio'] = eth['dvol'] / btc_dvol_on_eth
    eth_signals_g5 = {
        'BTC DVOL ≤ 40 → ETH':           eth['btc_dvol'] <= 40,
        'BTC DVOL ≥ 70 → ETH':           eth['btc_dvol'] >= 70,
        'ETH DVOL ≤ 40':                 eth['dvol'] <= 40,
        'ETH DVOL ≥ 70':                 eth['dvol'] >= 70,
        'ETH/BTC DVOL ratio ≥ 1.5':      eth['dvol_ratio'] >= 1.5,
        'ETH/BTC DVOL ratio ≤ 1.0':      eth['dvol_ratio'] <= 1.0,
    }
    event_table(eth, eth_signals_g5, "CROSS-ASSET: BTC DVOL → ETH returns")

    # ETH on its own
    eth_signals_g6 = {
        'ETH DVOL ≤ 40':                 eth['dvol'] <= 40,
        'ETH DVOL ≥ 70':                 eth['dvol'] >= 70,
        'ETH VRP ≤ -20':                 eth['vrp'] <= -20,
        'ETH VRP ≤ -10':                 eth['vrp'] <= -10,
        'ETH VRP ≥ +20':                 eth['vrp'] >= 20,
        'ETH VRP z30 ≤ -2':              eth['vrp_z30'] <= -2,
        'ETH VRP z30 ≥ +2':              eth['vrp_z30'] >= 2,
    }
    event_table(eth, eth_signals_g6, "ETH on ETH (DVOL/VRP)")

    # === Group 7: Composite signals ===
    btc_signals_g7 = {
        'DVOL low (≤45) AND VRP low (≤-5)':           (btc['dvol'] <= 45) & (btc['vrp'] <= -5),
        'DVOL low AND price up 30d':                   (btc['dvol'] <= 45) & (btc['ret_30d'] > 5),
        'DVOL low AND price down 30d':                 (btc['dvol'] <= 45) & (btc['ret_30d'] < -5),
        'DVOL z low AND VRP low':                      (btc['dvol_z30'] <= -1) & (btc['vrp_z30'] <= -1),
        'DVOL spike (z≥2) AND price down (-10%/30d)':  (btc['dvol_z30'] >= 2) & (btc['ret_30d'] < -10),
        'DVOL spike AND VRP spike (+) [panic]':        (btc['dvol_z30'] >= 2) & (btc['vrp_z30'] >= 1),
    }
    event_table(btc, btc_signals_g7, "BTC: COMPOSITE SIGNALS")

    # === Group 8: Top winners summary ===
    print(f"\n  {'='*150}")
    print(f"  🏆 TOP SIGNALS RANKED BY (60d P(up) × N events) [edge × frequency]")
    print(f"  {'='*150}")

    all_signals = []
    all_signals.append(('BTC', btc_signals_g1, btc))
    all_signals.append(('BTC', btc_signals_g2, btc))
    all_signals.append(('BTC', btc_signals_g3, btc))
    all_signals.append(('BTC', btc_signals_g4, btc))
    all_signals.append(('BTC', btc_signals_g7, btc))
    all_signals.append(('ETH-cross', eth_signals_g5, eth))
    all_signals.append(('ETH', eth_signals_g6, eth))

    ranked = []
    for asset_label, sigs, df_use in all_signals:
        for sname, mask in sigs.items():
            m = mask.fillna(False)
            n = m.sum()
            if n < 10: continue
            r60 = df_use.loc[m, 'fwd_60d'].dropna()
            if len(r60) < 5: continue
            p_up = (r60 > 0).mean() * 100
            mean60 = r60.mean()
            std60 = r60.std()
            t_stat = mean60 / (std60/np.sqrt(len(r60))) if std60 > 0 else 0
            sharpe_like = mean60 / std60 if std60 > 0 else 0
            score = p_up * np.log(len(r60))  # P(up) × log(N) — edge × frequency
            ranked.append({
                'asset': asset_label, 'signal': sname,
                'N': len(r60), 'p_up_60d': p_up, 'mean_60d': mean60,
                't_stat_60d': t_stat, 'score': score, 'sharpe': sharpe_like,
            })

    ranked_df = pd.DataFrame(ranked).sort_values('score', ascending=False)
    print(f"\n  {'asset':<10}  {'signal':<55}  {'N':>4}  {'P(up)':>6}  {'mean 60d':>10}  {'t':>6}  {'sharpe':>7}  {'score':>7}")
    print(f"  {'-'*120}")
    for _, r in ranked_df.head(25).iterrows():
        print(f"  {r['asset']:<10}  {r['signal']:<55}  {int(r['N']):>4}  "
              f"{r['p_up_60d']:>4.0f}%  {r['mean_60d']:>+8.1f}%  "
              f"{r['t_stat_60d']:>+5.1f}  {r['sharpe']:>+6.2f}  {r['score']:>6.1f}")

    # Bottom 10 (bearish signals)
    print(f"\n  🔴 BOTTOM 10 — BEARISH signals (P(up) lowest):")
    bear = ranked_df.sort_values('p_up_60d').head(10)
    for _, r in bear.iterrows():
        print(f"  {r['asset']:<10}  {r['signal']:<55}  {int(r['N']):>4}  "
              f"{r['p_up_60d']:>4.0f}%  {r['mean_60d']:>+8.1f}%  t={r['t_stat_60d']:>+5.1f}")


if __name__ == "__main__":
    main()
