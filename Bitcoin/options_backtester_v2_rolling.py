"""Options backtester V2: with ROLLING + multiple variants.

When option expires (or near expiry), close it and buy a new one with same params.
This gives continuous option exposure for the full spot strategy hold period.

Variants tested:
  A. ATM call rolling at expiry (30d, 90d, 180d, 365d)
  B. OTM +10% call rolling (30d, 90d)
  C. OTM +20% call rolling (30d, 90d)
  D. Proactive roll (roll when 7 days before expiry)
  E. Deep ITM call (acts like leveraged spot)
  F. Protective put + call (insurance first 30 days)
"""
import warnings; warnings.filterwarnings("ignore")
from pathlib import Path
import numpy as np
import pandas as pd
from scipy.stats import norm

ROOT = Path(__file__).parent
FEE, SLIP, INITIAL = 0.0005, 0.0003, 100.0
OPTION_FEE_PCT = 0.003   # 0.3% fee per option trade
OPTION_SLIPPAGE_PCT = 0.02  # 2% bid-ask spread
RISK_FREE_RATE = 0.05


def ema(arr, n):
    if hasattr(arr, 'values'): arr = arr.values
    out = np.full(len(arr), np.nan)
    if len(arr) < n: return out
    out[n-1] = arr[:n].mean(); k = 2.0/(n+1.0)
    for i in range(n, len(arr)):
        out[i] = arr[i]*k + out[i-1]*(1-k)
    return out

def rsi_a(close, n=14):
    s = pd.Series(close)
    delta = s.diff(); gain = delta.clip(lower=0); loss = -delta.clip(upper=0)
    avg_g = gain.ewm(alpha=1/n, adjust=False).mean()
    avg_l = loss.ewm(alpha=1/n, adjust=False).mean()
    return (100 - 100/(1 + avg_g/avg_l)).values


def bs_call(S, K, T, r, sigma):
    if T <= 0 or sigma <= 0:
        return max(S - K, 0)
    d1 = (np.log(S/K) + (r + sigma**2/2) * T) / (sigma * np.sqrt(T))
    d2 = d1 - sigma * np.sqrt(T)
    return S * norm.cdf(d1) - K * np.exp(-r*T) * norm.cdf(d2)

def bs_put(S, K, T, r, sigma):
    if T <= 0 or sigma <= 0:
        return max(K - S, 0)
    d1 = (np.log(S/K) + (r + sigma**2/2) * T) / (sigma * np.sqrt(T))
    d2 = d1 - sigma * np.sqrt(T)
    return K * np.exp(-r*T) * norm.cdf(-d2) - S * norm.cdf(-d1)


def compute_iv_proxy(close, idx, lookback=30, vrp=1.2, iv_min=0.4, iv_max=1.5):
    if idx < lookback: return 0.8
    returns = np.diff(np.log(close[idx-lookback:idx+1]))
    if len(returns) < 2: return 0.8
    realized = np.std(returns) * np.sqrt(365)
    iv = realized * vrp
    return max(iv_min, min(iv_max, iv))


def get_strategy_signals(daily, vix_df, vix_threshold=76.0):
    closes = daily['close'].values
    dates = daily.index
    n = len(closes)
    weekly_close = daily['close'].resample('W-MON', label='left', closed='left').last().dropna()
    w_ema200 = pd.Series(ema(weekly_close.values, 200), index=weekly_close.index)
    w_rsi = pd.Series(rsi_a(weekly_close.values, 14), index=weekly_close.index)
    w_rsi_sma = w_rsi.rolling(14).mean()

    cu = ((weekly_close < w_ema200) & (weekly_close.shift(1) >= w_ema200.shift(1)))
    ex = ((w_rsi_sma < 70) & (w_rsi_sma.shift(1) >= 70))

    cu_d = cu.shift(1).reindex(dates, method='ffill').fillna(False).astype(bool)
    ex_d = ex.shift(1).reindex(dates, method='ffill').fillna(False).astype(bool)
    cu_s = (cu_d & ~cu_d.shift(1).fillna(False)).values
    ex_s = (ex_d & ~ex_d.shift(1).fillna(False)).values

    vix_h = vix_df['high'].reindex(dates, method='ffill').values
    vix_t = vix_h >= vix_threshold

    trades = []; in_pos = False; ed = None; ep = 0
    for i in range(n):
        if not in_pos:
            if vix_t[i] or cu_s[i]:
                ed = dates[i]; ep = closes[i]; in_pos = True
        else:
            if ex_s[i]:
                trades.append({'entry_date': ed, 'exit_date': dates[i],
                              'entry_price': ep, 'exit_price': closes[i],
                              'hold_days': (dates[i] - ed).days,
                              'spot_pnl_pct': (closes[i]/ep - 1) * 100})
                in_pos = False
    if in_pos:
        trades.append({'entry_date': ed, 'exit_date': dates[-1],
                      'entry_price': ep, 'exit_price': closes[-1],
                      'hold_days': (dates[-1] - ed).days,
                      'spot_pnl_pct': (closes[-1]/ep - 1) * 100, 'still_open': True})
    return pd.DataFrame(trades)


def load_vix():
    vix = pd.read_csv(ROOT / 'vix_daily.csv', skiprows=3,
                       names=['Date','close','high','low','open','volume'])
    vix['Date'] = pd.to_datetime(vix['Date'])
    return vix.set_index('Date')


def backtest_options_rolling(spot_trades, daily, option_type='call',
                                strike_offset_pct=0.0, expiry_days=30,
                                roll_buffer_days=0, capital_pct=0.99,
                                protective_put_days=0):
    """Rolling options strategy.
    - At spot entry: buy first option
    - When current option has < roll_buffer_days remaining: close & buy new
    - When spot exit signal: close all options
    - If protective_put_days > 0: also buy put at entry for that many days
    """
    closes = daily['close'].values
    cash = INITIAL
    all_option_trades = []

    for _, st in spot_trades.iterrows():
        entry_date = st['entry_date']
        exit_date = st['exit_date']

        try:
            entry_idx = daily.index.get_loc(entry_date)
            exit_idx = daily.index.get_loc(exit_date)
        except KeyError:
            continue

        position_start_cash = cash
        n_contracts = 0
        current_strike = 0
        current_expiry_date = None
        current_premium_paid = 0
        roll_count = 0

        # Optional protective put at entry
        put_n_contracts = 0
        put_strike = 0
        put_expiry_date = None
        put_premium_paid = 0
        if protective_put_days > 0:
            spot_at_e = closes[entry_idx]
            iv = compute_iv_proxy(closes, entry_idx)
            T = protective_put_days / 365.0
            put_strike = spot_at_e * 0.95  # 5% OTM put
            put_prem = bs_put(spot_at_e, put_strike, T, RISK_FREE_RATE, iv)
            put_prem_eff = put_prem * (1 + OPTION_SLIPPAGE_PCT) + put_prem * OPTION_FEE_PCT
            # Allocate 5% of cash to protective put
            put_spend = cash * 0.05
            put_n_contracts = put_spend / put_prem_eff if put_prem_eff > 0 else 0
            cash -= put_n_contracts * put_prem_eff
            put_expiry_date = entry_date + pd.Timedelta(days=protective_put_days)
            put_premium_paid = put_prem_eff

        # Initial call entry
        spot_at_e = closes[entry_idx]
        iv = compute_iv_proxy(closes, entry_idx)
        T = expiry_days / 365.0
        K = spot_at_e * (1 + strike_offset_pct)
        prem = bs_call(spot_at_e, K, T, RISK_FREE_RATE, iv)
        prem_eff = prem * (1 + OPTION_SLIPPAGE_PCT) + prem * OPTION_FEE_PCT
        if prem_eff <= 0:
            continue
        spend = cash * capital_pct
        n_contracts = spend / prem_eff
        cash -= n_contracts * prem_eff
        current_strike = K
        current_expiry_date = entry_date + pd.Timedelta(days=expiry_days)
        current_premium_paid = prem_eff

        # Walk forward day by day from entry to exit
        for i in range(entry_idx + 1, min(exit_idx + 1, len(closes))):
            cur_date = daily.index[i]
            spot_now = closes[i]

            # Check protective put expiry (or close if exit signal coming)
            if put_n_contracts > 0:
                if cur_date >= put_expiry_date or i == exit_idx:
                    iv_now = compute_iv_proxy(closes, i)
                    days_left = max(0, (put_expiry_date - cur_date).days)
                    T_left = days_left / 365.0
                    p_val = bs_put(spot_now, put_strike, T_left, RISK_FREE_RATE, iv_now)
                    p_eff = max(0, p_val * (1 - OPTION_SLIPPAGE_PCT) - p_val * OPTION_FEE_PCT)
                    cash += put_n_contracts * p_eff
                    put_n_contracts = 0

            # Check call roll condition
            days_to_expiry = (current_expiry_date - cur_date).days
            should_roll = (days_to_expiry <= roll_buffer_days) and (i < exit_idx)

            if should_roll and n_contracts > 0:
                # Close current call
                iv_now = compute_iv_proxy(closes, i)
                T_left = max(0, days_to_expiry / 365.0)
                c_val = bs_call(spot_now, current_strike, T_left, RISK_FREE_RATE, iv_now)
                c_eff = max(0, c_val * (1 - OPTION_SLIPPAGE_PCT) - c_val * OPTION_FEE_PCT)
                cash += n_contracts * c_eff

                # Buy new call
                K_new = spot_now * (1 + strike_offset_pct)
                T_new = expiry_days / 365.0
                prem_new = bs_call(spot_now, K_new, T_new, RISK_FREE_RATE, iv_now)
                prem_new_eff = prem_new * (1 + OPTION_SLIPPAGE_PCT) + prem_new * OPTION_FEE_PCT
                if prem_new_eff > 0 and cash > 0:
                    spend = cash * capital_pct
                    n_contracts = spend / prem_new_eff
                    cash -= n_contracts * prem_new_eff
                    current_strike = K_new
                    current_expiry_date = cur_date + pd.Timedelta(days=expiry_days)
                    current_premium_paid = prem_new_eff
                    roll_count += 1
                else:
                    n_contracts = 0

        # Close at exit
        exit_spot = closes[exit_idx]
        if n_contracts > 0:
            iv_exit = compute_iv_proxy(closes, exit_idx)
            days_left = max(0, (current_expiry_date - daily.index[exit_idx]).days)
            T_left = days_left / 365.0
            c_val = bs_call(exit_spot, current_strike, T_left, RISK_FREE_RATE, iv_exit)
            c_eff = max(0, c_val * (1 - OPTION_SLIPPAGE_PCT) - c_val * OPTION_FEE_PCT)
            cash += n_contracts * c_eff

        # Close put if still open
        if put_n_contracts > 0:
            iv_now = compute_iv_proxy(closes, exit_idx)
            days_left = max(0, (put_expiry_date - daily.index[exit_idx]).days)
            T_left = days_left / 365.0
            p_val = bs_put(exit_spot, put_strike, T_left, RISK_FREE_RATE, iv_now)
            p_eff = max(0, p_val * (1 - OPTION_SLIPPAGE_PCT) - p_val * OPTION_FEE_PCT)
            cash += put_n_contracts * p_eff

        cash = max(cash, 0.001)
        pnl_pct = (cash/position_start_cash - 1) * 100
        all_option_trades.append({
            'entry_date': entry_date,
            'exit_date': daily.index[exit_idx],
            'spot_entry': st['entry_price'],
            'spot_exit': exit_spot,
            'spot_pnl_pct': (exit_spot/st['entry_price'] - 1) * 100,
            'cash_before': position_start_cash,
            'cash_after': cash,
            'option_pnl_pct': pnl_pct,
            'rolls': roll_count,
        })

    return pd.DataFrame(all_option_trades), cash


def report(label, opt_results, final_cash, spot_trades):
    if len(opt_results) == 0:
        print(f"  {label}: no trades"); return

    spot_total = INITIAL
    for _, t in spot_trades.iterrows():
        spot_total *= (1 + t['spot_pnl_pct']/100)

    opt_ret = (final_cash/INITIAL - 1) * 100
    spot_ret = (spot_total/INITIAL - 1) * 100

    n = len(opt_results)
    wr = (opt_results['option_pnl_pct'] > 0).sum() / n * 100
    avg_pnl = opt_results['option_pnl_pct'].mean()
    max_loss = opt_results['option_pnl_pct'].min()
    max_win = opt_results['option_pnl_pct'].max()
    total_rolls = opt_results['rolls'].sum()

    win_lose = "🟢 OPT BETTER" if opt_ret > spot_ret else ("🔴 spot better" if opt_ret < spot_ret*0.5 else "🟡 close")
    print(f"  {label:50}  Final: ${final_cash:>10,.0f} ({opt_ret:>+7.0f}%)  vs spot ${spot_total:>10,.0f} ({spot_ret:>+6.0f}%)  rolls={total_rolls:>3}  WR={wr:>3.0f}%  {win_lose}")
    return opt_ret


def show_details(label, opt_results):
    print(f"\n  {label} — trade detail:")
    print(f"    {'entry':>11}  {'exit':>11}  {'days':>4}  {'spot':>8}  {'option':>8}  {'rolls':>5}")
    for _, t in opt_results.iterrows():
        print(f"    {t['entry_date'].date()}  {t['exit_date'].date()}  "
              f"{(t['exit_date']-t['entry_date']).days:>4}  "
              f"{t['spot_pnl_pct']:>+7.1f}%  {t['option_pnl_pct']:>+7.1f}%  {int(t['rolls']):>4}")


def main():
    daily = pd.read_csv(ROOT/'btc_usd_daily_coinbase.csv', parse_dates=['date']).set_index('date').sort_index()
    daily = daily[daily['close']>0].dropna(subset=['close'])
    vix = load_vix()

    spot_trades = get_strategy_signals(daily, vix, vix_threshold=76.0)
    spot_total = INITIAL
    for _, t in spot_trades.iterrows():
        spot_total *= (1 + t['spot_pnl_pct']/100)
    print(f"\n{'='*180}")
    print(f"OPTIONS BACKTESTER V2 — with rolling on user's V1 strategy")
    print(f"{'='*180}")
    print(f"\nSpot strategy: {len(spot_trades)} trades, final equity = ${spot_total:,.0f} (+{(spot_total/INITIAL-1)*100:.0f}%)")
    for _, t in spot_trades.iterrows():
        print(f"  {t['entry_date'].date()} → {t['exit_date'].date()}  hold={t['hold_days']:>3}d  spot={t['spot_pnl_pct']:>+7.1f}%")

    # Variants to test
    variants = [
        # (label, type, strike_off, expiry, roll_buffer, protective_put_days)
        ('ATM 30d roll at expiry',        'call', 0.00,  30,  0,  0),
        ('ATM 30d roll 7d before',        'call', 0.00,  30,  7,  0),
        ('ATM 90d roll at expiry',        'call', 0.00,  90,  0,  0),
        ('ATM 90d roll 14d before',       'call', 0.00,  90, 14,  0),
        ('ATM 180d roll at expiry',       'call', 0.00, 180,  0,  0),
        ('ATM 365d roll at expiry',       'call', 0.00, 365,  0,  0),
        ('ATM 365d roll 30d before',      'call', 0.00, 365, 30,  0),
        ('OTM +10% 30d roll',             'call', 0.10,  30,  0,  0),
        ('OTM +10% 90d roll',             'call', 0.10,  90,  0,  0),
        ('OTM +20% 30d roll',             'call', 0.20,  30,  0,  0),
        ('OTM +20% 90d roll',             'call', 0.20,  90,  0,  0),
        ('Deep ITM -20% 90d roll',        'call', -0.20, 90,  0,  0),
        ('Deep ITM -30% 180d roll',       'call', -0.30,180,  0,  0),
        ('ATM 90d + protective put 60d',  'call', 0.00,  90,  0, 60),
        ('ATM 90d + protective put 90d',  'call', 0.00,  90,  0, 90),
        ('ATM 365d + protective put 90d', 'call', 0.00, 365,  0, 90),
    ]

    print(f"\n{'='*180}")
    print(f"All variants summary (compared to spot ${spot_total:,.0f} = +{(spot_total/INITIAL-1)*100:.0f}%):")
    print(f"{'='*180}")

    all_results = {}
    for label, opt_type, strike_off, exp_d, roll_buf, put_d in variants:
        opt_results, final_cash = backtest_options_rolling(
            spot_trades, daily,
            option_type=opt_type,
            strike_offset_pct=strike_off,
            expiry_days=exp_d,
            roll_buffer_days=roll_buf,
            protective_put_days=put_d)
        if len(opt_results) == 0:
            continue
        ret = report(label, opt_results, final_cash, spot_trades)
        all_results[label] = (opt_results, final_cash, ret)

    # Show details for top 5 variants
    sorted_variants = sorted(all_results.items(), key=lambda x: x[1][2], reverse=True)
    print(f"\n{'='*180}")
    print(f"TOP 5 variants — trade-by-trade detail:")
    print(f"{'='*180}")
    for label, (opt_results, _, _) in sorted_variants[:5]:
        show_details(label, opt_results)


if __name__ == "__main__":
    main()
