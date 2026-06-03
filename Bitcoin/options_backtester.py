"""Options backtester for user's V1/V2 strategy.

Pipeline:
  1. Run user's spot strategy → get entry/exit dates + spot prices
  2. At each entry, "buy" options using Black-Scholes pricing
  3. At each exit (or expiry, whichever first), sell options
  4. Compare option vs spot performance

Option variants tested:
  - ATM call 30 days
  - OTM call +10% strike 30 days
  - ATM call 90 days (LEAPS-light)
  - ATM call 180 days (LEAPS)
  - ATM call 365 days (full year)

IV approximation:
  - Realized volatility × 1.2 (capturing variance risk premium)
  - Cap at reasonable BTC range (40% to 150%)
"""
import warnings; warnings.filterwarnings("ignore")
from pathlib import Path
import numpy as np
import pandas as pd
from scipy.stats import norm

ROOT = Path(__file__).parent
FEE, SLIP, INITIAL = 0.0005, 0.0003, 100.0
OPTION_FEE_PCT = 0.003  # 0.3% per option trade (typical Deribit fee)
OPTION_SLIPPAGE_PCT = 0.02  # 2% bid-ask spread (conservative for crypto options)
RISK_FREE_RATE = 0.05  # 5% annual risk-free rate


# ============ Indicators ============
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


# ============ Black-Scholes pricing ============
def bs_call_price(S, K, T, r, sigma):
    """Black-Scholes call option price.
    S = spot, K = strike, T = time to expiry (years), r = rate, sigma = vol."""
    if T <= 0 or sigma <= 0:
        return max(S - K, 0)  # intrinsic value only
    d1 = (np.log(S/K) + (r + sigma**2/2) * T) / (sigma * np.sqrt(T))
    d2 = d1 - sigma * np.sqrt(T)
    return S * norm.cdf(d1) - K * np.exp(-r*T) * norm.cdf(d2)

def bs_put_price(S, K, T, r, sigma):
    if T <= 0 or sigma <= 0:
        return max(K - S, 0)
    d1 = (np.log(S/K) + (r + sigma**2/2) * T) / (sigma * np.sqrt(T))
    d2 = d1 - sigma * np.sqrt(T)
    return K * np.exp(-r*T) * norm.cdf(-d2) - S * norm.cdf(-d1)


# ============ Volatility proxy ============
def compute_iv_proxy(close, idx, lookback=30, vrp_multiplier=1.2, iv_min=0.4, iv_max=1.5):
    """Realized vol over `lookback` days × multiplier (variance risk premium).
    Returns IV as annualized decimal (e.g., 0.7 = 70%)."""
    if idx < lookback:
        return 0.8  # default
    returns = np.diff(np.log(close[idx-lookback:idx+1]))
    if len(returns) < 2:
        return 0.8
    realized = np.std(returns) * np.sqrt(365)  # annualized
    iv = realized * vrp_multiplier
    return max(iv_min, min(iv_max, iv))


# ============ User's strategy (V1: weekly logic + VIX 76) ============
def get_strategy_signals(daily, vix_df, vix_threshold=76.0):
    """Returns list of (entry_date, exit_date, entry_price, exit_price)."""
    closes = daily['close'].values
    dates = daily.index
    n = len(closes)

    weekly_close = daily['close'].resample('W-MON', label='left', closed='left').last().dropna()
    w_ema200 = pd.Series(ema(weekly_close.values, 200), index=weekly_close.index)
    w_ema20 = pd.Series(ema(weekly_close.values, 20), index=weekly_close.index)
    w_ema5 = pd.Series(ema(weekly_close.values, 5), index=weekly_close.index)
    w_rsi = pd.Series(rsi_a(weekly_close.values, 14), index=weekly_close.index)
    w_rsi_sma = w_rsi.rolling(14).mean()

    cross_under_ema200 = ((weekly_close < w_ema200) & (weekly_close.shift(1) >= w_ema200.shift(1)))
    exit_rsi = ((w_rsi_sma < 70) & (w_rsi_sma.shift(1) >= 70))

    cross_d = cross_under_ema200.shift(1).reindex(dates, method='ffill').fillna(False).astype(bool)
    exit_d = exit_rsi.shift(1).reindex(dates, method='ffill').fillna(False).astype(bool)
    cross_strict = (cross_d & ~cross_d.shift(1).fillna(False)).values
    exit_strict = (exit_d & ~exit_d.shift(1).fillna(False)).values

    vix_high = vix_df['high'].reindex(dates, method='ffill').values
    vix_trigger = vix_high >= vix_threshold

    trades = []
    in_position = False
    entry_date = None
    entry_price = 0
    for i in range(n):
        if not in_position:
            if vix_trigger[i] or cross_strict[i]:
                entry_date = dates[i]
                entry_price = closes[i]
                in_position = True
        else:
            if exit_strict[i]:
                trades.append({
                    'entry_date': entry_date,
                    'exit_date': dates[i],
                    'entry_price': entry_price,
                    'exit_price': closes[i],
                    'hold_days': (dates[i] - entry_date).days,
                    'spot_pnl_pct': (closes[i]/entry_price - 1) * 100,
                })
                in_position = False

    if in_position:
        trades.append({
            'entry_date': entry_date,
            'exit_date': dates[-1],
            'entry_price': entry_price,
            'exit_price': closes[-1],
            'hold_days': (dates[-1] - entry_date).days,
            'spot_pnl_pct': (closes[-1]/entry_price - 1) * 100,
            'still_open': True,
        })

    return pd.DataFrame(trades)


def load_vix():
    vix = pd.read_csv(ROOT / 'vix_daily.csv', skiprows=3,
                       names=['Date','close','high','low','open','volume'])
    vix['Date'] = pd.to_datetime(vix['Date'])
    return vix.set_index('Date')


# ============ Options backtest ============
def backtest_options(spot_trades, daily, option_type='call',
                       strike_offset_pct=0.0, expiry_days=30,
                       capital_pct_per_trade=0.99):
    """For each spot trade signal, buy options at entry, sell at exit (or expiry).

    strike_offset_pct = 0 → ATM, +0.1 → 10% OTM call, etc.
    expiry_days = days to expiration of the option we buy
    """
    closes = daily['close'].values
    dates_arr = daily.index

    cash = INITIAL
    eq_history = pd.Series(INITIAL, index=daily.index, dtype=float)
    eq_history.iloc[0] = INITIAL

    results = []
    last_exit_date = None

    for _, trade in spot_trades.iterrows():
        entry_date = trade['entry_date']
        exit_date = trade['exit_date']
        spot_entry = trade['entry_price']
        spot_exit = trade['exit_price']

        # Find indices
        try:
            entry_idx = daily.index.get_loc(entry_date)
            exit_idx = daily.index.get_loc(exit_date)
        except KeyError:
            continue

        # Compute IV at entry
        iv_entry = compute_iv_proxy(closes, entry_idx)
        # Define option params
        strike = spot_entry * (1 + strike_offset_pct)
        T_entry = expiry_days / 365.0

        # Premium per option
        if option_type == 'call':
            premium_entry = bs_call_price(spot_entry, strike, T_entry, RISK_FREE_RATE, iv_entry)
        else:
            premium_entry = bs_put_price(spot_entry, strike, T_entry, RISK_FREE_RATE, iv_entry)

        # Account for entry slippage + fee
        premium_entry_eff = premium_entry * (1 + OPTION_SLIPPAGE_PCT) + premium_entry * OPTION_FEE_PCT
        if premium_entry_eff <= 0:
            continue

        # How many contracts can we buy?
        spend = cash * capital_pct_per_trade
        n_contracts = spend / premium_entry_eff
        # Notional exposure
        notional = n_contracts * spot_entry
        leverage_effective = notional / spend

        # Hold until exit_date or expiry, whichever comes first
        expiry_date = entry_date + pd.Timedelta(days=expiry_days)
        actual_exit_date = min(exit_date, expiry_date)

        # If expiry comes first, option price at expiry = intrinsic value
        # If exit signal comes first, price option with BS at remaining T

        try:
            actual_exit_idx = daily.index.get_loc(actual_exit_date)
        except KeyError:
            # Find nearest date
            actual_exit_idx = daily.index.searchsorted(actual_exit_date)
            if actual_exit_idx >= len(daily):
                actual_exit_idx = len(daily) - 1

        spot_at_close = closes[actual_exit_idx]
        days_elapsed = (daily.index[actual_exit_idx] - entry_date).days
        T_remaining = max(0, (expiry_days - days_elapsed) / 365.0)
        iv_exit = compute_iv_proxy(closes, actual_exit_idx)

        # Option price at exit
        if option_type == 'call':
            premium_exit = bs_call_price(spot_at_close, strike, T_remaining, RISK_FREE_RATE, iv_exit)
        else:
            premium_exit = bs_put_price(spot_at_close, strike, T_remaining, RISK_FREE_RATE, iv_exit)

        # Effective premium received (with slippage/fees)
        premium_exit_eff = premium_exit * (1 - OPTION_SLIPPAGE_PCT) - premium_exit * OPTION_FEE_PCT
        premium_exit_eff = max(0, premium_exit_eff)

        proceeds = n_contracts * premium_exit_eff
        cost = n_contracts * premium_entry_eff
        cash = cash - cost + proceeds
        cash = max(cash, 0.001)

        pnl_pct = (proceeds / cost - 1) * 100 if cost > 0 else 0

        results.append({
            'entry_date': entry_date,
            'exit_date': daily.index[actual_exit_idx],
            'spot_entry': spot_entry,
            'spot_exit': spot_at_close,
            'spot_pnl_pct': (spot_at_close/spot_entry - 1) * 100,
            'strike': strike,
            'iv_entry_pct': iv_entry * 100,
            'iv_exit_pct': iv_exit * 100,
            'premium_entry': premium_entry,
            'premium_exit': premium_exit,
            'n_contracts': n_contracts,
            'notional': notional,
            'leverage': leverage_effective,
            'option_pnl_pct': pnl_pct,
            'expired_first': expiry_date < exit_date,
            'cash_after': cash,
        })

    final = cash if cash > 0 else 0.01
    return pd.DataFrame(results), final


def report(label, opt_results, final_cash, spot_trades):
    if len(opt_results) == 0:
        print(f"  {label}: no trades")
        return

    spot_total = INITIAL
    for _, t in spot_trades.iterrows():
        spot_total *= (1 + t['spot_pnl_pct']/100)

    opt_total_ret = (final_cash/INITIAL - 1) * 100
    spot_total_ret = (spot_total/INITIAL - 1) * 100

    n = len(opt_results)
    wr = (opt_results['option_pnl_pct'] > 0).sum() / n * 100
    avg_pnl = opt_results['option_pnl_pct'].mean()
    max_loss = opt_results['option_pnl_pct'].min()
    max_win = opt_results['option_pnl_pct'].max()
    avg_lev = opt_results['leverage'].mean()
    n_expired = opt_results['expired_first'].sum()

    print(f"\n  {label}:")
    print(f"    Trades: N={n}  WR={wr:.0f}%  avg PnL={avg_pnl:+.0f}%  max win={max_win:+.0f}%  max loss={max_loss:+.0f}%")
    print(f"    Avg leverage: {avg_lev:.1f}x  (notional/premium ratio)")
    print(f"    Expired before exit signal: {n_expired}/{n}")
    print(f"    Final equity: ${final_cash:,.0f}  ({opt_total_ret:+.0f}%)")
    print(f"    Spot equivalent: ${spot_total:,.0f}  ({spot_total_ret:+.0f}%)")
    print(f"    {'OPTIONS BETTER' if opt_total_ret > spot_total_ret else 'spot better'}")

    # Per-trade detail
    print(f"\n    Trade-by-trade:")
    print(f"    {'entry':>11}  {'exit':>11}  {'days':>5}  {'spot_pnl':>9}  {'opt_pnl':>9}  {'lev':>5}  {'IV_in':>5}")
    for _, t in opt_results.iterrows():
        print(f"    {t['entry_date'].date()}  {t['exit_date'].date()}  "
              f"{(t['exit_date'] - t['entry_date']).days:>4}d  "
              f"{t['spot_pnl_pct']:>+7.1f}%  {t['option_pnl_pct']:>+7.1f}%  "
              f"{t['leverage']:>4.1f}x  {t['iv_entry_pct']:>4.0f}%")


def main():
    daily = pd.read_csv(ROOT/'btc_usd_daily_coinbase.csv', parse_dates=['date']).set_index('date').sort_index()
    daily = daily[daily['close']>0].dropna(subset=['close'])
    vix = load_vix()

    print("=" * 130)
    print("OPTIONS BACKTESTER on user's V1 strategy (VIX ≥ 76)")
    print("=" * 130)

    # 1. Get spot strategy trades
    spot_trades = get_strategy_signals(daily, vix, vix_threshold=76.0)
    print(f"\nSpot strategy generated {len(spot_trades)} trades:")
    for _, t in spot_trades.iterrows():
        print(f"  {t['entry_date'].date()} → {t['exit_date'].date()}  "
              f"hold={t['hold_days']:>4}d  spot PnL={t['spot_pnl_pct']:>+7.1f}%")

    # 2. Test option variants
    variants = [
        ('ATM call 30 days',         'call', 0.00,  30),
        ('OTM call +10% 30 days',    'call', 0.10,  30),
        ('OTM call +20% 30 days',    'call', 0.20,  30),
        ('ATM call 90 days',         'call', 0.00,  90),
        ('ATM call 180 days',        'call', 0.00, 180),
        ('ATM call 365 days (LEAPS)', 'call', 0.00, 365),
        ('OTM call +10% 90 days',    'call', 0.10,  90),
        ('OTM call +20% 180 days',   'call', 0.20, 180),
    ]

    for label, opt_type, strike_off, exp_d in variants:
        opt_results, final_cash = backtest_options(spot_trades, daily,
            option_type=opt_type, strike_offset_pct=strike_off, expiry_days=exp_d)
        report(label, opt_results, final_cash, spot_trades)


if __name__ == "__main__":
    main()
