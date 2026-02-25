"""
Бэктест IMOEXF — зональные стратегии на основе анализа индикаторов.

Принципы честности:
  - Все индикаторы рассчитаны без заглядывания в будущее
  - Вход/выход на следующий день после сигнала (по close)
  - Учёт проскальзывания (slippage)
  - Разбивка на 2 периода для проверки устойчивости
"""
import sys
sys.stdout.reconfigure(encoding='utf-8')

import pandas as pd
import numpy as np
from dotenv import load_dotenv
from pathlib import Path
from sqlalchemy import create_engine, text
import os

load_dotenv(Path(__file__).parent / '.env')
engine = create_engine(os.getenv('DB_URL'))

# ══════════════════════════════════════════════
# 1. ЗАГРУЗКА ДАННЫХ
# ══════════════════════════════════════════════

def load_imoexf_candles():
    q = text("""
        SELECT begin_time::date as date, open, high, low, close, volume
        FROM candles WHERE sec_id = 'IMOEXF' AND interval = 24
        ORDER BY begin_time
    """)
    df = pd.read_sql(q, engine, parse_dates=['date'])
    df = df.drop_duplicates(subset='date', keep='last').set_index('date')
    return df

def load_oi_yur():
    q = text("""
        SELECT DISTINCT ON (tradedate) tradedate as date, pos as oi
        FROM open_interest
        WHERE sectype = 'IMOEXF' AND clgroup = 'YUR' AND interval = 24
        ORDER BY tradedate ASC, tradetime DESC
    """)
    df = pd.read_sql(q, engine, parse_dates=['date'])
    df = df.set_index('date')
    return df

def load_breadth_stocks():
    stocks = ['SBER','GAZP','LKOH','GMKN','ROSN','YDEX','NVTK','MTSS',
              'MGNT','VTBR','CHMF','NLMK','PLZL','TATN','ALRS']
    frames = {}
    for s in stocks:
        q = text("""
            SELECT begin_time::date as date, close
            FROM candles WHERE sec_id = :s AND interval = 24
            ORDER BY begin_time
        """)
        df = pd.read_sql(q, engine, params={'s': s}, parse_dates=['date'])
        df = df.drop_duplicates(subset='date', keep='last').set_index('date')
        frames[s] = df['close']
    return pd.DataFrame(frames)

def load_index_data(secid):
    q = text("""
        SELECT trade_date as date, close
        FROM index_data WHERE secid = :s
        ORDER BY trade_date
    """)
    df = pd.read_sql(q, engine, params={'s': secid}, parse_dates=['date'])
    df = df.drop_duplicates(subset='date', keep='last').set_index('date')
    return df['close']

def load_fund_nav(ticker):
    q = text("""
        SELECT fd.trade_date as date, fd.pay as nav
        FROM fund_data fd
        JOIN funds f ON f.fund_id = fd.fund_id
        WHERE f.ticker = :t
        ORDER BY fd.trade_date
    """)
    df = pd.read_sql(q, engine, params={'t': ticker}, parse_dates=['date'])
    df = df.drop_duplicates(subset='date', keep='last').set_index('date')
    return df['nav'].astype(float)

print("Загрузка данных...")
candles = load_imoexf_candles()
oi_data = load_oi_yur()
stocks_raw = load_breadth_stocks()
mcftr = load_index_data('MCFTR')
rgbitr = load_index_data('RGBITR')
lqdt_nav = load_fund_nav('LQDT')
tmos_nav = load_fund_nav('TMOS')

print(f"  IMOEXF: {candles.index[0].date()} — {candles.index[-1].date()}, {len(candles)} баров")

# ══════════════════════════════════════════════
# 2. РАСЧЁТ ИНДИКАТОРОВ (без look-ahead)
# ══════════════════════════════════════════════

df = candles.copy()

# --- RSI(14) + SMA(RSI, 14) ---
delta = df['close'].diff()
gain = delta.clip(lower=0)
loss = (-delta).clip(lower=0)
avg_gain = gain.ewm(alpha=1/14, min_periods=14).mean()
avg_loss = loss.ewm(alpha=1/14, min_periods=14).mean()
rs = avg_gain / avg_loss
df['rsi'] = 100 - (100 / (1 + rs))
df['rsi_ma'] = df['rsi'].rolling(14).mean()

# RSI exit: RSI был > 70, затем RSI пересёк вниз RSI_MA
rsi_exit_signal = pd.Series(False, index=df.index)
was_above = False
for i in range(len(df)):
    if df['rsi'].iloc[i] > 70:
        was_above = True
    if was_above and i > 0:
        if df['rsi'].iloc[i] < df['rsi_ma'].iloc[i] and df['rsi'].iloc[i-1] >= df['rsi_ma'].iloc[i-1]:
            rsi_exit_signal.iloc[i] = True
            was_above = False
df['rsi_exit'] = rsi_exit_signal

# --- OI юрлиц ---
df = df.join(oi_data, how='left')
df['oi'] = df['oi'].ffill()
df['oi_ema20'] = df['oi'].ewm(span=20).mean()
df['oi_ema60'] = df['oi'].ewm(span=60).mean()
df['oi_momentum'] = df['oi_ema20'] - df['oi_ema60']

# --- Breadth ---
breadth_aligned = stocks_raw.reindex(df.index).ffill()
for col in breadth_aligned.columns:
    ema200 = breadth_aligned[col].ewm(span=200).mean()
    breadth_aligned[col] = (breadth_aligned[col] > ema200).astype(float)
df['breadth'] = (breadth_aligned.mean(axis=1) * 100).ewm(span=10).mean()

# --- Баффетт Z-Score ---
mcftr_a = mcftr.reindex(df.index).ffill()
rgbitr_a = rgbitr.reindex(df.index).ffill()
ratio = mcftr_a / rgbitr_a
ratio_ema = ratio.ewm(span=60).mean()
ratio_mean = ratio_ema.rolling(252).mean()
ratio_std = ratio_ema.rolling(252).std()
df['buff_z'] = ((ratio_ema - ratio_mean) / ratio_std).fillna(0)

# --- Fund Fear ---
lqdt_a = lqdt_nav.reindex(df.index).ffill()
tmos_a = tmos_nav.reindex(df.index).ffill()
mm_roc = lqdt_a.pct_change(20) * 100
stock_roc = tmos_a.pct_change(20) * 100
rotation = mm_roc - stock_roc
df['fear'] = rotation.rolling(252).apply(
    lambda x: pd.Series(x).rank(pct=True).iloc[-1] * 100, raw=False
).ewm(span=10).mean()

# Убираем строки без рассчитанных индикаторов
df = df.dropna(subset=['rsi', 'rsi_ma', 'breadth', 'oi_momentum'])
print(f"  Рабочий период: {df.index[0].date()} — {df.index[-1].date()}, {len(df)} баров")

# ══════════════════════════════════════════════
# 3. ОПРЕДЕЛЕНИЕ СТРАТЕГИЙ (зональные)
# ══════════════════════════════════════════════

# Зоны входа (на основе analysis.py):
#   Breadth < 25    → сильный предиктор роста на 40-60д
#   Buffett Z < -1  → рынок дёшев, сильный рост на 20-60д
#   OI momentum > 0 → юрлица наращивают, подтверждение на 20-40д
#   Fear > 75       → контрариан, рост на 60д
#
# Зоны выхода:
#   RSI exit        → RSI был > 70, затем RSI < RSI_MA
#   Breadth > 75    → рынок перегрет, сильное падение на 40-60д

SLIPPAGE = 0.001  # 0.1% на сделку (вход + выход)

def backtest_zone(df, entry_fn, exit_fn, name, slippage=SLIPPAGE):
    """
    Зональный бэктест.
    entry_fn(row, prev_row) → True/False  — условие входа
    exit_fn(row, prev_row)  → True/False  — условие выхода
    Сигнал на bar[i-1], исполнение по close bar[i].
    """
    position = False
    entry_price = 0
    entry_date = None
    trades = []
    equity = [1.0]

    for i in range(1, len(df)):
        row = df.iloc[i]
        prev = df.iloc[i-1]

        if not position and entry_fn(prev, df.iloc[i-2] if i >= 2 else prev):
            position = True
            entry_price = row['close'] * (1 + slippage)  # покупаем чуть дороже
            entry_date = df.index[i]

        elif position and exit_fn(prev, df.iloc[i-2] if i >= 2 else prev):
            exit_price = row['close'] * (1 - slippage)  # продаём чуть дешевле
            pnl = (exit_price - entry_price) / entry_price
            trades.append({
                'entry_date': entry_date,
                'exit_date': df.index[i],
                'entry_price': entry_price,
                'exit_price': exit_price,
                'pnl_pct': pnl * 100,
                'days': (df.index[i] - entry_date).days
            })
            position = False

        # Equity curve
        if position:
            daily_ret = (row['close'] - df.iloc[i-1]['close']) / df.iloc[i-1]['close']
            equity.append(equity[-1] * (1 + daily_ret))
        else:
            equity.append(equity[-1])

    # Закрытие открытой позиции по последней цене
    if position:
        exit_price = df.iloc[-1]['close'] * (1 - slippage)
        pnl = (exit_price - entry_price) / entry_price
        trades.append({
            'entry_date': entry_date,
            'exit_date': df.index[-1],
            'entry_price': entry_price,
            'exit_price': exit_price,
            'pnl_pct': pnl * 100,
            'days': (df.index[-1] - entry_date).days
        })

    equity = pd.Series(equity, index=df.index[:len(equity)])
    trades_df = pd.DataFrame(trades) if trades else pd.DataFrame()

    # Метрики
    total_return = (equity.iloc[-1] / equity.iloc[0] - 1) * 100
    max_dd = ((equity / equity.cummax()) - 1).min() * 100
    bh_return = (df.iloc[-1]['close'] / df.iloc[0]['close'] - 1) * 100

    # Время в рынке
    in_market_days = 0
    pos = False
    for i in range(1, len(df)):
        prev = df.iloc[i-1]
        prev2 = df.iloc[i-2] if i >= 2 else prev
        if not pos and entry_fn(prev, prev2):
            pos = True
        elif pos and exit_fn(prev, prev2):
            pos = False
        if pos:
            in_market_days += 1
    time_in_market = in_market_days / len(df) * 100

    n_trades = len(trades_df)
    if n_trades > 0:
        win_rate = (trades_df['pnl_pct'] > 0).mean() * 100
        avg_pnl = trades_df['pnl_pct'].mean()
        avg_win = trades_df.loc[trades_df['pnl_pct'] > 0, 'pnl_pct'].mean() if (trades_df['pnl_pct'] > 0).any() else 0
        avg_loss = trades_df.loc[trades_df['pnl_pct'] <= 0, 'pnl_pct'].mean() if (trades_df['pnl_pct'] <= 0).any() else 0
        avg_days = trades_df['days'].mean()
    else:
        win_rate = avg_pnl = avg_win = avg_loss = avg_days = 0

    # Годовая доходность и Sharpe
    days_total = (df.index[-1] - df.index[0]).days
    years = days_total / 365.25
    ann_return = ((equity.iloc[-1] / equity.iloc[0]) ** (1 / years) - 1) * 100 if years > 0 else 0
    daily_returns = equity.pct_change().dropna()
    sharpe = (daily_returns.mean() / daily_returns.std() * np.sqrt(252)) if daily_returns.std() > 0 else 0

    return {
        'name': name,
        'total_return': total_return,
        'ann_return': ann_return,
        'buy_hold': bh_return,
        'max_dd': max_dd,
        'sharpe': sharpe,
        'time_in_market': time_in_market,
        'n_trades': n_trades,
        'win_rate': win_rate,
        'avg_pnl': avg_pnl,
        'avg_win': avg_win,
        'avg_loss': avg_loss,
        'avg_days': avg_days,
        'trades': trades_df,
        'equity': equity
    }

# ══════════════════════════════════════════════
# 4. СТРАТЕГИИ
# ══════════════════════════════════════════════

# Общие выходы
def exit_rsi(row, prev):
    return row.get('rsi_exit', False)

def exit_rsi_or_breadth75(row, prev):
    return row.get('rsi_exit', False) or row['breadth'] > 75

# --- S1: Breadth Zone ---
def entry_breadth_zone(row, prev):
    return row['breadth'] < 25

# --- S2: Buffett Zone ---
def entry_buffett_zone(row, prev):
    return row['buff_z'] < -1

# --- S3: OI Trend ---
def entry_oi_trend(row, prev):
    return row['oi_momentum'] > 0

def exit_oi_reverse(row, prev):
    return row.get('rsi_exit', False) or row['oi_momentum'] < 0

# --- S4: Breadth + OI Confirmation ---
def entry_breadth_oi(row, prev):
    return row['breadth'] < 25 and row['oi_momentum'] > 0

# --- S5: Buffett + OI Confirmation ---
def entry_buffett_oi(row, prev):
    return row['buff_z'] < -1 and row['oi_momentum'] > 0

# --- S6: Composite Score >= 2 ---
def entry_composite2(row, prev):
    score = 0
    if row['breadth'] < 25: score += 1
    if row['buff_z'] < -1: score += 1
    if row['oi_momentum'] > 0: score += 1
    fear = row.get('fear', np.nan)
    if not np.isnan(fear) and fear > 75: score += 1
    return score >= 2

def exit_composite0(row, prev):
    if row.get('rsi_exit', False):
        return True
    score = 0
    if row['breadth'] < 25: score += 1
    if row['buff_z'] < -1: score += 1
    if row['oi_momentum'] > 0: score += 1
    fear = row.get('fear', np.nan)
    if not np.isnan(fear) and fear > 75: score += 1
    return score == 0

# --- S7: Composite Score >= 3 (строгий) ---
def entry_composite3(row, prev):
    score = 0
    if row['breadth'] < 25: score += 1
    if row['buff_z'] < -1: score += 1
    if row['oi_momentum'] > 0: score += 1
    fear = row.get('fear', np.nan)
    if not np.isnan(fear) and fear > 75: score += 1
    return score >= 3

strategies = [
    (entry_breadth_zone, exit_rsi_or_breadth75, 'Breadth <25'),
    (entry_buffett_zone, exit_rsi_or_breadth75, 'Buffett Z<-1'),
    (entry_oi_trend,     exit_oi_reverse,       'OI Trend'),
    (entry_breadth_oi,   exit_rsi_or_breadth75, 'Breadth + OI'),
    (entry_buffett_oi,   exit_rsi_or_breadth75, 'Buffett + OI'),
    (entry_composite2,   exit_composite0,       'Composite >=2'),
    (entry_composite3,   exit_composite0,       'Composite >=3'),
    (entry_breadth_zone, exit_rsi,              'Breadth (RSI exit)'),
    (entry_buffett_zone, exit_rsi,              'Buffett (RSI exit)'),
]

# ══════════════════════════════════════════════
# 5. ЗАПУСК НА ПОЛНОМ ПЕРИОДЕ
# ══════════════════════════════════════════════

print("\n" + "=" * 120)
print("БЭКТЕСТ — ПОЛНЫЙ ПЕРИОД")
print(f"  {df.index[0].date()} — {df.index[-1].date()} | Slippage: {SLIPPAGE*100:.1f}% на сделку")
print("=" * 120)

header = f"{'Стратегия':<22s} {'Return%':>8s} {'Ann%':>6s} {'B&H%':>7s} {'MaxDD%':>7s} {'Sharpe':>7s} {'InMkt%':>7s} {'Trades':>7s} {'Win%':>6s} {'AvgPnL%':>8s} {'AvgWin':>7s} {'AvgLoss':>8s} {'AvgDays':>8s}"
print(header)
print("-" * len(header))

results = []
for entry_fn, exit_fn, name in strategies:
    r = backtest_zone(df, entry_fn, exit_fn, name)
    results.append(r)
    print(f"{r['name']:<22s} {r['total_return']:>8.1f} {r['ann_return']:>6.1f} {r['buy_hold']:>7.1f} {r['max_dd']:>7.1f} {r['sharpe']:>7.2f} {r['time_in_market']:>7.1f} {r['n_trades']:>7d} {r['win_rate']:>6.0f} {r['avg_pnl']:>8.1f} {r['avg_win']:>7.1f} {r['avg_loss']:>8.1f} {r['avg_days']:>8.0f}")

# ══════════════════════════════════════════════
# 6. РАЗБИВКА ПО ПЕРИОДАМ (проверка устойчивости)
# ══════════════════════════════════════════════

mid_date = df.index[len(df)//2]
print(f"\n{'=' * 120}")
print(f"ПРОВЕРКА УСТОЙЧИВОСТИ — два периода")
print(f"  Период 1: {df.index[0].date()} — {mid_date.date()} ({len(df)//2} баров)")
print(f"  Период 2: {mid_date.date()} — {df.index[-1].date()} ({len(df) - len(df)//2} баров)")
print(f"{'=' * 120}")

for period_name, period_df in [("Период 1", df.iloc[:len(df)//2]), ("Период 2", df.iloc[len(df)//2:])]:
    if len(period_df) < 30:
        continue
    print(f"\n  --- {period_name}: {period_df.index[0].date()} — {period_df.index[-1].date()} ---")
    bh = (period_df.iloc[-1]['close'] / period_df.iloc[0]['close'] - 1) * 100
    print(f"  {'B&H':22s} {bh:>8.1f}%")
    for entry_fn, exit_fn, name in strategies:
        r = backtest_zone(period_df, entry_fn, exit_fn, name)
        marker = " *" if r['total_return'] > bh else ""
        print(f"  {name:22s} {r['total_return']:>8.1f}% | DD {r['max_dd']:>6.1f}% | {r['n_trades']} trades | Win {r['win_rate']:.0f}%{marker}")

# ══════════════════════════════════════════════
# 7. СДЕЛКИ ЛУЧШИХ СТРАТЕГИЙ
# ══════════════════════════════════════════════

# Сортируем по Sharpe
ranked = sorted(results, key=lambda x: x['sharpe'], reverse=True)

print(f"\n{'=' * 120}")
print("СДЕЛКИ TOP-3 СТРАТЕГИЙ (по Sharpe)")
print(f"{'=' * 120}")

for r in ranked[:3]:
    if len(r['trades']) > 0:
        print(f"\n--- {r['name']} (Sharpe={r['sharpe']:.2f}, Return={r['total_return']:.1f}%, {r['n_trades']} сделок) ---")
        for _, t in r['trades'].iterrows():
            arrow = '+' if t['pnl_pct'] > 0 else ''
            print(f"  {t['entry_date'].strftime('%Y-%m-%d')} -> {t['exit_date'].strftime('%Y-%m-%d')}  "
                  f"{t['entry_price']:>10.1f} -> {t['exit_price']:>10.1f}  "
                  f"{arrow}{t['pnl_pct']:.1f}%  ({int(t['days'])}d)")

# ══════════════════════════════════════════════
# 8. ТЕКУЩИЕ СИГНАЛЫ
# ══════════════════════════════════════════════

print(f"\n{'=' * 120}")
print(f"ТЕКУЩЕЕ СОСТОЯНИЕ ИНДИКАТОРОВ ({df.index[-1].date()})")
print(f"{'=' * 120}")

last = df.iloc[-1]
print(f"  Цена:           {last['close']:.1f}")
print(f"  RSI(14):        {last['rsi']:.1f}  (MA: {last['rsi_ma']:.1f})")
print(f"  Breadth:        {last['breadth']:.1f}%  {'<-- ЗОНА ВХОДА' if last['breadth'] < 25 else ''}")
print(f"  Buffett Z:      {last['buff_z']:.2f}  {'<-- ЗОНА ВХОДА' if last['buff_z'] < -1 else ''}")
print(f"  OI momentum:    {last['oi_momentum']:.1f}  {'(юрлица наращивают)' if last['oi_momentum'] > 0 else '(юрлица сокращают)'}")
fear_val = last.get('fear', np.nan)
if not np.isnan(fear_val):
    print(f"  Fear:           {fear_val:.1f}  {'<-- КОНТРАРИАН БАЙ' if fear_val > 75 else ''}")

# Composite score
score = 0
if last['breadth'] < 25: score += 1
if last['buff_z'] < -1: score += 1
if last['oi_momentum'] > 0: score += 1
if not np.isnan(fear_val) and fear_val > 75: score += 1
print(f"  Composite:      {score}/4")

# Текущий сигнал лучшей стратегии
best = ranked[0]
print(f"\n  Лучшая стратегия: {best['name']} (Sharpe {best['sharpe']:.2f})")
