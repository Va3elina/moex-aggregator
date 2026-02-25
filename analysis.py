"""
Анализ поведения цены IMOEXF относительно каждого индикатора.
Без заглядывания в будущее — только forward returns после состояния индикатора.
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
pd.set_option('future.no_silent_downcasting', True)

# ══════════════════════════════════════════════
# ЗАГРУЗКА ДАННЫХ
# ══════════════════════════════════════════════

def load_data():
    # IMOEXF свечи
    q = text("SELECT begin_time::date as date, open, high, low, close, volume FROM candles WHERE sec_id = 'IMOEXF' AND interval = 24 ORDER BY begin_time")
    candles = pd.read_sql(q, engine, parse_dates=['date']).drop_duplicates('date', keep='last').set_index('date')

    # OI юрлиц
    q = text("SELECT DISTINCT ON (tradedate) tradedate as date, pos as oi FROM open_interest WHERE sectype = 'IMOEXF' AND clgroup = 'YUR' AND interval = 24 ORDER BY tradedate ASC, tradetime DESC")
    oi = pd.read_sql(q, engine, parse_dates=['date']).set_index('date')

    # Акции для breadth
    stocks_list = ['SBER','GAZP','LKOH','GMKN','ROSN','YDEX','NVTK','MTSS','MGNT','VTBR','CHMF','NLMK','PLZL','TATN','ALRS']
    frames = {}
    for s in stocks_list:
        q = text("SELECT begin_time::date as date, close FROM candles WHERE sec_id = :s AND interval = 24 ORDER BY begin_time")
        d = pd.read_sql(q, engine, params={'s': s}, parse_dates=['date']).drop_duplicates('date', keep='last').set_index('date')
        frames[s] = d['close']
    stocks = pd.DataFrame(frames)

    # Индексы
    def load_idx(secid):
        q = text("SELECT trade_date as date, close FROM index_data WHERE secid = :s ORDER BY trade_date")
        d = pd.read_sql(q, engine, params={'s': secid}, parse_dates=['date']).drop_duplicates('date', keep='last').set_index('date')
        return d['close']

    # Фонды
    def load_fund(ticker):
        q = text("SELECT fd.trade_date as date, fd.pay as nav FROM fund_data fd JOIN funds f ON f.fund_id = fd.fund_id WHERE f.ticker = :t ORDER BY fd.trade_date")
        d = pd.read_sql(q, engine, params={'t': ticker}, parse_dates=['date']).drop_duplicates('date', keep='last').set_index('date')
        return d['nav'].astype(float)

    return candles, oi, stocks, load_idx('MCFTR'), load_idx('RGBITR'), load_fund('LQDT'), load_fund('TMOS')

print("Загрузка данных...")
candles, oi_data, stocks, mcftr, rgbitr, lqdt_nav, tmos_nav = load_data()

# ══════════════════════════════════════════════
# РАСЧЁТ ИНДИКАТОРОВ (без look-ahead)
# ══════════════════════════════════════════════

df = candles.copy()

# Forward returns (то что мы анализируем)
for d in [5, 10, 20, 40, 60]:
    df[f'fwd_{d}d'] = df['close'].shift(-d) / df['close'] - 1

# --- 1. OI юрлиц ---
df = df.join(oi_data, how='left')
df['oi'] = df['oi'].ffill()
df['oi_ema20'] = df['oi'].ewm(span=20, adjust=False).mean()
df['oi_ema60'] = df['oi'].ewm(span=60, adjust=False).mean()
df['oi_momentum'] = df['oi_ema20'] - df['oi_ema60']
df['oi_regime'] = np.where(df['oi_ema20'] > df['oi_ema60'], 'OI_UP', 'OI_DOWN')
# Также разделим по знаку OI
df['oi_sign'] = np.where(df['oi'] > 0, 'OI_POS', 'OI_NEG')

# --- 2. Breadth ---
ba = stocks.reindex(df.index).ffill().infer_objects(copy=False)
for col in ba.columns:
    ema200 = ba[col].ewm(span=200, adjust=False).mean()
    ba[col] = (ba[col] > ema200).astype(float)
df['breadth'] = ba.mean(axis=1) * 100
df['breadth_ema'] = df['breadth'].ewm(span=10, adjust=False).mean()
# Зоны
df['breadth_zone'] = pd.cut(df['breadth_ema'], bins=[0, 20, 40, 60, 80, 100],
                             labels=['0-20', '20-40', '40-60', '60-80', '80-100'], include_lowest=True)

# --- 3. Баффетт ---
mcftr_a = mcftr.reindex(df.index).ffill()
rgbitr_a = rgbitr.reindex(df.index).ffill()
ratio = mcftr_a / rgbitr_a
ratio_ema = ratio.ewm(span=60, adjust=False).mean()
ratio_mean = ratio_ema.rolling(252, min_periods=60).mean()
ratio_std = ratio_ema.rolling(252, min_periods=60).std()
df['buff_z'] = np.where(ratio_std > 0, (ratio_ema - ratio_mean) / ratio_std, 0)
df['buff_zone'] = pd.cut(df['buff_z'], bins=[-np.inf, -1.5, -1, -0.5, 0, 0.5, 1, 1.5, np.inf],
                          labels=['<-1.5', '-1.5/-1', '-1/-0.5', '-0.5/0', '0/0.5', '0.5/1', '1/1.5', '>1.5'])

# --- 4. Fund Fear ---
lqdt_a = lqdt_nav.reindex(df.index).ffill()
tmos_a = tmos_nav.reindex(df.index).ffill()
mm_roc = lqdt_a.pct_change(20) * 100
stock_roc = tmos_a.pct_change(20) * 100
rotation = mm_roc - stock_roc
# percentrank через expanding (без look-ahead)
df['fear'] = rotation.expanding(min_periods=60).apply(
    lambda x: pd.Series(x).rank(pct=True).iloc[-1] * 100, raw=False
).ewm(span=10, adjust=False).mean()
df['fear_zone'] = pd.cut(df['fear'], bins=[0, 25, 50, 75, 100],
                          labels=['0-25 Greed', '25-50 Calm', '50-75 Worry', '75-100 Fear'], include_lowest=True)

# --- 5. RSI ---
delta = df['close'].diff()
gain = delta.clip(lower=0)
loss = (-delta).clip(lower=0)
avg_gain = gain.ewm(alpha=1/14, min_periods=14, adjust=False).mean()
avg_loss = loss.ewm(alpha=1/14, min_periods=14, adjust=False).mean()
rs = avg_gain / avg_loss
df['rsi'] = 100 - (100 / (1 + rs))
df['rsi_ma'] = df['rsi'].rolling(14).mean()
df['rsi_zone'] = pd.cut(df['rsi'], bins=[0, 30, 50, 70, 100],
                          labels=['0-30', '30-50', '50-70', '70-100'], include_lowest=True)

# ══════════════════════════════════════════════
# АНАЛИЗ
# ══════════════════════════════════════════════

def analyze_zones(df, zone_col, name, horizons=[5, 10, 20, 40, 60]):
    """Средние forward returns по зонам индикатора"""
    print(f"\n{'='*70}")
    print(f"  {name}")
    print(f"{'='*70}")

    valid = df.dropna(subset=[zone_col] + [f'fwd_{h}d' for h in horizons])
    if len(valid) == 0:
        print("  Нет данных")
        return

    header = f"  {'Зона':<18s} {'N':>5s}"
    for h in horizons:
        header += f" {'fwd'+str(h)+'d':>8s}"
    print(header)
    print("  " + "-" * (24 + 9 * len(horizons)))

    for zone in valid[zone_col].cat.categories if hasattr(valid[zone_col], 'cat') else valid[zone_col].unique():
        subset = valid[valid[zone_col] == zone]
        if len(subset) < 3:
            continue
        row = f"  {str(zone):<18s} {len(subset):>5d}"
        for h in horizons:
            mean_ret = subset[f'fwd_{h}d'].mean() * 100
            row += f" {mean_ret:>+7.1f}%"
        print(row)

    # Общая
    row = f"  {'ВСЕГО':<18s} {len(valid):>5d}"
    for h in horizons:
        mean_ret = valid[f'fwd_{h}d'].mean() * 100
        row += f" {mean_ret:>+7.1f}%"
    print(row)


def analyze_binary(df, cond_col, name, horizons=[5, 10, 20, 40, 60]):
    """Forward returns когда условие True vs False"""
    print(f"\n{'='*70}")
    print(f"  {name}")
    print(f"{'='*70}")

    valid = df.dropna(subset=[f'fwd_{h}d' for h in horizons])
    if len(valid) == 0:
        print("  Нет данных")
        return

    header = f"  {'Состояние':<18s} {'N':>5s}"
    for h in horizons:
        header += f" {'fwd'+str(h)+'d':>8s}"
    print(header)
    print("  " + "-" * (24 + 9 * len(horizons)))

    for label, mask in [("TRUE", valid[cond_col] == True), ("FALSE", valid[cond_col] == False)]:
        subset = valid[mask]
        if len(subset) < 3:
            continue
        row = f"  {label:<18s} {len(subset):>5d}"
        for h in horizons:
            mean_ret = subset[f'fwd_{h}d'].mean() * 100
            row += f" {mean_ret:>+7.1f}%"
        print(row)


def analyze_correlation(df, ind_col, name, horizons=[5, 10, 20, 40, 60]):
    """Корреляция индикатора с forward returns"""
    print(f"\n  Корреляция {name} с forward returns:")
    valid = df.dropna(subset=[ind_col] + [f'fwd_{h}d' for h in horizons])
    for h in horizons:
        corr = valid[ind_col].corr(valid[f'fwd_{h}d'])
        bar = '+' * int(abs(corr) * 20)
        sign = '+' if corr > 0 else '-'
        print(f"    fwd_{h:>2d}d: {corr:>+.3f}  {sign}{bar}")


# ══════════════════════════════════════════════
# ОТЧЁТ
# ══════════════════════════════════════════════

print(f"\nПериод: {df.index[0].date()} — {df.index[-1].date()} ({len(df)} баров)")
print(f"Цена: {df.iloc[0]['close']:.0f} -> {df.iloc[-1]['close']:.0f} ({(df.iloc[-1]['close']/df.iloc[0]['close']-1)*100:+.1f}%)")

# 1. OI
analyze_binary(df, 'oi_regime', "OI Юрлиц: EMA20 > EMA60 (наращивают)")
analyze_binary(df, 'oi_sign', "OI Юрлиц: позиция > 0")
analyze_correlation(df, 'oi_momentum', "OI momentum (EMA20-EMA60)")
analyze_correlation(df, 'oi', "OI raw")

# 2. Breadth
analyze_zones(df, 'breadth_zone', "Сила рынка (Breadth): зоны")
analyze_correlation(df, 'breadth_ema', "Breadth EMA")

# 3. Баффетт
analyze_zones(df, 'buff_zone', "Баффетт Z-Score: зоны")
analyze_correlation(df, 'buff_z', "Buffett Z-Score")

# 4. Fund Fear
analyze_zones(df, 'fear_zone', "Fund Fear: зоны")
analyze_correlation(df, 'fear', "Fear index")

# 5. RSI
analyze_zones(df, 'rsi_zone', "RSI(14): зоны")
analyze_correlation(df, 'rsi', "RSI")

# ══════════════════════════════════════════════
# ДОПОЛНИТЕЛЬНО: анализ переходов (crossovers)
# ══════════════════════════════════════════════

print(f"\n{'='*70}")
print(f"  СОБЫТИЯ (crossovers) — forward returns после события")
print(f"{'='*70}")

events = {
    'OI: EMA20 cross UP EMA60': (df['oi_ema20'].shift(1) <= df['oi_ema60'].shift(1)) & (df['oi_ema20'] > df['oi_ema60']),
    'OI: EMA20 cross DN EMA60': (df['oi_ema20'].shift(1) >= df['oi_ema60'].shift(1)) & (df['oi_ema20'] < df['oi_ema60']),
    'OI: cross zero UP':         (df['oi'].shift(1) <= 0) & (df['oi'] > 0),
    'OI: cross zero DN':         (df['oi'].shift(1) >= 0) & (df['oi'] < 0),
    'Breadth: cross 25 UP':     (df['breadth_ema'].shift(1) <= 25) & (df['breadth_ema'] > 25),
    'Breadth: cross 25 DN':     (df['breadth_ema'].shift(1) >= 25) & (df['breadth_ema'] < 25),
    'Breadth: cross 75 UP':     (df['breadth_ema'].shift(1) <= 75) & (df['breadth_ema'] > 75),
    'Breadth: cross 75 DN':     (df['breadth_ema'].shift(1) >= 75) & (df['breadth_ema'] < 75),
    'Buffett: cross -1 UP':     (df['buff_z'].shift(1) <= -1) & (df['buff_z'] > -1),
    'Buffett: cross 0 UP':      (df['buff_z'].shift(1) <= 0) & (df['buff_z'] > 0),
    'Buffett: cross 1.5 UP':    (df['buff_z'].shift(1) <= 1.5) & (df['buff_z'] > 1.5),
    'Fear: cross 75 UP':        (df['fear'].shift(1) <= 75) & (df['fear'] > 75),
    'Fear: cross 25 DN':        (df['fear'].shift(1) >= 25) & (df['fear'] < 25),
    'RSI: cross 30 UP':         (df['rsi'].shift(1) <= 30) & (df['rsi'] > 30),
    'RSI: cross 70 UP':         (df['rsi'].shift(1) <= 70) & (df['rsi'] > 70),
    'RSI: cross 70 DN':         (df['rsi'].shift(1) >= 70) & (df['rsi'] < 70),
    'RSI: cross MA UP':         (df['rsi'].shift(1) <= df['rsi_ma'].shift(1)) & (df['rsi'] > df['rsi_ma']),
    'RSI: cross MA DN':         (df['rsi'].shift(1) >= df['rsi_ma'].shift(1)) & (df['rsi'] < df['rsi_ma']),
}

horizons = [5, 10, 20, 40, 60]
header = f"  {'Событие':<30s} {'N':>4s}"
for h in horizons:
    header += f" {'fwd'+str(h)+'d':>8s}"
print(header)
print("  " + "-" * (35 + 9 * len(horizons)))

for name, mask in events.items():
    valid = df[mask].dropna(subset=[f'fwd_{h}d' for h in horizons])
    n = len(valid)
    if n == 0:
        row = f"  {name:<30s} {0:>4d}    —"
    else:
        row = f"  {name:<30s} {n:>4d}"
        for h in horizons:
            mean_ret = valid[f'fwd_{h}d'].mean() * 100
            row += f" {mean_ret:>+7.1f}%"
    print(row)

# Buy & Hold reference
valid_bh = df.dropna(subset=[f'fwd_{h}d' for h in horizons])
row = f"  {'(Buy & Hold средний)':<30s} {len(valid_bh):>4d}"
for h in horizons:
    mean_ret = valid_bh[f'fwd_{h}d'].mean() * 100
    row += f" {mean_ret:>+7.1f}%"
print(row)
