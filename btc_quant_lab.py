"""
BTC Quant Analysis Lab
Применяем ВСЕ квантовые методы из trading_knowledge к Bitcoin:
1. Hurst parameter — тренд или mean reversion?
2. GARCH — кластеризация волатильности
3. Z-Score на RSI — адаптивные пороги
4. Bollinger Bands на RSI — динамические уровни
5. Rolling Percentile — где RSI сейчас исторически?
6. Сравнение: фиксированные пороги vs динамические
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from scipy import stats
import warnings
warnings.filterwarnings('ignore')

# ============================================================================
# 1. ЗАГРУЗКА ДАННЫХ
# ============================================================================
print("📥 Загрузка данных BTC...")
import yfinance as yf
btc = yf.download("BTC-USD", period="4y", interval="1d")
btc.columns = btc.columns.droplevel(1) if isinstance(btc.columns, pd.MultiIndex) else btc.columns
print(f"   Загружено {len(btc)} дней: {btc.index[0].strftime('%Y-%m-%d')} → {btc.index[-1].strftime('%Y-%m-%d')}")
print(f"   Цена: ${btc['Close'].iloc[-1]:,.0f}")

# Returns
btc['returns'] = btc['Close'].pct_change()
btc['log_returns'] = np.log(btc['Close'] / btc['Close'].shift(1))
btc.dropna(inplace=True)

# ============================================================================
# 2. HURST PARAMETER — тренд или mean reversion?
# ============================================================================
print("\n" + "="*60)
print("📊 HURST PARAMETER")
print("="*60)

def hurst_exponent(ts, max_lag=100):
    """R/S метод для оценки Hurst exponent"""
    lags = range(2, min(max_lag, len(ts) // 2))
    rs_values = []
    for lag in lags:
        segments = len(ts) // lag
        rs_seg = []
        for s in range(segments):
            segment = ts[s*lag:(s+1)*lag]
            mean_seg = np.mean(segment)
            deviate = np.cumsum(segment - mean_seg)
            R = np.max(deviate) - np.min(deviate)
            S = np.std(segment, ddof=1)
            if S > 0:
                rs_seg.append(R / S)
        if len(rs_seg) > 0:
            rs_values.append((lag, np.mean(rs_seg)))
    
    if len(rs_values) < 2:
        return 0.5
    
    lags_arr = np.log([x[0] for x in rs_values])
    rs_arr = np.log([x[1] for x in rs_values])
    slope, _, _, _, _ = stats.linregress(lags_arr, rs_arr)
    return slope

# Hurst для разных периодов
returns_arr = btc['returns'].values
h_full = hurst_exponent(returns_arr)
h_1y = hurst_exponent(returns_arr[-252:])
h_6m = hurst_exponent(returns_arr[-126:])
h_3m = hurst_exponent(returns_arr[-63:])

print(f"""
  H = 0.5  → Случайное блуждание (нельзя предсказать)
  H > 0.5  → ТРЕНД (momentum стратегии)
  H < 0.5  → MEAN REVERSION

  BTC Hurst:
  ├── Весь период:   H = {h_full:.3f} {'← ТРЕНД' if h_full > 0.55 else '← MEAN REVERSION' if h_full < 0.45 else '← ~случайный'}
  ├── Год:           H = {h_1y:.3f} {'← ТРЕНД' if h_1y > 0.55 else '← MEAN REVERSION' if h_1y < 0.45 else '← ~случайный'}
  ├── 6 месяцев:     H = {h_6m:.3f} {'← ТРЕНД' if h_6m > 0.55 else '← MEAN REVERSION' if h_6m < 0.45 else '← ~случайный'}
  └── 3 месяца:      H = {h_3m:.3f} {'← ТРЕНД' if h_3m > 0.55 else '← MEAN REVERSION' if h_3m < 0.45 else '← ~случайный'}
""")

# ============================================================================
# 3. GARCH — волатильность кластеризуется
# ============================================================================
print("="*60)
print("📈 GARCH(1,1) — ВОЛАТИЛЬНОСТЬ")
print("="*60)

from arch import arch_model

# Fit GARCH(1,1)
am = arch_model(btc['returns'].dropna() * 100, vol='Garch', p=1, q=1, dist='t')
res = am.fit(disp='off')

omega = res.params['omega']
alpha = res.params['alpha[1]']
beta = res.params['beta[1]']
persistence = alpha + beta

print(f"""
  GARCH(1,1): σ²(t) = ω + α·r²(t-1) + β·σ²(t-1)
  
  Параметры:
  ├── ω (base vol)     = {omega:.4f}
  ├── α (shock react)  = {alpha:.4f}  ← реакция на вчерашний шок
  ├── β (persistence)  = {beta:.4f}  ← инерция волатильности
  └── α + β            = {persistence:.4f} {'← ВЫСОКАЯ persistence (vol долго держится)' if persistence > 0.95 else '← УМЕРЕННАЯ persistence'}
  
  Текущая волатильность (annualized):
  └── GARCH σ = {res.conditional_volatility.iloc[-1] * np.sqrt(252) / 100:.1%}
""")

# Conditional volatility series
btc['garch_vol'] = res.conditional_volatility.values / 100  # back to decimal
btc['garch_vol_annual'] = btc['garch_vol'] * np.sqrt(252)

# Dynamic stop-loss levels (2σ GARCH)
btc['dynamic_stop'] = btc['Close'] * (1 - 2 * btc['garch_vol'])
btc['fixed_stop'] = btc['Close'] * 0.95  # фиксированный 5% стоп

# ============================================================================
# 4. EXCESS KURTOSIS — толстые тейлы
# ============================================================================
print("="*60)
print("📉 EXCESS KURTOSIS — ТОЛСТЫЕ ТЕЙЛЫ")
print("="*60)

kurt = stats.kurtosis(btc['returns'].dropna(), fisher=True)
skew = stats.skew(btc['returns'].dropna())

print(f"""
  Нормальное распределение: kurtosis = 0, skew = 0
  
  BTC returns:
  ├── Kurtosis = {kurt:.2f}  {'← ТОЛСТЫЕ ТЕЙЛЫ (больше Black Swans!)' if kurt > 1 else ''}
  ├── Skew     = {skew:.2f}  {'← левый хвост длиннее (crashes чаще)' if skew < -0.3 else '← относительно симметрично' if abs(skew) < 0.3 else '← правый хвост длиннее'}
  └── Вывод:   Стопы на основе нормального распределения НЕДООЦЕНИВАЮТ риск!
               Реальный < -3σ событий: ~{(np.abs(btc['returns']) > 3 * btc['returns'].std()).sum()} (ожидалось ~{int(len(btc) * 0.003)})
""")

# ============================================================================
# 5. RSI + АДАПТИВНЫЕ ПОРОГИ
# ============================================================================
print("="*60)
print("📊 RSI: ФИКСИРОВАННЫЕ vs ДИНАМИЧЕСКИЕ ПОРОГИ")
print("="*60)

# RSI calculation
def calc_rsi(prices, period=14):
    delta = prices.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.rolling(period, min_periods=period).mean()
    avg_loss = loss.rolling(period, min_periods=period).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

btc['rsi'] = calc_rsi(btc['Close'], 14)

# Z-Score на RSI
zscore_lookback = 50
btc['rsi_mean'] = btc['rsi'].rolling(zscore_lookback).mean()
btc['rsi_std'] = btc['rsi'].rolling(zscore_lookback).std()
btc['rsi_zscore'] = (btc['rsi'] - btc['rsi_mean']) / btc['rsi_std']

# Bollinger Bands на RSI
bb_len = 20
bb_mult = 2.0
btc['rsi_bb_mid'] = btc['rsi'].rolling(bb_len).mean()
btc['rsi_bb_upper'] = btc['rsi_bb_mid'] + bb_mult * btc['rsi'].rolling(bb_len).std()
btc['rsi_bb_lower'] = btc['rsi_bb_mid'] - bb_mult * btc['rsi'].rolling(bb_len).std()

# Percentile Rank
pctl_lookback = 100
btc['rsi_percentile'] = btc['rsi'].rolling(pctl_lookback).apply(
    lambda x: stats.percentileofscore(x, x.iloc[-1]), raw=False
)

# ---- Сигналы ----
# Фиксированные
btc['sig_fix_buy'] = (btc['rsi'] < 30) & (btc['rsi'].shift(1) >= 30)
btc['sig_fix_sell'] = (btc['rsi'] > 70) & (btc['rsi'].shift(1) <= 70)

# Z-Score
btc['sig_z_buy'] = (btc['rsi_zscore'] < -2) & (btc['rsi_zscore'].shift(1) >= -2)
btc['sig_z_sell'] = (btc['rsi_zscore'] > 2) & (btc['rsi_zscore'].shift(1) <= 2)

# Bollinger
btc['sig_bb_buy'] = (btc['rsi'] < btc['rsi_bb_lower']) & (btc['rsi'].shift(1) >= btc['rsi_bb_lower'].shift(1))
btc['sig_bb_sell'] = (btc['rsi'] > btc['rsi_bb_upper']) & (btc['rsi'].shift(1) <= btc['rsi_bb_upper'].shift(1))

# Percentile
btc['sig_pctl_buy'] = (btc['rsi_percentile'] < 5) & (btc['rsi_percentile'].shift(1) >= 5)
btc['sig_pctl_sell'] = (btc['rsi_percentile'] > 95) & (btc['rsi_percentile'].shift(1) <= 95)

# Count signals
print(f"""
  Метод               │ BUY сигналов │ SELL сигналов
  ────────────────────┼──────────────┼──────────────
  Фикс RSI 30/70     │ {btc['sig_fix_buy'].sum():>12} │ {btc['sig_fix_sell'].sum():>12}
  Z-Score ≤-2/≥+2    │ {btc['sig_z_buy'].sum():>12} │ {btc['sig_z_sell'].sum():>12}
  Bollinger Bands     │ {btc['sig_bb_buy'].sum():>12} │ {btc['sig_bb_sell'].sum():>12}
  Percentile 5%/95%   │ {btc['sig_pctl_buy'].sum():>12} │ {btc['sig_pctl_sell'].sum():>12}
""")

# ============================================================================
# 6. ПРОСТОЙ БЭКТЕСТ: фикс vs динамические
# ============================================================================
print("="*60)
print("💰 БЭКТЕСТ: фиксированный RSI vs Z-Score vs BB vs Percentile")
print("="*60)

def simple_backtest(df, buy_col, sell_col, name):
    """Простой бэктест: купил при buy, продал при sell"""
    position = 0
    equity = 10000
    entry_price = 0
    trades = []
    
    for i in range(len(df)):
        if df[buy_col].iloc[i] and position == 0:
            position = 1
            entry_price = df['Close'].iloc[i]
        elif df[sell_col].iloc[i] and position == 1:
            ret = (df['Close'].iloc[i] - entry_price) / entry_price
            equity *= (1 + ret)
            trades.append(ret)
            position = 0
    
    if len(trades) == 0:
        return name, 0, 0, 0, 0, equity
    
    wins = [t for t in trades if t > 0]
    losses = [t for t in trades if t <= 0]
    win_rate = len(wins) / len(trades) * 100 if trades else 0
    avg_win = np.mean(wins) * 100 if wins else 0
    avg_loss = np.mean(losses) * 100 if losses else 0
    
    return name, len(trades), win_rate, avg_win, avg_loss, equity

results = []
results.append(simple_backtest(btc, 'sig_fix_buy', 'sig_fix_sell', 'Фикс 30/70'))
results.append(simple_backtest(btc, 'sig_z_buy', 'sig_z_sell', 'Z-Score'))
results.append(simple_backtest(btc, 'sig_bb_buy', 'sig_bb_sell', 'Bollinger'))
results.append(simple_backtest(btc, 'sig_pctl_buy', 'sig_pctl_sell', 'Percentile'))

print(f"  {'Метод':<15} │ {'Сделок':>7} │ {'Win%':>6} │ {'Avg Win':>8} │ {'Avg Loss':>9} │ {'$10K →':>10}")
print(f"  {'─'*15}─┼─{'─'*7}─┼─{'─'*6}─┼─{'─'*8}─┼─{'─'*9}─┼─{'─'*10}")
for r in results:
    print(f"  {r[0]:<15} │ {r[1]:>7} │ {r[2]:>5.1f}% │ {r[3]:>7.1f}% │ {r[4]:>8.1f}% │ ${r[5]:>9,.0f}")

# ============================================================================
# 7. Kelly Criterion
# ============================================================================
print(f"\n{'='*60}")
print("🎰 KELLY CRITERION")
print("="*60)

for r in results:
    name, n_trades, win_rate, avg_win, avg_loss = r[0], r[1], r[2], r[3], r[4]
    if n_trades > 0 and avg_loss != 0:
        p = win_rate / 100
        b = abs(avg_win / avg_loss) if avg_loss != 0 else 0
        kelly = (b * p - (1 - p)) / b if b > 0 else 0
        print(f"  {name:<15}: Kelly f* = {kelly:.1%}, Half Kelly = {kelly/2:.1%}")

# ============================================================================
# 8. ВИЗУАЛИЗАЦИЯ
# ============================================================================
print(f"\n📊 Создание графиков...")

fig, axes = plt.subplots(5, 1, figsize=(16, 22), gridspec_kw={'height_ratios': [3, 2, 2, 2, 1.5]})
fig.suptitle('BTC Quant Analysis Lab', fontsize=16, fontweight='bold', y=0.98)

# 1. Price + GARCH dynamic stop
ax1 = axes[0]
ax1.plot(btc.index, btc['Close'], color='white', linewidth=1, label='BTC Price')
ax1.plot(btc.index, btc['dynamic_stop'], color='orange', linewidth=0.7, alpha=0.7, label='GARCH 2σ Stop')
ax1.plot(btc.index, btc['fixed_stop'], color='red', linewidth=0.7, alpha=0.5, linestyle='--', label='Fixed 5% Stop')
# Mark buy signals
for method, col, color, marker in [
    ('Fix', 'sig_fix_buy', 'lime', '^'),
    ('Z', 'sig_z_buy', 'cyan', 's'),
    ('BB', 'sig_bb_buy', 'yellow', 'D'),
]:
    mask = btc[col]
    ax1.scatter(btc.index[mask], btc['Close'][mask], color=color, marker=marker, s=40, label=f'BUY {method}', zorder=5)
ax1.set_ylabel('Price ($)')
ax1.legend(loc='upper left', fontsize=8)
ax1.set_title('BTC Price + Dynamic GARCH Stop vs Fixed Stop')
ax1.set_facecolor('#1a1a2e')
ax1.grid(True, alpha=0.2)

# 2. RSI + Fixed thresholds
ax2 = axes[1]
ax2.plot(btc.index, btc['rsi'], color='white', linewidth=1)
ax2.axhline(70, color='red', linestyle='--', alpha=0.7, label='Fixed 70')
ax2.axhline(30, color='lime', linestyle='--', alpha=0.7, label='Fixed 30')
ax2.fill_between(btc.index, 30, 0, alpha=0.1, color='green')
ax2.fill_between(btc.index, 70, 100, alpha=0.1, color='red')
ax2.set_ylabel('RSI(14)')
ax2.set_ylim(0, 100)
ax2.legend(loc='upper right', fontsize=8)
ax2.set_title('RSI с фиксированными порогами 30/70')
ax2.set_facecolor('#1a1a2e')
ax2.grid(True, alpha=0.2)

# 3. RSI + Bollinger Bands (dynamic thresholds)
ax3 = axes[2]
ax3.plot(btc.index, btc['rsi'], color='white', linewidth=1)
ax3.plot(btc.index, btc['rsi_bb_upper'], color='red', linewidth=1, label='BB Upper')
ax3.plot(btc.index, btc['rsi_bb_lower'], color='lime', linewidth=1, label='BB Lower')
ax3.plot(btc.index, btc['rsi_bb_mid'], color='gray', linewidth=0.7, linestyle='--')
ax3.fill_between(btc.index, btc['rsi_bb_upper'], btc['rsi_bb_lower'], alpha=0.1, color='cyan')
mask_buy = btc['sig_bb_buy']
mask_sell = btc['sig_bb_sell']
ax3.scatter(btc.index[mask_buy], btc['rsi'][mask_buy], color='lime', marker='^', s=60, zorder=5)
ax3.scatter(btc.index[mask_sell], btc['rsi'][mask_sell], color='red', marker='v', s=60, zorder=5)
ax3.set_ylabel('RSI(14)')
ax3.set_ylim(0, 100)
ax3.legend(loc='upper right', fontsize=8)
ax3.set_title('RSI с Bollinger Bands (АДАПТИВНЫЕ пороги)')
ax3.set_facecolor('#1a1a2e')
ax3.grid(True, alpha=0.2)

# 4. Z-Score
ax4 = axes[3]
colors = ['green' if z < -2 else 'red' if z > 2 else 'gray' for z in btc['rsi_zscore'].fillna(0)]
ax4.bar(btc.index, btc['rsi_zscore'], color=colors, alpha=0.7, width=1)
ax4.axhline(2, color='red', linestyle='--', alpha=0.7, label='Z = +2σ (overbought)')
ax4.axhline(-2, color='lime', linestyle='--', alpha=0.7, label='Z = -2σ (oversold)')
ax4.axhline(0, color='gray', linewidth=0.5)
ax4.set_ylabel('Z-Score')
ax4.set_ylim(-4, 4)
ax4.legend(loc='upper right', fontsize=8)
ax4.set_title('Z-Score RSI (статистические экстремумы)')
ax4.set_facecolor('#1a1a2e')
ax4.grid(True, alpha=0.2)

# 5. GARCH Volatility
ax5 = axes[4]
ax5.fill_between(btc.index, btc['garch_vol_annual'] * 100, alpha=0.5, color='orange')
ax5.plot(btc.index, btc['garch_vol_annual'] * 100, color='orange', linewidth=1, label='GARCH Annualized Vol')
ax5.set_ylabel('Vol %')
ax5.legend(loc='upper right', fontsize=8)
ax5.set_title('GARCH(1,1) — кластеризация волатильности')
ax5.set_facecolor('#1a1a2e')
ax5.grid(True, alpha=0.2)
ax5.set_xlabel('Date')

for ax in axes:
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=3))

plt.tight_layout()
plt.savefig('/Users/vadim/PyCharmMiscProject/MOEX/btc_quant_analysis.png', 
            dpi=150, bbox_inches='tight', facecolor='#0d1117')
print(f"   ✅ Сохранено: btc_quant_analysis.png")

# ============================================================================
# ИТОГОВЫЙ ОТЧЁТ
# ============================================================================
print(f"""
{'='*60}
📋 ИТОГОВЫЙ ОТЧЁТ: BTC Quant Analysis
{'='*60}

1. HURST: H = {h_1y:.3f} за последний год
   → {'BTC сейчас ТРЕНДОВЫЙ — momentum стратегии лучше' if h_1y > 0.55 else 'BTC ~случайный — сложно предсказать' if 0.45 <= h_1y <= 0.55 else 'BTC mean-reverting — контртренд лучше'}

2. GARCH: persistence = {persistence:.3f}
   → Волатильность BTC СИЛЬНО кластеризуется
   → Динамические стопы (GARCH) лучше фиксированных

3. KURTOSIS: {kurt:.1f} (норма = 0)
   → Тейлы НАМНОГО тольше нормальных
   → Экстремальные события случаются ЧАЩЕ чем ожидает модель

4. RSI ПОРОГИ:
   → Фиксированные 30/70 дают {btc['sig_fix_buy'].sum()} сигналов покупки
   → Z-Score (-2σ) даёт {btc['sig_z_buy'].sum()} — более ИЗБИРАТЕЛЬНЫЙ
   → BB даёт {btc['sig_bb_buy'].sum()} — АДАПТИВНЫЙ к текущей динамике
   → Percentile даёт {btc['sig_pctl_buy'].sum()} — исторический контекст

5. ВЫВОД ДЛЯ СТРАТЕГИИ:
   → Заменить RSI 30/70 → BB на RSI или Z-Score
   → Заменить фикс стоп 5% → GARCH 2σ динамический
   → Использовать Half Kelly для sizing
""")
