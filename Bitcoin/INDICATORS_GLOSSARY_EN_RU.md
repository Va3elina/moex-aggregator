# Справочник индикаторов: опционы + фьючерсы (EN / RU)
## БЕЗ метрик на основе OI и без futoi (физ/юр лиц)

================================================================================
ЧАСТЬ 1. ОПЦИОННЫЕ ИНДИКАТОРЫ (20)
================================================================================

## A. Volatility Level / Уровень волатильности

### 1. ATM Implied Volatility (ATM IV) / Подразумеваемая волатильность ATM
- **EN**: At-the-Money Implied Volatility
- **RU**: Подразумеваемая волатильность на центральном страйке
- **Формула**: solve σ из Black-76: `C = exp(-rT) × [F·N(d1) - K·N(d2)]`
  где d1 = (ln(F/K) + 0.5·σ²·T) / (σ·√T), для ATM K=F
- **Что значит**: ожидаемая годовая волатильность которую закладывают в цены опционов

### 2. Variance Swap Rate / Ставка вариационного свопа
- **EN**: Variance Swap Rate, Model-Free Implied Variance (MFIV)
- **RU**: Модельно-свободная подразумеваемая волатильность (как VIX)
- **Формула** (CBOE/VIX style):
  ```
  σ²_var = (2·e^{rT}/T) · Σ [ΔK_i / K_i² · Q(K_i)] - (1/T)·(F/K_0 - 1)²
  ```
  где Q(K) = OTM цена опциона на страйке K, K_0 = ближайший страйк ≤ F
- **Что значит**: «честная» волатильность по всему смайлу, не только ATM

### 3. RVI (Russian Volatility Index) / Российский индекс волатильности
- **EN**: Russian Volatility Index
- **RU**: Индекс волатильности RTS (аналог VIX)
- **Endpoint**: `iss.moex.com/iss/engines/stock/markets/index/securities/RVI.json`

### 4. Realized / Historical Volatility / Реализованная волатильность
- **EN**: Realized Volatility, Historical Volatility (RV / HV)
- **RU**: Фактическая историческая волатильность
- **Формула**: `RV = std(log(close_t / close_{t-1}), window=30) × √252 × 100`
  Для RU акций именно √252 (торговых дней)

## B. Volatility Risk Premium / Премия за риск волатильности

### 5. VRP (Variance Risk Premium) / Премия за риск волатильности
- **EN**: Variance Risk Premium
- **RU**: Премия за риск волатильности
- **Формула**: `VRP = ATM_IV - RV30`
- **Что значит**: переплата опционов относительно реальных движений (страховая премия)

### 6. Variance-Swap VRP / VRP через варсвоп
- **EN**: Variance-Swap VRP
- **RU**: Премия за риск через варсвоп
- **Формула**: `VRP_vs = varswap_iv - RV30`

## C. Volatility Smile Shape / Форма улыбки волатильности

### 7. Risk Reversal 25Δ (RR25) / Перекос риска 25-дельта
- **EN**: Risk Reversal 25-delta, 25Δ RR
- **RU**: Перекос риска 25-дельта (асимметрия улыбки)
- **Формула**: `RR25 = IV(25Δ call) - IV(25Δ put)`
- **Что значит**: насколько путы дороже коллов = «страх падения»

### 8. Butterfly 25Δ (BF25) / Бабочка 25-дельта
- **EN**: Butterfly 25-delta, 25Δ Butterfly
- **RU**: Бабочка 25-дельта (выпуклость улыбки)
- **Формула**: `BF25 = (IV(25Δc) + IV(25Δp))/2 - IV(ATM)`
- **Что значит**: насколько края улыбки дороже центра = ожидание скачка

### 9. Skew Slope / Наклон улыбки
- **EN**: Skew slope, IV skew
- **RU**: Наклон IV вдоль log-страйков
- **Формула**: коэффициент `b` в регрессии `IV(K) = a + b·log(K/F)` около ATM

## D. Term Structure / Временная структура

### 10. IV Term Structure Ratio / Соотношение IV по срокам
- **EN**: IV Term Ratio, Term Structure Slope
- **RU**: Соотношение IV на разных сроках
- **Формула**: `term_ratio = IV(30d) / IV(90d)`
  - ratio > 1: бэквардация (краткосрочная IV > долгосрочной) = текущий стресс
  - ratio < 1: контанго (долгосрочная > краткосрочной) = норма

### 11. IV by Tenor / IV по срокам
- **EN**: IV 7d / 30d / 60d / 90d / 180d
- **RU**: Подразумеваемая волатильность по разным горизонтам
- **Формула интерполяции**:
  ```
  σ(T_target) = √((T2-T_target)·σ1²·T1 + (T_target-T1)·σ2²·T2) / ((T2-T1)·T_target)
  ```

## E. Greeks Aggregation / Греческая агрегация

> **Примечание**: эти метрики используют OI как **весовой коэффициент** при агрегации
> гаммы по страйкам — это не «сигнал OI», а сигнал гаммы дилеров. Они остаются.

### 12. GEX (Gamma Exposure) / Гамма-экспозиция
- **EN**: Gamma Exposure
- **RU**: Совокупная гамма-экспозиция маркетмейкеров
- **Формула**:
  ```
  GEX = Σ_strike [(-OI_call + OI_put) × Γ × multiplier × spot² × 0.01]
  ```
  Знак: дилеры **продают** коллы (-1) и **покупают** путы (+1) — SpotGamma convention
- **Что значит**: где маркетмейкеры хеджируются
  - Negative GEX = дилеры short gamma → усиление движений (vol-expansion)
  - Positive GEX = дилеры long gamma → цена «прилипает» к страйкам (pinning)

### 13. Dollar Gamma / Долларовая гамма
- **EN**: Dollar Gamma, $ Gamma
- **RU**: Долларовая гамма
- **Формула**: `$Gamma = Σ OI × Γ × multiplier × spot`

### 14. Vanna / Ванна
- **EN**: Vanna
- **RU**: Чувствительность дельты к волатильности
- **Формула**: `Vanna = ∂Δ/∂σ = -e^{-rT}·φ(d1)·d2/σ`
- **Что значит**: при росте IV дельты OTM-путов растут → дилеры покупают больше базы → каскад

### 15. Charm / Чарм
- **EN**: Charm, Delta Decay
- **RU**: Распад дельты во времени
- **Формула**: `Charm = ∂Δ/∂t` (производная дельты по времени)
- **Что значит**: pinning эффект возле экспирации

### 16. Flip Strike / Страйк смены знака
- **EN**: Flip strike, Zero-Gamma strike
- **RU**: Страйк где гамма меняет знак
- **Формула**: K где кумулятивная dealer gamma переходит через 0

## F. Activity / Активность

### 17. Put/Call Volume Ratio / Соотношение Put/Call по объёму
- **EN**: Put/Call Volume Ratio
- **RU**: Соотношение путов и коллов по дневному объёму
- **Формула**: `PC_Vol = Σ Volume_puts / Σ Volume_calls`
- **Что значит**: «текущее настроение» (по дневной торговой активности)

## G. Derived Market Expectations / Производные ожидания рынка

### 18. Straddle Implied Move / Ожидаемое движение через стрэддл
- **EN**: Straddle Implied Move, Expected Move
- **RU**: Ожидаемое движение к экспирации (по цене стрэддла)
- **Формула**: `implied_move% = (ATM_call_price + ATM_put_price) / spot × 100`

### 19. Breeden-Litzenberger Density / Плотность Бридена-Литценбергера
- **EN**: Breeden-Litzenberger Density, Risk-Neutral Density (RND)
- **RU**: Риск-нейтральное распределение вероятностей
- **Формула**: `pdf(K) = e^{rT} × d²C/dK²` (вторая производная цены call по страйку)
- **Производные метрики**:
  - `rn_p_down10` — вероятность падения на 10%+
  - `rn_p_up10` — вероятность роста на 10%+
  - `rn_mean` — ожидаемая цена
  - `rn_std` — стандартное отклонение
  - `rn_skew` — асимметрия распределения

### 20. Implied Forward / Подразумеваемый форвард
- **EN**: Implied Forward Price
- **RU**: Подразумеваемая форвардная цена через put-call parity
- **Формула**: `F = K + (C - P) × e^{rT}` (из паритета put-call)

================================================================================
ЧАСТЬ 2. ФЬЮЧЕРСНЫЕ ИНДИКАТОРЫ (15)
================================================================================

## A. Term Structure / Временная структура

### 1. Basis / Базис
- **EN**: Basis, Futures Basis
- **RU**: Базис (разница между фьючерсом и спотом)
- **Формула**:
  ```
  basis = (futures_price - spot_price) / spot_price
  basis_annualized = basis × 365 / days_to_expiry × 100
  ```
- **Единицы**: важно учесть масштаб контракта
  - SBER/GAZP futures = копейки × 100 (1 контракт = 100 акций) → делить на 100
  - Si futures = пункты × 1000 → делить на 1000
  - RTS futures = индексные пункты
- **Что значит**:
  - Контанго (basis > 0): фьючерс дороже спота = норма
  - Бэквардация (basis < 0): фьючерс дешевле спота = стресс

### 2. Calendar Spread / Календарный спред
- **EN**: Calendar Spread, Inter-month Spread
- **RU**: Календарный спред между фьючерсами разных месяцев
- **Формула**: `spread = front_month_futures - next_quarter_futures`
- **MOEX**: спреды даже листятся как отдельные инструменты (SBER: SRH1SRM1)

### 3. Carry Residual / Остаток керри
- **EN**: Carry Residual
- **RU**: Чистая премия позиционирования (за вычетом ставки)
- **Формула**: `carry_residual = basis_annualized - RUSFAR_rate`
- **Что значит**: премия что осталась после вычета стоимости денег
- **RUSFAR**: ставка от ЦБ МосБиржи (`iss.moex.com/iss/statistics/engines/state/markets/repo/securities`)

### 4. Term Structure of Basis / Структура базиса по срокам
- **EN**: Term structure of basis
- **RU**: Соотношение базиса front-month vs back-month
- **Применение**: инвертированная структура (back < front) = острый стресс

### 5. Funding-like Carry / Аналог funding на RU
- **EN**: Carry cost from quarterly basis
- **RU**: Эффективная стоимость carry через базис (нет perpetual на RU)
- **Формула**: `carry_cost = basis_annualized - RUSFAR`

## B. Flow / Поток

### 6. CVD (Cumulative Volume Delta) / Кумулятивная дельта объёма
- **EN**: Cumulative Volume Delta
- **RU**: Кумулятивная дельта объёма (агрессивные покупки минус продажи)
- **Формула**:
  ```
  delta_minute = up_volume - down_volume
  CVD_day = Σ delta_minute за день
  CVD_20d = CVD_day.rolling(20).sum()
  ```
- **Что значит**: больше агрессивных покупателей или продавцов
- **Источник**: тиковые данные или минутные свечи (proxy через знак свечи × объём)

### 7. Volume Profile / Профиль объёма
- **EN**: Volume Profile, Volume-by-Price
- **RU**: Распределение объёма по уровням цены
- **Применение**: зоны высокого объёма = уровни поддержки/сопротивления

### 8. Aggressive vs Passive Volume / Агрессивный vs пассивный объём
- **EN**: Aggressive (Taker) vs Passive (Maker) Volume
- **RU**: Объём по агрессивным заявкам vs лимитным
- **MOEX**: поле `BUYSELL` в trades endpoint (агрессор: B/S)
- **Что значит**: aggressive = informed money, passive = маркетмейкеры/ритейл

## C. Microstructure / Микроструктура (нужен Algopack)

### 9. Order Flow Imbalance / Дисбаланс потока заявок
- **EN**: Order Flow Imbalance (OFI)
- **RU**: Дисбаланс ордер-флоу в стакане
- **Формула**: накопленный bid_volume_change - ask_volume_change

### 10. Bid-Ask Spread Dynamics / Динамика спреда
- **EN**: Bid-Ask Spread
- **RU**: Динамика спреда между лучшим бидом и аском
- **Применение**: расширение спреда = неопределённость, маркетмейкеры боятся

## D. Smart Flow / Поток "умных денег"

### 11. Block Trades / Блок-сделки
- **EN**: Block Trades, OTC Trades
- **RU**: Крупные внебиржевые сделки
- **MOEX**: флаг `OFFMARKETDEAL` в trades endpoint + `BUYSELL` сторона
- **Что значит**: институциональные сделки OTC
- **Ограничение**: публично только текущий день

## E. Stress / Margin / Стресс и маржа

### 12. Initial Margin (ГО) Changes / Изменения гарантийного обеспечения
- **EN**: Initial Margin (IM), Margin Requirement Changes
- **RU**: ГО — Гарантийное Обеспечение (что биржа замораживает как залог)
- **Метрики**:
  - `margin_chg_pct` — изменение ГО день к дню
  - Спайки ГО опережают большие движения
- **Что значит**: биржа поднимает ГО когда видит риск → ранний сигнал стресса
- **Ограничение**: публично только текущее значение (платный Algopack для истории)

## F. Cross-Market / Кросс-рыночные сигналы

### 13. Cross-asset Divergence / Расхождение между активами
- **EN**: Cross-asset divergence, Inter-market divergence
- **RU**: Расхождение между связанными активами
- **Применение**:
  - Si vs RTS — рублёвая часть индекса
  - SBER vs IMOEX — лидер vs индекс
  - Sectorial spreads (нефтянка, банки, металлы)

### 14. Correlation Signals / Корреляционные сигналы
- **EN**: Rolling correlation, Correlation breakdown
- **RU**: Скользящая корреляция между активами
- **Что значит**: резкое падение корреляции = кто-то лидирует
- **Применение**: 30-дневная rolling correlation между Si и RTS

### 15. Lead-Lag Signals / Сигналы опережения
- **EN**: Lead-lag relationships
- **RU**: Кто двигается раньше (Si часто опережает SBER в кризисы)
- **Применение**: использовать «лидер» как опережающий индикатор для «отстающего»

================================================================================
ИСТОЧНИКИ ДАННЫХ
================================================================================

**Бесплатные (MOEX ISS):**
- Options chain: `iss.moex.com/iss/engines/futures/markets/options/securities.json`
- Options board (с VOLAT): `iss.moex.com/iss/statistics/engines/futures/markets/options/assets/{ASSET}/optionboard.json`
- Futures: `iss.moex.com/iss/engines/futures/markets/forts/securities.json`
- RVI: `iss.moex.com/iss/engines/stock/markets/index/securities/RVI.json`
- RUSFAR: `iss.moex.com/iss/statistics/engines/state/markets/repo/securities.json`
- Minute candles (CVD): `iss.moex.com/iss/engines/futures/markets/forts/securities/{SECID}/candles.json?interval=1`
- Block trades (only today): `iss.moex.com/iss/engines/futures/markets/options/securities/{SECID}/trades.json`

**Платные (Algopack):**
- История ГО
- Tick-данные за всю историю
- История block trades
- Cross-exchange aggregated data
- L2 order book history
