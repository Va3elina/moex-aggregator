# Полный справочник индикаторов: опционы + фьючерсы (EN / RU)

Полный каталог всех индикаторов которые мы исследовали для торговли на MOEX FORTS.
Английские названия для технической документации, русские объяснения и формулы.

================================================================================
ЧАСТЬ 1. ОПЦИОННЫЕ ИНДИКАТОРЫ
================================================================================

## A. Volatility Level / Уровень волатильности

### 1. ATM Implied Volatility (ATM IV) / Подразумеваемая волатильность ATM
- **EN**: At-the-Money Implied Volatility
- **RU**: Подразумеваемая волатильность на центральном страйке
- **Формула**: solve sigma из Black-76: `C = exp(-rT) × [F·N(d1) - K·N(d2)]`
  где d1 = (ln(F/K) + 0.5·σ²·T) / (σ·√T), для ATM K=F
- **Что значит**: ожидаемая годовая волатильность которую закладывают в цены опционов
- **Применение**: база для VRP, term structure, всех vol-метрик
- **Single proxy**: можно взять `RVI` (официальный индекс MOEX) если нет своего chain

### 2. Variance Swap Rate / Ставка вариационного свопа
- **EN**: Variance Swap Rate, Model-Free Implied Variance (MFIV)
- **RU**: Модельно-свободная подразумеваемая волатильность (как VIX)
- **Формула** (CBOE/VIX style):
  ```
  σ²_var = (2·e^{rT}/T) · Σ [ΔK_i / K_i² · Q(K_i)] - (1/T)·(F/K_0 - 1)²
  ```
  где Q(K) = OTM цена опциона на страйке K, K_0 = ближайший страйк ≤ F
- **Что значит**: «честная» волатильность по всему смайлу, не только ATM
- **Применение**: лучше чем ATM IV для активов с сильным skew (на RTS дала лучший результат)

### 3. RVI (Russian Volatility Index) / Российский индекс волатильности
- **EN**: Russian Volatility Index
- **RU**: Индекс волатильности RTS (аналог VIX)
- **Формула**: считает MOEX по варсвоповой формуле на опционах RTS
- **Endpoint**: `iss.moex.com/iss/engines/stock/markets/index/securities/RVI.json`
- **Что значит**: официальный индекс «страха» российского рынка
- **Применение**: panic LONG сигнал (RVI ≥ 40)

### 4. Historical / Realized Volatility / Историческая (реализованная) волатильность
- **EN**: Realized Volatility, Historical Volatility (RV / HV)
- **RU**: Реализованная (фактическая) волатильность
- **Формула**: `RV = std(log(close_t / close_{t-1}), window=30) × √252 × 100`
  Для RU акций именно √252 (торговых дней), не √365.
- **Что значит**: фактическая волатильность последних N дней
- **Применение**: знаменатель для VRP

## B. Volatility Risk Premium / Премия за риск волатильности

### 5. VRP (Variance Risk Premium) / Премия за риск волатильности
- **EN**: Variance Risk Premium
- **RU**: Премия за риск волатильности
- **Формула**: `VRP = ATM_IV - RV30`
- **Что значит**: переплата опционов относительно реальных движений (страховая премия)
- **Применение**: на RU HIGH VRP → bullish (обратно крипте где LOW VRP bullish)

### 6. Variance-Swap VRP / VRP через варсвоп
- **EN**: Variance-Swap VRP
- **RU**: Премия за риск через варсвоп
- **Формула**: `VRP_vs = varswap_iv - RV30`
- **Применение**: чище чем ATM-based VRP, учитывает весь смайл

## C. Volatility Smile Shape / Форма улыбки волатильности

### 7. Risk Reversal 25Δ (RR25) / Перекос риска 25-дельта
- **EN**: Risk Reversal 25-delta, 25Δ RR
- **RU**: Перекос риска 25-дельта (асимметрия улыбки)
- **Формула**: `RR25 = IV(25Δ call) - IV(25Δ put)`
  где 25Δ call: страйк где call-дельта ≈ +0.25 (OTM call)
  25Δ put: страйк где put-дельта ≈ -0.25 (OTM put)
- **Что значит**: насколько путы дороже коллов = «страх падения»
- **Применение**: RR25 << 0 = страх паники → разворот вверх

### 8. Butterfly 25Δ (BF25) / Бабочка 25-дельта
- **EN**: Butterfly 25-delta, 25Δ Butterfly
- **RU**: Бабочка 25-дельта (выпуклость улыбки)
- **Формула**: `BF25 = (IV(25Δc) + IV(25Δp))/2 - IV(ATM)`
- **Что значит**: насколько края улыбки дороже центра = ожидание скачка
- **Применение**: высокий BF25 = ждут jump (хороший или плохой)

### 9. Skew Slope / Наклон улыбки
- **EN**: Skew slope, IV skew
- **RU**: Наклон IV вдоль log-страйков
- **Формула**: коэффициент `b` в регрессии `IV(K) = a + b·log(K/F)` около ATM
- **Что значит**: насколько круто убывает IV с ростом страйка
- **Применение**: тонкая мера skew, дополняет RR25

## D. Term Structure / Временная структура

### 10. IV Term Structure Ratio / Соотношение IV по срокам
- **EN**: IV Term Ratio, Term Structure Slope
- **RU**: Соотношение IV на разных сроках
- **Формула**: `term_ratio = IV(30d) / IV(90d)`
  - ratio > 1: бэквардация (краткосрочная IV > долгосрочной) = текущий стресс
  - ratio < 1: контанго (долгосрочная > краткосрочной) = норма
- **Что значит**: «срочность» паники
- **Применение**: бэквардация = пик паники → разворот близко (но осторожно — переобучается)

### 11. IV by Tenor / IV по срокам
- **EN**: IV 7d / 30d / 60d / 90d / 180d
- **RU**: Подразумеваемая волатильность по разным горизонтам
- Каждая отдельная IV считается через интерполяцию по варианту в разрезе времени:
  ```
  σ(T_target) = √((T2-T_target)·σ1²·T1 + (T_target-T1)·σ2²·T2) / ((T2-T1)·T_target)
  ```

## E. Greeks Aggregation / Греческая агрегация

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
- **Применение**: на Si/RTS робастный, на SBER провалил OOS

### 13. Dollar Gamma / Долларовая гамма
- **EN**: Dollar Gamma, $ Gamma
- **RU**: Долларовая гамма
- **Формула**: `$Gamma = Σ OI × Γ × multiplier × spot`
- **Что значит**: магнитные страйки (без квадрата цены)
- **Применение**: уровни поддержки/сопротивления

### 14. Vanna / Ванна
- **EN**: Vanna
- **RU**: Чувствительность дельты к волатильности
- **Формула**: `Vanna = ∂Δ/∂σ = -e^{-rT}·φ(d1)·d2/σ`
- **Что значит**: при росте IV дельты OTM-путов растут → дилеры покупают больше базы → каскад
- **Применение**: позитивная ванна + рост IV = squeeze setup

### 15. Charm / Чарм
- **EN**: Charm, Delta Decay
- **RU**: Распад дельты во времени
- **Формула**: `Charm = ∂Δ/∂t` (производная дельты по времени)
- **Что значит**: pinning эффект возле экспирации (особенно для weekly options)
- **Применение**: bias для timing exit'а перед экспирацией

### 16. Flip Strike / Страйк смены знака
- **EN**: Flip strike, Zero-Gamma strike
- **RU**: Страйк где гамма меняет знак
- **Формула**: K где кумулятивная dealer gamma переходит через 0
- **Применение**: ключевой технический уровень

## F. Positioning / Позиционирование

### 17. Put/Call OI Ratio / Соотношение Put/Call по открытому интересу
- **EN**: Put/Call Open Interest Ratio
- **RU**: Соотношение путов и коллов по открытому интересу
- **Формула**: `PC_OI = Σ OI_puts / Σ OI_calls`
- **Что значит**: позиционирование толпы. Высокий = страх максимальный (контрариан-логика)
- **Применение на MOEX**: **ОТРАВЛЕН** ритейлом (80% физлица), малоинформативен

### 18. Put/Call Volume Ratio / Соотношение Put/Call по объёму
- **EN**: Put/Call Volume Ratio
- **RU**: Соотношение путов и коллов по дневному объёму
- **Формула**: `PC_Vol = Σ Volume_puts / Σ Volume_calls`
- **Что значит**: «текущее настроение» (vs накопленное в OI)
- **Применение**: чище OI ratio, ROBUST на SBER

### 19. Max Pain / Максимальная боль
- **EN**: Max Pain
- **RU**: Точка максимальной боли (страйк магнита)
- **Формула**: K где `Σ [OI_call × max(K-strike,0) + OI_put × max(strike-K,0)]` минимален
- **Что значит**: страйк где продавцы опционов теряют меньше всего → цена туда тянется на expiry
- **Применение**: ROBUST на Si

### 20. OI Concentration / Концентрация открытого интереса
- **EN**: OI Concentration, Top OI Strikes
- **RU**: Концентрация OI по топ-страйкам
- **Формула**: топ 3-5 страйков по OI (call+put)
- **Применение**: ключевые уровни поддержки/сопротивления

## G. Derived Market Expectations / Производные ожидания рынка

### 21. Straddle Implied Move / Ожидаемое движение через стрэддл
- **EN**: Straddle Implied Move, Expected Move
- **RU**: Ожидаемое движение к экспирации (по цене стрэддла)
- **Формула**: `implied_move% = (ATM_call_price + ATM_put_price) / spot × 100`
- **Что значит**: сколько % движения ждут к экспирации
- **Применение**: ROBUST на Si (LOW = bullish, низкая ожидаемая vol → продолжение тренда)

### 22. Breeden-Litzenberger Density / Плотность Бридена-Литценбергера
- **EN**: Breeden-Litzenberger Density, Risk-Neutral Density (RND)
- **RU**: Риск-нейтральное распределение вероятностей
- **Формула**: `pdf(K) = e^{rT} × d²C/dK²` (вторая производная цены call по страйку)
- **Производные метрики**:
  - `rn_p_down10` — вероятность падения на 10%+
  - `rn_p_up10` — вероятность роста на 10%+
  - `rn_mean` — ожидаемая цена
  - `rn_std` — стандартное отклонение
  - `rn_skew` — асимметрия распределения
- **Применение**: rn_p_down10 ROBUST на RTS

### 23. Implied Forward / Подразумеваемый форвард
- **EN**: Implied Forward Price
- **RU**: Подразумеваемая форвардная цена через put-call parity
- **Формула**: `F = K + (C - P) × e^{rT}` (из паритета put-call)
- **Что значит**: где рынок видит цену на экспирацию
- **Применение**: для расчёта VRP без явной цены фьючерса

================================================================================
ЧАСТЬ 2. ФЬЮЧЕРСНЫЕ ИНДИКАТОРЫ
================================================================================

## A. Participant Positioning / Позиционирование участников

### 1. futoi (FIZ / YUR Positioning) / Позиционирование физлиц / юрлиц
- **EN**: futoi (futures open interest by participant type)
- **RU**: futoi — позиционирование физлиц (FIZ) и юрлиц (YUR)
- **Endpoint**: `iss.moex.com/iss/analyticalproducts/futoi/securities/{ticker}.json`
- **Производные**:
  - `fiz_lsr = fiz_long / |fiz_short|` — соотношение длинных/коротких физлиц
  - `yur_lsr = yur_long / |yur_short|` — то же для юрлиц
  - `fiz_net = fiz_long + fiz_short` (short отрицательный) — чистая позиция
  - `yur_delta` — изменение позиций юрлиц день к дню
- **Что значит**: COT-аналог в реальном времени. Каждые 5 минут MOEX публикует разбивку позиций
- **Применение**: ГЛАВНЫЙ EDGE на RU.
  - На SBER/IMOEX: FIZ LOW (паника физиков) → LONG
  - На Si: FIZ контрариан работает только при крайней панике (≤1.4)
  - На RTS: знак может инвертироваться

## B. Term Structure / Временная структура

### 2. Basis / Базис
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
- **Применение**: бэквардация → bull на SBER/GAZP/Si

### 3. Calendar Spread / Календарный спред
- **EN**: Calendar Spread, Inter-month Spread
- **RU**: Календарный спред между фьючерсами разных месяцев
- **Формула**: `spread = front_month_futures - next_quarter_futures`
- **MOEX**: спреды даже листятся как отдельные инструменты (SBER: SRH1SRM1)
- **Применение**: ROBUST на Si

### 4. Carry Residual / Остаток керри
- **EN**: Carry Residual
- **RU**: Чистая премия позиционирования (за вычетом ставки)
- **Формула**: `carry_residual = basis_annualized - RUSFAR_rate`
- **Что значит**: премия что осталась после вычета стоимости денег = чистое позиционирование
- **RUSFAR**: ставка от ЦБ МосБиржи (`iss.moex.com/iss/statistics/engines/state/markets/repo/securities`)

### 5. Term Structure of Basis / Структура базиса по срокам
- **EN**: Term structure of basis
- **RU**: Соотношение базиса front-month vs back-month
- **Применение**: инвертированная структура (back < front) = острый стресс

## C. Flow & Interest / Поток и интерес

### 6. Open Interest Dynamics / Динамика открытого интереса
- **EN**: Open Interest changes, OI dynamics
- **RU**: Изменение открытого интереса
- **Метрики**:
  - `OI_change = OI_today - OI_yesterday`
  - Комбинации с ценой:
    - Price ↑ + OI ↑ = новые лонги (бычий сигнал)
    - Price ↑ + OI ↓ = шорты закрываются (слабее)
    - Price ↓ + OI ↑ = новые шорты (медвежий)
    - Price ↓ + OI ↓ = лонги закрываются (слабее)

### 7. CVD (Cumulative Volume Delta) / Кумулятивная дельта объёма
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
- **Применение**: Si CVD20 weak SHORT +14% CAGR (2024-2026)

### 8. Volume Profile / Профиль объёма
- **EN**: Volume Profile, Volume-by-Price
- **RU**: Распределение объёма по уровням цены
- **Применение**: зоны высокого объёма = уровни поддержки/сопротивления

## D. Microstructure / Микроструктура (нужен Algopack)

### 9. Order Flow Imbalance / Дисбаланс потока заявок
- **EN**: Order Flow Imbalance (OFI)
- **RU**: Дисбаланс ордер-флоу в стакане
- **Формула**: накопленный bid_volume_change - ask_volume_change

### 10. Bid-Ask Spread Dynamics / Динамика спреда
- **EN**: Bid-Ask Spread
- **RU**: Динамика спреда между лучшим бидом и аском
- **Применение**: расширение спреда = неопределённость, маркетмейкеры боятся

### 11. Aggressive vs Passive Volume / Агрессивный vs пассивный объём
- **EN**: Aggressive (Taker) vs Passive (Maker) Volume
- **RU**: Объём по агрессивным заявкам vs лимитным
- **MOEX**: поле `BUYSELL` в trades endpoint (агрессор: B/S)
- **Что значит**: aggressive = informed money, passive = маркетмейкеры/ритейл

## E. Smart Flow / Поток "умных денег"

### 12. Block Trades / Блок-сделки
- **EN**: Block Trades, OTC Trades
- **RU**: Крупные внебиржевые сделки
- **MOEX**: флаг `OFFMARKETDEAL` в trades endpoint + `BUYSELL` сторона
- **Что значит**: институциональные сделки OTC
- **Ограничение**: публично только текущий день (для истории — Algopack или копить snapshot)

## F. Stress / Margin Indicators / Стресс / маржинальные индикаторы

### 13. ГО / Initial Margin Changes / Изменения гарантийного обеспечения
- **EN**: Initial Margin (IM), Margin Requirement Changes
- **RU**: ГО — Гарантийное Обеспечение (что биржа замораживает как залог)
- **Метрики**:
  - `margin_chg_pct` — изменение ГО день к дню
  - Спайки ГО опережают большие движения
- **Что значит**: биржа поднимает ГО когда видит риск → ранний сигнал стресса
- **Ограничение**: публично только текущее значение, истории нет (платный Algopack)

### 14. Funding-like Carry on RU / Аналог funding на RU
- **EN**: Carry cost from quarterly basis
- **RU**: Эффективная стоимость carry через базис (нет perpetual на RU)
- **Формула**: `carry_cost = basis_annualized - RUSFAR`

## G. Cross-Market / Кросс-рыночные сигналы

### 15. Cross-asset Divergence / Расхождение между активами
- **EN**: Cross-asset divergence, Inter-market divergence
- **RU**: Расхождение между связанными активами
- **Применение**:
  - Si vs RTS — рублёвая часть индекса
  - SBER vs IMOEX — лидер vs индекс
  - Sectorial spreads

### 16. Correlation Signals / Корреляционные сигналы
- **EN**: Rolling correlation, Correlation breakdown
- **RU**: Скользящая корреляция между активами
- **Что значит**: резкое падение корреляции = кто-то лидирует
- **Применение**: 30-дневная rolling correlation между Si и RTS

### 17. Lead-Lag Signals / Сигналы опережения
- **EN**: Lead-lag relationships
- **RU**: Кто двигается раньше (Si часто опережает SBER в кризисы)
- **Применение**: использовать «лидер» как опережающий индикатор для «отстающего»

================================================================================
ЧАСТЬ 3. ROBUSTNESS МАТРИЦА (что работает на каком активе)
================================================================================

| Indicator | SBER | GAZP | Si | RTS |
|-----------|------|------|----|----|
| VRP (ATM) | ✓ | ✓ | - | - |
| VRP varswap | ✓ | - | - | ✓ best |
| RR25 | ✓ | - | - | - |
| BF25 | - | - | - | ✓ |
| Term ratio 30/90 | OVERFIT | - | - | - |
| GEX | OOS FAIL | - | ✓ | ✓ |
| Dollar gamma | ✓ | - | - | - |
| Max pain | ✓ | - | ✓ | - |
| PC vol ratio | ✓ | - | ✓ | - |
| Straddle move LOW | - | - | ✓ best | - |
| rn_p_down10 (BL) | - | - | - | ✓ |
| atm_iv level | - | - | - | ✓ |
| futoi (fiz_lsr) | ✓ best | ✓ | partial | flip |
| Basis backwardation | ✓ | ✓ | ✓ | - |
| Calendar spread | - | - | ✓ | - |
| CVD (real tick) | weak | weak | ✓ | ✓ |

Главный edge: **futoi (fiz_lsr)** — позиционирование физлиц как контрарианский сигнал.
В портфеле 4 активов с FIZ ≤ 1.2 дал CAGR 33% при DD 35%.

================================================================================
ЧАСТЬ 4. ССЫЛКИ НА ИСТОЧНИКИ ДАННЫХ
================================================================================

**Бесплатные (MOEX ISS):**
- Options chain: `iss.moex.com/iss/engines/futures/markets/options/securities.json`
- Options board (с VOLAT): `iss.moex.com/iss/statistics/engines/futures/markets/options/assets/{ASSET}/optionboard.json`
- futoi: `iss.moex.com/iss/analyticalproducts/futoi/securities/{ticker}.json`
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
