# Options Data Catalog — для торговли BTC/ETH фьючерсами

Каталог метрик из options markets и их применение в системах для перпов/futures.
Составлен 2026-06-03 как research backlog для развития VRP стратегии.

---

## Что мы уже используем

| Метрика | Источник | Endpoint | Применение |
|---|---|---|---|
| **DVOL** (BTC/ETH Volatility Index) | Deribit | `/public/get_volatility_index_data?currency=BTC&resolution=86400` | VRP signal: `vrp = dvol - rv_30d`. LONG при VRP <= -10 |
| **Funding rate** (perpetual) | Deribit | `/public/get_funding_rate_history?instrument_name=BTC-PERPETUAL` | Cost modelling для backtest, не для сигнала |
| **RVI** (Russian VIX) | MOEX ISS | `/iss/history/engines/stock/markets/index/securities/RVI.json` | RU market analog VRP signal (обратное направление) |

Из всего богатства options market сейчас используем **~5%** доступной информации.

---

## Полный каталог метрик (что ещё есть)

### 1. Volatility surface — структура IV

| Метрика | Что говорит | Сигнал | Deribit endpoint |
|---|---|---|---|
| **Term structure** (DVOL 7d vs 30d vs 90d) | Contango/backwardation IV | Backwardation = стресс → mean revert | `get_book_summary_by_currency` (агрегировать IV по expiries) |
| **25-delta Risk Reversal** (call IV − put IV) | Skew = fear/greed asymmetry | RR << 0 = страх put'ов = local bottom | Считать вручную из option chain |
| **Butterfly** (OTM IV − ATM IV) | Хвостовой риск | High butterfly = jump expected | Считать из chain |
| **ATM IV vs RV ratio** | То же что VRP, но точнее (без OTM шума) | Альтернатива нашему VRP | Вручную |
| **Implied skew slope** | Наклон smile | Strong slope = tail risk pricing | Вручную |

### 2. Greeks aggregation — позиционирование маркетмейкеров

| Метрика | Что говорит | Сигнал |
|---|---|---|
| **GEX (Gamma Exposure)** | Net dealer gamma по всему open interest | Negative GEX = volatility expansion ahead; Positive = pinning |
| **Dealer dollar gamma** | Magnet strikes | Цена «прилипает» к большим OI strikes |
| **Vanna exposure** | IV-spot correlation в хедже | Positive vanna + rising IV → squeeze probability |
| **Charm exposure** | Time decay of delta near expiry | Pinning effect возле expiry — bias для exit timing |
| **DDOI (Dealer Direct OI)** | Чистая dealer pozицияz | Smart money positioning |

Расчёт GEX:
```
GEX = sum over all options of: OI * contract_size * gamma * spot^2 * 0.01
```

### 3. Positioning / sentiment

| Метрика | Что говорит | Extreme threshold |
|---|---|---|
| **Put/Call OI ratio** | Crowd positioning | >1.5 = bear capitulation; <0.5 = bull mania |
| **Put/Call volume ratio** | Daily fear/greed | >2 = panic day |
| **Max pain** | Strike с max total OI dollar value | Цена тянется туда на expiry (особенно weekly) |
| **OI concentration** | Top 3 strikes by OI | = support/resistance levels |
| **Net premium delta** | Total call premium − put premium | Positive = bullish positioning |

### 4. Flow / smart money

| Метрика | Что говорит |
|---|---|
| **Block trades** | OTC институционалы — обычно informed |
| **Volume / OI > 1** | Свежее positioning (опасный signal) |
| **Sweep orders** (рынковые большие ордера) | Aggressive buyers — usually informed |
| **Far OTM call buying** | Lottery-ticket FOMO → contrarian bear |
| **Deep ITM put buying** | Smart money hedging → bear early warning |
| **Premium-weighted vs notional flow** | Где «реальные» деньги |

### 5. Derived market expectations

| Метрика | Что говорит | Применение |
|---|---|---|
| **Options-implied probability distribution** | Распределение price в день expiry (Breeden-Litzenberger) | Density над strikes |
| **Straddle implied move** | Expected range к expiry | Volatility expectations |
| **Implied forward price** | Где market думает spot будет на expiry | Direction bias |
| **Variance swap rate** | «Чистый» IV без strike bias | Pure vol exposure |
| **Volatility cone** | RV percentiles по lookbacks | Где текущий IV в historical context |

### 6. Cross-market / macro

| Метрика | Что говорит |
|---|---|
| **VIX-DVOL correlation** | Crypto-equity coupling/decoupling |
| **MOVE index** | Bond vol — early warning macro stress |
| **CVI (composite Crypto Vol Index)** | Multi-asset crypto vol |
| **DXY / 2Y rates / gold** | Macro drivers |

### 7. Futures basis (родственно)

| Метрика | Что говорит |
|---|---|
| **Funding rate** (мы используем) | Контанго perp vs spot |
| **Quarterly basis** (Mar/Jun/Sep/Dec futures) | Forward curve slope. >5% bull, <0% bear |
| **Term structure of basis** | Stress indicator. Inverted = панике |
| **Calendar spread** | Diff между нelement contracts |

---

## Приоритет для развития нашей стратегии

Рейтинг по «дополняет VRP edge + легко доступно + понятный сигнал»:

| Priority | Метрика | Зачем | Сложность |
|---|---|---|---|
| **1** | **25-delta Risk Reversal** | Confirmation filter: VRP ≤-10 + RR < -3 = ультра-сильный long | Easy — chain calc |
| **2** | **Term structure** (DVOL ratio short/long) | Backwardation = capitulation = совпадает с VRP | Easy — нужны DVOL разных tenors |
| **3** | **Put/Call OI ratio extremes** | Подтверждение oversold/overbought | Medium — агрегация OI |
| **4** | **Quarterly basis** | Free signal: contango → bull; backwardation → top | Easy — fetch quarterly futures |
| **5** | **GEX** | Volatility regime detection → подстраивать holds | Medium-Hard — нужна полная chain + greeks |
| **6** | **Max pain proximity** | На weekly expiries цена тянется к max pain — bias для exits | Medium |

---

## Deribit API endpoints (бесплатные)

| Endpoint | Что отдаёт |
|---|---|
| `/public/get_volatility_index_data` | DVOL (мы используем) |
| `/public/get_funding_rate_history` | Funding rate (мы используем) |
| `/public/get_historical_volatility` | Realized vol |
| `/public/get_index_price` | Index price |
| `/public/get_book_summary_by_currency` | IV, OI, volume по всем strikes — base для всех skew/term/GEX расчётов |
| `/public/get_instruments?currency=BTC&kind=option` | Список всех инструментов с expiries |
| `/public/get_order_book?instrument_name=...` | Полный order book для конкретного strike |
| `/public/ticker?instrument_name=...` | Real-time greeks (delta, gamma, vega, theta) |
| `/public/get_last_trades_by_instrument` | Recent trades для flow analysis |
| `/public/get_settlement_history_by_currency` | Historical settlements |

---

## Платные провайдеры (где-то нужны)

| Provider | Что даёт | Цена ориентир |
|---|---|---|
| **Amberdata** | Aggregated cross-exchange options + chain | $$$ enterprise |
| **Genesis Volatility (gvol.io)** | Real-time options analytics для крипто | $200-2000/mo |
| **Skew.com / Coinglass** | Free dashboards, частичные данные | Free + premium |
| **Laevitas** | Options screener для BTC/ETH | $50-200/mo |
| **Block scholes** | Institutional options research | $$$ enterprise |

Для retail research **Deribit публичный API + ручной расчёт** покрывает 80% потребностей.

---

## Конкретные идеи для тестирования

### Идея A: VRP + Risk Reversal двойной фильтр
```
Entry: VRP <= -10 AND RR_25d < -3 (puts panic-priced)
Hypothesis: reduces false signals из чисто VRP edge
```

### Идея B: Term structure regime switch
```
DVOL_30d / DVOL_90d > 1.0 (backwardation):
  → агрессивный entry (VRP <= -5 достаточно)
DVOL_30d / DVOL_90d < 0.95 (contango):
  → консервативный (VRP <= -15)
```

### Идея C: Max pain exit
Закрывать LONG не на DVOL spike, а когда близко к max pain в ±5 дней до major
quarterly expiry. Захватывает «pinning effect».

### Идея D: OI extremes как catalyst
```
Put/Call OI > 2.0 = panic bottom signal
Combined with VRP <= -10 = ultra-high conviction long
```

### Идея E: GEX-based hold adjustment
```
Negative GEX environment → shorter holds (volatility regime)
Positive GEX → longer holds (rangebound regime)
```

### Идея F: Quarterly basis stress signal
```
Quarterly basis < 0% (backwardation) = institutional panic
→ extreme long signal
Quarterly basis > 15% (deep contango) = FOMO peak
→ exit / consider short
```

---

## Что НЕ делать (research dead ends)

- **Open Interest (raw, total OI)** — проверено пользователем, **бесполезный signal сам по себе** (без разбивки по сторонам/участникам). Не использовать как primary signal.
- **Pure put/call OI ratio** — на крипто noisy (retail dominated). На MOEX отравлен — 80% retail в options OI.
- **Pure put/call volume ratio** — тот же noise.
- **Pin risk arbitrage** — требует HFT infrastructure
- **Cross-exchange basis arb** — execution costs killing
- **Vega-neutral straddle harvesting** — нужны опционы, а не perps
- **Calendar spread** (для retail size) — мелкий edge, большая execution complexity

### Что РАБОТАЕТ как positioning signal вместо OI

- **MOEX futoi (FIZ vs YUR breakdown)** — реальное разделение institutional vs retail.
  FIZ long/short ratio extremes = contrarian signal. Доказано +205% за 6 лет на SBER.
- **futures basis term structure** — economic positioning без noise.
- **Vol surface skew (Risk Reversal 25d)** — direction sentiment пристойный.

---

## Files index (где смотреть код)

| Purpose | File |
|---|---|
| DVOL fetcher | `Bitcoin/fetch_dvol_full_history.py` |
| Funding fetcher | `Bitcoin/fetch_bybit_funding.py` |
| RVI fetcher | `Bitcoin/fetch_moex_rvi.py` |
| Options general fetcher (existing skeleton) | `Bitcoin/fetch_options_data_full.py` |
| VRP strategy notes | `Bitcoin/VRP_STRATEGY_NOTES.md` |
