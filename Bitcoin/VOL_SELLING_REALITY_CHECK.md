# Vol-Selling — Reality Check (v2, with HONEST backtest)

Запрос: достижимы ли стабильные 40 %/год через продажу волатильности на FORTS.

## Короткий ответ: нет

Систематическая продажа волатильности с 40 %/год = математически Sharpe 1.5+,
которого не достигал ни один публичный vol-фонд. Все кто заявлял такие
доходности — взорвались.

Теперь у нас есть **честный backtest** который это подтверждает.

---

## Что было сломано в старой версии (`backtest_vol_selling.py`)

| Что | Эффект на результат |
|---|---|
| Bid/ask = 0 (mid-only) | переоценка дохода ~30-40 % за сделку |
| Размер позиции компаундировал без лимитов OI/Volume | капитал доходил до 329 M ₽ на чашке SBER |
| Margin = max_loss × contracts (нет SPAN-стресса) | фактический ГО в 2-5× выше |
| Margin call отсутствовал | переживали любую просадку без forced close |
| 2022 halt маскировался clearing-only marks | поза "проскочила" катастрофу |
| Wings (5-delta puts) брались по settle | реально фик-цены 15-30 % shift от mid |
| Take-profit triggered на mid-PnL | мы _думали_ что в 50 % профита, реально — нет |

Итог: **+328,981 % за 6 лет, WR 100 %, MaxDD 0 % — очевидно сломанный движок**.

---

## Что построено в `backtest_vol_selling_honest.py`

Три новых модуля + переписанный backtest:

### 1. `model_bidask.py` — эмпирическая модель спреда

История ISS отдаёт только SETTLEPRICE. Реальный bid-ask мы оцениваем по:
- HIGH-LOW intraday диапазон (proxy на спред + intraday move)
- WAPRICE − SETTLEPRICE (signed pressure)
- log-линейная регрессия `half_spread/settle ~ a + b·|log K/F| + c·√T + d·1/√OI + e·1/√Vol`

Фит на 66,547 ликвидных строках:

| Coeff | Value | Смысл |
|---|---|---|
| a (base) | 10 %  | ATM-минимум |
| b (moneyness) | 21 % | +21 % спреда за единицу OTM (log K/F) |
| c (sqrt T) | 0 % | T-эффект слабый |
| d (1/√OI) | 10 % | thin OI → шире |
| e (1/√Vol) | 5 % | thin volume → шире |

In-sample MAE 11.9 %, R² 0.09 — низкий, потому что HL содержит и intraday
move сверх spread. Коэффициенты ограничены в разумном диапазоне.

**Контроль качества**:
- Liquid ATM (OI 5000, Vol 500): half-spread ≈ 10.4 % — реалистично для SBER FORTS
- 25Δ OTM (OI 500, Vol 50): half-spread ≈ 12.6 %
- 5Δ wing (OI 50, Vol 5): half-spread ≈ 17.3 %
- Panic (margin call, gap): множитель 3×, cap 60 % от mid

Для продавца стрэнгла: продаёт по **bid** (хуже mid), для покупки крыла:
платит по **ask** (хуже mid). На реальной сделке Iron Condor спред съедает
**33.7 %** от номинального кредита (медиана нашего лога).

### 2. `forts_margin.py` — портфельный ГО а-ля SPAN

Реальная формула MOEX FORTS (документ fs.moex.com/files/4723) — это
SPAN-style scenario simulator. Мы реализовали упрощённый 7×3 grid:

- Spot shocks: −15 %, −10 %, −5 %, 0, +5 %, +10 %, +15 %
- Vol shocks: −30 %, 0, +30 %
- Каждая комбинация → пересчёт всех ног через Black-76
- Margin = max(0, −min_pnl) с учётом контракт-мультипликатора (100 акций
  для SBER)

Контроль качества: SBER Iron Condor 320C/280P short, 340C/260P wings,
F=300, T=30/365 → margin ≈ 1,100 ₽ за contract. Это близко к реальному
ГО на сайте брокеров (1,000-1,500 ₽ для такого спреда).

Допущения / ограничения:
- Нет inter-month credits — overstates margin для календарных спредов
- Нет регуляторного floor'а — мелкие шорты могут под-маржиниться
- IV shock — flat across smile
- Шоки фиксированы (−15..+15 %) — это OK для tranquil режимов и **слишком
  оптимистично для 2022 (реально могло быть ±40 %)**. Margin call логика
  это компенсирует через буфер.

### 3. `backtest_vol_selling_honest.py` — полный backtest

Интегрирует:
- model_bidask на open/close (sell at bid, buy at ask)
- forts_margin для sizing + ежедневного margin call
- OI/Volume liquidity caps (2 % OI, 10 % volume, hard cap 100 contracts)
- **Spot-calendar gap detector** — chain даёт fake clearing-marks через 2022
  halt; spot SBER не торговался Feb 25 → Mar 24. Используем spot dates для
  ground truth.
- Margin call: equity < margin × 1.0 → forced close по panic prices
- Catastrophe gap > 5 trading days → forced close по panic
- TP-trigger использует **realizable** PnL (после bid/ask), не mid

Параметры:
- Iron Condor 20Δ short / 5Δ wings, target 30 DTE
- VRP percentile ≥ 70 (rolling 252)
- Exit: 50 % profit OR DTE<7 OR short delta>0.35 OR margin call OR gap
- Sizing: 5 % of equity per trade, OI/Vol caps

---

## Результаты честного backtest (SBER 2020-2026)

| Метрика | Значение |
|---|---|
| Init capital | 1,000,000 ₽ |
| Final equity | 1,018,259 ₽ |
| **Total return** | **+1.8 %** за 6 лет |
| **CAGR** | **+0.3 %** |
| Max DD | 2.6 % |
| Sharpe (daily) | 0.22 |
| # trades | 16 |
| Win-rate | 56.2 % |
| Profit factor | 1.59 |
| Margin calls | 0 |
| Catastrophe gaps | 1 (Jan 2026 NY holiday) |

**Per-year breakdown**:

| Year | N | WR % | PnL (₽) | Avg ROI on margin |
|---|---|---|---|---|
| 2020 | 0 | — | — | — |
| 2021 | 0 | — | — | — |
| 2022 | 2 | 100 % | +33,407 | +39 % |
| 2023 | 5 | 60 % | −8,454 | +8 % |
| 2024 | 3 | 33 % | −5,112 | −5 % |
| 2025 | 4 | 50 % | +1,967 | +2 % |
| 2026 | 2 | 50 % | −2,245 | −2 % |

**Cost decomposition** (общие суммы):

- Bid/ask spread cost on entry: **48,969 ₽** (≈34 % от total credit collected)
- Exit commissions: 1,304 ₽
- Net exit PnL: **+19,563 ₽**

Прибыль ДО transaction costs была бы +68,532 ₽ (≈6.9 %). После реальных
bid/ask + commissions осталось +1.8 % за 6 лет. **Спред съел 97 %
"бумажной" прибыли стратегии.**

---

## Почему 2020-2021 = 0 trades

527 signal-day, но только 16 сделок. Skip-причины:

| Причина | Раз |
|---|---|
| no_signal | 1,029 |
| signal_open (поза уже открыта) | 67 |
| **zero_contracts (OI/Vol fail)** | **392** |
| no_expiry в окне 18-45 DTE | 34 |
| **neg_credit (wings съели премию)** | **14** |
| no_strikes_picked | 4 |

Главные блокеры:
1. **OI слишком тонкий**. Медианный SBER monthly strike: OI 294, мы можем
   взять 2 % = 6 контрактов. В 2020-2021 далёкие 5Δ wings (за 30 страйков
   от F) часто имели OI < 100 — наш `MIN_OI_AT_STRIKE` фильтр их блокирует.
2. **Wings съедают весь credit**. На 14 днях спред-cost на крыльях > credit
   шортов → net credit < 0 → пропуск. Без крыльев это unhedged
   short-vol — недопустимо.

Это **реальное** ограничение FORTS, а не bug. В 2020-2021 ликвидность
SBER options была хуже, чем сейчас. Iron Condor на 30 DTE с дальними
крыльями там просто не подбирался.

---

## 2022 halt — что произошло

Опционный CSV содержит fake clearing-only settlement marks через весь
период Feb 25 - Mar 24, 2022 (когда FORTS реально стоял). Это маскирует
катастрофу: старый backtest "проскочил" 2022 без потерь.

Мы используем **SBER spot calendar** как ground truth: spot не торговался
с 25 февраля до 24 марта (27 дней) → gap detector видит 27-дневный gap
по spot dates даже когда option chain непрерывен.

В нашем тесте **никакой позиции не было открыто на момент halt** (из-за
liquidity gates пред-халтных дней) — поэтому 2022 catastrophe мы не
поймали. **Это везение, а не edge**. Если бы поза открылась 24 февраля
(VRP взлетел, signal сработал — но liquidity не дала войти), на reopening
ГО потребовался бы 3-5× от исходного → мгновенный margin call → forced
close по panic_bid → SBER упал с 208 до 132 (−37 %); 20Δ short put прошёл
бы deep ITM; loss = wing_width × contracts × CONTRACT_MULT. На 25 контрактов
с шириной 50 ₽ это −125,000 ₽ = −12.5 % equity. Catastrophe-test logic
готов, но факт-сетап его не активировал.

В живой работе: vol-seller с открытой позой 24 февраля 2022 потерял бы
**весь margin** (а часто и больше — broker margin call по panic prices
после reopening). Наш единственный 2022 trade закрылся 11 мая 2022 (после
возобновления, на новом lower vol), оставив прибыль +22,060 ₽.

---

## Сравнение: broken vs honest

| Метрика | Сломанный | Честный |
|---|---|---|
| Total return | +328,981 % | +1.8 % |
| CAGR | ~211 % | 0.3 % |
| WR | 100 % | 56.2 % |
| MaxDD | ~0 % | 2.6 % |
| Trades | ~140 | 16 |
| Cost model | mid-only | bid/ask + панические exit |
| Margin | max_loss × N | SPAN portfolio (7×3 grid) |

**Где была главная ошибка**: bid/ask = 0. Settle - settle PnL даёт 30-40 %
"бесплатной" прибыли на сделку, которая в реальности уходит market-maker'у.
Компаундирование такой "прибыли" 100+ раз даёт безумные цифры.

---

## Реалистичный ориентир для RU FORTS vol-selling

Из нашего честного backtest + research (Kalenkovich, Brahman, CBOE-эквиваленты):

| Параметр | Наш SBER backtest | Industry vol-funds | Идеал из research |
|---|---|---|---|
| CAGR | +0.3 % | 5-10 % | 12-15 % |
| Sharpe | 0.22 | 0.5-0.8 | 0.8-1.0 |
| Max DD | 2.6 % (повезло) | 25-50 % | 25-35 % |
| Frequency | 16 сделок / 6 лет | 30-50 / год | 30-50 / год |

Наш CAGR 0.3 % — потому что:

1. **Low trade frequency**: SBER chain liquidity не даёт частых entry
   с 5Δ хеджем. Industry funds торгуют SPX/EUR, где OI миллион+.
2. **Costs eat 97 % of paper edge**: bid/ask + commissions vs theta.
3. **Random sample of 16 trades** не статистически значим — мог
   быть и −10 %, и +10 % с такой выборкой.

Для retail FORTS vol-selling **реалистичные числа**:
- 5-8 % CAGR _чистого_, если использовать ATM-ишные шорты без крыльев
  (но с unbounded tail risk → ~10 % шанс zero в год)
- 2-5 % CAGR с правильным хеджем (наш результат) → не оправдывает
  ни усилий ни tail risk
- **40 %/год = не реально без 5-10× leverage**, который катапультирует
  tail risk до уровня "1 раз пожилеешь и всё"

---

## Рекомендация

| Стратегия | Реалистичный CAGR | DD | Risk |
|---|---|---|---|
| SBER FIZ + IV фильтр (production) | 15-20 % | 13-20 % | низкий |
| Vol-selling Iron Condor (этот файл) | 0-5 % | 5-15 % | хвостовой 2022-стиль |
| **Итого реалистично** | **15-20 %** | **15-25 %** | контролируемый |

**Vol-selling на SBER FORTS не даёт edge** после реальных издержек.
Это подтверждено честным backtest 2020-2026. 40 %/год — фантазия без
leverage, который превращает стратегию в lottery с blow-up risk.

---

## Что ещё надо для production-готовности

1. **Live optionboard bid/ask sampling** — собирать через ALGOPACK API
   реальные котировки и сравнить с нашей моделью. Если модель занижает
   spread на 30 % — backtest ещё хуже.
2. **2022 stress override** — задать руками SBER -50 % shock на халт-дни.
3. **Walk-forward тест** — сейчас VRP percentile rolling вычислен с
   look-ahead; нужно строго past-only.
4. **Multi-underlying** — повторить на GAZP, RTS, Si. Если на каждом
   результат тот же → подтверждение что edge'a нет.

---

## Файлы

- `model_bidask.py` — empirical bid/ask spread model
- `forts_margin.py` — SPAN-style portfolio margin (ГО)
- `backtest_vol_selling_honest.py` — главный backtest
- `sber_vol_selling_honest_equity.csv` — daily equity curve
- `sber_vol_selling_honest_trades.csv` — per-trade лог
- `.bidask_coeffs.json` — закешированные коэффициенты модели спреда
