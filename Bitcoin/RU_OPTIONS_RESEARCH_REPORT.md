# RU Options Data — Research Report (6 параллельных агентов)

Дата: 2026-06-03. Сессия: исследование возможности перенести VRP / options-based стратегии
с криптовалют (Deribit DVOL) на российский рынок (MOEX FORTS).

Базовые активы интереса: **SBER, AFKS, GAZP, IMOEX**.

---

## TL;DR — что мы узнали

| Что | Статус |
|---|---|
| MOEX **публикует IV** | ✓ Через `optionboard.json` (поле `VOLAT`) |
| Полный options chain с OI/volume | ✓ Через `engines/futures/markets/options/securities.json` |
| Historical depth | ✓ С **2014** (chain), **2022** (OI snapshots) |
| Per-asset VRP (как наш DVOL) | ✓ Считается из VOLAT + RV30 |
| Term structure IV | ✓ Live и historical |
| 25d Risk Reversal, Butterfly | ✓ Через delta-interpolation |
| GEX (raw gamma intensity) | ✓ Считается, но без dealer-side |
| P/C OI ratio | ⚠ Считается, но "отравлен" ритейлом (80% phys persons) |
| Quarterly basis | ✓ Главный edge на RU |
| Block trades / smart money | ⚠ Только snapshot, без истории |
| Greeks (delta/gamma/vega) | ✗ Не публикуются → считать самим |

---

## Доступные тикеры опционов

| Базовый актив | ASSETCODE | Тип | Liquidity | Weekly? |
|---|---|---|---|---|
| **SBER** | `SBRF` | акция | Liquid (top 5) | ✓ |
| **GAZP** | `GAZR` | фьюч GZM6 | Liquid | ✓ |
| **AFKS** | `AFKS` | акция | **Illiquid** (0 trades/day, 1-2 контракта в стакане) | ✗ |
| **IMOEX** | `MIX` (на фьюч MXM6) + `IMX` (на индекс) | I+F | Средняя, растёт +47% в 2024 | ✗ |
| RTS Index | `RTS` | фьюч | **Top liquid** | ✓ |
| USD/RUB | `Si` | фьюч | Top liquid | ✓ |

**Вывод**: AFKS — потеря, для остальных есть данные.

---

## Полный каталог endpoints MOEX ISS

### Vol surface metrics
| Endpoint | Что отдаёт |
|---|---|
| `/iss/statistics/engines/futures/markets/options/assets.json` | Список underlying'ов + spot |
| `/iss/statistics/engines/futures/markets/options/assets/{ASSET}.json` | Все expiries (W/M/Q) |
| `/iss/statistics/engines/futures/markets/options/assets/{ASSET}/optionboard.json` | **Live IV (VOLAT %)**, theor price, OI per strike per side |
| `/iss/engines/futures/markets/options/securities.json` | Полный chain, 15k+ контрактов одним запросом |
| `/iss/history/engines/futures/markets/options/securities.json?date=YYYY-MM-DD` | Historical chain с 2014 (без VOLAT — считать через Black-76) |

### Open Interest / positioning
| Endpoint | Что отдаёт |
|---|---|
| `/iss/analyticalproducts/futoi/securities/{ticker}.json` | **OI разбит на YUR/FIZ** (институты vs физики), long/short. История с 2020. На RI, Si, SBERF, GAZPF, IMOEXF, VI |
| `/iss/statistics/engines/futures/markets/options/openpositions/{ASSET}.json` | OI by C/P + FIZ/YUR. Только текущий snapshot. |

### Basis / term structure
| Endpoint | Что отдаёт |
|---|---|
| `/iss/engines/futures/markets/forts/securities.json` | Все futures (quarterly + monthly). LAST, OI, expiry |
| `/iss/statistics/engines/state/markets/repo/securities.json` | **RUSFAR** (risk-free rate для cost-of-carry) |

### Volatility indices
| Endpoint | Что отдаёт |
|---|---|
| `/iss/engines/stock/markets/index/securities/RVI.json` | RVI (уже используем) |
| `/iss/engines/futures/markets/forts/securities/VIM6.json` | **RVI futures** (VIM6, VIN6) → term structure spread |

### Flow / trades (только snapshot)
| Endpoint | Что отдаёт |
|---|---|
| `/iss/engines/futures/markets/options/securities/{SECID}/trades.json` | Tape с `OFFMARKETDEAL` flag + `BUYSELL` side |

### НЕ публикуется (нужны платные)
- Implied volatility в исторических снапшотах (нужен Black-76 inversion)
- Greeks (delta/gamma/vega) — считать самим
- Member-level flow / counterparty
- Cross-exchange aggregation

---

## Реальные числа SBER на 2026-06-03

Spot 324.42 RUB.

### Term structure IV
- IV(7d) = 14.09%
- IV(30d) = 17.19%
- IV(90d) = 23.27%
- IV(30d)/IV(90d) = **0.74 → steep contango** (рынок ожидает рост vol осенью)

### Smile
- 14d expiry: ATM IV 14.2%, RR25 = **−1.28pp**, BF25 = +0.39pp
- 105d expiry: ATM IV 19.8%, RR25 = **−7.06pp** (strong put fear к сентябрю), BF25 = +1.43pp

### Positioning
- P/C OI ratio = **0.56** (call-heavy)
- P/C Volume ratio = 1.05
- Max pain = **320** vs spot 324.5 → magnet −1.4%
- Top OI strikes: 320 (1.12M), 310 (366k), 330 (253k) — **gamma wall at 320**

### GEX
- Net GEX SBRF SRM6 = **−27.8M** (short gamma → volatility expansion ahead)
- Net GEX Si SiM6 = **+1.4B** (long gamma → pinning at strike 72607)

### Basis
- SRM6 (Jun) = +0.70% (mild contango)
- SRU6 (Sep) = **−6.81%** (backwardation = embedded dividend ~$1.50)
- GAZR full contango +0.7% → +11.4% (к 12.26)
- RTS RIM6 = −0.77% slight backwardation

---

## Главные стратегические инсайты по RU специфике

### 1. RU dividend cut в basis = unique edge
RU dividends paid as **discrete drop**. Basis несёт embedded `(implied_div − rate)`.
SBER SRU6 −6.81% сейчас ≈ implied 2026 dividend. **Deviation от consensus dividend forecast → tradeable signal**.

### 2. Inverted basis term structure = rare stress signal
Когда `RIH7 < RIZ6 < RIU6 < RIM6` (back-month дешевле front) — это происходило:
- Feb 2022 (war)
- Sept 2022 (mobilisation)
- Aug 2024 (mini sell-off)

**Backtest как regime switch** — высокая надёжность сигнала.

### 3. Pre-roll calendar spread
Front-month basis spikes 5-10 дней до expiry на dealer roll flow → calendar spread arb.

### 4. P/C OI ratio "отравлен" — НЕ использовать как smart money
80% RU options OI = ритейл (vs 50/50 futures). На SPX P/C это institutional positioning,
на MOEX — просто шум физиков. **Vol surface skew (RR25) >> P/C ratio** для signal.

### 5. futoi (FIZ vs YUR) = real smart money proxy
`/iss/analyticalproducts/futoi/securities/{ticker}.json` отдаёт positioning разбит
по типу участника. Если YUR long / FIZ short → smart money bullish.
**Это уникально для RU**, на Deribit/CBOE такого нет.

### 6. GEX на RU работает, но БЕЗ dealer side
Можем посчитать **raw gamma intensity by strike** (где cohenирована OI gamma) — это
даст magnet strikes. Но **кто net long/short gamma — неизвестно** (нет classifier'а).
Полезно как support/resistance, не как trend direction.

### 7. Никто публично не считает GEX/dealer gamma для RU
**Ниша свободна** — нет SqueezeMetrics / SpotGamma-аналога для MOEX.

---

## Противоречия между агентами (требуют верификации)

### Поле `IMP` — что это?
- **Agent #4 (Vol Surface)**: IMP = "theoretical marker price, не IV"
  Доказательство: SR320CF6A имел IMP=55.5 и price=6.82, а реальный VOLAT=79.7%
- **Agent #6 (GEX)**: IMP = "волатильность в рублях, sigma_pct = IMP / spot"
  Получил sigma=15.4% для SBRF которая "экономически осмысленна"

**TODO**: проверить когда будем имплементировать. Скорее всего prav agent #4 — IV публикуется
только через `optionboard.json` поле `VOLAT`. IMP — это margin или theor marker.

---

## Приоритет implementation

### Phase 1 — quick wins (без вычислений)
1. **futoi (FIZ vs YUR)** для RI, SBERF, GAZPF — smart money positioning
2. **Quarterly basis term structure** SBER, GAZP, RTS — RU-specific edge
3. **VIM6/VIN6 spread** — RVI term structure

### Phase 2 — vol surface (требует optionboard endpoint)
4. **Per-asset VRP** (ATM IV vs RV30) для SBER, GAZP, IMOEX отдельно
5. **RR25 skew** как фильтр / confirmation
6. **Term structure ATM IV** (IV30/IV90 ratio) как regime indicator

### Phase 3 — advanced
7. **Inverted basis regime detector** — backtest as stress switch
8. **Pre-roll calendar spread** strategy
9. **GEX magnet strikes** для exit timing
10. **Daily IMP snapshot DB** — собственный historical archive для backtest

### Что НЕ делать
- ✗ P/C ratio как primary signal (отравлен ритейлом)
- ✗ AFKS options (illiquid)
- ✗ Block trades flow на ритейл-объёмах (1-2 контракта в стакане)
- ✗ Делать direct options execution > 10M RUB на trade (рыночные impact-issues)

---

## Файлы (прототипы готовы)

| Файл | Что делает |
|---|---|
| `Bitcoin/test_moex_options_api.py` | Базовый клиент: list_options, history, snapshot |
| `Bitcoin/test_vol_surface_ru.py` | Term structure, RR25, BF25, Black-76 IV inverter |
| `Bitcoin/test_gex_ru.py` | GEX, DDG, vanna, charm + flip strike |
| `Bitcoin/test_positioning_ru.py` | P/C ratios, max pain, top OI strikes |
| `Bitcoin/test_basis_ru.py` | Quarterly basis, term structure, calendar spread |

Все скрипты работают live против MOEX ISS, протестированы на 2026-06-03.

---

## Сравнение с крипто-стратегией

| Параметр | Deribit BTC | MOEX FORTS |
|---|---|---|
| **Vol index публикуется** | DVOL native | RVI (composite) + VOLAT per strike |
| **History depth** | DVOL с 2021-03 | Chain с 2014, OI с 2022 |
| **Greeks** | Через chain | Считать самим |
| **24/7 trading** | ✓ | ✗ только биржа |
| **Smart money proxy** | Funding rate, block trades | **futoi FIZ/YUR** (уникально) |
| **Liquidity** | Глубокая на BTC/ETH | Только Si, RI, SBRF, GAZR |
| **Edge dispersion** | Скваты квантов | Mало квантов после 2022 → behavioural mispricings |
| **Funding cost** | Известна (perp funding) | Нет perp, но basis carry = (futures-spot)-RUSFAR |
| **Dividend in basis** | N/A | **Уникальный сигнал — discrete dividend drop** |

---

## Следующий шаг (предложение)

Самое quick-and-cheap для proof-of-concept:
1. Скачать historical `futoi` для RI/SBERF за 2020-2026
2. Скачать historical chain для SBER за 2018-2026, invert IV через Black-76
3. Посчитать per-asset VRP для SBER (ATM IV vs RV30)
4. Backtest простую long-only strategy: vrp_sber ≤ X → LONG SBER futures
5. Сравнить с baseline RVI strategy которую мы уже сделали

Если работает на SBER — переносим на GAZP. IMOEX через MIX options.

Это даст нам **первую options-derived стратегию на MOEX** через 1-2 дня работы.
