# RU Options Strategy — Full Metrics Coverage Plan

Полный план реализации **всех 24 метрик** из `OPTIONS_DATA_CATALOG.md` на MOEX данных.
Чек-лист чтобы ничего не упустить.

Базовые активы: **SBER, GAZP, IMOEX** (AFKS отброшен — illiquid).

---

## 1. Volatility surface — структура IV

| # | Метрика | Сигнал | Status | Реализация |
|---|---|---|---|---|
| 1.1 | **Term structure** (IV 7d / 30d / 90d) | Backwardation = stress → mean revert | 🟡 Скрипт готов, ждёт chain | `build_iv_term_structure.py` |
| 1.2 | **25-delta Risk Reversal** (callIV − putIV) | RR << 0 = страх put'ов = local bottom | 🟡 Готов в `test_vol_surface_ru.py`, надо historical | + Black-76 inversion |
| 1.3 | **Butterfly 25d** (OTM IV − ATM IV) | High butterfly = jump expected | 🟡 Готов | то же |
| 1.4 | **ATM IV vs RV30d** (per-asset VRP) | Negative VRP = oversold vol → LONG | 🟡 Готов `build_atm_iv_history.py` | то же |
| 1.5 | **Skew slope** (full smile gradient) | Steep = tail risk pricing | 🆕 TODO | надо добавить |

## 2. Greeks aggregation — gamma positioning

| # | Метрика | Сигнал | Status | Реализация |
|---|---|---|---|---|
| 2.1 | **GEX raw** (Σ OI × gamma × spot²) | Magnet zones | 🟡 `test_gex_ru.py` — live snapshot работает | надо historical |
| 2.2 | **Dealer Dollar Gamma** (top strikes by DDG) | Support/resistance levels | 🟡 Готов | то же |
| 2.3 | **Vanna exposure** (∂delta/∂vol) | Squeeze potential | 🟡 Готов | то же |
| 2.4 | **Charm** (theta-decay delta) | Pinning к expiry | 🟡 Готов | то же |
| 2.5 | ⚠ **Dealer-side direction** | Кто net long/short | ❌ Невозможно — нет classifier MOEX | — |

**Замечание**: на MOEX нельзя определить кто net long gamma (dealers vs end-users). Поэтому GEX-метрики дают "raw intensity by strike", не dealer hedge direction. Полезно для support/resistance levels, не для regime detection.

## 3. Positioning / sentiment

| # | Метрика | Сигнал | Status | Реализация |
|---|---|---|---|---|
| 3.1 | ❌ **Put/Call OI ratio** | Отравлен ритейлом 80%, **user verified не работает** | dead-end | — |
| 3.2 | ❌ **Put/Call volume ratio** | То же | dead-end | — |
| 3.3 | **Max pain** | Pinning to strike | 🟡 Готов `test_positioning_ru.py` | надо historical |
| 3.4 | **OI concentration** (top strikes) | Support/resistance | 🟡 Готов | то же |
| 3.5 | 🆕 **futoi FIZ/YUR breakdown** | Уникальный для RU smart money proxy | ✅ **Доказан +205% на SBER** | `analyze_sber_futoi.py` |

## 4. Flow / smart money

| # | Метрика | Сигнал | Status | Доступно? |
|---|---|---|---|---|
| 4.1 | **Block trades** (OFFMARKETDEAL flag) | OTC institutional | ⚠ Live only, нет history | можно scrape ежедневно |
| 4.2 | **Volume / OI > 1** detector | Fresh positioning | 🟡 TODO | из chain history |
| 4.3 | **Sweep orders** | Aggressive informed | ⚠ Нет в публичном API | — |
| 4.4 | **Far OTM call buying** | Lottery FOMO → contrarian bear | 🟡 TODO | из trades + chain |
| 4.5 | **Deep ITM put buying** | Smart money hedging → bear | 🟡 TODO | то же |
| 4.6 | 🆕 **futoi 7d flow** (Δ YUR positioning) | Институциональный re-positioning | ✅ В analyze | partial signal |

## 5. Derived market expectations

| # | Метрика | Сигнал | Status | Реализация |
|---|---|---|---|---|
| 5.1 | **Options-implied probability** (Breeden-Litzenberger) | Density над strikes на expiry | 🆕 TODO | из chain |
| 5.2 | **Straddle implied move** (ATM call+put) | Expected range к expiry | 🆕 TODO | простой расчёт |
| 5.3 | **Implied forward** (put-call parity) | Where market thinks spot will be | 🆕 TODO | из chain |
| 5.4 | **Variance swap rate** | Чистый IV без strike bias | 🆕 TODO | из full chain |

## 6. Futures basis

| # | Метрика | Сигнал | Status | Реализация |
|---|---|---|---|---|
| 6.1 | **Quarterly basis** | Contango/backwardation | 🟡 Готов live `test_basis_ru.py` | надо historical |
| 6.2 | **Term structure of basis** (front/back) | Stress indicator | 🟡 Готов | то же |
| 6.3 | **Calendar spread** (между quarterlies) | Mechanical roll arb | 🟡 Готов | то же |
| 6.4 | **Carry residual** = basis% − RUSFAR% | Чистый positioning premium | 🆕 TODO | basis + RUSFAR (есть) |
| 6.5 | **Embedded dividend** в basis | RU-specific signal (deviation from consensus) | 🆕 TODO | basis минус theoretical |
| 6.6 | 🆕 **Inverted basis regime** (back-month < front) | Rare stress signal (Feb 2022, мобилизация) | 🆕 TODO | detector |

---

## Implementation roadmap

### Phase 1 — БЕЗ options chain (можем делать сейчас)
- [x] futoi FIZ LSR signal → +205% на SBER ✅
- [ ] **Quarterly basis backtest** (есть SBER futures data partial)
- [ ] **Carry residual** = basis − RUSFAR
- [ ] **Inverted basis regime detector** на 2020-2026
- [ ] **Calendar spread signal** (front vs next quarter)

### Phase 2 — С options chain (когда дозальётся, ~30 мин)
- [ ] **ATM IV history** через Black-76 inversion (ready pipeline)
- [ ] **Per-asset VRP** = ATM_IV − RV30
- [ ] **Term structure** (7d/30d/90d) historical
- [ ] **25d Risk Reversal** historical
- [ ] **25d Butterfly** historical
- [ ] **Max pain** historical
- [ ] **Volume/OI > 1** detector
- [ ] **Straddle implied move** historical
- [ ] **Implied forward** (put-call parity check)

### Phase 3 — продвинутые
- [ ] **GEX raw intensity** historical (per-strike gamma)
- [ ] **Dealer Dollar Gamma** historical
- [ ] **Vanna / Charm** historical
- [ ] **Variance swap rate** (более robust чем ATM IV)
- [ ] **Options-implied probability distribution** (Breeden-Litzenberger)
- [ ] **Embedded dividend** в basis

### Phase 4 — Mega-signal комбинация
Объединить top сигналы в **ensemble strategy**:
1. **FIZ extremes** (proven +205%)
2. **VRP** (Phase 2)
3. **RR25 skew** (Phase 2)
4. **Inverted basis** (Phase 1)
5. **Carry residual** (Phase 1)

Каждый сигнал = vote. Когда 3+ голосов совпали → high-conviction entry.

---

## Что отброшено навсегда

| Метрика | Причина |
|---|---|
| Raw Open Interest | User-verified бесполезный сам по себе |
| P/C OI ratio | Retail-poisoned (80% phys persons) + OI не работает |
| P/C volume ratio | То же |
| Sweep orders detection | Нет в публичном API MOEX |
| Cross-exchange basis arb | Нет ликвидной альтернативы MOEX для RU underlying |
| Dealer-side GEX direction | Нет classifier'а кто dealer на MOEX |

---

## Файлы (что готово / что строится)

| Файл | Phase | Status |
|---|---|---|
| `black76_iv.py` | 2 | ✅ Tested 9/9 |
| `build_atm_iv_history.py` | 2 | ✅ Waiting for data |
| `build_iv_term_structure.py` | 2 | ✅ Waiting for data |
| `test_vol_surface_ru.py` | 2 | ✅ Live works |
| `test_gex_ru.py` | 3 | ✅ Live works |
| `test_positioning_ru.py` | 2 | ✅ Live works |
| `test_basis_ru.py` | 1 | ✅ Live works |
| `analyze_sber_futoi.py` | 1 | ✅ Done |
| `backtest_sber_fiz_signal.py` | 1 | ✅ +205% |
| `fetch_sber_chain_fast.py` + `_fast2.py` | 1→2 | 🟡 Running ~30 min |
| TODO: `backtest_sber_basis.py` | 1 | новый |
| TODO: `build_imp_distribution.py` (Breeden-Litz) | 3 | новый |
| TODO: `build_straddle_move.py` | 2 | новый |
| TODO: `build_variance_swap.py` | 3 | новый |
| TODO: `backtest_ensemble.py` | 4 | новый |
