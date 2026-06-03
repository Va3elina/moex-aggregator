# SBER Options Indicators — Results Summary

Все опционные индикаторы построены для SBER на реальных данных MOEX FORTS,
2020-01 → 2026-05 (1592 торговых дня). SBER Buy & Hold за период: **+27%**.

Данные: 2.64M строк options chain → per-strike IV (Black-76 inversion) → полная surface.

---

## Полный набор построенных индикаторов (19)

| Категория | Индикаторы | Файл |
|---|---|---|
| **Vol surface** | atm_iv, rr25 (risk reversal), bf25 (butterfly), skew_slope | build_sber_option_indicators.py |
| **Term structure** | iv_7d, iv_30d, iv_90d, term_ratio_30_90 | то же |
| **Greeks agg** | gex_norm, dollar_gamma, flip_strike | то же |
| **Positioning** | max_pain, max_pain_dist, pc_oi_ratio, pc_vol_ratio | то же |
| **Derived** | straddle_move, varswap_iv, BL tail probs (rn_p_down10/up10/skew) | + option_advanced_metrics.py |
| **VRP** | vrp (atm−rv), vrp_vs (varswap−rv) | analyze_all_indicators.py |
| **Positioning (futoi)** | fiz_lsr, yur_lsr | analyze_sber_futoi.py |
| **Basis** | quarterly basis, carry residual | backtest_sber_basis.py |

Daily values: `sber_master_features.csv` (gitignored, rebuildable).

---

## Ранжирование по directional edge (Q5−Q1 spread на 60 дней)

| Индикатор | Q5−Q1 60d | Монотонность | Направление |
|---|---|---|---|
| pc_oi_ratio | +15.1% | +0.87 | ⚠ см. ниже |
| **fiz_lsr** | −12.8% | −0.87 | low → bullish (паника физиков = дно) |
| **term_ratio_30_90** | +11.8% | +0.76 | high → bullish (inverted term = стресс → разворот) |
| vrp_vs | +6.7% | +0.89 | high → bullish |
| pc_vol_ratio | +6.4% | +0.92 | high → bullish |
| gex_norm | +5.5% | +0.54 | high → bullish |
| rr25 | +5.1% | +0.78 | high → bullish |
| vrp (atm) | +4.5% | +0.85 | high → bullish |
| max_pain_dist | −4.4% | −0.71 | low → bullish |
| dollar_gamma | +3.7% | +0.72 | high → bullish |
| rn_p_down10 | +3.3% | +0.85 | high → bullish |
| atm_iv | −1.4% | +0.11 | шум (нелинейно) |
| varswap_iv | +1.3% | +0.49 | слабо |
| straddle_move | −0.7% | +0.11 | нет edge |
| bf25 | −0.8% | −0.01 | нет edge |

⚠ **pc_oi_ratio** показал сильнейший spread, НО: пользователь верно отметил что
OI/PC ненадёжны. Здесь паттерн нелинейный (экстремально call-heavy Q1 = retail
FOMO → bearish; средние значения → bullish). Использовать только как
confirmation, не primary. pc_vol_ratio чище (mono 0.92).

---

## Backtest результаты (LONG-only, hold 10-60d, fee 0.03%+slip)

### Одиночные сигналы
| Сигнал | Total | DD | N | WR |
|---|---|---|---|---|
| **term (IV30/IV90 ≥ 1.05)** | **+233%** | 32% | 77 | 52% |
| **pos (FIZ ≤ 1.6)** | **+160%** | 20% | 43 | 58% |
| volp (vrp_vs ≥ 8) | +80% | 42% | 68 | 53% |
| skew (rr25 ≥ −5) | +30% | 58% | 87 | 54% |
| flow (pc_vol ≥ 0.6) | −4% | 76% | 109 | 57% |

### Лучшие комбинации
| Комбо | Total | DD | WR |
|---|---|---|---|
| **FIZ≤1.4 AND ATM_IV≥30** (champion) | **+196%** | **13%** | **89%** |
| pos+flow | +146% | 19% | 60% |
| pos+term | +130% | 22% | 68% |
| pos+skew (lowest DD) | +71% | 17% | 50% |
| ensemble votes≥3 | +198% | 29% | 51% |

---

## Главные выводы

### 1. Два независимых сильных edge
- **Positioning (FIZ)** — паника физлиц = контртренд long. −12.8% spread.
- **Term structure (IV30/IV90)** — инвертированная кривая IV = острый стресс →
  разворот вверх. +11.8% spread. Это **чистый опционный сигнал**, которого нет
  на споте/futoi. Разные семейства данных → комбинируются.

### 2. Лучшая risk-adjusted: FIZ + опционный IV-фильтр
`FIZ ≤ 1.4 AND ATM_IV ≥ 30` → +196%, **DD всего 13%**, WR **89%**.
Опционный IV-фильтр (входим в контртренд только при повышенной волатильности)
**снижает просадку почти вдвое** vs FIZ alone (23%→13%). Это и есть value от
опционных данных.

### 3. RU работает обратно крипте (подтверждено по всем vol-метрикам)
- HIGH VRP → bullish (на крипте LOW VRP → bullish)
- HIGH varswap, HIGH term ratio → bullish
- Покупка пиковой паники (atm_iv Q5, экстрим) — НЕ работает (падающий нож)

### 4. Что НЕ дало edge
- atm_iv level (нелинейно, шум)
- straddle_move, bf25 (нет монотонности)
- flow (pc_vol) в одиночку (DD 76%)
- raw basis сигналы (см. backtest_sber_basis.py — слабо)

---

## Статус по RU_FULL_METRICS_PLAN

- ✅ Phase 1 (basis, futoi) — FIZ +205% проверен
- ✅ Phase 2 (vol surface: ATM IV, RR25, BF25, term, max pain, straddle) — построено
- ✅ Phase 3 (GEX [исправлены формулы], varswap, BL density) — построено
- ✅ Phase 4 (ensemble) — протестировано

---

## Следующие шаги (предложения)
1. **Out-of-sample / walk-forward** валидация (split 2020-2023 train / 2024-2026 test)
   — критично, многие комбо имеют малый N.
2. Перенести на **GAZP** (есть chain, ASSETCODE=GAZR) — проверить переносимость.
3. **IMOEX** через MIX опционы — индексный уровень.
4. SHORT side: использовать FIZ≥3.5 (euphoria) + term contango для шортов.
5. Pine implementation лучшей стратегии для live.

⚠ Caveat: B&H за период всего +27% (включает 2022 crash). Многие сигналы ловят
2022-bottom rally и пост-2022 recovery. Walk-forward обязателен перед выводами.
