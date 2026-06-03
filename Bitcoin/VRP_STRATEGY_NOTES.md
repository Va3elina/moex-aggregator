# BTC/ETH VRP Adaptive Strategy — Research Notes

Research conducted 2026-05-30 to 2026-06-03 on /home/user/moex-aggregator.
Branch: `claude/bitcoin-candle-data-BSPBi`

## Core idea

Variance Risk Premium (VRP) = Implied Volatility (DVOL) − Realized Volatility (30d).

When VRP becomes very negative (IV cheap vs realized), market is oversold on volatility
expectations → spot tends to mean-revert UP. Used as LONG entry signal.

When LONG exits on DVOL spike, open SHORT (Always-In-Market mode) to capture
post-spike volatility crush and price retracement.

## Data sources

- **DVOL**: Deribit BTC-DVOL / ETH-DVOL, daily, 2021-03-24 onwards (1898 days)
  - Fetcher: `Bitcoin/fetch_dvol_full_history.py`
- **Funding**: Deribit BTC-PERPETUAL / ETH-PERPETUAL interest_8h, hourly
  - Mean +7.5%/yr LONG-pays, max +72%/yr in FOMO
  - Fetcher: `Bitcoin/fetch_bybit_funding.py`
- **Price**: Coinbase BTC-USD daily 2015+, ETH similar
- **RVI**: MOEX Russian Volatility Index, 2013-11 onwards (3154 days)
  - Fetcher: `Bitcoin/fetch_moex_rvi.py`

All CSV data is gitignored (refetchable from APIs).

---

## STRATEGY V1 — Original AIM (NO stop loss)

### Pine file
`Bitcoin/btc_vrp_adaptive_aim.pine` (generator: `Bitcoin/generate_crypto_aim_pines.py`)

### Logic
```
LONG entry:  VRP <= -10 (BTC) or VRP <= -25 (ETH)
LONG exit:   DVOL >= 70 after min 60 days, or max hold 180 days
SHORT entry: immediately after LONG exit (if Always-In-Market enabled)
SHORT exit:  next VRP signal (which opens new LONG), or max hold 365 days safety
```

### Pine parameters
- `vrpEntry` = -10 (BTC) / -25 (ETH)
- `dvolExitLevel` = 70
- `minHoldBull` = 60, `maxHoldBull` = 180
- `minHoldBear` = 30, `maxHoldBear` = 90
- `shortMaxHold` = 365 (safety)
- `leverage` = 1.0..5.0 input
- `margin_long` = 10, `margin_short` = 10 (constants, give 10x buffer)

### Results — Cherry-picked window 2023-09 to 2026-05 (977d, BH +211%)
| Leverage | Total | DD | Trades |
|---|---|---|---|
| 1x | +521% | 20% | 4L+3S |
| 2x | +2399% | 36% | 4L+3S |
| 3x | +7687% | 50% | 4L+3S |
| 5x | +47245% | 72% | 4L+3S |

With realistic Bybit funding (1.5x Deribit) + 0.055% fee + 3bp slippage.
WR = 100% (every trade profitable).

### Results — FULL HISTORY 2021-03 to 2026-05 (1875d, BH +54%)
| Leverage | Total | DD | Outcome |
|---|---|---|---|
| 1x | +242% | 74% | Survived (16 trades, WR 62%) |
| 2x | **-100%** | 100% | **LIQUIDATED** in 2021-10 |
| 3x | -100% | 100% | LIQUIDATED |
| 5x | -99% | 99% | LIQUIDATED |

### Critical failure mode
2021-08-01 AIM opened SHORT after LONG exit. BTC then rallied from ~$40k to $69k ATH
(+73%) by November 2021. At 2x leverage, ~-45% adverse move = liquidation.

### Verdict
**1x only**. Original AIM is FRAGILE — works in calm bull markets, dies in
parabolic rallies. Backtest 2023-2026 was statistically biased (cherry-picked period).

---

## STRATEGY V2 — Improved AIM with SHORT stop loss

### Logic difference from V1
- SHORT closes immediately if BTC moves +20% against entry
- Everything else unchanged

### Results — FULL HISTORY 2021-2026
| Leverage | Total | DD | Outcome |
|---|---|---|---|
| 1x | **+268%** | 67% | Survived |
| 2x | +325% | 95% | Survived but close to liquidation |
| 3x | -100% | 100% | Still liquidated |

### Other variants tested (from `Bitcoin/vrp_stress_with_filters.py`)
| Variant | 1x | 2x |
|---|---|---|
| SHORT max hold 60d only | +119% / DD 62% | +25% / DD 93% |
| SHORT max hold 30d only | +87% / DD 66% | +24% / DD 94% |
| SHORT only in BEAR regime | +75% / DD 74% | LIQUIDATED |
| Combo: stop+20% + bear-only + max60d | +128% / DD 62% | +101% / DD 93% |
| **LONG-ONLY (no SHORT)** | +159% / DD 62% | +197% / DD 92% |

### Verdict
For real trading: **1x leverage + SHORT stop +20%** is the responsible config.
At 1x, alpha over BH = +268% − 54% = +214pp over 5 years.

Pine V2 implementation is **NOT YET DONE** — TODO: add `shortStopPct` input
to `generate_crypto_aim_pines.py` and regenerate Pine files.

---

## RU MARKET — Different mechanics

### Key finding
On Russian market VRP works **OPPOSITE direction** to crypto:
- Crypto: BUY when VRP <= -10 (fear oversold)
- RU:     BUY when VRP >= +20 OR RVI >= 40 (panic spike, mean reversion)

AlwaysInMarket SHORT mode **DOES NOT WORK on RU** (-2% to -68% across IMOEX).
Cause: Russian retail can't short cheaply; funding costs eat alpha.

### Pine files
- `Bitcoin/imoex_rvi_strategy.pine`
- `Bitcoin/afks_rvi_strategy.pine`
- Generator: `Bitcoin/generate_ru_pines.py`

### Best stock result
AFKS: strategy +62% vs Buy & Hold -72% (+134pp alpha) over 2014-2026.

---

## Realistic execution costs (Bybit)

| Cost | Value | Per round trip |
|---|---|---|
| Maker fee | 0.02% | 0.04% |
| Taker fee | 0.055% | 0.11% |
| Slippage | 3bp | 6bp |
| Funding (LONG, avg) | +7.5%/yr | scales with hold time × leverage |
| Funding (SHORT) | -7.5%/yr (received) | partially offsets long cost |

For 2x AIM on 977d period: total fees ~$11, total funding NET ~$68 on $100 initial.
Real-world drag: ~7% of gross profit.

---

## Bugs encountered and fixed (Pine v6 quirks)

1. `margin_long=0` → infinite leverage (caused fake +31M% returns)
   - Fix: use `margin_long=10` (not 0)
2. `strategy.entry(qty=99)` → 99 raw BTC contracts, not 99% equity
   - Fix: use `qty = leverage * strategy.equity * 0.99 / close`
3. `default_qty_value=99` is OK when `qty` not passed explicitly
4. Multi-line ternaries don't work in Pine — rewrite as `if/else`
5. `input.float` for margin params doesn't work — must be const in `strategy()`

---

## Open TODO

- [ ] Update Pine V1 → V2 with SHORT stop loss in generator
- [ ] Test on ETH with full 2021-2026 history (probably same liquidation risk)
- [ ] Add intraday entry timing (don't open at close on signal day)
- [ ] Test combined RU + crypto portfolio (uncorrelated VRP signals)
- [ ] Forward test 6 months before going live

---

## Files index

| Purpose | File |
|---|---|
| BTC adaptive Pine generator | `Bitcoin/generate_crypto_aim_pines.py` |
| RU RVI Pine generator | `Bitcoin/generate_ru_pines.py` |
| DVOL history fetcher | `Bitcoin/fetch_dvol_full_history.py` |
| Funding history fetcher | `Bitcoin/fetch_bybit_funding.py` |
| RVI fetcher | `Bitcoin/fetch_moex_rvi.py` |
| Cherry-picked backtest | `Bitcoin/vrp_aim_with_funding.py` |
| Real-funding backtest | `Bitcoin/vrp_aim_real_funding.py` |
| Full-history stress test | `Bitcoin/vrp_stress_test_2021_2026.py` |
| Variants comparison | `Bitcoin/vrp_stress_with_filters.py` |
| RU backtest | `Bitcoin/vrp_ru_with_rvi.py` |
