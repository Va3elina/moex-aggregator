---
name: moex-methodology-verify
description: Verify indicator methodology by comparing Фрейм data with external references. Use when user says "проверь методологию", "сверь с референсом", "почему у нас X% а у Y Z%", "данные не сходятся", "верни сезонность из TradingView", or any request involving numerical comparison between our API and external data sources (Seasonax, TradingView, headlines QUANTS, Investing.com, etc.). Also use when debugging why indicator values seem off.
---

# Verify Indicator Methodology

When our indicator doesn't match a reference source, this is a structured diagnostic process. Learned from multiple seasonality verification sessions (with Seasonax, TradingView PineScript, headlines QUANTS) where shallow comparisons wasted hours.

## ⚠️ Critical Rules

1. **Never assume the reference is right** — sometimes THEIR methodology is the problem
2. **Check period first** — 99% of "discrepancies" turn out to be different date ranges
3. **Arithmetic vs geometric mean** is the #1 methodology trap with volatile assets
4. **SA vs non-SA data** is the #2 trap (FRED sends SA, Rosstat raw)
5. **Our data is NOT always wrong** — verify before changing methodology

## The Diagnostic Checklist

Go through these IN ORDER. Most discrepancies resolve at step 1 or 2.

### Step 1: Are we comparing the same period?

Check:
- Start date: 2000 Q1 or 2000 first trading day or 1999 Dec close?
- End date: 2024 Dec or 2025 Q3 or "latest"?
- Current year included? (we learned: 2025 was bad for IMOEX, including it drops average)
- Holidays excluded? (e.g., bourse closed Jan 1-8)

**Real example:** Seasonality Q3 2024 +19.6% vs reference ~20% — turned out both used 2000-2024 excluding current year, values matched once we aligned.

### Step 2: Are we using the same base?

For YTD/seasonality:
- **Jan1-to-Dec31** — uses first trading day of year as base
- **Dec-to-Dec** — uses previous year's last close (captures NY gap ~1.65% for IMOEX)
- **Calendar-aligned** — uses exact calendar date (harder for MOEX with Russian holidays)

**Real example:** IMOEX Jan 2000: our result +4.23% vs Seasonax +3.36%. Difference = NY gap we included by using Dec close as base.

### Step 3: Arithmetic vs Geometric mean?

For multi-year averaging:

**Arithmetic mean** (our default, headlines QUANTS):
```
avg = sum(yearly_YTDs) / N
```
Fine for low-volatility assets. For volatile IMOEX (2009 +120%, 2008 -67%), arithmetic mean is **biased upward** by extremes.

**Geometric mean** (Seasonax, industry standard for returns):
```
geo_avg = (product(1 + yearly_YTDs))^(1/N) - 1
```
Represents "average year for a buy-and-hold investor". For IMOEX gives ~12% vs ~20% arithmetic.

**Compound daily averages** (Seasonax chart):
```
For each calendar date: avg daily return across all years
Then: cumulative product of those daily averages
```
Smooth curve, ends ~11.98% for IMOEX 2000-2024.

### Step 4: Same data source?

Different providers give slightly different IMOEX values:
- **ISS MOEX** (our source) — official exchange data
- **ICE:USDRUB** (TradingView) — different data for currency
- **Yahoo Finance** — adjusted for splits/divs differently
- **FRED** — OECD data, often seasonally adjusted

**Difference magnitude:** ±1-3% per month accumulates to several percent annually.

### Step 5: Same close-to-close methodology?

For monthly returns:
- **Close-to-close** (our default): `close(end_month) / close(end_prev_month) - 1`
- **Within-month**: `close(end_month) / close(first_day_of_month) - 1`
- **Open-to-close** (rare): based on daily open

**Real example:** Jan return difference:
- Ours (Dec-to-Dec close-to-close): +4.23%
- Seasonax (first-trade-day base): +3.36%
- Gap ≈ +0.87% = avg NY holiday gap

## Known References and Their Methods

### Seasonax (seasonax.com)
- Base: `first_close_of_year` (first trading day)
- Monthly: arithmetic mean of monthly returns
- Chart: compound of average daily returns (geometric style)
- Coverage: from IPO, no hard start
- API: paid, returns JSON with chart values + monthlyChartData

### headlines QUANTS (Telegram)
- Base: `last_close_of_prev_year` (Dec-to-Dec, matches our!)
- Yearly average: arithmetic mean of YTDs
- Coverage: "с 2000 г." usually means 2000-latest
- Verification: screenshots only, read carefully

### TradingView PineScript v6
- Default uses ticker's daily bars
- If you need specific methodology — write custom script
- Already have `scripts/tv_seasonality_check.pine` for seasonality comparison
- Coverage: depends on data provider in TV

### Investing.com / TradingEconomics
- Usually annual/quarterly summaries only
- Often SA (seasonally adjusted) — NOT comparable to our raw data for growth rates

### FRED (St. Louis Fed)
- Russian data mostly OECD-sourced, SEASONALLY ADJUSTED (SA)
- **Cannot stitch with our raw data** — growth rates differ by 1-5pp
- CSV download: `https://fred.stlouisfed.org/graph/fredgraph.csv?id={SERIES_ID}`

### World Bank API
- Only annual data for most series
- Russian ruble values via `NE.GDI.FTOT.CN` etc.
- Use for annual checks, not quarterly

## Standard Verification Workflow

### 1. Gather data

```bash
# Our API
curl -sk "https://xn--80aklbnczmv.xn--p1ai/api/INDICATOR?params"

# Or direct to container (bypasses guest limits)
ssh root@103.88.243.232 "docker exec frame-api-1 python3 -c '...'"
```

### 2. Parse reference (usually JSON or screenshot)

For Seasonax JSON: extract `monthlyChartData.values` or `chart.values`
For screenshots: carefully transcribe numbers (easy to misread)
For TradingView: use our existing PineScript

### 3. Build comparison table

```python
# Template
months = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
our = [4.23, 0.53, 2.91, 2.26, 0.22, -0.11, 0.30, 2.41, -0.62, 0.42, 0.85, 2.24]
ref = [3.36, 0.13, 2.49, 2.05, -0.18, -0.28, 0.12, 2.18, -1.08, 0.02, 0.50, 2.10]

for i in range(12):
    d = our[i] - ref[i]
    print(f"{months[i]}: {our[i]:+.2f}% vs {ref[i]:+.2f}% → Δ={d:+.2f}%")
```

### 4. Check systematic vs random differences

- **Random** (some +, some -): different data sources, small data issues
- **Systematic** (all same direction): methodology difference
- **One month outlier**: specific event or base difference (January usually = NY gap)

### 5. Apply reference method to our data

If reference uses different base/method, try it on our data:

```python
# Method A: Dec-to-Dec (our default)
# Method B: Within-month
# Method C: Compound of daily averages

# Compare our method C vs their method → if match, methodology is the issue
```

## Decision Tree: Our Data Looks Wrong

```
START: "Our Y% vs reference X%"
│
├── Period identical?  ──── NO → Align periods and retry
│                         YES
│                          │
├── Data source same? ─── NO → Difference is data provider (accept as-is)
│                         YES
│                          │
├── Methodology same? ─── NO → We match different method (user chooses)
│                         YES
│                          │
└── Both should match    YES → Bug in our calculation (debug code)
```

## Our Current Defaults (memorize)

**Seasonality:**
- Monthly: close-to-close, Dec-to-Dec base
- Yearly: arithmetic mean of YTDs, calendar-aligned 252 buckets
- Excludes current incomplete year from averages

**Buffett:**
- Cap/GDP: monthly cap aggregation, GDP TTM (last 4 quarters interpolated)
- EMA(12) smoothing for monthly, EMA(60) for MCFTR/M2

**All indicators:**
- Russian rubles, current prices (not real)
- Raw data (not seasonally adjusted)
- ISS MOEX as primary source where applicable

## When to Trust the Reference

Good signs:
- Reference publishes their methodology
- Their numbers are internally consistent (compound ≈ chart end)
- Multiple references agree

Red flags:
- Suspicious single-source claims
- Screenshot without methodology
- Numbers "too round" (likely estimated)
- Big Russian market provider before 2007 (data integrity)

## Escalation

If after this checklist the discrepancy is unexplained:
1. Try the `moex-data-investigator` agent — deeper analysis
2. Document the investigation in `/memory/methodology_refs.md`
3. Ask the user to contact the reference source for their exact formula

## Торги выходного дня (с 2026-06-20) — учитывать при сверке
MOEX торгует в выходные (акции уже; валютные фьючерсы с 18.07; вечные S&P500/Nasdaq).
В БД есть субботние/воскресные свечи (спот type='stock') и выходной OI (interval=5).
Что это значит для методологии:
- **Сезонность (monthly) и breadth** считаются ТОЛЬКО по будням
  (`EXTRACT(ISODOW FROM ...) BETWEEN 1 AND 5`) — субботние свечи исключены. Если
  референс включает/исключает выходные иначе — это законный источник расхождения.
- **Heatmap** и weekday/monthday-сезонность тоже фильтруют выходные.
- **OI дневной (сигналы, interval=24)** — из ISS openpositions, выходные не отдаёт →
  только будни; выходная активность сворачивается биржей в клиринг понедельника.
- **OI/фьючерс-графики (intraday interval=5/60)** МОГУТ показывать выходные сессии.
- Если сверяешь OI/breadth/сезонность и видишь «лишний» выходной день — проверь
  ISODOW-фильтр в запросе. Детали — память `weekend_trading.md`.
