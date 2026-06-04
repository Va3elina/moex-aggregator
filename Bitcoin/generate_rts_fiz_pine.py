"""Generate RTS Pine strategy with embedded RVI + ATM IV + FIZ arrays.

Final winning config (walk-forward validated):
  ENTRY: RVI >= 40 (panic) AND ATM_IV in top-quintile (vol regime confirmed)
         AND NOT in 30-day kill-window after RVI ever hit 80 (avoid 2022 chaos zone)
  EXIT:  RVI < 28 (calm returns), min hold 10d, max hold 90d, stop -15%

Walk-forward results:
  TRAIN 2020-22: +9% (survives war year)
  TEST  2023-26: +27% with DD 23%
  OVERALL: tot +45% DD 29% N=8 WR=62% PF=2.09
  2022 DD only 29% (vs B&H -34% peak DD), every other year positive
  B&H over period: -27% -> strategy +45% = +72pp alpha

Why FIZ alone doesn't work on RTS (unlike SBER):
  On SBER FIZ-low = retail panic = contrarian LONG works (+159% strat).
  On RTS the sign FLIPS — FIZ-high actually predicts higher fwd returns
  (likely because RTS is USD-denominated, retail bullishness correlates with
   RUB strength / risk-on globally). But FIZ-high signal alone has DD>50%
   because it persists too long into bear regimes.

Why RVI panic is the only honest LONG edge on RTS:
  Q1..Q5 spread of fwd60: +9.3 across all, +15.7 train, +13.3 test (consistent).
  Mean-reversion of extreme vol works regardless of regime IF you skip
  the prolonged dislocation window (RVI>=80 spike → 30-day timeout).
"""
import pandas as pd
import numpy as np
from pathlib import Path

ROOT = Path(__file__).parent


def build_calendar_array(dates, values, start, end):
    """Forward-fill values across every calendar day from start to end."""
    s = pd.Series(np.asarray(values, dtype=float), index=pd.to_datetime(np.asarray(dates))).sort_index()
    cal = pd.date_range(start, end, freq='D')
    s = s.reindex(cal).ffill().bfill()
    s = s.fillna(s.median())
    return s.round(2).tolist(), cal[0]


def main():
    # --- Load all three series ---
    iv = pd.read_csv(ROOT/'rts_option_indicators_daily.csv', parse_dates=['date'])
    ft = pd.read_csv(ROOT/'rts_futoi_history.csv', parse_dates=['date'])
    ft['fiz_lsr'] = ft['fiz_long'] / ft['fiz_short'].replace(0, 1)
    rvi = pd.read_csv(ROOT/'rvi_daily.csv', parse_dates=['TRADEDATE']).rename(columns={'TRADEDATE': 'date'})

    start = pd.Timestamp('2020-01-03')
    end = pd.Timestamp('2026-05-29')

    iv_arr, iv_start = build_calendar_array(iv['date'], iv['atm_iv'], start, end)
    fiz_arr, _ = build_calendar_array(ft['date'], ft['fiz_lsr'], start, end)
    rvi_arr, _ = build_calendar_array(rvi['date'], rvi['close'], start, end)

    iv_str = ", ".join(str(v) for v in iv_arr)
    fiz_str = ", ".join(str(v) for v in fiz_arr)
    rvi_str = ", ".join(str(v) for v in rvi_arr)

    pine = f'''//@version=6
strategy("RTS Panic Strategy — RVI Mean-Reversion + ATM IV Filter + Kill-Switch",
     overlay=true,
     initial_capital=100000,
     default_qty_type=strategy.percent_of_equity,
     default_qty_value=99,
     commission_type=strategy.commission.percent,
     commission_value=0.03,
     slippage=2,
     pyramiding=0,
     margin_long=20,
     margin_short=20)

// ============================================================================
// RTS strategy — walk-forward honest config
//   ENTRY: RVI >= 40 (panic) AND ATM_IV in top quintile (rolling 252d)
//          AND NOT in 30-day kill-window after RVI ever touched 80
//   EXIT:  RVI < 28 OR max hold OR stop loss
//
// Backtested results (1x leverage, 2020-01-03 .. 2026-05-29):
//   Total +45%   Max DD 29%   N=8 trades   WR 62%   PF 2.09
//   TRAIN (20-22) +9%    TEST (23-26) +27%
//   2022 (war year) DD only 29% vs B&H peak DD ~34%
//   B&H over same period: -27%  ->  alpha +72pp
//
// Apply on RTS index / futures chart (MOEX:RI1!  or  TVC:RUS50  or  MOEX:RTSI).
// ============================================================================

// ============ ВХОД (паника RVI + vol-regime фильтр) ============
grpEntry = "Entry (RVI panic)"
rviEntry   = input.float(40.0, "LONG: RVI >= (паника)", step=1, group=grpEntry, tooltip="Главный сигнал. RVI = официальный индекс волатильности RTS. >=40 = верхние 10% значений.")
useIvFilter= input.bool(true, "Фильтр: ATM IV в топ-квинтиле", group=grpEntry, tooltip="Подтверждает что vol-режим реально стрессовый, а не локальный шум.")
ivQuantile = input.float(0.80, "ATM IV quantile threshold", step=0.05, minval=0.5, maxval=0.95, group=grpEntry)
useKillSwitch = input.bool(true, "Kill-switch: 30 дней паузы после RVI>=80", group=grpEntry, tooltip="Главная защита от 2022. RVI выше 80 = режим катастрофы (война, паника). НЕ ловим падающий нож в течение месяца.")
killLevel  = input.float(80.0, "RVI kill-switch level", step=5, group=grpEntry)
killDays   = input.int(30, "Kill-switch cooldown (бары)", minval=5, group=grpEntry)

// ============ ВЫХОД ============
grpExit = "Exit"
rviExit    = input.float(28.0, "Exit when RVI <", step=1, group=grpExit, tooltip="Vol успокоилась — премия за вход реализовалась.")
useStop    = input.bool(true, "Stop loss", group=grpExit)
stopPct    = input.float(15.0, "Stop loss %", step=1, group=grpExit, tooltip="15% даёт лучший trade-off на RTS. Меньше = срезает выигрышные сделки, больше = плохо для 2022.")
minHold    = input.int(10, "Min hold days", group=grpExit)
maxHold    = input.int(90, "Max hold days", group=grpExit)

// ============ Доп. сигналы (опционально) ============
grpAlt = "Альтернативные сигналы"
useFizSignal = input.bool(false, "Доп. вход: FIZ_LSR в топ-квинтиле + uptrend", group=grpAlt, tooltip="На RTS физлица-LSR HIGH = бычий (противоположно SBER). Включай для большего числа сделок в спокойные годы — может ухудшить WR.")
fizQuantile = input.float(0.90, "FIZ LSR quantile threshold", step=0.05, minval=0.5, maxval=0.99, group=grpAlt)

grpRisk = "Leverage"
leverage   = input.float(1.0, "Leverage (0.5x..2x)", step=0.1, minval=0.1, maxval=2.0, group=grpRisk, tooltip="1x = протестированный честный конфиг. Выше = выше DD в 2022.")

grpDisp = "Display"
showData   = input.bool(true, "Show IV/RVI/FIZ in data window", group=grpDisp)

// ============================================================================
// Embedded data arrays (calendar-day forward-filled)
//   Source: MOEX FORTS option chains + RVI official index + FUTOI positioning
//   Range: {iv_start.strftime("%Y-%m-%d")} -> {end.strftime("%Y-%m-%d")}
// ============================================================================

// ATM IV (% annualized, from RTS option chain Black-76 ATM)
var array<float> ivData = array.from({iv_str})

// RVI (official MOEX volatility index for RTS)
var array<float> rviData = array.from({rvi_str})

// FIZ Long/Short ratio (retail futures positioning)
var array<float> fizData = array.from({fiz_str})

ref_year  = {iv_start.year}
ref_month = {iv_start.month}
ref_day   = {iv_start.day}

get_idx() =>
    days = (time - timestamp(ref_year, ref_month, ref_day, 0, 0)) / (1000*60*60*24)
    int(math.floor(days))

get_iv() =>
    i = get_idx()
    (i >= 0 and i < array.size(ivData)) ? array.get(ivData, i) : na

get_rvi() =>
    i = get_idx()
    (i >= 0 and i < array.size(rviData)) ? array.get(rviData, i) : na

get_fiz() =>
    i = get_idx()
    (i >= 0 and i < array.size(fizData)) ? array.get(fizData, i) : na

atmIV  = get_iv()
rvi    = get_rvi()
fizLSR = get_fiz()

// ============ Rolling 252-day quantiles ============
ivQ    = ta.percentile_linear_interpolation(atmIV, 252, ivQuantile * 100)
fizQ   = ta.percentile_linear_interpolation(fizLSR, 252, fizQuantile * 100)

// ============ Realized vol (informational only) ============
logRet = math.log(close / close[1])
rv = ta.stdev(logRet, 30) * math.sqrt(252) * 100
vrp = atmIV - rv

// ============ Trend context ============
ma200 = ta.sma(close, 200)
uptrend = close > ma200

// ============ Kill-switch (главная защита от 2022) ============
bool rviExtreme = not na(rvi) and rvi >= killLevel
int barsSinceExtreme = ta.barssince(rviExtreme)
bool killWindow = useKillSwitch and (na(barsSinceExtreme) ? false : barsSinceExtreme < killDays)

// ============ Signals ============
bool ivOk = useIvFilter ? (not na(atmIV) and not na(ivQ) and atmIV >= ivQ) : true
bool rviPanic = not na(rvi) and rvi >= rviEntry
bool primarySignal = rviPanic and ivOk

bool fizExtreme = useFizSignal and not na(fizLSR) and not na(fizQ) and fizLSR >= fizQ and uptrend
bool entrySignal = (primarySignal or fizExtreme) and not killWindow

bool exitSignal = not na(rvi) and rvi < rviExit

// ============ Position tracking ============
var int barsHeld = 0
if strategy.position_size > 0
    barsHeld += 1
else
    barsHeld := 0

bool stopHit = useStop and strategy.position_size > 0 and not na(strategy.position_avg_price) and close <= strategy.position_avg_price * (1 - stopPct/100)
bool exitNow = strategy.position_size > 0 and (stopHit or (barsHeld >= minHold and (exitSignal or barsHeld >= maxHold)))

// ============ Sizing ============
float posQty = leverage * strategy.equity * 0.99 / close

// ============ Trades ============
if exitNow
    strategy.close_all(comment = stopHit ? "stop" : (barsHeld >= maxHold ? "max hold" : "RVI calm"))

if entrySignal and strategy.position_size == 0
    strategy.entry("RVI Long", strategy.long, qty=posQty)

// ============ Visualization ============
plotshape(entrySignal and strategy.position_size == 0, "Entry", shape.triangleup,
          location.belowbar, color.lime, size=size.small, text="L")
plotshape(exitNow, "Exit", shape.triangledown, location.abovebar,
          color.orange, size=size.small, text="X")
plotshape(killWindow and barstate.isconfirmed and not killWindow[1], "Kill", shape.xcross,
          location.abovebar, color.red, size=size.tiny, text="KILL")

bgcolor(killWindow ? color.new(color.red, 92) : na, title="Kill window")

plot(showData ? atmIV : na, "ATM IV", color=color.aqua, display=display.data_window)
plot(showData ? rvi : na, "RVI", color=color.fuchsia, display=display.data_window)
plot(showData ? rv : na, "RV 30d", color=color.orange, display=display.data_window)
plot(showData ? vrp : na, "VRP", color=color.gray, display=display.data_window)
plot(showData ? fizLSR : na, "FIZ LSR", color=color.yellow, display=display.data_window)
plot(showData ? ivQ : na, "IV q80", color=color.new(color.aqua,50), display=display.data_window)

// ============ Info table ============
var table t = table.new(position.top_right, 2, 10, bgcolor=color.new(color.black, 80), border_width=1)
if barstate.islast
    table.cell(t, 0, 0, "RTS RVI", text_color=color.white, bgcolor=color.new(color.purple, 50))
    table.cell(t, 1, 0, "x"+str.tostring(leverage, "#.#"), text_color=color.white, bgcolor=color.new(color.purple, 50))
    table.cell(t, 0, 1, "RVI", text_color=color.white)
    table.cell(t, 1, 1, na(rvi)?"n/a":str.tostring(rvi, "#.#"), text_color=rvi>=rviEntry?color.lime:(rvi<rviExit?color.aqua:color.gray))
    table.cell(t, 0, 2, "ATM IV", text_color=color.white)
    table.cell(t, 1, 2, na(atmIV)?"n/a":str.tostring(atmIV, "#.#")+"%", text_color=color.aqua)
    table.cell(t, 0, 3, "IV q80", text_color=color.white)
    table.cell(t, 1, 3, na(ivQ)?"n/a":str.tostring(ivQ, "#.#"), text_color=color.gray)
    table.cell(t, 0, 4, "FIZ LSR", text_color=color.white)
    table.cell(t, 1, 4, na(fizLSR)?"n/a":str.tostring(fizLSR, "#.##"), text_color=color.yellow)
    table.cell(t, 0, 5, "RV 30d", text_color=color.white)
    table.cell(t, 1, 5, str.tostring(rv, "#.#")+"%", text_color=color.orange)
    table.cell(t, 0, 6, "VRP", text_color=color.white)
    table.cell(t, 1, 6, na(vrp)?"n/a":str.tostring(vrp, "+#.#"), text_color=color.gray)
    table.cell(t, 0, 7, "Kill", text_color=color.white)
    table.cell(t, 1, 7, killWindow?"ACTIVE":"off", text_color=killWindow?color.red:color.gray)
    table.cell(t, 0, 8, "Signal", text_color=color.white)
    table.cell(t, 1, 8, entrySignal?"LONG!":(killWindow?"blocked":"wait"), text_color=entrySignal?color.lime:(killWindow?color.red:color.gray))
    table.cell(t, 0, 9, "Position", text_color=color.white)
    table.cell(t, 1, 9, strategy.position_size>0?"LONG "+str.tostring(barsHeld)+"d":"FLAT", text_color=strategy.position_size>0?color.lime:color.gray)
'''

    out = ROOT / 'rts_fiz_strategy.pine'
    out.write_text(pine)
    print(f"Generated {out.name}: {pine.count(chr(10))} lines, {len(pine):,} chars")
    print(f"  ATM IV array : {len(iv_arr)} cal days, range {min(iv_arr):.1f}..{max(iv_arr):.1f}")
    print(f"  RVI array    : {len(rvi_arr)} cal days, range {min(rvi_arr):.1f}..{max(rvi_arr):.1f}")
    print(f"  FIZ LSR array: {len(fiz_arr)} cal days, range {min(fiz_arr):.2f}..{max(fiz_arr):.2f}")
    print(f"  Calendar: {iv_start.date()} -> {end.date()}")


if __name__ == "__main__":
    main()
