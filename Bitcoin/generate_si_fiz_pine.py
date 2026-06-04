"""Generate Si (USD/RUB futures) FIZ + VRP Pine strategy with embedded data.

Si specifics vs SBER:
  - FIZ are net LONG USD ~93% of days. Median LSR ~3.35.
  - FIZ-contrarian (LSR<=1.4 = retail panic on dip) is the only robust LONG entry.
  - Trend-following on USD/RUB fails — the pair round-trips (B&H only +13% in 6yr).
  - VRP exit (VRP<=-5) protects against volatility collapse.
  - Stop-loss critical: USD/RUB has explosive 1-day moves (e.g. 2022-03 +20%).

Best config from walkforward_si.py:
  FIZ<=1.4 entry, exit on FIZ>=3.0 or VRP<-5,
  stop=8%, minHold=10d, maxHold=60d, leverage 1.0-2.0x.

Embedded arrays = calendar-day-filled (ffill) ATM IV + FIZ LSR.
"""
import pandas as pd
import numpy as np
from pathlib import Path

ROOT = Path(__file__).parent


def build_calendar_array(dates, values, start, end):
    s = pd.Series(np.asarray(values), index=pd.to_datetime(np.asarray(dates))).sort_index()
    cal = pd.date_range(start, end, freq='D')
    s = s.reindex(cal).ffill().bfill()
    s = s.fillna(s.median())
    return s.round(2).tolist(), cal[0]


def main():
    iv = pd.read_csv(ROOT/'si_option_indicators_daily.csv', parse_dates=['date'])
    ft = pd.read_csv(ROOT/'si_futoi_history.csv', parse_dates=['date'])
    if 'fiz_lsr' not in ft.columns:
        ft['fiz_lsr'] = ft['fiz_long'] / ft['fiz_short'].replace(0, 1)

    start = pd.Timestamp('2020-01-03')
    end = pd.Timestamp('2026-05-19')

    iv_arr, iv_start = build_calendar_array(iv['date'], iv['atm_iv'], start, end)
    fiz_arr, fiz_start = build_calendar_array(ft['date'], ft['fiz_lsr'], start, end)

    iv_str = ", ".join(str(v) for v in iv_arr)
    fiz_str = ", ".join(str(v) for v in fiz_arr)

    pine = f'''//@version=6
strategy("Si FIZ Strategy — USD/RUB Retail Panic Entry + VRP Risk-Off Exit",
     overlay=true,
     initial_capital=100000,
     default_qty_type=strategy.percent_of_equity,
     default_qty_value=99,
     commission_type=strategy.commission.percent,
     commission_value=0.03,
     slippage=2,
     pyramiding=0,
     margin_long=10,
     margin_short=10)

// ============ Параметры ============
// Si (USD/RUB фьючерс) специфика:
//   - Физлица NET LONG USD 93% дней (median LSR ~3.35) — не как SBER где fiz обычно нетто short.
//   - FIZ-contrarian работает ТОЛЬКО на дне паники (LSR<=1.4) — редкий, но качественный сигнал.
//   - Trend-following умер: USD/RUB круглит, B&H 2020-26 всего +13%.
//   - VRP exit (VRP<=-5) защищает от схлопывания премии.
//   - Стоп критичен: USD/RUB взрывной (2022-03 +20%/день, 2025 разворот -25%).
//
// Walkforward (train 2020-22 vs test 2023-26): FIZ<=1.4 lev2 stop8 -> CAGR +14% DD 23% WR 55% PF 4.47.
// Test (true OOS 2023-26): CAGR +1% — честный потолок ~8-14% CAGR при leverage 1-2x.
grpEntry = "Entry (FIZ retail panic)"
fizEntry   = input.float(1.4, "LONG: FIZ LSR <= (паника физлиц USD)", step=0.1, group=grpEntry, tooltip="1.4 = winner (WR55 PF4.47). 1.6 = больше сделок, шире покрытие лет. 1.2 = строже.")

grpExit = "Exit"
fizExit    = input.float(3.0, "Exit when FIZ LSR >= (эйфория физлиц)", step=0.1, group=grpExit)
vrpExit    = input.float(-5.0, "Exit when VRP drops below (risk-off)", step=1, group=grpExit, tooltip="VRP схлопнулся = премия за риск исчезла, режут лосеров.")
useStop    = input.bool(true, "Stop loss", group=grpExit)
stopPct    = input.float(10.0, "Stop loss %", step=1, group=grpExit, tooltip="USD/RUB взрывной. 8-10% защита от 2025-разворотов.")
minHold    = input.int(10, "Min hold days", group=grpExit)
maxHold    = input.int(60, "Max hold days", group=grpExit)

grpRisk = "Leverage"
leverage   = input.float(1.0, "Leverage (1x..3x)", step=0.1, minval=0.1, maxval=3.0, group=grpRisk, tooltip="x1=safe (CAGR+8%), x2=AGGRESSIVE (CAGR+14% DD23%), x3=YOLO.")

grpDisp = "Display"
showData   = input.bool(true, "Show IV/RV/VRP/FIZ in data window", group=grpDisp)

// ============ Embedded ATM IV (Black-76 from MOEX Si option chain) ============
// Calendar-ffilled. Range: {iv_start.strftime("%Y-%m-%d")} -> {end.strftime("%Y-%m-%d")}
var array<float> ivData = array.from({iv_str})

// ============ Embedded FIZ Long/Short Ratio (Si futoi) ============
var array<float> fizData = array.from({fiz_str})

ref_year = {iv_start.year}
ref_month = {iv_start.month}
ref_day = {iv_start.day}

get_idx() =>
    days = (time - timestamp(ref_year, ref_month, ref_day, 0, 0)) / (1000*60*60*24)
    int(math.floor(days))

get_iv() =>
    i = get_idx()
    (i >= 0 and i < array.size(ivData)) ? array.get(ivData, i) : na

get_fiz() =>
    i = get_idx()
    (i >= 0 and i < array.size(fizData)) ? array.get(fizData, i) : na

atmIV = get_iv()
fizLSR = get_fiz()

// ============ Realized vol (RU: 252 trading days) ============
logRet = math.log(close / close[1])
rv = ta.stdev(logRet, 30) * math.sqrt(252) * 100

// ============ VRP ============
vrp = atmIV - rv

// ============ Signal ============
// ВХОД: физлица в панике на USD/RUB (LSR<=fizEntry) = локальное дно ruble strength
bool entrySignal = not na(fizLSR) and fizLSR <= fizEntry
// ВЫХОД: эйфория физлиц ИЛИ VRP схлопнулся (волатильность исчезла)
bool exitSignal  = (not na(fizLSR) and fizLSR >= fizExit) or (not na(vrp) and vrp <= vrpExit)

// ============ Position tracking ============
var int barsHeld = 0
if strategy.position_size > 0
    barsHeld += 1
else
    barsHeld := 0

// стоп-лосс: цена упала на stopPct% от средней входа
bool stopHit = useStop and strategy.position_size > 0 and not na(strategy.position_avg_price) and close <= strategy.position_avg_price * (1 - stopPct/100)
bool exitNow = strategy.position_size > 0 and (stopHit or (barsHeld >= minHold and (exitSignal or barsHeld >= maxHold)))

// ============ Sizing ============
float posQty = leverage * strategy.equity * 0.99 / close

// ============ Trades ============
if exitNow
    strategy.close_all(comment = stopHit ? "stop" : (barsHeld >= maxHold ? "max hold" : "signal exit"))

if entrySignal and strategy.position_size == 0
    strategy.entry("Si Long USD", strategy.long, qty=posQty)

// ============ Visualization ============
plotshape(entrySignal and strategy.position_size == 0, "Entry", shape.triangleup,
          location.belowbar, color.lime, size=size.small, text="L")
plotshape(exitNow, "Exit", shape.triangledown, location.abovebar,
          color.orange, size=size.small, text="X")

plot(showData ? atmIV : na, "ATM IV", color=color.aqua, display=display.data_window)
plot(showData ? rv : na, "Realized Vol 30d", color=color.orange, display=display.data_window)
plot(showData ? vrp : na, "VRP", color=vrp <= vrpExit ? color.red : color.gray, display=display.data_window)
plot(showData ? fizLSR : na, "FIZ LSR", color=fizLSR <= fizEntry ? color.lime : color.fuchsia, display=display.data_window)

// ============ Info table ============
var table t = table.new(position.top_right, 2, 8, bgcolor=color.new(color.black, 80), border_width=1)
if barstate.islast
    table.cell(t, 0, 0, "Si FIZ", text_color=color.white, bgcolor=color.new(color.blue, 50))
    table.cell(t, 1, 0, "x"+str.tostring(leverage, "#.#"), text_color=color.white, bgcolor=color.new(color.blue, 50))
    table.cell(t, 0, 1, "FIZ LSR", text_color=color.white)
    table.cell(t, 1, 1, na(fizLSR)?"n/a":str.tostring(fizLSR, "#.##"), text_color=fizLSR<=fizEntry?color.lime:(fizLSR>=fizExit?color.red:color.gray))
    table.cell(t, 0, 2, "ATM IV", text_color=color.white)
    table.cell(t, 1, 2, na(atmIV)?"n/a":str.tostring(atmIV, "#.#")+"%", text_color=color.aqua)
    table.cell(t, 0, 3, "VRP", text_color=color.white)
    table.cell(t, 1, 3, na(vrp)?"n/a":str.tostring(vrp, "+#.#"), text_color=vrp<=vrpExit?color.red:color.gray)
    table.cell(t, 0, 4, "RV 30d", text_color=color.white)
    table.cell(t, 1, 4, str.tostring(rv, "#.#")+"%", text_color=color.orange)
    table.cell(t, 0, 5, "Signal", text_color=color.white)
    table.cell(t, 1, 5, entrySignal?"LONG!":"wait", text_color=entrySignal?color.lime:color.gray)
    table.cell(t, 0, 6, "Position", text_color=color.white)
    table.cell(t, 1, 6, strategy.position_size>0?"LONG "+str.tostring(barsHeld)+"d":"FLAT", text_color=strategy.position_size>0?color.lime:color.gray)
    table.cell(t, 0, 7, "Equity", text_color=color.white)
    table.cell(t, 1, 7, str.tostring(strategy.equity, "#"), text_color=color.white)
'''

    out = ROOT / 'si_fiz_strategy.pine'
    out.write_text(pine)
    print(f"Generated {out.name}: {pine.count(chr(10))} lines, {len(pine)} chars")
    print(f"  IV array: {len(iv_arr)} calendar days, IV range {min(iv_arr):.1f}-{max(iv_arr):.1f}%")
    print(f"  FIZ array: {len(fiz_arr)} days, range {min(fiz_arr):.2f}-{max(fiz_arr):.2f}")
    print(f"  Apply to: MOEX:SI1!  (continuous Si futures)")


if __name__ == "__main__":
    main()
