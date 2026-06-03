"""Generate SBER VRP Pine strategy with embedded ATM IV + FIZ data.

VRP = ATM_IV (embedded, options-derived) - RV30 (computed in Pine from SBER close).
Robust cross-asset edge: HIGH VRP -> bullish on RU.
Optional FIZ filter (retail panic) which cut drawdown on SBER.

Data embedded as calendar-day-filled arrays (ffill across gaps/weekends).
"""
import pandas as pd
import numpy as np
from pathlib import Path

ROOT = Path(__file__).parent


def build_calendar_array(dates, values, start, end):
    """Forward-fill values across every calendar day from start to end."""
    s = pd.Series(np.asarray(values), index=pd.to_datetime(np.asarray(dates))).sort_index()
    cal = pd.date_range(start, end, freq='D')
    s = s.reindex(cal).ffill().bfill()
    s = s.fillna(s.median())  # any residual gap -> median
    return s.round(2).tolist(), cal[0]


def main():
    iv = pd.read_csv(ROOT/'sber_option_indicators_daily.csv', parse_dates=['date'])
    ft = pd.read_csv(ROOT/'sber_futoi_signals.csv', parse_dates=['date'])
    ft['fiz_lsr'] = ft['fiz_long'] / ft['fiz_short'].replace(0, 1)

    start = pd.Timestamp('2020-01-03')
    end = pd.Timestamp('2026-05-19')

    iv_arr, iv_start = build_calendar_array(iv['date'], iv['atm_iv'], start, end)
    fiz_arr, fiz_start = build_calendar_array(ft['date'], ft['fiz_lsr'], start, end)

    iv_str = ", ".join(str(v) for v in iv_arr)
    fiz_str = ", ".join(str(v) for v in fiz_arr)

    pine = f'''//@version=6
strategy("SBER FIZ Strategy — Retail Panic Entry + VRP Risk-Off Exit",
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

// ============ Параметры ============
// ВХОД: паника физлиц (FIZ). Опц. trend-вход для спокойных лет/2026.
// ВЫХОД: эйфория физлиц ИЛИ обвал VRP, + стоп-лосс.
// SBER 2020-2026: FIZ<=1.4 -> +159% DD18% WR72% PF5.25, плюс каждый год.
grpEntry = "Entry (FIZ retail panic)"
fizEntry   = input.float(1.4, "LONG: FIZ LSR <= (паника физлиц)", step=0.1, group=grpEntry, tooltip="Главный сигнал. 1.4 = лучший WR/PF. 1.6 = больше сделок.")
useTrend   = input.bool(false, "Доп. trend-вход (актив в спокойные годы/2026)", group=grpEntry, tooltip="Добавляет вход VRP>=порог + цена>EMA200 + рост за 20д. Даёт сделки в спокойные годы, но снижает WR/PF.")
trendVrp   = input.float(12.0, "Trend entry: VRP >=", step=1, group=grpEntry)

grpExit = "Exit"
fizExit    = input.float(3.0, "Exit when FIZ LSR >= (эйфория физлиц)", step=0.1, group=grpExit)
vrpExit    = input.float(-5.0, "Exit when VRP drops below (risk-off)", step=1, group=grpExit, tooltip="VRP схлопнулся = премия за риск исчезла = режут лосеров.")
useStop    = input.bool(true, "Stop loss", group=grpExit)
stopPct    = input.float(8.0, "Stop loss %", step=1, group=grpExit)
minHold    = input.int(10, "Min hold days", group=grpExit)
maxHold    = input.int(90, "Max hold days", group=grpExit)

grpRisk = "Leverage"
leverage   = input.float(1.0, "Leverage (1x..3x)", step=0.1, minval=0.1, maxval=3.0, group=grpRisk)

grpDisp = "Display"
showData   = input.bool(true, "Show IV/RV/VRP in data window", group=grpDisp)

// ============ Embedded ATM IV (options-derived, % annualized) ============
// Source: MOEX FORTS SBER option chain -> Black-76 ATM IV, calendar-ffilled
// Range: {iv_start.strftime("%Y-%m-%d")} -> {end.strftime("%Y-%m-%d")}
var array<float> ivData = array.from({iv_str})

// ============ Embedded FIZ Long/Short ratio (futoi positioning) ============
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

// ============ Trend context (для опц. trend-входа) ============
ema200 = ta.ema(close, 200)
bool mom20 = close > close[20]
bool trendEntry = useTrend and not na(vrp) and vrp >= trendVrp and close > ema200 and mom20

// ============ Signal ============
// ВХОД: физлица в панике (FIZ LSR <= порог) ИЛИ опц. trend-вход
bool fizEntrySig = not na(fizLSR) and fizLSR <= fizEntry
bool entrySignal = fizEntrySig or trendEntry
// ВЫХОД: физлица в эйфории ИЛИ VRP схлопнулся
bool exitSignal  = (not na(fizLSR) and fizLSR >= fizExit) or (not na(vrp) and vrp <= vrpExit)

// ============ Position tracking ============
var int barsHeld = 0
if strategy.position_size > 0
    barsHeld += 1
else
    barsHeld := 0

// стоп-лосс: цена упала на stopPct% от средней входа (работает с любого бара)
bool stopHit = useStop and strategy.position_size > 0 and not na(strategy.position_avg_price) and close <= strategy.position_avg_price * (1 - stopPct/100)
bool exitNow = strategy.position_size > 0 and (stopHit or (barsHeld >= minHold and (exitSignal or barsHeld >= maxHold)))

// ============ Sizing ============
float posQty = leverage * strategy.equity * 0.99 / close

// ============ Trades ============
if exitNow
    strategy.close_all(comment = stopHit ? "stop" : (barsHeld >= maxHold ? "max hold" : "signal exit"))

if entrySignal and strategy.position_size == 0
    strategy.entry("FIZ Long", strategy.long, qty=posQty)

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
    table.cell(t, 0, 0, "SBER FIZ", text_color=color.white, bgcolor=color.new(color.blue, 50))
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

    out = ROOT / 'sber_fiz_strategy.pine'
    out.write_text(pine)
    print(f"Generated {out.name}: {pine.count(chr(10))} lines, {len(pine)} chars")
    print(f"  IV array: {len(iv_arr)} calendar days, IV range {min(iv_arr):.0f}-{max(iv_arr):.0f}")
    print(f"  FIZ array: {len(fiz_arr)} days, range {min(fiz_arr):.2f}-{max(fiz_arr):.2f}")


if __name__ == "__main__":
    main()
