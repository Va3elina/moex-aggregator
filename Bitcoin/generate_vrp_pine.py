"""Generate Pine v6 strategy with embedded BTC DVOL data.

Strategy:
  - Realized vol computed in Pine (annualized 30-day stdev of log returns × √365 × 100)
  - DVOL from embedded array (Deribit data 2023-09 onwards)
  - VRP = DVOL - Realized
  - BUY when VRP ≤ -10 (rare oversold-vol condition)
  - HOLD 60 days
  - SELL
"""
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(__file__).parent


def main():
    # Load DVOL
    dvol = pd.read_csv(ROOT / 'btc_dvol_daily.csv', parse_dates=['date']).set_index('date').sort_index()
    print(f"DVOL data: {dvol.index[0].date()} → {dvol.index[-1].date()} ({len(dvol)} days)")

    # Use close values
    values = dvol['close'].round(1).tolist()
    values_str = ", ".join(str(v) for v in values)

    start_date = dvol.index[0]
    end_date = dvol.index[-1]
    print(f"Reference start (index 0): {start_date.date()}")

    pine = f'''//@version=6
strategy("BTC VRP Signal — Smart Exit (Min Hold + DVOL Spike)",
     overlay=true,
     initial_capital=100,
     default_qty_type=strategy.percent_of_equity,
     default_qty_value=99,
     commission_type=strategy.commission.percent,
     commission_value=0.05,
     slippage=3)

// === Параметры ===
vrpThreshold  = input.float(-10.0, "VRP threshold to ENTER", step=1)
minHoldDays   = input.int(60,    "Min hold days before exit allowed", minval=1)
dvolExitLevel = input.float(70.0, "DVOL spike exit threshold", step=1)
maxHoldDays   = input.int(180,   "Max hold days (force exit)", minval=1)
rvWindow      = input.int(30,    "Realized vol lookback (days)", minval=5)
showVRP       = input.bool(true, "Show VRP info")

// === DVOL embedded data ===
// Reference start: {start_date.strftime("%Y-%m-%d")} (index 0)
// Source: Deribit BTC Volatility Index (DVOL)
// Updated through: {end_date.strftime("%Y-%m-%d")}

var array<float> dvolData = array.from({values_str})

ref_year  = {start_date.year}
ref_month = {start_date.month}
ref_day   = {start_date.day}

// === Get DVOL for current bar ===
get_dvol_idx() =>
    days_since = (time - timestamp(ref_year, ref_month, ref_day, 0, 0)) / (1000 * 60 * 60 * 24)
    int(math.floor(days_since))

get_dvol() =>
    idx = get_dvol_idx()
    if idx >= 0 and idx < array.size(dvolData)
        array.get(dvolData, idx)
    else
        na

dvol = get_dvol()

// === Realized volatility ===
logRet = math.log(close / close[1])
rv = ta.stdev(logRet, rvWindow) * math.sqrt(365) * 100

// === VRP ===
vrp = dvol - rv

// === Entry signal ===
entrySignal = not na(vrp) and vrp <= vrpThreshold

// === Position tracking ===
var int barsHeld = 0
if strategy.position_size > 0
    barsHeld += 1
else
    barsHeld := 0

// === Entry ===
if entrySignal and strategy.position_size == 0
    strategy.entry("VRP Buy", strategy.long)

// === SMART Exit ===
// Two-stage:
//   Stage 1 (first 60 days): hold no matter what
//   Stage 2 (after 60 days): exit when DVOL spikes above 70 (market re-panicked)
//   Stage 3 (after 180 days): force exit
exitOnDvolSpike = barsHeld >= minHoldDays and not na(dvol) and dvol >= dvolExitLevel
exitOnMaxHold   = barsHeld >= maxHoldDays
exitNow = exitOnDvolSpike or exitOnMaxHold

if strategy.position_size > 0 and exitNow
    reason = exitOnDvolSpike ? "DVOL spike" : "max hold"
    strategy.close_all(comment=reason)

// === Visualization ===
plotshape(entrySignal, "VRP entry signal",
          shape.triangleup, location.belowbar,
          color.lime, size=size.small, text="BUY")

bgcolor(entrySignal ? color.new(color.green, 85) : na, title="Entry signal")
bgcolor(strategy.position_size > 0 and barsHeld < minHoldDays ? color.new(color.blue, 92) : na, title="Min hold zone")
bgcolor(strategy.position_size > 0 and barsHeld >= minHoldDays ? color.new(color.yellow, 92) : na, title="Exit-eligible zone")

plot(showVRP ? vrp : na, "VRP (IV - Realized)",
     color=vrp <= vrpThreshold ? color.red : color.yellow,
     linewidth=2, display=display.data_window)
plot(showVRP ? dvol : na, "DVOL (Deribit IV)",
     color=dvol >= dvolExitLevel ? color.fuchsia : color.aqua,
     linewidth=1, display=display.data_window)
plot(showVRP ? rv : na, "Realized Vol 30d",
     color=color.orange, linewidth=1, display=display.data_window)

// === Info table ===
var table info = table.new(position.top_right, 2, 8,
    bgcolor=color.new(color.black, 80), border_width=1)
if barstate.islast
    table.cell(info, 0, 0, "VRP STRATEGY V2", text_color=color.white, bgcolor=color.new(color.blue, 50))
    table.cell(info, 1, 0, "", text_color=color.white, bgcolor=color.new(color.blue, 50))
    table.cell(info, 0, 1, "DVOL (IV)", text_color=color.white)
    table.cell(info, 1, 1, str.tostring(dvol, "#.#") + "%",
               text_color=dvol >= dvolExitLevel ? color.fuchsia : color.aqua)
    table.cell(info, 0, 2, "Realized Vol", text_color=color.white)
    table.cell(info, 1, 2, str.tostring(rv, "#.#") + "%", text_color=color.orange)
    table.cell(info, 0, 3, "VRP", text_color=color.white)
    table.cell(info, 1, 3, str.tostring(vrp, "+#.#") + "%",
               text_color=vrp <= vrpThreshold ? color.red : color.yellow)
    table.cell(info, 0, 4, "Status", text_color=color.white)
    statusTxt = strategy.position_size > 0 ?
       (barsHeld < minHoldDays ?
         "LONG (lock " + str.tostring(minHoldDays - barsHeld) + "d more)" :
         "LONG (" + str.tostring(barsHeld) + "/" + str.tostring(maxHoldDays) + "d) — exit eligible") :
       "FLAT"
    table.cell(info, 1, 4, statusTxt,
               text_color=strategy.position_size > 0 ? color.lime : color.gray)
    table.cell(info, 0, 5, "Entry signal", text_color=color.white)
    table.cell(info, 1, 5, entrySignal ? "TRIGGER!" : "wait",
               text_color=entrySignal ? color.lime : color.gray)
    table.cell(info, 0, 6, "Exit signal", text_color=color.white)
    table.cell(info, 1, 6, strategy.position_size > 0 and exitOnDvolSpike ? "DVOL spike!" :
                          strategy.position_size > 0 and barsHeld >= minHoldDays ? "Eligible (waiting)" :
                          strategy.position_size > 0 ? "Locked (min hold)" : "—",
               text_color=color.yellow)
    table.cell(info, 0, 7, "Days held", text_color=color.white)
    table.cell(info, 1, 7, str.tostring(barsHeld) + " / " + str.tostring(maxHoldDays),
               text_color=color.white)
'''

    out_path = ROOT / 'btc_vrp_strategy.pine'
    out_path.write_text(pine)
    print(f"\nGenerated: {out_path}")
    print(f"  Lines: {pine.count(chr(10))}, Size: {len(pine)} chars")
    print(f"  Embedded DVOL values: {len(values)}")


if __name__ == "__main__":
    main()
