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
strategy("BTC VRP Signal — Buy when IV undershoots Realized Vol",
     overlay=true,
     initial_capital=100,
     default_qty_type=strategy.percent_of_equity,
     default_qty_value=99,
     commission_type=strategy.commission.percent,
     commission_value=0.05,
     slippage=3)

// === Параметры ===
vrpThreshold  = input.float(-10.0, "VRP threshold to ENTER (more negative = stricter)", step=1)
holdDays      = input.int(60,    "Hold position N days after entry", minval=1)
rvWindow      = input.int(30,    "Realized vol lookback (days)", minval=5)
showVRP       = input.bool(true, "Show VRP value")

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

// === Realized volatility (annualized %) ===
logRet = math.log(close / close[1])
rv = ta.stdev(logRet, rvWindow) * math.sqrt(365) * 100

// === VRP ===
vrp = dvol - rv

// === Signal ===
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

// === Exit after holdDays ===
if strategy.position_size > 0 and barsHeld >= holdDays
    strategy.close_all(comment="Hold expired")

// === Visualization ===
plotshape(entrySignal, "VRP entry signal",
          shape.triangleup, location.belowbar,
          color.lime, size=size.small, text="BUY")

bgcolor(entrySignal ? color.new(color.green, 85) : na, title="Entry signal")

// Show VRP in separate display
plot(showVRP ? vrp : na, "VRP (IV - Realized)",
     color=vrp <= vrpThreshold ? color.red : color.yellow,
     linewidth=2, display=display.data_window)
plot(showVRP ? dvol : na, "DVOL (Deribit IV)",
     color=color.aqua, linewidth=1, display=display.data_window)
plot(showVRP ? rv : na, "Realized Vol 30d",
     color=color.orange, linewidth=1, display=display.data_window)

// === Info table ===
var table info = table.new(position.top_right, 2, 6,
    bgcolor=color.new(color.black, 80), border_width=1)
if barstate.islast
    table.cell(info, 0, 0, "VRP STRATEGY", text_color=color.white, bgcolor=color.new(color.blue, 50))
    table.cell(info, 1, 0, "", text_color=color.white, bgcolor=color.new(color.blue, 50))
    table.cell(info, 0, 1, "DVOL (IV)", text_color=color.white)
    table.cell(info, 1, 1, str.tostring(dvol, "#.#") + "%", text_color=color.aqua)
    table.cell(info, 0, 2, "Realized Vol", text_color=color.white)
    table.cell(info, 1, 2, str.tostring(rv, "#.#") + "%", text_color=color.orange)
    table.cell(info, 0, 3, "VRP", text_color=color.white)
    table.cell(info, 1, 3, str.tostring(vrp, "+#.#") + "%",
               text_color=vrp <= vrpThreshold ? color.red : color.yellow)
    table.cell(info, 0, 4, "Status", text_color=color.white)
    table.cell(info, 1, 4, strategy.position_size > 0 ? "LONG (" + str.tostring(barsHeld) + "/" + str.tostring(holdDays) + "d)" : "FLAT",
               text_color=strategy.position_size > 0 ? color.lime : color.gray)
    table.cell(info, 0, 5, "Signal", text_color=color.white)
    table.cell(info, 1, 5, entrySignal ? "ENTRY!" : "wait",
               text_color=entrySignal ? color.lime : color.gray)
'''

    out_path = ROOT / 'btc_vrp_strategy.pine'
    out_path.write_text(pine)
    print(f"\nGenerated: {out_path}")
    print(f"  Lines: {pine.count(chr(10))}, Size: {len(pine)} chars")
    print(f"  Embedded DVOL values: {len(values)}")


if __name__ == "__main__":
    main()
