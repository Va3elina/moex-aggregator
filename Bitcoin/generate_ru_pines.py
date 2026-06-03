"""Generate IMOEX and AFKS RVI-based adaptive Pine scripts.

Key insight: on Russian market VRP works OPPOSITE to crypto:
  - Crypto: BUY when VRP <= -10 (fear oversold)
  - RU:     BUY when VRP >= +20 (panic spike, mean reversion)

IMOEX: RVI >= 40 LONG signal (works on real backtest)
AFKS:  same signal — best alpha (+62% strategy vs -72% BH)

No always-in-market on RU — SHORT side fails (Russian retail can't easily short).
"""
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).parent


def generate_ru_pine(asset, title_suffix, vrp_default=20.0, rvi_default=40.0):
    rvi = pd.read_csv(ROOT/'rvi_daily.csv', parse_dates=['TRADEDATE']).set_index('TRADEDATE').sort_index()
    values = rvi['close'].round(2).tolist()
    values_str = ", ".join(str(v) for v in values)
    start_date = rvi.index[0]
    end_date = rvi.index[-1]

    pine = f'''//@version=6
strategy("{asset} RVI Strategy — {title_suffix}",
     overlay=true,
     initial_capital=100,
     default_qty_type=strategy.percent_of_equity,
     default_qty_value=99,
     commission_type=strategy.commission.percent,
     commission_value=0.05,
     slippage=3,
     pyramiding=0,
     margin_long=0,
     margin_short=0)

// ============ Параметры ============
grpMode = "Signal mode"
useVRP           = input.bool(false, "Use VRP (RVI-RV) instead of raw RVI",  group=grpMode)

grpEntry = "Entry signals"
rviEntry         = input.float({rvi_default}, "LONG: RVI threshold (panic spike)", step=1, group=grpEntry)
vrpEntry         = input.float({vrp_default}, "LONG: VRP threshold (if VRP mode)",  step=1, group=grpEntry)

grpExit = "Exit rules"
minHold          = input.int(14,  "Min hold days",            group=grpExit)
maxHold          = input.int(90,  "Max hold days (force)",    group=grpExit)
rviExitLevel     = input.float(60.0, "Exit on RVI spike >=",  step=1, group=grpExit)

grpRisk = "Risk / Sizing"
leverage         = input.float(1.0, "LEVERAGE multiplier", step=0.1, minval=0.1, maxval=5.0, group=grpRisk)

grpDisplay = "Display"
showVol          = input.bool(true, "Show RVI/RV/VRP (data window)", group=grpDisplay)

// ============ RVI embedded data ============
// Reference start: {start_date.strftime("%Y-%m-%d")} (index 0)
// Source: MOEX RVI (Russian Volatility Index)
// Updated through: {end_date.strftime("%Y-%m-%d")}

var array<float> rviData = array.from({values_str})

ref_year  = {start_date.year}
ref_month = {start_date.month}
ref_day   = {start_date.day}

get_rvi_idx() =>
    days_since = (time - timestamp(ref_year, ref_month, ref_day, 0, 0)) / (1000 * 60 * 60 * 24)
    int(math.floor(days_since))

get_rvi() =>
    idx = get_rvi_idx()
    if idx >= 0 and idx < array.size(rviData)
        array.get(rviData, idx)
    else
        na

rvi = get_rvi()

// ============ Realized vol ============
logRet = math.log(close / close[1])
rv = ta.stdev(logRet, 30) * math.sqrt(365) * 100

// ============ VRP ============
vrp = rvi - rv

// ============ Signals ============
bool entrySignal = useVRP ? (not na(vrp) and vrp >= vrpEntry) : (not na(rvi) and rvi >= rviEntry)
bool exitSpike   = not na(rvi) and rvi >= rviExitLevel

// ============ Position tracking ============
var int barsHeld = 0
var bool inLong = false

if strategy.position_size > 0
    inLong := true
    barsHeld += 1
else
    inLong := false
    barsHeld := 0

// ============ Exit logic ============
bool exitMinHoldSpike = inLong and barsHeld >= minHold and exitSpike
bool exitMaxHold      = inLong and barsHeld >= maxHold
bool exitNow          = exitMinHoldSpike or exitMaxHold

// ============ Position sizing ============
float posPct = leverage * 99.0

// ============ Trades ============
if exitNow
    string reason = exitMinHoldSpike ? "RVI spike" : "max hold"
    strategy.close_all(comment=reason)

if entrySignal and strategy.position_size == 0
    strategy.entry("RVI Long", strategy.long, qty=posPct)

// ============ Visualization ============
plotshape(entrySignal and strategy.position_size == 0, "Entry",
          shape.triangleup, location.belowbar,
          color.lime, size=size.small, text="L")
plotshape(exitNow, "Exit",
          shape.triangledown, location.abovebar,
          color.orange, size=size.small, text="X")

plot(showVol ? rvi : na, "RVI", color=rvi >= rviEntry ? color.red : color.aqua, linewidth=2, display=display.data_window)
plot(showVol ? rv : na, "Realized Vol 30d", color=color.orange, linewidth=1, display=display.data_window)
plot(showVol ? vrp : na, "VRP", color=vrp >= vrpEntry ? color.fuchsia : color.yellow, linewidth=1, display=display.data_window)

// ============ Info table ============
var table info = table.new(position.top_right, 2, 9, bgcolor=color.new(color.black, 80), border_width=1)
if barstate.islast
    table.cell(info, 0, 0, "{asset} RVI", text_color=color.white, bgcolor=color.new(color.blue, 50))
    table.cell(info, 1, 0, "x" + str.tostring(leverage, "#.#"), text_color=color.white, bgcolor=color.new(color.blue, 50))

    table.cell(info, 0, 1, "Mode", text_color=color.white)
    table.cell(info, 1, 1, useVRP ? "VRP >= " + str.tostring(vrpEntry) : "RVI >= " + str.tostring(rviEntry), text_color=color.aqua)

    table.cell(info, 0, 2, "RVI", text_color=color.white)
    string rviCol = na(rvi) ? "n/a" : str.tostring(rvi, "#.#")
    table.cell(info, 1, 2, rviCol, text_color=na(rvi) ? color.gray : rvi >= rviEntry ? color.red : color.aqua)

    table.cell(info, 0, 3, "RV 30d", text_color=color.white)
    table.cell(info, 1, 3, str.tostring(rv, "#.#") + "%", text_color=color.orange)

    table.cell(info, 0, 4, "VRP", text_color=color.white)
    table.cell(info, 1, 4, str.tostring(vrp, "+#.#"), text_color=vrp >= vrpEntry ? color.fuchsia : color.yellow)

    table.cell(info, 0, 5, "Signal", text_color=color.white)
    table.cell(info, 1, 5, entrySignal ? "TRIGGER!" : "wait", text_color=entrySignal ? color.lime : color.gray)

    table.cell(info, 0, 6, "Status", text_color=color.white)
    string statusTxt = "FLAT"
    if inLong
        if barsHeld < minHold
            statusTxt := "LONG (lock " + str.tostring(minHold - barsHeld) + "d)"
        else
            statusTxt := "LONG (" + str.tostring(barsHeld) + "/" + str.tostring(maxHold) + "d)"
    table.cell(info, 1, 6, statusTxt, text_color=inLong ? color.lime : color.gray)

    table.cell(info, 0, 7, "Equity", text_color=color.white)
    table.cell(info, 1, 7, str.tostring(strategy.equity, "$#,###"), text_color=color.white)

    table.cell(info, 0, 8, "Leverage", text_color=color.white)
    table.cell(info, 1, 8, "x" + str.tostring(leverage, "#.#"), text_color=leverage > 1 ? color.yellow : color.white)
'''

    out_path = ROOT / f'{asset.lower()}_rvi_strategy.pine'
    out_path.write_text(pine)
    print(f"  Generated {asset}: {out_path}")
    print(f"  Lines: {pine.count(chr(10))}, Size: {len(pine)} chars")


def main():
    print("Generating RU Pine scripts (RVI-based):")
    generate_ru_pine('IMOEX', "RVI panic spike LONG", rvi_default=40.0, vrp_default=20.0)
    generate_ru_pine('AFKS',  "RVI panic spike LONG", rvi_default=40.0, vrp_default=20.0)


if __name__ == "__main__":
    main()
