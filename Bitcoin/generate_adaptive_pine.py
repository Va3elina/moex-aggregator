"""Generate Pine v6 adaptive strategy: bull/bear regime + leverage.

Logic:
  Regime: BTC > weekly EMA200 = BULL, < = BEAR

  LONG (always available):
    Entry: VRP ≤ vrpEntry
    Exit (bull regime): min 60d held AND DVOL ≥ dvolExitLevel, or maxHoldDays
    Exit (bear regime): min 30d held AND DVOL ≥ dvolExitLevel, or maxHoldBear

  SHORT (only in bear regime):
    Entry: DVOL ≤ shortDvolEntry AND no LONG open
    Exit: 14 days or DVOL ≥ shortExitDvol

  Leverage:
    Position size = leverage × 99% of equity
    No liquidation simulation in Pine (need to handle in real trading)
"""
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).parent


def main():
    dvol = pd.read_csv(ROOT / 'btc_dvol_daily.csv', parse_dates=['date']).set_index('date').sort_index()
    values = dvol['close'].round(1).tolist()
    values_str = ", ".join(str(v) for v in values)
    start_date = dvol.index[0]
    end_date = dvol.index[-1]

    pine = f'''//@version=6
strategy("BTC VRP Adaptive — Bull/Bear regime + Leverage",
     overlay=true,
     initial_capital=100,
     default_qty_type=strategy.percent_of_equity,
     default_qty_value=99,
     commission_type=strategy.commission.percent,
     commission_value=0.05,
     slippage=3,
     pyramiding=0)

// ============ Параметры ============
grpEntry = "Entry signals"
vrpEntry         = input.float(-10.0, "LONG: VRP threshold (more negative = stricter)", step=1, group=grpEntry)
shortDvolEntry   = input.float(35.0,  "SHORT: DVOL threshold (only in bear regime)",  step=1, group=grpEntry)

grpExit = "Exit rules"
minHoldBull      = input.int(60,  "Bull: min hold days before exit allowed", group=grpExit)
maxHoldBull      = input.int(180, "Bull: max hold (force exit)",            group=grpExit)
minHoldBear      = input.int(30,  "Bear: min hold days",                    group=grpExit)
maxHoldBear      = input.int(90,  "Bear: max hold",                         group=grpExit)
dvolExitLevel    = input.float(70.0, "LONG exit: DVOL spike threshold", step=1, group=grpExit)
shortHoldDays    = input.int(14,  "SHORT: fixed hold days",                 group=grpExit)
shortExitDvol    = input.float(50.0, "SHORT exit: DVOL above", step=1, group=grpExit)

grpRisk = "Risk / Sizing"
leverage         = input.float(1.0, "LEVERAGE multiplier (1x = no leverage)", step=0.1, minval=0.1, maxval=10.0, group=grpRisk)
enableShorts     = input.bool(true,  "Enable SHORT side in bear regime", group=grpRisk)

grpDisplay = "Display"
showVRP          = input.bool(true, "Show VRP / DVOL / RV in data window", group=grpDisplay)
showRegime       = input.bool(true, "Color background by regime", group=grpDisplay)

// ============ DVOL embedded data ============
// Reference start: {start_date.strftime("%Y-%m-%d")} (index 0)
// Source: Deribit BTC Volatility Index (DVOL)
// Updated through: {end_date.strftime("%Y-%m-%d")}

var array<float> dvolData = array.from({values_str})

ref_year  = {start_date.year}
ref_month = {start_date.month}
ref_day   = {start_date.day}

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

// ============ Realized volatility ============
logRet = math.log(close / close[1])
rv = ta.stdev(logRet, 30) * math.sqrt(365) * 100

// ============ VRP ============
vrp = dvol - rv

// ============ Bull/Bear Regime (weekly EMA200) ============
weeklyClose  = request.security(syminfo.tickerid, "W", close,                        barmerge.gaps_off, barmerge.lookahead_off)
weeklyEma200 = request.security(syminfo.tickerid, "W", ta.ema(close, 200),           barmerge.gaps_off, barmerge.lookahead_off)
isBullRegime = weeklyClose > weeklyEma200
isBearRegime = not isBullRegime

// ============ Entry signals ============
vrpEntrySignal = not na(vrp) and vrp <= vrpEntry
shortEntrySignal = not na(dvol) and dvol <= shortDvolEntry and isBearRegime and enableShorts

// ============ Position tracking ============
var int barsHeld = 0
var bool inLong = false
var bool inShort = false

if strategy.position_size > 0
    inLong := true
    inShort := false
    barsHeld += 1
else if strategy.position_size < 0
    inLong := false
    inShort := true
    barsHeld += 1
else
    inLong := false
    inShort := false
    barsHeld := 0

// ============ Exit logic ============
// LONG exit
int minHoldNow = isBullRegime ? minHoldBull : minHoldBear
int maxHoldNow = isBullRegime ? maxHoldBull : maxHoldBear

bool longExitDvolSpike = inLong and barsHeld >= minHoldNow and not na(dvol) and dvol >= dvolExitLevel
bool longExitMaxHold   = inLong and barsHeld >= maxHoldNow
bool longExitNow       = longExitDvolSpike or longExitMaxHold

// SHORT exit
bool shortExitDvolUp   = inShort and not na(dvol) and dvol >= shortExitDvol
bool shortExitMaxHold  = inShort and barsHeld >= shortHoldDays
bool shortExitNow      = shortExitDvolUp or shortExitMaxHold

// ============ Trades ============
// Close LONG
if longExitNow
    reason = longExitDvolSpike ? "DVOL spike" : "max hold"
    strategy.close_all(comment=reason)

// Close SHORT
if shortExitNow
    reason = shortExitDvolUp ? "DVOL up" : "max hold"
    strategy.close_all(comment=reason)

// LONG entry (only if no position)
if vrpEntrySignal and strategy.position_size == 0
    qty = leverage * strategy.equity * 0.99 / close
    strategy.entry("VRP Long", strategy.long, qty=qty)

// SHORT entry (only in bear, only if no position)
if shortEntrySignal and strategy.position_size == 0
    qty = leverage * strategy.equity * 0.99 / close
    strategy.entry("Bear Short", strategy.short, qty=qty)

// ============ Visualization ============
// Background colors by regime
bgcolor(showRegime and isBullRegime ? color.new(color.green, 95) : na, title="Bull regime")
bgcolor(showRegime and isBearRegime ? color.new(color.red,   95) : na, title="Bear regime")

// Entry markers
plotshape(vrpEntrySignal and strategy.position_size == 0, "VRP Long entry",
          shape.triangleup, location.belowbar,
          color.lime, size=size.small, text="L")
plotshape(shortEntrySignal and strategy.position_size == 0, "Bear Short entry",
          shape.triangledown, location.abovebar,
          color.red, size=size.small, text="S")

// Weekly EMA200
plot(weeklyEma200, "Weekly EMA200", color=color.yellow, linewidth=2)

// Data window plots
plot(showVRP ? vrp : na, "VRP (IV-Realized)",
     color=vrp <= vrpEntry ? color.red : color.yellow,
     linewidth=2, display=display.data_window)
plot(showVRP ? dvol : na, "DVOL",
     color=dvol >= dvolExitLevel ? color.fuchsia : dvol <= shortDvolEntry ? color.orange : color.aqua,
     linewidth=1, display=display.data_window)
plot(showVRP ? rv : na, "Realized Vol 30d", color=color.orange, linewidth=1, display=display.data_window)

// ============ Info table ============
var table info = table.new(position.top_right, 2, 10,
    bgcolor=color.new(color.black, 80), border_width=1)
if barstate.islast
    table.cell(info, 0, 0, "ADAPTIVE VRP", text_color=color.white, bgcolor=color.new(color.blue, 50))
    table.cell(info, 1, 0, "x" + str.tostring(leverage, "#.#") + " leverage",
               text_color=color.white, bgcolor=color.new(color.blue, 50))

    table.cell(info, 0, 1, "Regime", text_color=color.white)
    table.cell(info, 1, 1, isBullRegime ? "BULL" : "BEAR",
               text_color=isBullRegime ? color.lime : color.red)

    table.cell(info, 0, 2, "BTC vs W EMA200", text_color=color.white)
    pct = (close / weeklyEma200 - 1) * 100
    table.cell(info, 1, 2, str.tostring(pct, "+#.#") + "%",
               text_color=pct > 0 ? color.lime : color.red)

    table.cell(info, 0, 3, "DVOL", text_color=color.white)
    string dvolCol = na(dvol) ? "n/a" : str.tostring(dvol, "#.#") + "%"
    table.cell(info, 1, 3, dvolCol,
               text_color=na(dvol) ? color.gray : dvol >= dvolExitLevel ? color.fuchsia : dvol <= shortDvolEntry ? color.orange : color.aqua)

    table.cell(info, 0, 4, "RV 30d", text_color=color.white)
    table.cell(info, 1, 4, str.tostring(rv, "#.#") + "%", text_color=color.orange)

    table.cell(info, 0, 5, "VRP", text_color=color.white)
    table.cell(info, 1, 5, str.tostring(vrp, "+#.#") + "%",
               text_color=vrp <= vrpEntry ? color.red : color.yellow)

    table.cell(info, 0, 6, "Status", text_color=color.white)
    string statusTxt = "FLAT"
    if inLong
        if barsHeld < minHoldNow
            statusTxt := "LONG (lock " + str.tostring(minHoldNow - barsHeld) + "d)"
        else
            statusTxt := "LONG (" + str.tostring(barsHeld) + "/" + str.tostring(maxHoldNow) + "d eligible)"
    else if inShort
        statusTxt := "SHORT (" + str.tostring(barsHeld) + "/" + str.tostring(shortHoldDays) + "d)"
    table.cell(info, 1, 6, statusTxt,
               text_color=inLong ? color.lime : inShort ? color.red : color.gray)

    table.cell(info, 0, 7, "Long signal", text_color=color.white)
    table.cell(info, 1, 7, vrpEntrySignal ? "TRIGGER!" : "wait",
               text_color=vrpEntrySignal ? color.lime : color.gray)

    table.cell(info, 0, 8, "Short signal", text_color=color.white)
    string shortLabel = "wait"
    if shortEntrySignal
        shortLabel := "TRIGGER!"
    else if not enableShorts
        shortLabel := "disabled"
    else if isBullRegime
        shortLabel := "bull regime"
    table.cell(info, 1, 8, shortLabel,
               text_color=shortEntrySignal ? color.red : color.gray)

    table.cell(info, 0, 9, "Equity", text_color=color.white)
    table.cell(info, 1, 9, str.tostring(strategy.equity, "$#,###"),
               text_color=color.white)
'''

    out_path = ROOT / 'btc_vrp_adaptive.pine'
    out_path.write_text(pine)
    print(f"Generated: {out_path}")
    print(f"  Lines: {pine.count(chr(10))}, Size: {len(pine)} chars")


if __name__ == "__main__":
    main()
