"""Generate ETH and BTC adaptive Pine scripts with always-in-market option.

Both share same template, just different DVOL data and default VRP threshold:
  BTC: VRP <= -10 (works well)
  ETH: VRP <= -25 (stricter needed)
"""
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).parent


def generate_pine(asset, vrp_default, title_suffix):
    dvol = pd.read_csv(ROOT / f'{asset.lower()}_dvol_daily.csv', parse_dates=['date']).set_index('date').sort_index()
    values = dvol['close'].round(1).tolist()
    values_str = ", ".join(str(v) for v in values)
    start_date = dvol.index[0]
    end_date = dvol.index[-1]

    pine = f'''//@version=6
strategy("{asset} VRP Adaptive — {title_suffix}",
     overlay=true,
     initial_capital=100,
     default_qty_type=strategy.percent_of_equity,
     default_qty_value=99,
     commission_type=strategy.commission.percent,
     commission_value=0.05,
     slippage=3,
     pyramiding=0,
     margin_long=10,
     margin_short=10)

// ============ Параметры ============
grpStrategy = "Strategy mode"
alwaysInMarket = input.bool(true, "Always-in-market (SHORT between LONGs)", group=grpStrategy)
enableShorts   = input.bool(true, "Enable bear-regime shorts (DVOL ≤ 35)",  group=grpStrategy)

grpEntry = "Entry signals"
vrpEntry         = input.float({vrp_default}, "LONG: VRP threshold",                    step=1, group=grpEntry)
shortDvolEntry   = input.float(35.0, "SHORT: DVOL threshold (bear regime)",       step=1, group=grpEntry)

grpExit = "Exit rules"
minHoldBull      = input.int(60,  "Bull: min hold days",        group=grpExit)
maxHoldBull      = input.int(180, "Bull: max hold (force)",     group=grpExit)
minHoldBear      = input.int(30,  "Bear: min hold days",        group=grpExit)
maxHoldBear      = input.int(90,  "Bear: max hold",             group=grpExit)
dvolExitLevel    = input.float(70.0, "LONG exit: DVOL spike",  step=1, group=grpExit)
shortMaxHold     = input.int(365, "SHORT: max hold safety (days)", group=grpExit)

grpRisk = "Leverage"
leverage         = input.float(1.0, "Leverage multiplier (1x..5x)", step=0.1, minval=0.1, maxval=5.0, group=grpRisk, tooltip="1.0 = 99% equity. 2.0 = 198% (2x). Margin buffer allows up to 10x but >5x is unsafe.")

grpDisplay = "Display"
showVRP          = input.bool(true, "Show VRP/DVOL/RV (data window)", group=grpDisplay)
showRegime       = input.bool(true, "Color background by regime",      group=grpDisplay)

// ============ DVOL embedded data ============
// Reference start: {start_date.strftime("%Y-%m-%d")} (index 0)
// Source: Deribit {asset} Volatility Index (DVOL)
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

// ============ Realized vol ============
logRet = math.log(close / close[1])
rv = ta.stdev(logRet, 30) * math.sqrt(365) * 100

// ============ VRP ============
vrp = dvol - rv

// ============ Bull/Bear Regime ============
weeklyClose  = request.security(syminfo.tickerid, "W", close,              barmerge.gaps_off, barmerge.lookahead_off)
weeklyEma200 = request.security(syminfo.tickerid, "W", ta.ema(close, 200), barmerge.gaps_off, barmerge.lookahead_off)
isBullRegime = weeklyClose > weeklyEma200
isBearRegime = not isBullRegime

// ============ Signals ============
vrpEntrySignal   = not na(vrp) and vrp <= vrpEntry
shortBearSignal  = not na(dvol) and dvol <= shortDvolEntry and isBearRegime and enableShorts

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
int minHoldNow = isBullRegime ? minHoldBull : minHoldBear
int maxHoldNow = isBullRegime ? maxHoldBull : maxHoldBear

bool longExitDvolSpike = inLong and barsHeld >= minHoldNow and not na(dvol) and dvol >= dvolExitLevel
bool longExitMaxHold   = inLong and barsHeld >= maxHoldNow
bool longExitNow       = longExitDvolSpike or longExitMaxHold

// SHORT exits ONLY on next LONG signal (handled in entry block below) or safety max hold
bool shortExitMaxHold  = inShort and barsHeld >= shortMaxHold
bool shortExitNow      = shortExitMaxHold

// ============ Trades ============
// Position sizing in contracts: leverage * 99% of equity / current price.
// Example: leverage=2, equity=$100, BTC=$60k → qty = 2*100*0.99/60000 = 0.0033 BTC (= $198 exposure = 2x leverage).

float posQty = leverage * strategy.equity * 0.99 / close

// LONG exit
if longExitNow
    string reason = longExitDvolSpike ? "DVOL spike" : "max hold"
    strategy.close_all(comment=reason)

// SHORT safety exit (only if held too long without new LONG signal)
if shortExitNow
    strategy.close_all(comment="short max hold")

// LONG entry (closes any open SHORT first)
if vrpEntrySignal
    if strategy.position_size < 0
        strategy.close_all(comment="long signal - close short")
    if strategy.position_size == 0
        strategy.entry("VRP Long", strategy.long, qty=posQty)

// SHORT entry:
//   1. Bear regime + DVOL <= 35 (tactical)
//   2. After LONG exit if always-in-market enabled
bool alwaysInShortEntry = alwaysInMarket and longExitNow

if alwaysInShortEntry
    strategy.entry("AlwaysIn Short", strategy.short, qty=posQty)
else if shortBearSignal and strategy.position_size == 0
    strategy.entry("Bear Short", strategy.short, qty=posQty)

// ============ Visualization ============
bgcolor(showRegime and isBullRegime ? color.new(color.green, 95) : na, title="Bull regime")
bgcolor(showRegime and isBearRegime ? color.new(color.red, 95) : na, title="Bear regime")

plotshape(vrpEntrySignal and strategy.position_size == 0, "VRP Long entry",
          shape.triangleup, location.belowbar,
          color.lime, size=size.small, text="L")
plotshape((shortBearSignal or alwaysInShortEntry) and strategy.position_size == 0, "Short entry",
          shape.triangledown, location.abovebar,
          color.red, size=size.small, text="S")

plot(weeklyEma200, "Weekly EMA200", color=color.yellow, linewidth=2)

plot(showVRP ? vrp : na, "VRP (IV-Realized)", color=vrp <= vrpEntry ? color.red : color.yellow, linewidth=2, display=display.data_window)
plot(showVRP ? dvol : na, "DVOL", color=dvol >= dvolExitLevel ? color.fuchsia : color.aqua, linewidth=1, display=display.data_window)
plot(showVRP ? rv : na, "Realized Vol 30d", color=color.orange, linewidth=1, display=display.data_window)

// ============ Info table ============
var table info = table.new(position.top_right, 2, 10, bgcolor=color.new(color.black, 80), border_width=1)
if barstate.islast
    table.cell(info, 0, 0, "{asset} ADAPTIVE VRP", text_color=color.white, bgcolor=color.new(color.blue, 50))
    table.cell(info, 1, 0, "x" + str.tostring(leverage, "#.#") + (alwaysInMarket ? " AIM" : ""), text_color=color.white, bgcolor=color.new(color.blue, 50))

    table.cell(info, 0, 1, "Regime", text_color=color.white)
    table.cell(info, 1, 1, isBullRegime ? "BULL" : "BEAR", text_color=isBullRegime ? color.lime : color.red)

    table.cell(info, 0, 2, "Price vs W EMA200", text_color=color.white)
    float pct = (close / weeklyEma200 - 1) * 100
    table.cell(info, 1, 2, str.tostring(pct, "+#.#") + "%", text_color=pct > 0 ? color.lime : color.red)

    table.cell(info, 0, 3, "DVOL", text_color=color.white)
    string dvolCol = na(dvol) ? "n/a" : str.tostring(dvol, "#.#") + "%"
    table.cell(info, 1, 3, dvolCol, text_color=na(dvol) ? color.gray : dvol >= dvolExitLevel ? color.fuchsia : color.aqua)

    table.cell(info, 0, 4, "RV 30d", text_color=color.white)
    table.cell(info, 1, 4, str.tostring(rv, "#.#") + "%", text_color=color.orange)

    table.cell(info, 0, 5, "VRP", text_color=color.white)
    table.cell(info, 1, 5, str.tostring(vrp, "+#.#") + "%", text_color=vrp <= vrpEntry ? color.red : color.yellow)

    table.cell(info, 0, 6, "Status", text_color=color.white)
    string statusTxt = "FLAT"
    if inLong
        if barsHeld < minHoldNow
            statusTxt := "LONG (lock " + str.tostring(minHoldNow - barsHeld) + "d)"
        else
            statusTxt := "LONG (" + str.tostring(barsHeld) + "/" + str.tostring(maxHoldNow) + "d)"
    else if inShort
        statusTxt := "SHORT (" + str.tostring(barsHeld) + "d, waits LONG signal)"
    table.cell(info, 1, 6, statusTxt, text_color=inLong ? color.lime : inShort ? color.red : color.gray)

    table.cell(info, 0, 7, "Long signal", text_color=color.white)
    table.cell(info, 1, 7, vrpEntrySignal ? "TRIGGER!" : "wait", text_color=vrpEntrySignal ? color.lime : color.gray)

    table.cell(info, 0, 8, "Always-in mode", text_color=color.white)
    table.cell(info, 1, 8, alwaysInMarket ? "ON" : "OFF", text_color=alwaysInMarket ? color.lime : color.gray)

    table.cell(info, 0, 9, "Equity", text_color=color.white)
    table.cell(info, 1, 9, str.tostring(strategy.equity, "$#,###"), text_color=color.white)
'''

    out_path = ROOT / f'{asset.lower()}_vrp_adaptive_aim.pine'
    out_path.write_text(pine)
    print(f"  Generated {asset}: {out_path}")
    print(f"  Lines: {pine.count(chr(10))}, Size: {len(pine)} chars")


def main():
    print("Generating Pine scripts:")
    generate_pine('BTC', -10.0, "Bull/Bear + AIM + Leverage")
    generate_pine('ETH', -25.0, "Bull/Bear + AIM + Leverage")


if __name__ == "__main__":
    main()
