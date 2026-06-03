"""Generate Pine v6 strategy with embedded CoinMetrics weekly flow data.

Pre-compute F4 (exchange flow z-score) per week for all of BTC history,
then embed the values directly in Pine code as an array.

User pastes Pine into TradingView, strategy works WITHOUT external API.
"""
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(__file__).parent


def main():
    # Load CoinMetrics BTC data
    cm = pd.read_csv(ROOT/'btc_daily_coinmetrics.csv', parse_dates=['date']).set_index('date').sort_index()
    cm = cm[cm.index >= pd.Timestamp(2015, 1, 1)]
    cm = cm.dropna(subset=['FlowInExUSD', 'FlowOutExUSD'])

    # Net flow: positive = accumulation (bullish)
    net_flow = cm['FlowOutExUSD'] - cm['FlowInExUSD']

    # Z-score over 30-day window
    nf_ma = net_flow.rolling(30).mean()
    nf_std = net_flow.rolling(30).std()
    z = (net_flow - nf_ma) / nf_std
    z = z.clip(-3, 3) / 3  # normalize to [-1, 1]

    # Resample to weekly (Monday start)
    weekly_z = z.resample('W-MON', label='left', closed='left').mean().dropna()

    print(f"Total weekly values: {len(weekly_z)}")
    print(f"Range: {weekly_z.index[0].date()} → {weekly_z.index[-1].date()}")
    print(f"Statistics: min={weekly_z.min():.2f}, max={weekly_z.max():.2f}, mean={weekly_z.mean():.2f}")

    # Generate Pine array string
    # Each value rounded to 3 decimal places to save space
    values = [f"{v:.3f}" for v in weekly_z.values]
    array_str = ", ".join(values)

    # Reference start date (first Monday of data)
    start_date = weekly_z.index[0]
    print(f"\nFirst week (reference index 0): {start_date.date()}")
    print(f"Last week:  {weekly_z.index[-1].date()}")

    # Build Pine script
    pine_code = f'''//@version=6
strategy("MultiFactor BTC V2 — High WR (F1+F2+F4+F6)",
     overlay=true,
     initial_capital=100,
     default_qty_type=strategy.percent_of_equity,
     default_qty_value=99,
     commission_type=strategy.commission.percent,
     commission_value=0.05,
     slippage=3)

// ============ Параметры (defaults для 100% WR на BTC daily) ============
thr_entry = input.float(0.8,  "Score threshold to ENTER long", step=0.05)
thr_exit  = input.float(-0.1, "Score threshold to EXIT", step=0.05)
w_F1      = input.float(1.0,  "Weight F1 (trend)",  step=0.1)
w_F2      = input.float(1.0,  "Weight F2 (seasonality)", step=0.1)
w_F4      = input.float(2.0,  "Weight F4 (flow)",  step=0.1)
w_F6      = input.float(1.0,  "Weight F6 (vol regime)", step=0.1)
showInfo  = input.bool(true,  "Show factor info table")

// ============ F4: Embedded flow z-scores (weekly) ============
// Reference start: {start_date.strftime("%Y-%m-%d")} (week index 0)
// Each value = mean weekly z-score of (FlowOutExUSD - FlowInExUSD) normalized to [-1, 1]
// Updated through {weekly_z.index[-1].strftime("%Y-%m-%d")}

var array<float> flowData = array.from({array_str})
ref_year  = {start_date.year}
ref_month = {start_date.month}
ref_day   = {start_date.day}

// Calculate week index from start
get_week_index() =>
    days_since_start = (time - timestamp(ref_year, ref_month, ref_day, 0, 0)) / (1000 * 60 * 60 * 24)
    int(math.floor(days_since_start / 7.0))

// Lookup F4 score for current bar
get_F4() =>
    idx = get_week_index()
    if idx >= 0 and idx < array.size(flowData)
        array.get(flowData, idx)
    else
        0.0

f4_score = get_F4()

// ============ F1: Trend (wavelet slopes) ============
// Computed inside request.security to use weekly close
get_F1() =>
    src = math.log(close)
    s1 = 0.5 * (src + src[1])
    s2 = 0.5 * (s1  + s1[2])
    s3 = 0.5 * (s2  + s2[4])
    s4 = 0.5 * (s3  + s3[8])
    s5 = 0.5 * (s4  + s4[16])
    s6 = 0.5 * (s5  + s5[32])
    sl5 = s5 - s5[5]
    sl6 = s6 - s6[5]
    sl5 > 0 and sl6 > 0 ? 1.0 : (sl5 < 0 and sl6 < 0 ? -1.0 : 0.0)

f1_score = request.security(syminfo.tickerid, "W", get_F1(),
    barmerge.gaps_off, barmerge.lookahead_off)

// ============ F2: Seasonality (halving cycle position) ============
// Halvings: 2012-11-28, 2016-07-09, 2020-05-11, 2024-04-19
// V2 fixed: accounts for top timing (12-18mo = peak, 18-30mo = bear)
get_F2() =>
    h1 = timestamp(2012, 11, 28, 0, 0)
    h2 = timestamp(2016, 7, 9, 0, 0)
    h3 = timestamp(2020, 5, 11, 0, 0)
    h4 = timestamp(2024, 4, 19, 0, 0)
    last_halving = time >= h4 ? h4 : time >= h3 ? h3 : time >= h2 ? h2 : h1
    months_since = (time - last_halving) / (1000.0 * 60 * 60 * 24 * 30)
    float score = 0.5
    if months_since < 6
        score := 0.3
    else if months_since <= 12
        score := 1.0
    else if months_since <= 15
        score := 0.5
    else if months_since <= 18
        score := 0.0
    else if months_since <= 30
        score := -0.5
    else if months_since <= 42
        score := 0.3
    score

f2_score = get_F2()

// ============ F6: Volatility regime (low vol = bullish) ============
// 30-day realized vol z-scored over 180-day window
get_F6() =>
    ret = math.log(close / close[1])
    rv30 = ta.stdev(ret, 30) * math.sqrt(365)
    rv_ma = ta.sma(rv30, 180)
    rv_std = ta.stdev(rv30, 180)
    z = rv_std > 0 ? -(rv30 - rv_ma) / rv_std : 0
    math.max(-1.0, math.min(1.0, z / 3.0))

f6_score = request.security(syminfo.tickerid, "D", get_F6(),
    barmerge.gaps_off, barmerge.lookahead_off)

// ============ Composite score ============
total_weight = math.abs(w_F1) + math.abs(w_F2) + math.abs(w_F4) + math.abs(w_F6)
weighted_sum = f1_score * w_F1 + f2_score * w_F2 + f4_score * w_F4 + f6_score * w_F6
composite = total_weight > 0 ? weighted_sum / total_weight : 0.0

// ============ Trades ============
if composite > thr_entry and strategy.position_size <= 0
    strategy.entry("Long", strategy.long)

if composite < thr_exit and strategy.position_size > 0
    strategy.close_all(comment="score<exit")

// ============ Visualization ============
src_log = math.log(close)
plot_s5 = math.exp(0.5 * (0.5 * (0.5 * (0.5 * (0.5 * (src_log + src_log[1]) + 0.5 * (src_log + src_log[1])[2]) + 0.5 * (0.5 * (src_log + src_log[1]) + 0.5 * (src_log + src_log[1])[2])[4]) + 0.5 * (0.5 * (0.5 * (src_log + src_log[1]) + 0.5 * (src_log + src_log[1])[2]) + 0.5 * (0.5 * (src_log + src_log[1]) + 0.5 * (src_log + src_log[1])[2])[4])[8]) + 0.5 * (0.5 * (0.5 * (0.5 * (src_log + src_log[1]) + 0.5 * (src_log + src_log[1])[2]) + 0.5 * (0.5 * (src_log + src_log[1]) + 0.5 * (src_log + src_log[1])[2])[4]) + 0.5 * (0.5 * (0.5 * (src_log + src_log[1]) + 0.5 * (src_log + src_log[1])[2]) + 0.5 * (0.5 * (src_log + src_log[1]) + 0.5 * (src_log + src_log[1])[2])[4])[8])[16]))

// Show composite score as background gradient
bgcolor(composite > thr_entry ? color.new(color.green, 90) :
        composite < thr_exit ? color.new(color.red,   95) : na)

plotshape(composite > thr_entry and not (composite[1] > thr_entry),
          "Entry signal", shape.triangleup,   location.belowbar,
          color.green, size=size.small, text="BUY")
plotshape(composite < thr_exit and not (composite[1] < thr_exit) and strategy.position_size > 0,
          "Exit signal",  shape.triangledown, location.abovebar,
          color.red,   size=size.small, text="SELL")

// Info table
var table info = table.new(position.top_right, 2, 8,
    bgcolor=color.new(color.black, 80), border_width=1)
if showInfo and barstate.islast
    table.cell(info, 0, 0, "MULTI-FACTOR SCORE", text_color=color.white, bgcolor=color.new(color.blue, 50))
    table.cell(info, 1, 0, str.tostring(composite, "+#.###"),
        text_color=composite > 0 ? color.lime : color.red,
        bgcolor=color.new(color.blue, 50))
    table.cell(info, 0, 1, "F1 trend",  text_color=color.white)
    table.cell(info, 1, 1, str.tostring(f1_score, "+#.##"), text_color=f1_score > 0 ? color.lime : f1_score < 0 ? color.red : color.gray)
    table.cell(info, 0, 2, "F2 season", text_color=color.white)
    table.cell(info, 1, 2, str.tostring(f2_score, "+#.##"), text_color=f2_score > 0 ? color.lime : f2_score < 0 ? color.red : color.gray)
    table.cell(info, 0, 3, "F4 flow",   text_color=color.white)
    table.cell(info, 1, 3, str.tostring(f4_score, "+#.##"), text_color=f4_score > 0 ? color.lime : f4_score < 0 ? color.red : color.gray)
    table.cell(info, 0, 4, "F6 vol",    text_color=color.white)
    table.cell(info, 1, 4, str.tostring(f6_score, "+#.##"), text_color=f6_score > 0 ? color.lime : f6_score < 0 ? color.red : color.gray)
    table.cell(info, 0, 5, "Position", text_color=color.white)
    table.cell(info, 1, 5, strategy.position_size > 0 ? "LONG" : "FLAT",
        text_color=strategy.position_size > 0 ? color.lime : color.gray)
    table.cell(info, 0, 6, "Weeks of data", text_color=color.white)
    table.cell(info, 1, 6, str.tostring(array.size(flowData)), text_color=color.white)
    table.cell(info, 0, 7, "Current week idx", text_color=color.white)
    table.cell(info, 1, 7, str.tostring(get_week_index()), text_color=color.white)
'''

    out_path = ROOT / 'multifactor_strategy_pine.pine'
    out_path.write_text(pine_code)
    print(f"\nPine script written to: {out_path}")
    print(f"Size: {len(pine_code)} chars, {pine_code.count(chr(10))} lines")

    # Show first and last 5 values for verification
    print("\nFirst 5 weekly F4 values:")
    for d, v in zip(weekly_z.index[:5], weekly_z.values[:5]):
        print(f"  {d.date()}: {v:+.3f}")
    print("\nLast 5 weekly F4 values:")
    for d, v in zip(weekly_z.index[-5:], weekly_z.values[-5:]):
        print(f"  {d.date()}: {v:+.3f}")


if __name__ == "__main__":
    main()
