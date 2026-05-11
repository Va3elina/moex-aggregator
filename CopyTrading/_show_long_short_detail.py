"""Per-pair detail for best long+short config."""
import warnings; warnings.filterwarnings("ignore")
import sys
sys.path.insert(0, '.')
exec(open("research_long_short_dynamic.py").read().split("def main")[0])

PAIRS = ['AFKS','SMLT','POSI','ASTR','VKCO','SOFL','WUSH','DELI']
print("=" * 100)
print("BEST LONG: rsi>70, pos>0.7, ret_20>10%, ADX>25, EMA trend up, TP5%, GB25%")
print("=" * 100)
print(f"{'Pair':6} {'Trades':>6} {'WR%':>5} {'Avg%':>6} {'AvgHold':>8} {'MaxL':>7} {'MaxW':>7}")
print("-" * 60)
all_long = []
for p in PAIRS:
    bars = features(load(p))
    sig = long_signal(bars, rsi_min=70, pos_min=0.7, ret_min=0.10, adx_min=25, require_trend=True)
    tr, _ = backtest_dynamic(bars, sig, side='long', tp_pct=5.0, gb_pct=25.0)
    if len(tr) == 0:
        print(f"{p:6} {'0':>6}"); continue
    wr = (tr['pnl_pct']>0).sum()/len(tr)*100
    print(f"{p:6} {len(tr):>6} {wr:>4.1f}% {tr['pnl_pct'].mean():>+5.2f}% "
          f"{tr['hold'].mean():>7.1f}d {tr['pnl_pct'].min():>+6.2f}% {tr['pnl_pct'].max():>+6.2f}%")
    all_long.append(tr)

print()
print("=" * 100)
print("BEST SHORT: rsi<30, pos<0.1, ret_20<-10%, ADX>25, EMA trend dn, TP5%, GB25%")
print("=" * 100)
print(f"{'Pair':6} {'Trades':>6} {'WR%':>5} {'Avg%':>6} {'AvgHold':>8} {'MaxL':>7} {'MaxW':>7}")
print("-" * 60)
all_short = []
for p in PAIRS:
    bars = features(load(p))
    sig = short_signal(bars, rsi_max=30, pos_max=0.1, ret_max=-0.10, adx_min=25, require_trend=True)
    tr, _ = backtest_dynamic(bars, sig, side='short', tp_pct=5.0, gb_pct=25.0)
    if len(tr) == 0:
        print(f"{p:6} {'0':>6}"); continue
    wr = (tr['pnl_pct']>0).sum()/len(tr)*100
    print(f"{p:6} {len(tr):>6} {wr:>4.1f}% {tr['pnl_pct'].mean():>+5.2f}% "
          f"{tr['hold'].mean():>7.1f}d {tr['pnl_pct'].min():>+6.2f}% {tr['pnl_pct'].max():>+6.2f}%")
    all_short.append(tr)

print("\n" + "=" * 100)
print("COMBINED PORTFOLIO (long + short across 8 pairs):")
print("=" * 100)
combined = pd.concat(all_long + all_short)
wr = (combined['pnl_pct']>0).sum() / len(combined) * 100
pos_pnl = combined[combined['pnl_pct']>0]['pnl_pct'].sum()
neg_pnl = abs(combined[combined['pnl_pct']<0]['pnl_pct'].sum())
print(f"  Total trades: {len(combined)}")
print(f"  Win rate    : {wr:.1f}%")
print(f"  PF          : {pos_pnl/neg_pnl:.2f}")
print(f"  Avg / trade : {combined['pnl_pct'].mean():+.2f}%")
print(f"  Long  count : {sum(1 for t in all_long for _ in range(len(t)))}")
print(f"  Short count : {sum(1 for t in all_short for _ in range(len(t)))}")
