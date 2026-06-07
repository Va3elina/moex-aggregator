#!/usr/bin/env python3
"""
Постобработка дневных риск-параметров -> курируемый датасет, breakpoint'ы и PineScript.

Вход:  RiskRates/data/limits_daily.csv, RiskRates/data/currency_daily.csv
Выход:
  RiskRates/datasets/fear_rates_daily.csv  — tidy: date, instrument, source, rate_pct
  RiskRates/datasets/breakpoints.json      — точки изменения ставки по инструментам
  RiskRates/pine/<instrument>.pine         — индикатор на TradingView (ступенчатый ряд)

PineScript не умеет ходить в API MOEX, поэтому ставка зашивается breakpoint'ами
(ставка меняется редко -> точек немного). Ступенька держится до следующей даты.
"""

import csv
import json
from pathlib import Path

BASE = Path(__file__).parent
DATA = BASE / "data"
DATASETS = BASE / "datasets"
PINE = BASE / "pine"

# instrument -> (источник, код/актив). rate приводим к процентам.
#   limits:   mr1 — доля (0.10 = 10%)  -> *100
#   currency: discount — уже проценты  -> как есть
INSTRUMENTS = {
    "IMOEX_index": ("limits", "MIX",  "Индекс МосБиржи"),
    "BRENT_oil":   ("limits", "BR",   "Нефть Brent"),
    "GAS_henryhub":("limits", "NG",   "Газ (Henry Hub)"),
    "GAZP":        ("limits", "GAZR", "Газпром"),
    "VTBR":        ("limits", "VTBR", "ВТБ"),
    "USD":         ("currency", "USD", "Доллар США/RUB"),
    "CNY":         ("currency", "CNY", "Юань/RUB"),
}


def load_limits():
    """code -> [(date, mr1_pct)] отсортировано."""
    out = {}
    with open(DATA / "limits_daily.csv", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            try:
                v = round(float(row["mr1"]) * 100.0, 4)
            except (ValueError, TypeError):
                continue
            out.setdefault(row["assetcode"], []).append((row["date"], v))
    for k in out:
        out[k].sort()
    return out


def load_currency():
    """asset -> [(date, discount_pct)] отсортировано."""
    out = {}
    with open(DATA / "currency_daily.csv", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            try:
                v = float(row["base_discount"])
            except (ValueError, TypeError):
                continue
            out.setdefault(row["asset"], []).append((row["date"], v))
    for k in out:
        out[k].sort()
    return out


def breakpoints(series):
    """[(date, val)] -> только точки, где значение меняется."""
    bp, prev = [], None
    for d, v in series:
        if v != prev:
            bp.append((d, v))
            prev = v
    return bp


def gen_pine(instrument, title, bp):
    L = []
    L.append("//@version=6")
    L.append(f'indicator("НКЦ Fear · {title} (ставка риска)", overlay=false)')
    L.append("")
    L.append(f"// Ставка рыночного риска (% от цены) по данным НКЦ/MOEX ISS для: {title}.")
    L.append("// Ступенчатый ряд: значение держится до следующей даты-перелома.")
    L.append("// Сгенерировано из дневной выгрузки RiskRates/build_indicators.py.")
    L.append("riskRate() =>")
    d0, v0 = bp[0]
    L.append(f"    float r = {v0}    // с {d0}")
    for d, v in bp[1:]:
        y, m, day = d.split("-")
        L.append(f"    if time >= timestamp({int(y)},{int(m)},{int(day)},0,0)")
        L.append(f"        r := {v}")
    L.append("    r")
    L.append("")
    vals = [v for _, v in bp]
    lo, hi = min(vals), max(vals)
    # пороги: нижняя/верхняя треть исторического диапазона
    greed = round(lo + (hi - lo) * 0.20, 2)
    fear = round(lo + (hi - lo) * 0.60, 2)
    L.append("rr = riskRate()")
    L.append(f"greedLvl = {greed}   // <= покой/жадность")
    L.append(f"fearLvl  = {fear}    // >= повышенный страх")
    L.append("")
    L.append('plot(rr, "Ставка риска, %", color=color.new(color.red,0), linewidth=2, style=plot.style_stepline)')
    L.append('hline(greedLvl, "Жадность", color=color.new(color.green,40), linestyle=hline.style_dashed)')
    L.append('hline(fearLvl,  "Страх",    color=color.new(color.red,40),   linestyle=hline.style_dashed)')
    L.append("bgcolor(rr >= fearLvl ? color.new(color.red,88) : rr <= greedLvl ? color.new(color.green,90) : na)")
    L.append("")
    L.append("// Смена режима: НКЦ снизил ставку после пика страха (часто = разрядка/вход),")
    L.append("// или повысил с низов (риск-офф нарастает).")
    L.append("cut  = rr < rr[1]")
    L.append("hike = rr > rr[1]")
    L.append('plotshape(cut  and rr[1] >= fearLvl, "Разрядка",  shape.triangleup,   location.bottom, color.new(color.green,0), size=size.tiny)')
    L.append('plotshape(hike and rr[1] <= greedLvl,"Рост риска", shape.triangledown, location.top,    color.new(color.red,0),   size=size.tiny)')
    return "\n".join(L) + "\n"


def main():
    DATASETS.mkdir(exist_ok=True)
    PINE.mkdir(exist_ok=True)
    limits = load_limits()
    currency = load_currency()

    tidy_rows = []
    all_bp = {}
    summary = []
    for inst, (src, code, title) in INSTRUMENTS.items():
        series = (limits if src == "limits" else currency).get(code, [])
        if not series:
            summary.append(f"  {inst:14} — НЕТ ДАННЫХ ({src}:{code})")
            continue
        for d, v in series:
            tidy_rows.append({"date": d, "instrument": inst, "source": src, "rate_pct": v})
        bp = breakpoints(series)
        all_bp[inst] = bp
        (PINE / f"{inst}.pine").write_text(gen_pine(inst, title, bp), encoding="utf-8")
        summary.append(f"  {inst:14} {series[0][0]}…{series[-1][0]}  "
                       f"дней={len(series):5}  переломов={len(bp):3}  "
                       f"диапазон={min(v for _,v in series)}…{max(v for _,v in series)}%")

    tidy_rows.sort(key=lambda r: (r["instrument"], r["date"]))
    with open(DATASETS / "fear_rates_daily.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["date", "instrument", "source", "rate_pct"])
        w.writeheader()
        w.writerows(tidy_rows)
    json.dump(all_bp, open(DATASETS / "breakpoints.json", "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)

    print("Инструменты:")
    print("\n".join(summary))
    print(f"\n→ datasets/fear_rates_daily.csv ({len(tidy_rows)} строк)")
    print(f"→ datasets/breakpoints.json")
    print(f"→ pine/*.pine ({len(all_bp)} файлов)")


if __name__ == "__main__":
    main()
