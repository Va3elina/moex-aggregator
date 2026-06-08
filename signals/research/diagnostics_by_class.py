"""
Diagnostics by class — проверяем гипотезу что некоторые features работают
только для определённого подмножества активов.

Идея: aggregate t-stat может маскировать subgroup signal. Например,
price_vs_ema20_pct на 10 активах (все Tier 1-2) был -2.92. На 65 — -0.07.
Возможно paттерн жив для крупных активов, но мертв для tail (мелкие/exotic).

Два измерения:
  1. TYPE (вручную): equity / fx_rub / fx_cross / index / metal / energy / soft
  2. TIER (по avg volume contracts): 1 = top liquid, 5 = tail

Выходы:
  - Matrix t-stat per feature × group (type, tier)
  - Specific focus: price_vs_ema20_pct и top features

Запуск:
  cd /opt/frame && /opt/frame/signals/.venv/bin/python -m signals.research.diagnostics_by_class
"""
from __future__ import annotations
import csv
import math
from collections import defaultdict
from statistics import fmean
from typing import Optional

CSV_PATH = "/tmp/research_v2_raw.csv"

# ───────────────────────────────────────────────────────────────
# Classification (вручную based on memory + Алgopack docs)
# ───────────────────────────────────────────────────────────────
TYPE_MAP = {
    # FX RUB pairs (фьючерсы валютных пар к рублю)
    "USDRUBF": "fx_rub", "EURRUBF": "fx_rub", "CNYRUBF": "fx_rub",
    "Si": "fx_rub", "Eu": "fx_rub", "CR": "fx_rub",
    # FX cross (не-рублевые валютные пары)
    "ED": "fx_cross", "AU": "fx_cross", "UC": "fx_cross",
    "DX": "fx_cross", "MY": "fx_cross",
    # Indices (индексы)
    "IMOEXF": "index", "MX": "index", "MM": "index",
    "RI": "index", "IB": "index", "VI": "index",
    # RU Equities (российские акции — отдельные эмитенты)
    "SR": "equity", "GZ": "equity", "LK": "equity", "VB": "equity",
    "GK": "equity", "SBERF": "equity", "GAZPF": "equity",
    "AL": "equity", "AF": "equity", "CH": "equity", "CE": "equity",
    "HY": "equity", "MC": "equity", "ME": "equity", "MN": "equity",
    "NM": "equity", "NK": "equity", "RN": "equity",
    "SN": "equity", "SP": "equity", "SS": "equity",
    "TN": "equity", "TT": "equity", "X5": "equity", "YD": "equity",
    "MG": "equity", "RL": "equity", "RM": "equity", "RB": "equity",
    "SE": "equity", "SF": "equity", "HS": "equity", "NA": "equity",
    # Metals (драгоценные металлы)
    "GD": "metal", "GL": "metal", "GLDRUBF": "metal",
    "SV": "metal", "PT": "metal", "PD": "metal", "HK": "metal",
    # Energy (энергоресурсы)
    "BR": "energy", "BM": "energy", "NG": "energy",
    "FF": "energy", "NR": "energy",
    # Soft commodities (с/х)
    "CC": "soft", "KC": "soft", "OJ": "soft", "W4": "soft",
}

# Tier by avg daily volume contracts (calculated from DB query)
TIER_MAP = {
    # Tier 1 — Top liquid (>100K avg volume)
    "CNYRUBF": 1, "CR": 1, "IMOEXF": 1, "GLDRUBF": 1, "NR": 1,
    "NG": 1, "Si": 1, "CC": 1, "SV": 1, "USDRUBF": 1,
    # Tier 2 — High (30-100K)
    "GK": 2, "IB": 2, "VB": 2, "MM": 2, "GD": 2, "GZ": 2,
    "SR": 2, "SS": 2, "GL": 2, "BR": 2, "MX": 2, "KC": 2,
    "FF": 2, "ED": 2, "Eu": 2,
    # Tier 3 — Mid (10-30K)
    "RB": 3, "YD": 3, "RI": 3, "UC": 3, "SF": 3,
    "X5": 3, "BM": 3, "MN": 3, "PT": 3, "CE": 3, "SE": 3,
    "SBERF": 3, "PD": 3, "TN": 3,
    # Tier 4 — Low (1-10K)
    "EURRUBF": 4, "RL": 4, "AF": 4, "LK": 4, "AL": 4,
    "HS": 4, "MC": 4, "ME": 4, "NM": 4, "RM": 4, "SP": 4,
    "SN": 4, "RN": 4, "TT": 4, "HY": 4, "MG": 4, "NK": 4,
    "OJ": 4, "NA": 4,
    # Tier 5 — Tail (<1K)
    "DX": 5, "CH": 5, "MY": 5, "VI": 5, "W4": 5,
    "HK": 5, "AU": 5,
}

# Главные features которые хотим перепроверить per class
FOCUS_FEATURES = [
    "price_vs_ema20_pct",   # тот что развалился на 65 — проверка hypothesis
    "price_vs_ema50_pct",   # аналог
    "atr_pct",              # сильнейший на full dataset
    "channel_pos",          # новый сильный
    "oi_price_corr_20d",    # OI confirmation
    "daily_range_pct",      # vola
    "fiz_avg_long_size",    # участники
    "yur_part_balance",
    "fiz_long_share",
    "fiz_net_change_5d",    # contrarian
    "yur_net_change_5d",    # smart money
    "volume_z",
]


# ───────────────────────────────────────────────────────────────
def correlation(xs: list[float], ys: list[float]) -> tuple[float, int]:
    n = len(xs)
    if n < 3 or n != len(ys):
        return 0.0, n
    mx, my = fmean(xs), fmean(ys)
    cov = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    sx = math.sqrt(sum((x - mx) ** 2 for x in xs))
    sy = math.sqrt(sum((y - my) ** 2 for y in ys))
    if sx == 0 or sy == 0:
        return 0.0, n
    return cov / (sx * sy), n


def t_stat(r: float, n: int) -> float:
    if abs(r) >= 1 or n < 3:
        return 0.0
    return r * math.sqrt((n - 2) / (1 - r * r))


def to_float(v) -> Optional[float]:
    if v in (None, "", "None"):
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def feature_t_stat(rows: list[dict], feature: str) -> tuple[float, int]:
    """t-statistic correlation feature ↔ fwd_return для подвыборки rows."""
    xs, ys = [], []
    for r in rows:
        xv = to_float(r.get(feature))
        yv = to_float(r.get("fwd_return"))
        if xv is None or yv is None:
            continue
        xs.append(xv)
        ys.append(yv)
    if len(xs) < 50:
        return 0.0, len(xs)
    r_val, n = correlation(xs, ys)
    return t_stat(r_val, n), n


def fmt_t(t: float, n: int) -> str:
    """Форматирование t-stat: ⚠ если n мал, цвета по силе."""
    if n < 50:
        return f" n={n}"
    if abs(t) > 3:
        return f"{t:+5.2f} ★★★"
    if abs(t) > 2:
        return f"{t:+5.2f} ★★ "
    if abs(t) > 1:
        return f"{t:+5.2f} ★  "
    return f"{t:+5.2f}    "


# ───────────────────────────────────────────────────────────────
def main():
    rows = []
    with open(CSV_PATH) as f:
        for r in csv.DictReader(f):
            sectype = r["sectype"]
            r["type"] = TYPE_MAP.get(sectype, "other")
            r["tier"] = TIER_MAP.get(sectype, 5)
            rows.append(r)
    print(f"Loaded {len(rows)} rows.\n")

    # ─────────────────────────────────────────────────────
    # BY TYPE
    # ─────────────────────────────────────────────────────
    types = sorted({r["type"] for r in rows})
    type_groups = {t: [r for r in rows if r["type"] == t] for t in types}

    print("=" * 105)
    print("ПО TYPE — t-stat features × категория")
    print("=" * 105)
    header = f"{'feature':>22} |"
    for t in types:
        header += f" {t:>11} |"
    print(header)
    sub = f"{'(n)':>22} |"
    for t in types:
        sub += f" {'n='+str(len(type_groups[t])):>11} |"
    print(sub)
    print("-" * 105)
    for feat in FOCUS_FEATURES:
        line = f"{feat:>22} |"
        for t in types:
            t_val, n = feature_t_stat(type_groups[t], feat)
            line += f" {fmt_t(t_val, n):>11} |"
        print(line)
    print()

    # ─────────────────────────────────────────────────────
    # BY TIER
    # ─────────────────────────────────────────────────────
    tier_groups = {tier: [r for r in rows if r["tier"] == tier]
                   for tier in range(1, 6)}

    print("=" * 100)
    print("ПО TIER — t-stat features × ликвидность (1 = top, 5 = tail)")
    print("=" * 100)
    header = f"{'feature':>22} |"
    for tier in range(1, 6):
        header += f" {'Tier '+str(tier):>11} |"
    print(header)
    sub = f"{'(n)':>22} |"
    for tier in range(1, 6):
        sub += f" {'n='+str(len(tier_groups[tier])):>11} |"
    print(sub)
    print("-" * 100)
    for feat in FOCUS_FEATURES:
        line = f"{feat:>22} |"
        for tier in range(1, 6):
            t_val, n = feature_t_stat(tier_groups[tier], feat)
            line += f" {fmt_t(t_val, n):>11} |"
        print(line)
    print()

    # ─────────────────────────────────────────────────────
    # Specific focus: price_vs_ema20_pct cross matrix
    # ─────────────────────────────────────────────────────
    print("=" * 100)
    print("FOCUS — price_vs_ema20_pct (тот что развалился) по TYPE × TIER")
    print("=" * 100)
    matrix = defaultdict(list)
    for r in rows:
        matrix[(r["type"], r["tier"])].append(r)

    type_tier_keys = sorted(matrix.keys())
    # By type, для каждого tier
    print(f"{'type \\ tier':>13} |" + "".join(f" Tier {t:>4} |" for t in range(1, 6)))
    print("-" * 100)
    for typ in types:
        line = f"{typ:>13} |"
        for tier in range(1, 6):
            group = matrix.get((typ, tier), [])
            t_val, n = feature_t_stat(group, "price_vs_ema20_pct")
            cell = fmt_t(t_val, n) if n >= 50 else f"n={n}"
            line += f" {cell:>11} |"
        print(line)
    print()

    # ─────────────────────────────────────────────────────
    # Per-asset top spotters
    # ─────────────────────────────────────────────────────
    print("=" * 80)
    print("PER-ASSET — топ-10 ассетов где price_vs_ema20_pct сильнее всего")
    print("=" * 80)
    asset_results = []
    for sectype in sorted({r["sectype"] for r in rows}):
        asset_rows = [r for r in rows if r["sectype"] == sectype]
        t_val, n = feature_t_stat(asset_rows, "price_vs_ema20_pct")
        if n >= 100:  # минимум данных
            asset_results.append((sectype, t_val, n))
    asset_results.sort(key=lambda x: -abs(x[1]))
    print(f"{'asset':>10} | {'type':>10} | {'tier':>5} | {'n':>5} | {'t-stat':>9} | direction")
    print("-" * 80)
    for sectype, t_val, n in asset_results[:15]:
        direction = "↑ выше → bearish" if t_val < 0 else "↑ выше → bullish"
        print(f"{sectype:>10} | {TYPE_MAP.get(sectype, '?'):>10} | "
              f"{TIER_MAP.get(sectype, '?'):>5} | {n:>5} | {t_val:>+7.2f}  | {direction}")
    print()


if __name__ == "__main__":
    main()
