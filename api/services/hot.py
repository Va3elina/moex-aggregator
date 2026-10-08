"""Витрина «Главное» (/hot, пока только админы).

Самое необычное на последний день данных по индикаторам, которые смотрит завод постов:
  • позиции физлиц — то же, что скринер сигналов сегодня (api/services/oi_screener.py): резкий сдвиг
    за день, за 2 недели и рекорд перекоса (от полугода);
  • деньги в фондах — рекорд недели или месяца, разворот после серии, серия от 4 месяцев;
  • сделки фондов — бумага, которую ≥10 фондов купили (продали), и никто не шёл против;
  • сезонность — следующие 3 месяца, в которые инструмент рос (падал) в ≥65% лет.

Карточка на фронте — график, на котором событие видно глазами: цветная зона (окно рекорда, две недели,
день), точки прошлых таких же пиков, линия прежнего максимума, подсвеченный столбик. Поэтому ручка
отдаёт не только подписи, но и сами ряды для графика (поле chart).
Контракты не показываем: позиция — перекос, как в скринере (чистая позиция в % от всех позиций).
"""
from __future__ import annotations

import statistics
from collections import deque
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

from sqlalchemy import text

from api.logger import get_logger
from api.services import oi_screener as scr

log = get_logger()

WAR = date(2022, 2, 24)
REC_WINDOWS: Tuple[Tuple[str, Optional[int]], ...] = (
    ("all", None), ("5y", 1825), ("4y", 1460), ("3y", 1095), ("2y", 730), ("1y", 365), ("6m", 182))
PERIOD_WORD = {"all": "за всё время", "5y": "за 5 лет", "4y": "за 4 года", "3y": "за 3 года",
               "2y": "за 2 года", "1y": "за год", "6m": "за полгода"}
ZONE_WORD = {"all": "вся история", "5y": "5 лет", "4y": "4 года", "3y": "3 года", "2y": "2 года", "1y": "год",
             "6m": "полгода"}
MAX_OI_CARDS = 10
EP_GAP = 40                          # торговых дней: прошлые пики ближе — один эпизод
SAME_CASE_DAYS = 60                  # прошлый пик ближе 2 месяцев к нынешнему — тот же эпизод

ALLOWED_GROUPS = {"Акции", "Валюта", "Индексы", "Сырьё"}
# Не наш рынок и экзотика: зарубежные индексы и бумаги, крипта, ставки, редкие валюты, агро.
FOREIGN = {
    "SF", "SP500F", "NA", "QQQF", "DJ", "DX", "N2", "HS", "SX", "R2", "SQ", "TL", "EM", "AA", "AG", "BZ", "CI",
    "ND", "KR", "SD", "BB", "BD", "JD", "DD", "TC", "XI", "TS", "SK", "A2", "AP", "NO", "TO", "SY", "HX",
    "IB", "BT", "EH", "ET", "S3", "XR", "TX", "I2", "BY", "AR", "RF", "RR", "MF", "KK", "W4", "CC", "KC", "OJ",
    "Su", "SA", "92", "95", "DL",
}
PRICE_INDEX = {
    "MX": "IMOEX", "MM": "IMOEX", "IMOEXF": "IMOEX", "MY": "IMOEX", "RI": "RTSI", "RM": "RTSI",
    "RB": "RGBI", "RGBIF": "RGBI", "Si": "USD000UTSTOM", "USDRUBF": "USD000UTSTOM",
    "CR": "CNYRUB_TOM", "CNYRUBF": "CNYRUB_TOM", "Eu": "EUR_RUB__TOM", "EURRUBF": "EUR_RUB__TOM",
    "GD": "GLDRUB_TOM", "GL": "GLDRUB_TOM", "GLDRUBF": "GLDRUB_TOM",
}
# Код базового актива в futures_contracts не всегда равен тикеру акции в candles.
STOCK_ALIAS = {"GAZR": "GAZP", "SBRF": "SBER", "SBPR": "SBERP", "SNGR": "SNGS", "SNGP": "SNGSP", "NOTK": "NVTK",
               "NOTKM": "NVTK", "MTSI": "MTSS", "TATP": "TATNP", "TRNF": "TRNFP", "PLZLM": "PLZL", "CHMFM": "CHMF",
               "BELUGA": "BELU", "BELUGAM": "BELU"}
MON = ["янв", "фев", "мар", "апр", "мая", "июн", "июл", "авг", "сен", "окт", "ноя", "дек"]


def _day(d: date) -> str:
    return f"{d.day} {MON[d.month - 1]}"


def _r(v: Optional[float], n: int = 1) -> Optional[float]:
    return None if v is None else round(v, n)


def skew(pos_long: float, pos_short: float) -> Optional[float]:
    """Перекос физлиц, как в скринере: чистая позиция в % от всех позиций (pos_short < 0)."""
    gross = pos_long - pos_short
    return (pos_long + pos_short) / gross * 100 if gross > 0 else None


# ── Позиции физлиц ──────────────────────────────────────────────────────────

def prior_extremes(dates: Sequence[date], vals: Sequence[float], days: Optional[int]) -> List[Tuple[Optional[float], Optional[float]]]:
    """(min, max) значений строго до i: в окне [d_i − days, d_i) или за всю историю (days=None).
    Скользящие очереди — O(n), окна те же, что у _prior_extremes скринера."""
    out: List[Tuple[Optional[float], Optional[float]]] = []
    if days is None:
        lo = hi = None
        for v in vals:
            out.append((lo, hi))
            lo = v if lo is None else min(lo, v)
            hi = v if hi is None else max(hi, v)
        return out
    qmin: deque = deque()
    qmax: deque = deque()
    for i, v in enumerate(vals):
        start = dates[i] - timedelta(days=days)
        while qmin and dates[qmin[0]] < start:
            qmin.popleft()
        while qmax and dates[qmax[0]] < start:
            qmax.popleft()
        out.append((vals[qmin[0]] if qmin else None, vals[qmax[0]] if qmax else None))
        while qmin and vals[qmin[-1]] >= v:
            qmin.pop()
        qmin.append(i)
        while qmax and vals[qmax[-1]] <= v:
            qmax.pop()
        qmax.append(i)
    return out


def record_at(v: float, ext_i: Dict[str, Tuple[Optional[float], Optional[float]]]) -> Optional[Tuple[str, str]]:
    """Сильнейший пробитый рекорд перекоса, как _record_for скринера: (kind, period)."""
    for p, _ in REC_WINDOWS:
        lo, hi = ext_i.get(p, (None, None))
        if hi is not None and v > hi:
            return "high", p
        if lo is not None and v < lo:
            return "low", p
    return None


def verb_for(net_now: float, skew_now: float, skew_before: float) -> str:
    """Глагол скринера по ходу перекоса: нога — по знаку чистой позиции."""
    if net_now >= 0:
        return "Физлица набрали лонг" if skew_now >= skew_before else "Физлица сократили лонг"
    return "Физлица нарастили шорт" if skew_now <= skew_before else "Физлица сократили шорт"


def screener_verb(net: float, direction: str) -> str:
    """Глагол ровно как в OiScreenerTable: по модулю чистой позиции (рост |net| — «набрали/нарастили»)."""
    net_long = net >= 0
    grew = net_long == (direction == "up")
    if net_long:
        return "Физлица набрали лонг" if grew else "Физлица сократили лонг"
    return "Физлица нарастили шорт" if grew else "Физлица сократили шорт"


def record_verb(kind: str, net: float) -> str:
    if kind == "low":
        return "Физлица нарастили шорт" if net < 0 else "Физлица сократили лонг"
    return "Физлица набрали лонг" if net >= 0 else "Физлица сократили шорт"


def cluster_firsts(idx: Sequence[int], gap: int) -> List[int]:
    """Первые дни эпизодов: срабатывания ближе gap торговых дней — один эпизод."""
    out: List[int] = []
    last = None
    for i in idx:
        if last is None or i - last > gap:
            out.append(i)
        last = i
    return out


def peak_of(vals: Sequence[float], idx: Sequence[int], kind: str, gap: int) -> List[int]:
    """Вершина каждого эпизода (не первый день): на графике кружок стоит на самом пике."""
    groups: List[List[int]] = []
    for i in idx:
        if groups and i - groups[-1][-1] <= gap:
            groups[-1].append(i)
        else:
            groups.append([i])
    pick = min if kind == "low" else max
    return [pick(g, key=lambda k: vals[k]) for g in groups]


def price_after(px: Sequence[Tuple[date, float]], day: date, n: int = 21) -> Optional[float]:
    """Изменение цены за n торговых дней от первого закрытия не раньше day, %."""
    lo, hi = 0, len(px)
    while lo < hi:
        mid = (lo + hi) // 2
        if px[mid][0] < day:
            lo = mid + 1
        else:
            hi = mid
    if lo + n >= len(px) or px[lo][1] <= 0:
        return None
    return round((px[lo + n][1] / px[lo][1] - 1) * 100, 1)


def _positions(db, sectypes: Sequence[str]) -> Dict[str, List[tuple]]:
    """Дневные ряды физлиц (последний бар дня, будни) за всю историю — в формате _bulk_series скринера:
    (tradedate, net, npart, oi, pos_long, pos_short)."""
    rows = db.execute(text("""
        SELECT sectype, tradedate, pos_long, pos_short, pos_long_num, pos_short_num, pos FROM (
          SELECT DISTINCT ON (sectype, tradedate) sectype, tradedate, pos_long, pos_short,
                 pos_long_num, pos_short_num, pos
            FROM open_interest
           WHERE clgroup = 'FIZ' AND interval = 24 AND sectype = ANY(:s)
             AND EXTRACT(ISODOW FROM tradedate) BETWEEN 1 AND 5
           ORDER BY sectype, tradedate, tradetime DESC) t
         ORDER BY sectype, tradedate
    """), {"s": list(sectypes)}).fetchall()
    out: Dict[str, List[tuple]] = {}
    for s, d, L, S, nL, nS, oi in rows:
        L, S = float(L or 0), float(S or 0)
        out.setdefault(s, []).append((d, L + S, int(nL or 0) + int(nS or 0), float(oi or 0), L, S))
    return out


def _prices(db, sectypes: Sequence[str]) -> Dict[str, List[Tuple[date, float]]]:
    """Цена базового актива: акция, индекс или курс; у металлов и нефти — склейка ближнего фьючерса (front_month).
    Нет и её — ряда нет (линии цены на графике не будет)."""
    codes = dict(db.execute(text(
        "SELECT sectype, max(assetcode) FROM futures_contracts WHERE sectype = ANY(:s) GROUP BY sectype"),
        {"s": list(sectypes)}).fetchall())
    out: Dict[str, List[Tuple[date, float]]] = {}
    for s in sectypes:
        if s in PRICE_INDEX:
            rows = db.execute(text("SELECT trade_date, close FROM index_data WHERE secid = :x AND close > 0 ORDER BY 1"),
                              {"x": PRICE_INDEX[s]}).fetchall()
        else:
            code = codes.get(s) or ""
            rows = []
            for secid in (STOCK_ALIAS.get(code), code, code[:-1] if code.endswith("F") else None):
                if not secid:
                    continue
                rows = db.execute(text("""
                    SELECT begin_time::date, close FROM candles
                     WHERE secid = :x AND interval = 24 AND type = 'stock' AND close > 0 ORDER BY 1"""),
                    {"x": secid}).fetchall()
                if rows:
                    break
        if not rows:
            rows = front_month(db.execute(text("""
                SELECT c.begin_time::date, c.close, f.lsttrade, f.is_perpetual
                  FROM candles c JOIN futures_contracts f ON f.secid = c.secid
                 WHERE f.sectype = :s AND c.interval = 24 AND c.type = 'futures' AND c.close > 0
                 ORDER BY 1"""), {"s": s}).fetchall())
        if rows:
            out[s] = [(r[0], float(r[1])) for r in rows]
    return out


def front_month(rows: Sequence[tuple]) -> List[Tuple[date, float]]:
    """Склейка цены фьючерса без базового актива (металлы, нефть): на каждый день — ближайший к экспирации
    контракт, который ещё торгуется; вечный фьючерс — когда срочных нет. rows: (день, close, lsttrade, вечный)."""
    best: Dict[date, Tuple[date, float]] = {}
    far = date(9999, 1, 1)
    for d, close, lst, perp in rows:
        exp = far if perp or lst is None else lst
        if exp < d:
            continue
        if d not in best or exp < best[d][0]:
            best[d] = (exp, float(close))
    return [(d, best[d][1]) for d in sorted(best)]


def _thin(dates: Sequence[date], keep_daily_from: date) -> List[int]:
    """Индексы точек графика: до keep_daily_from — по пятницам (длинная история), дальше — каждый день."""
    return [i for i, d in enumerate(dates) if d >= keep_daily_from or d.weekday() == 4 or i == 0]


def _price_on(px: Sequence[Tuple[date, float]], dates: Sequence[date]) -> List[Optional[float]]:
    """Цена на каждую дату графика — последнее известное закрытие не позже даты."""
    out, j = [], 0
    for d in dates:
        while j + 1 < len(px) and px[j + 1][0] <= d:
            j += 1
        out.append(px[j][1] if px and px[j][0] <= d else None)
    return out


def scan_positions(db) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """Позиции физлиц — ровно сегодняшние сигналы скринера: день (×2+), 2 недели (×3+), рекорд перекоса."""
    short = scr.compute_screener(db, "FIZ", "short")
    medium = scr.compute_screener(db, "FIZ", "medium")
    srows = {r["sectype"]: r for r in short["rows"]}
    mrows = {r["sectype"]: r for r in medium["rows"]}
    shown = {p for p, _ in REC_WINDOWS}
    rank_of = {p: k for k, (p, _) in enumerate(REC_WINDOWS)}
    cands = []
    for s, r in srows.items():
        if r["status"] == "illiquid" or r.get("group") not in ALLOWED_GROUPS or s in FOREIGN:
            continue
        m = mrows.get(s) or {}
        rec = r.get("record") if (r.get("record") or {}).get("period") in shown else None
        day = r["ratio"] if r["status"] == "sharp" else None
        wk = m.get("ratio") if m.get("status") == "sharp" else None
        if not (rec or day or wk):
            continue
        # рекорд сильнее любого сдвига; сдвиги — по силе относительно своего порога «резко»
        strength = (rank_of[rec["period"]] if rec else 10) - max((day or 0) / 2, (wk or 0) / 3) / 100
        cands.append({"s": s, "row": r, "mrow": m, "rec": rec, "day": day, "wk": wk, "strength": strength})
    priced = lambda c: c["s"] in PRICE_INDEX or c["row"].get("group") == "Акции"  # noqa: E731
    cands.sort(key=lambda c: (c["strength"], not priced(c)))
    cands = cands[:MAX_OI_CARDS]
    if not cands:
        return [], short.get("signal_date")
    series = _positions(db, [c["s"] for c in cands])
    px = _prices(db, [c["s"] for c in cands])
    cards = []
    for c in cands:
        s, r = c["s"], c["row"]
        pts = [p for p in series.get(s, []) if skew(p[4], p[5]) is not None]
        if len(pts) < 60:
            continue
        dates, vals = [p[0] for p in pts], [skew(p[4], p[5]) for p in pts]
        last = len(pts) - 1
        chart: Dict[str, Any] = {"type": "line"}
        tags: List[Dict[str, Any]] = []
        if c["rec"]:
            kind, period = c["rec"]["kind"], c["rec"]["period"]
            days = dict(REC_WINDOWS)[period]
            ext = prior_extremes(dates, vals, days)
            lo, hi = ext[last]
            level = hi if kind == "high" else lo
            # день прежнего рекорда — на графике точка (окно то же, что у prior_extremes: [сегодня − days, сегодня))
            lim = dates[last] - timedelta(days=days) if days else dates[0]
            k_lv = (max if kind == "high" else min)((k for k in range(last) if dates[k] >= lim),
                                                     key=lambda k: vals[k], default=None)
            need = days or 365
            hits = [k for k in range(len(vals)) if (dates[k] - dates[0]).days >= need and pts[k][2] >= scr.ATR_MIN_PART
                    and ((ext[k][1] is not None and vals[k] > ext[k][1]) if kind == "high"
                         else (ext[k][0] is not None and vals[k] < ext[k][0]))]
            peaks = [k for k in peak_of(vals, hits, kind, EP_GAP) if (dates[last] - dates[k]).days > SAME_CASE_DAYS]
            span = max(days or 0, 365) * 2 if days else None
            start = dates[last] - timedelta(days=span) if span else dates[0]
            chart.update(zone={"from": (dates[last] - timedelta(days=days)).isoformat() if days else dates[0].isoformat(),
                               "label": ZONE_WORD[period]},
                         level={"value": _r(level), "date": dates[k_lv].isoformat() if k_lv is not None else None,
                                "label": "прежний " + ("максимум" if kind == "high" else "минимум")})
            word = ("Макс " if kind == "high" else "Мин ") + PERIOD_WORD[period]
            tags.append({"tone": "fill" if period == "all" else "accent", "text": word})
            signal = screener_verb(r["net"], r["direction"]) if (c["day"] or c["wk"]) and r.get("direction") \
                else record_verb(kind, r["net"])
        else:
            peaks = []
            k_lv = None
            if c["wk"]:
                k0 = max(0, last - scr.MED_WINDOW)
                start = dates[last] - timedelta(days=122)
                chart.update(zone={"from": dates[k0].isoformat(), "label": "2 недели"},
                             start={"date": dates[k0].isoformat(), "value": _r(vals[k0])})
                direction = c["mrow"].get("direction") or ("up" if vals[last] >= vals[k0] else "down")
            else:
                k0 = max(0, last - 1)
                start = dates[last] - timedelta(days=92)
                chart.update(zone={"from": dates[k0].isoformat(), "label": "день"},
                             start={"date": dates[k0].isoformat(), "value": _r(vals[k0])})
                direction = r.get("direction") or ("up" if vals[last] >= vals[k0] else "down")
            signal = screener_verb(r["net"], direction)
        if c["wk"]:
            tags.append({"tone": "pill", "text": f"×{c['wk']:.1f}".replace(".", ","), "note": "за 2 недели"})
        if c["day"]:
            tags.append({"tone": "pill", "text": f"×{c['day']:.1f}".replace(".", ","), "note": "за день"})
        keep = _thin(dates, dates[last] - timedelta(days=760))
        idx = [i for i in keep if dates[i] >= start]
        if k_lv is not None and k_lv not in idx:     # прореженная история могла пропустить день рекорда
            idx = sorted(idx + [k_lv])
        if not c["rec"] and chart.get("start"):
            # сдвиг: с чем сравнить — прежний максимум (минимум) видимого периода до начала сдвига
            before = [i for i in idx if dates[i] < date.fromisoformat(chart["start"]["date"])]
            if before:
                up = vals[last] >= chart["start"]["value"]
                k = (max if up else min)(before, key=lambda q: vals[q])
                chart["level"] = {"value": _r(vals[k]), "date": dates[k].isoformat(),
                                  "label": "прежний " + ("максимум" if up else "минимум")}
                peaks = [k]
        sel_dates = [dates[i] for i in idx]
        chart["series"] = [[d.isoformat(), _r(vals[i])] for d, i in zip(sel_dates, idx)]
        if s in px:
            chart["price"] = [_r(v, 4) for v in _price_on(px[s], sel_dates)]
        chart["peaks"] = [[dates[k].isoformat(), _r(vals[k])] for k in peaks if dates[k] >= start]
        chart["now"] = {"date": dates[last].isoformat(), "value": _r(vals[last])}
        cards.append({
            "kind": "oi", "id": f"oi:{s}", "sectype": s, "name": r["name"], "signal": signal, "tags": tags,
            "date": dates[last].isoformat(), "chart": chart,
        })
    return cards, short.get("signal_date")


# ── Деньги в фондах ─────────────────────────────────────────────────────────

FUND_CATS = (("money_market", "Денежные фонды"), ("stocks", "Фонды акций"), ("bonds", "Облигационные фонды"),
             ("gold", "Фонды золота"), ("yuan", "Юаневые фонды"))
MIN_FLOW_BN = 0.1
MONTHS_FROM = "2022-01"
WEEKS_SHOWN = 104


def _bn(v: float) -> str:
    a = abs(v)
    s = f"{a:.0f}" if a >= 10 else f"{a:.2f}".rstrip("0").rstrip(".") if a < 1 else f"{a:.1f}"
    return ("+" if v > 0 else "−" if v < 0 else "") + s.replace(".", ",")


def runs(vals: Sequence[float]) -> List[Tuple[int, int, int]]:
    """Серии одного знака: (знак, начало, конец)."""
    out: List[Tuple[int, int, int]] = []
    for i, v in enumerate(vals):
        sg = 1 if v > 0 else -1 if v < 0 else 0
        if out and out[-1][0] == sg:
            out[-1] = (sg, out[-1][1], i)
        else:
            out.append((sg, i, i))
    return out


def fund_case(months: List[Tuple[str, float]], weeks: List[Tuple[str, str, float]], today: date) -> Optional[Dict[str, Any]]:
    """Сильнейший случай категории: рекорд недели → рекорд месяца → разворот → серия.
    Возвращает подписи и разметку графика (какой столбик подсветить, прежний рекорд, серия)."""
    cur_month = today.strftime("%Y-%m")
    full = [(m, v) for m, v in months if m < cur_month]
    if len(full) < 6:
        return None
    vals = [v for _, v in full]
    m_last, v_last = full[-1]
    # 1) рекорд недели среди трёх последних полных недель
    wk = [w for w in weeks if w[1] < today.isoformat()]
    for k in range(len(wk) - 1, max(len(wk) - 4, 26), -1):
        prior = [w[2] for w in wk[:k]]
        v = wk[k][2]
        if abs(v) >= MIN_FLOW_BN and (v < min(prior) or v > max(prior)):
            kind = "отток" if v < 0 else "приток"
            prev = min(range(len(prior)), key=lambda q: prior[q]) if v < 0 else max(range(len(prior)), key=lambda q: prior[q])
            d0, d1 = date.fromisoformat(wk[k][0]), date.fromisoformat(wk[k][1])
            first = max(0, min(prev - 4, len(wk) - WEEKS_SHOWN))
            return {"case": "week_record", "signal": f"Рекордный {kind} за неделю", "amount": v,
                    "date_label": f"{d0.day}–{d1.day} {MON[d1.month - 1]}",
                    "chart": {"type": "bars", "unit": "млрд ₽", "weekly": True,
                              "bars": [[w[0], round(w[2], 3)] for w in wk[first:]],
                              "hl": wk[k][0], "prev": wk[prev][0], "level": round(prior[prev], 3)}}
    if abs(v_last) < MIN_FLOW_BN:
        return None
    month_word = MON[int(m_last[5:7]) - 1]
    rr = runs(vals)
    sg, a, b = rr[-1]
    length = b - a + 1
    turn = length == 1 and len(rr) >= 2 and rr[-2][0] != 0 and rr[-2][2] - rr[-2][1] + 1 >= 3
    n_prev = rr[-2][2] - rr[-2][1] + 1 if len(rr) >= 2 else 0
    was = ("притока" if rr[-2][0] > 0 else "оттока") if len(rr) >= 2 else ""
    shown = [(m, v) for m, v in full if m >= MONTHS_FROM]
    chart: Dict[str, Any] = {"type": "bars", "unit": "млрд ₽", "weekly": False,
                             "bars": [[m, round(v, 3)] for m, v in shown], "hl": m_last}
    # 2) рекорд месяца (если это ещё и разворот — пометкой)
    if v_last < min(vals[:-1]) or v_last > max(vals[:-1]):
        kind = "отток" if v_last < 0 else "приток"
        prior = vals[:-1]
        prev = min(range(len(prior)), key=lambda q: prior[q]) if v_last < 0 else max(range(len(prior)), key=lambda q: prior[q])
        chart.update(prev=full[prev][0], level=round(prior[prev], 3))
        if turn:
            chart["run"] = {"from": full[rr[-2][1]][0], "to": full[rr[-2][2]][0], "label": f"{n_prev} мес {was}"}
        return {"case": "month_record", "signal": f"Рекордный {kind} за месяц", "amount": v_last,
                "date_label": month_word, "note": f"первый после {n_prev} мес {was}" if turn else None, "chart": chart}
    # 3) разворот после серии от 3 месяцев
    if turn:
        what = "отток" if v_last < 0 else "приток"
        chart["run"] = {"from": full[rr[-2][1]][0], "to": full[rr[-2][2]][0], "label": f"{n_prev} мес {was}"}
        return {"case": "reversal", "signal": f"Первый {what} после {n_prev} мес {was}", "amount": v_last,
                "date_label": month_word, "chart": chart}
    # 4) серия от 4 месяцев
    if length >= 4 and sg != 0:
        what = "Приток" if sg > 0 else "Отток"
        chart["run"] = {"from": full[a][0], "to": full[b][0], "label": f"{length} мес подряд"}
        return {"case": "streak", "signal": f"{what} {length}-й месяц подряд", "amount": v_last,
                "date_label": month_word, "chart": chart}
    return None


def scan_funds(user, today: date) -> List[Dict[str, Any]]:
    from api.routers import funds as F   # роут-функции сайта: тот же расчёт потоков, что на странице
    cards = []
    for cat, name in FUND_CATS:
        try:
            mo = F.get_funds_flows(category=cat, timeframe="1m", period="all", fund_ids_filter=None, rolling=None, user=user)
            wk = F.get_funds_flows(category=cat, timeframe="1w", period="all", fund_ids_filter=None, rolling=None, user=user)
        except Exception as e:  # noqa: BLE001 — одна категория не роняет витрину
            log.warning(f"hot: потоки {cat} не посчитались: {e}")
            continue
        months = [(r["period_start"][:7], float(r["flow"])) for r in mo.get("flows", [])]
        weeks = [(r["period_start"], r["period_end"], float(r["flow"])) for r in wk.get("flows", [])]
        case = fund_case(months, weeks, today)
        if case:
            cards.append({"kind": "flows", "id": f"flows:{cat}", "category": cat, "name": name, **case})
    order = {"week_record": 0, "month_record": 1, "reversal": 2, "streak": 3}
    return sorted(cards, key=lambda c: order[c["case"]])


# ── Сделки фондов ───────────────────────────────────────────────────────────

UNANIMOUS_MIN = 10


def scan_trades(db, user) -> List[Dict[str, Any]]:
    from api.routers import fund_trades as T
    r = T.top_movers(period="1m", category="stocks", as_of=None, range_from=None, range_to=None, manager=None,
                     funds=None, sort="amount", limit=100, scope="movers", user=user, db=db)
    month = (r.get("resolved_month") or "")[:7]
    n = r.get("funds_in_month") or 0
    picked = []
    for side, lst in (("buy", r.get("top_accumulated", [])), ("sell", r.get("top_reduced", []))):
        for x in lst:
            yes, no = (x["funds_buying"], x["funds_selling"]) if side == "buy" else (x["funds_selling"], x["funds_buying"])
            if yes >= UNANIMOUS_MIN and no == 0:
                picked.append((side, yes, x))
    picked.sort(key=lambda p: -abs(p[2]["total_delta_amount"]))
    cards = []
    for side, yes, x in picked[:2]:
        secid = db.execute(text("SELECT secid FROM securities_ref WHERE isin = :i AND secid IS NOT NULL LIMIT 1"),
                           {"i": x["akey"]}).scalar()
        flows = T.company_flows(isin=x["akey"], asset_name=None, metric="amount", user=user, db=db)
        ms, tot = flows.get("months", []), flows.get("total", [])
        first = max(0, len(ms) - 24)
        bars = [[m, round((v or 0) / 1e6, 1)] for m, v in zip(ms[first:], tot[first:])]
        cards.append({
            "kind": "trades", "id": f"trades:{x['akey']}", "isin": x["akey"], "name": x["asset_name"], "secid": secid,
            "signal": "Фонды покупали единодушно" if side == "buy" else "Фонды продавали единодушно",
            "funds": f"{yes} из {n} фондов", "amount": round(x["total_delta_amount"] / 1e9, 2),
            "date_label": MON[int(month[5:7]) - 1] if month else "",
            "chart": {"type": "bars", "unit": "млн ₽", "weekly": False, "bars": bars, "hl": month},
        })
    return cards


# ── Сезонность ──────────────────────────────────────────────────────────────

SEASON_ASSETS = (("IMOEX", "Индекс МосБиржи", "MX", 1997), ("USD000UTSTOM", "Доллар", "Si", 2003),
                 ("CNYRUB_TOM", "Юань", "CR", 2014), ("GLDRUB_TOM", "Золото", "GD", 2014))
SEASON_DAYS = 91
SEASON_EDGE = 0.65


def season_years(closes: Sequence[Tuple[date, float]], today: date, first_year: int) -> List[Tuple[int, float]]:
    """Доходность окна [та же дата, +SEASON_DAYS] по каждому полному году, %."""
    out = []
    for y in range(first_year, today.year):
        try:
            d0 = today.replace(year=y)
        except ValueError:                       # 29 февраля
            d0 = today.replace(year=y, day=28)
        d1 = d0 + timedelta(days=SEASON_DAYS)
        c0 = [c for d, c in closes if d <= d0]
        c1 = [c for d, c in closes if d <= d1]
        if c0 and c1 and (d0 - next(d for d, _ in closes)).days > 0:
            out.append((y, round((c1[-1] / c0[-1] - 1) * 100, 1)))
    return out


def scan_season(db, today: date) -> List[Dict[str, Any]]:
    from api.database import get_engine
    from api.routers.seasonality import _compute_yearly_seasonality   # та же кривая, что на странице «Сезонность»
    cards = []
    for secid, name, sectype, y0 in SEASON_ASSETS:
        rows = db.execute(text("SELECT trade_date, close FROM index_data WHERE secid = :s AND close > 0 ORDER BY 1"),
                          {"s": secid}).fetchall()
        closes = [(r[0], float(r[1])) for r in rows if r[0].year >= y0]
        yrs = season_years(closes, today, y0)
        if len(yrs) < 12:
            continue
        up = sum(1 for _, v in yrs if v > 0)
        share = up / len(yrs)
        if SEASON_EDGE > share > 1 - SEASON_EDGE:
            continue                                   # монетка — карточку не ставим
        rising = share >= SEASON_EDGE
        data = _compute_yearly_seasonality(get_engine(), secid, {}, since_year=None, exclude_years=[], agg_type="avg",
                                           live=False) or {}
        avg = [[p["td"], p["avg_pct"], p["month"]] for p in data.get("average", [])]
        cur = [[p["td"], p["pct"]] for p in data.get("current", [])]
        if not avg or not cur:
            continue
        today_td = cur[-1][0]
        cards.append({
            "kind": "season", "id": f"season:{secid}", "secid": secid, "sectype": sectype, "name": name,
            "signal": "Следующие 3 месяца " + ("чаще рос" if rising else "чаще падал"),
            "hits": f"{'рост' if rising else 'падение'} в {up if rising else len(yrs) - up} из {len(yrs)} лет",
            "date_label": f"{_day(today)} → {_day(today + timedelta(days=SEASON_DAYS))}",
            "chart": {"type": "season", "avg": avg, "cur": cur, "today": today_td,
                      "zone_to": min(today_td + 63, avg[-1][0]), "rising": rising},
        })
    return cards


# ── Вся витрина ─────────────────────────────────────────────────────────────

def compute_hot(db, user, today: Optional[date] = None) -> Dict[str, Any]:
    today = today or date.today()
    out: Dict[str, Any] = {"generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"), "cards": []}
    as_of = None
    for name in ("positions", "funds", "trades", "season"):
        try:
            if name == "positions":
                cards, as_of = scan_positions(db)
            elif name == "funds":
                cards = scan_funds(user, today)
            elif name == "trades":
                cards = scan_trades(db, user)
            else:
                cards = scan_season(db, today)
            out["cards"].extend(cards)
        except Exception as e:  # noqa: BLE001 — один раздел не роняет страницу
            log.exception(f"hot: раздел {name} упал: {e}")
            out.setdefault("errors", []).append(name)
            try:
                db.rollback()
            except Exception:  # noqa: BLE001
                pass
    out["as_of"] = as_of or today.isoformat()
    return out
