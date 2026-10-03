"""Витрина «Главное» (/hot, пока только админы).

Самое необычное за последние две недели по данным, которые смотрит завод постов:
  • позиции физлиц — логика скринера (api/services/oi_screener.py): рекорд перекоса за год и дольше
    или сдвиг за 2 недели от ×3; прошлые такие же случаи и цена через месяц после каждого;
  • деньги в фондах — рекорд недели или месяца, разворот после серии, серия от 4 месяцев;
    тот же срок в прошлые годы;
  • сделки фондов — бумага, которую ≥10 фондов купили (продали), и никто не шёл против;
  • сезонность — следующие 3 месяца, в которые инструмент рос (падал) в ≥65% лет.

Графики рисует фронт теми же компонентами и ручками, что и страницы индикаторов (SimpleChart,
FlowsHistogram, CompanyFlowsHistogram, YearlySeasonalityChart) — здесь только отбор и подписи.
Контракты не показываем: мерка позиции — перекос, как в скринере (чистая позиция в % от всех).
После 24.02.2022 рынок другой — прошлые случаи помечены, до этой даты или после.
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
RECENT_DAYS = 10                     # торговых дней, в которых ищем свежий сигнал
EXP_BEFORE, EXP_AFTER = 7, 3         # дни у экспирации: перенос позиций выглядит как рекорд или сброс
REC_WINDOWS: Tuple[Tuple[str, Optional[int]], ...] = (
    ("all", None), ("5y", 1825), ("4y", 1460), ("3y", 1095), ("2y", 730), ("1y", 365))
PERIOD_WORD = {"all": "за всё время", "5y": "за 5 лет", "4y": "за 4 года", "3y": "за 3 года",
               "2y": "за 2 года", "1y": "за год"}
MAX_RECORD_CARDS, MAX_MOVE_CARDS = 8, 3
EP_GAP_REC, EP_GAP_MOVE = 40, 20     # торговых дней: ближе — тот же случай
SAME_CASE_DAYS = 60                  # прошлый случай ближе 2 месяцев к нынешнему — тот же эпизод
AFTER_DAYS = 21                      # «через месяц» — 21 торговый день
TAIL = scr.MED_WINDOW + scr.MED_BASE_WINDOW + 6   # хвост ряда, которого хватает среднесрочному сигналу

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
WHAT = {"Акции": "Акция", "Валюта": "Курс", "Индексы": "Индекс", "Сырьё": "Цена"}
MON = ["янв", "фев", "мар", "апр", "мая", "июн", "июл", "авг", "сен", "окт", "ноя", "дек"]


def _day(d: date) -> str:
    return f"{d.day} {MON[d.month - 1]}"


def skew(pos_long: float, pos_short: float) -> Optional[float]:
    """Перекос физлиц, как в скринере: чистая позиция в % от всех позиций (pos_short < 0)."""
    gross = pos_long - pos_short
    return (pos_long + pos_short) / gross * 100 if gross > 0 else None


# ── Позиции физлиц ──────────────────────────────────────────────────────────

def prior_extremes(dates: Sequence[date], vals: Sequence[float], days: Optional[int]) -> List[Tuple[Optional[float], Optional[float]]]:
    """(min, max) значений строго до i: в окне [d_i − days, d_i) или за всю историю (days=None).
    Скользящие очереди — O(n), как окна _prior_extremes скринера, только на каждый день."""
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
    """Сильнейший пробитый рекорд перекоса (≥ года), как _record_for скринера: (kind, period)."""
    for p, _ in REC_WINDOWS:
        lo, hi = ext_i[p]
        if hi is not None and v > hi:
            return "high", p
        if lo is not None and v < lo:
            return "low", p
    return None


def verb_for(net_now: float, skew_now: float, skew_before: float) -> str:
    """Глагол скринера: нога — по знаку чистой позиции, «набрали/сократили» — по ходу перекоса."""
    if net_now >= 0:
        return "Физлица набрали лонг" if skew_now >= skew_before else "Физлица сократили лонг"
    return "Физлица нарастили шорт" if skew_now <= skew_before else "Физлица сократили шорт"


def cluster_firsts(idx: Sequence[int], gap: int) -> List[int]:
    """Первые дни эпизодов: срабатывания ближе gap торговых дней — один эпизод."""
    out: List[int] = []
    last = None
    for i in idx:
        if last is None or i - last > gap:
            out.append(i)
        last = i
    return out


def price_after(px: Sequence[Tuple[date, float]], day: date, n: int = AFTER_DAYS) -> Optional[float]:
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


def _expiry_mask(db, sectypes: Sequence[str]) -> Dict[str, List[date]]:
    rows = db.execute(text("""
        SELECT sectype, lsttrade FROM futures_contracts
         WHERE sectype = ANY(:s) AND lsttrade IS NOT NULL AND NOT COALESCE(is_perpetual, false)
    """), {"s": list(sectypes)}).fetchall()
    out: Dict[str, List[date]] = {}
    for s, lt in rows:
        out.setdefault(s, []).append(lt)
    return out


def _masked(exp: List[date], d: date) -> bool:
    return any(lt - timedelta(days=EXP_BEFORE) <= d <= lt + timedelta(days=EXP_AFTER) for lt in exp)


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
    """Цена базового актива для «что было после»: акция, индекс или курс. Нет — ряда нет."""
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
        if rows:
            out[s] = [(r[0], float(r[1])) for r in rows]
    return out


def _oi_series_meta(pts: List[tuple]) -> Tuple[List[date], List[Optional[float]]]:
    return [p[0] for p in pts], [skew(p[4], p[5]) for p in pts]


def scan_positions(db, as_of: Optional[date] = None) -> List[Dict[str, Any]]:
    """Сигналы скринера за последние RECENT_DAYS торговых дней, по карточке на актив."""
    base = scr.compute_screener(db, "FIZ", "medium")
    meta = {r["sectype"]: r for r in base["rows"]
            if r["status"] != "illiquid" and r.get("group") in ALLOWED_GROUPS and r["sectype"] not in FOREIGN}
    if not meta:
        return []
    series = _positions(db, list(meta))
    expiry = _expiry_mask(db, list(meta))
    found: List[Dict[str, Any]] = []
    for s, pts in series.items():
        pts = [p for p in pts if skew(p[4], p[5]) is not None]
        if len(pts) < 300:
            continue
        dates, vals = [p[0] for p in pts], [skew(p[4], p[5]) for p in pts]
        ext = {p: prior_extremes(dates, vals, days) for p, days in REC_WINDOWS}
        best = None
        for i in range(len(pts) - 1, max(len(pts) - 1 - RECENT_DAYS, scr.MED_WINDOW), -1):
            d = dates[i]
            if as_of and d > as_of:
                continue
            if _masked(expiry.get(s, []), d) or pts[i][2] < scr.ATR_MIN_PART:
                continue
            rec = record_at(vals[i], {p: ext[p][i] for p, _ in REC_WINDOWS})
            med = scr._row_signal_medium(pts[max(0, i + 1 - TAIL):i + 1], scr.ATR_MIN_PART)
            cand = None
            if rec:
                rank = [p for p, _ in REC_WINDOWS].index(rec[1])
                cand = {"type": "record", "kind": rec[0], "period": rec[1], "rank": rank, "i": i,
                        "move": med["ratio"] if med["status"] == "sharp" else None}
            elif med["status"] == "sharp":
                # рекорды (ранг 0–5) всегда выше сдвигов; среди сдвигов — сильнее выше
                cand = {"type": "move", "ratio": med["ratio"], "dir": med["direction"],
                        "rank": 6 + (10 - min(med["ratio"], 9.9)) / 10, "i": i}
            if cand and (best is None or (cand["rank"], -cand["i"]) < (best["rank"], -best["i"])):
                best = cand            # в одном ряду: сильнее, при равенстве — свежее
        if best:
            best.update(sectype=s, pts=pts, dates=dates, vals=vals, ext=ext)
            found.append(best)
    # между активами — по силе рекорда; при равной силе выше те, у кого есть цена для «что было после»
    # и заодно резкий сдвиг за 2 недели, потом свежие (номера дней у рядов разные — сравниваем даты)
    def priced(sec: str) -> bool:
        return sec in PRICE_INDEX or meta[sec].get("group") == "Акции"
    found.sort(key=lambda c: (c["rank"], not priced(c["sectype"]), c.get("move") is None,
                              -c["dates"][c["i"]].toordinal()))
    # рекорды не вытесняют сдвиги за 2 недели целиком: витрине нужны оба вида
    found = [c for c in found if c["type"] == "record"][:MAX_RECORD_CARDS] + \
            [c for c in found if c["type"] == "move"][:MAX_MOVE_CARDS]
    px = _prices(db, [c["sectype"] for c in found])
    cards = []
    for c in found:
        s, pts, dates, vals, i = c["sectype"], c["pts"], c["dates"], c["vals"], c["i"]
        m = meta[s]
        j = max(0, i - scr.MED_WINDOW)
        verb = verb_for(pts[i][1], vals[i], vals[j])
        if c["type"] == "record":
            days = dict(REC_WINDOWS)[c["period"]]
            lo, hi = c["ext"][c["period"]][i]
            level = hi if c["kind"] == "high" else lo
            # прошлый «минимум за 2 года» — только там, где за спиной уже были полные 2 года истории
            need = days or 365
            hits = [k for k in range(len(vals)) if (dates[k] - dates[0]).days >= need
                    and record_hit(vals[k], c["ext"][c["period"]][k], c["kind"])
                    and pts[k][2] >= scr.ATR_MIN_PART and not _masked(expiry.get(s, []), dates[k])]
            firsts = cluster_firsts(hits, EP_GAP_REC)
            word = PERIOD_WORD[c["period"]]
            tag = {"type": "record", "text": ("Макс " if c["kind"] == "high" else "Мин ") + word,
                   "all": c["period"] == "all",
                   "move": f"×{c['move']:.1f}".replace(".", ",") if c.get("move") else None}
            window_start = (dates[i] - timedelta(days=days)).isoformat() if days else None
            chart_period = "all"
        else:
            verbs = []
            for k in range(len(pts)):
                med = scr._row_signal_medium(pts[max(0, k + 1 - TAIL):k + 1], scr.ATR_MIN_PART)
                if med["status"] == "sharp" and pts[k][2] >= scr.ATR_MIN_PART \
                        and verb_for(pts[k][1], vals[k], vals[max(0, k - scr.MED_WINDOW)]) == verb:
                    verbs.append(k)
            firsts = cluster_firsts(verbs, EP_GAP_MOVE)
            level = None
            tag = {"type": "move", "text": f"×{c['ratio']:.1f}".replace(".", ","), "note": "за 2 недели"}
            window_start = dates[j].isoformat()
            chart_period = "1y"
        eps = []
        for k in firsts:
            if (dates[i] - dates[k]).days <= SAME_CASE_DAYS:
                continue
            a = price_after(px[s], dates[k]) if s in px else None
            if a is None:
                continue
            eps.append({"date": dates[k].isoformat(), "after": a, "post2022": dates[k] >= WAR})
        cards.append({
            "kind": "oi", "id": f"oi:{s}", "sectype": s, "name": m["name"], "group": m.get("group"),
            "section": "Позиции физлиц", "signal": verb, "tag": tag,
            "date": dates[i].isoformat(), "date_label": _day(dates[i]),
            "skew_now": round(vals[-1], 1), "level": round(level, 1) if level is not None else None,
            "window_start": window_start, "chart_period": chart_period,
            "what": WHAT.get(m.get("group"), "Цена"), "has_price": s in px, "episodes": eps,
        })
    return cards


def record_hit(v: float, ext_i: Tuple[Optional[float], Optional[float]], kind: str) -> bool:
    lo, hi = ext_i
    return (hi is not None and v > hi) if kind == "high" else (lo is not None and v < lo)


# ── Деньги в фондах ─────────────────────────────────────────────────────────

FUND_CATS = (("money_market", "Денежные фонды"), ("stocks", "Фонды акций"), ("bonds", "Облигационные фонды"),
             ("gold", "Фонды золота"), ("yuan", "Юаневые фонды"))
MIN_FLOW_BN = 0.1


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
    """Сильнейший случай категории: рекорд недели → рекорд месяца → разворот → серия."""
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
            nxt = [w[2] for w in wk[prev + 1:prev + 5]]
            d0, d1 = date.fromisoformat(wk[k][0]), date.fromisoformat(wk[k][1])
            return {"case": "week_record", "signal": f"Рекордный {kind} за неделю", "amount": wk[k][2],
                    "date_label": f"{d0.day}–{d1.day} {MON[d1.month - 1]}", "timeframe": "1w", "period": "3y",
                    "compare": {"label": f"После прошлого рекорда ({MON[date.fromisoformat(wk[prev][0]).month - 1]} "
                                         f"{wk[prev][0][2:4]}) за месяц ещё", "chips": [{"text": _bn(sum(nxt)) + " млрд",
                                         "cls": "up" if sum(nxt) > 0 else "dn"}]} if len(nxt) == 4 else None}
    if abs(v_last) < MIN_FLOW_BN:
        return None
    month_word = MON[int(m_last[5:7]) - 1]
    rr = runs(vals)
    sg, a, b = rr[-1]
    length = b - a + 1
    ytd = _ytd_chips(full, m_last)
    turn = length == 1 and len(rr) >= 2 and rr[-2][0] != 0 and rr[-2][2] - rr[-2][1] + 1 >= 3
    n_prev = rr[-2][2] - rr[-2][1] + 1 if len(rr) >= 2 else 0
    was = ("притока" if rr[-2][0] > 0 else "оттока") if len(rr) >= 2 else ""
    # 2) рекорд месяца (если это ещё и разворот — пометкой)
    if v_last < min(vals[:-1]) or v_last > max(vals[:-1]):
        kind = "отток" if v_last < 0 else "приток"
        return {"case": "month_record", "signal": f"Рекордный {kind} за месяц", "amount": v_last,
                "date_label": month_word, "timeframe": "1m", "period": "all", "compare": ytd,
                "note": f"первый после {n_prev} мес {was}" if turn else None,
                "run": [full[rr[-2][1]][0], full[rr[-2][2]][0]] if turn else None}
    # 3) разворот после серии от 3 месяцев
    if turn:
        what = "отток" if v_last < 0 else "приток"
        chips = []
        for (s0, a0, b0), nxt in zip(rr[:-2], rr[1:-1]):
            if s0 == rr[-2][0] and b0 - a0 + 1 >= 3 and b0 + 3 < len(vals) - 1:
                tot = sum(vals[b0 + 1:b0 + 4])
                yr = full[b0 + 1][0][2:4]
                chips.append({"text": _bn(tot), "cls": "up" if tot > 0 else "dn", "sub": yr,
                              "old": full[b0 + 1][0] < "2022-03"})
        return {"case": "reversal", "signal": f"Первый {what} после {n_prev} мес {was}", "amount": v_last,
                "date_label": month_word, "timeframe": "1m", "period": "all",
                "run": [full[rr[-2][1]][0], full[rr[-2][2]][0]],
                "compare": {"label": "Следующие 3 месяца после прошлых разворотов, млрд ₽", "chips": chips[-4:]}
                if chips else ytd}
    # 4) серия от 4 месяцев
    if length >= 4 and sg != 0:
        what = "Приток" if sg > 0 else "Отток"
        return {"case": "streak", "signal": f"{what} {length}-й месяц подряд", "amount": v_last,
                "date_label": month_word, "timeframe": "1m", "period": "all", "run": [full[a][0], full[b][0]],
                "compare": ytd}
    return None


def _ytd_chips(full: List[Tuple[str, float]], m_last: str) -> Dict[str, Any]:
    """Тот же срок с начала года по годам: так видно, необычно ли это."""
    mm = m_last[5:7]
    per: Dict[str, float] = {}
    for m, v in full:
        if m[5:7] <= mm:
            per[m[:4]] = per.get(m[:4], 0.0) + v
    years = sorted(per, reverse=True)
    first_mon, last_mon = "январь", ["январь", "февраль", "март", "апрель", "май", "июнь", "июль", "август",
                                     "сентябрь", "октябрь", "ноябрь", "декабрь"][int(mm) - 1]
    label = f"{first_mon.capitalize()}–{last_mon}, млрд ₽" if mm != "01" else "Январь, млрд ₽"
    return {"label": label, "chips": [{"text": _bn(per[y]), "cls": "up" if per[y] > 0 else "dn", "sub": y[2:],
                                       "old": y < "2022"} for y in years[:8]]}


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
            cards.append({"kind": "flows", "id": f"flows:{cat}", "category": cat, "name": name,
                          "section": "Деньги в фондах", **case})
    order = {"week_record": 0, "month_record": 1, "reversal": 2, "streak": 3}
    return sorted(cards, key=lambda c: order[c["case"]])


# ── Сделки фондов ───────────────────────────────────────────────────────────

UNANIMOUS_MIN = 10


def scan_trades(db, user) -> List[Dict[str, Any]]:
    from api.routers import fund_trades as T
    r = T.top_movers(period="1m", category="stocks", as_of=None, range_from=None, range_to=None, manager=None,
                     funds=None, sort="amount", limit=100, scope="movers", user=user, db=db)
    month = r.get("resolved_month") or ""
    n = r.get("funds_in_month") or 0
    cards = []
    for side, lst in (("buy", r.get("top_accumulated", [])), ("sell", r.get("top_reduced", []))):
        for x in lst:
            yes, no = (x["funds_buying"], x["funds_selling"]) if side == "buy" else (x["funds_selling"], x["funds_buying"])
            if yes >= UNANIMOUS_MIN and no == 0:
                secid = db.execute(text("SELECT secid FROM securities_ref WHERE isin = :i AND secid IS NOT NULL LIMIT 1"),
                                   {"i": x["akey"]}).scalar()
                cards.append({
                    "kind": "trades", "id": f"trades:{x['akey']}", "isin": x["akey"], "asset_name": x["asset_name"],
                    "secid": secid, "section": "Сделки фондов",
                    "signal": "Фонды покупали единодушно" if side == "buy" else "Фонды продавали единодушно",
                    "funds": f"{yes} из {n} фондов", "amount_rub": x["total_delta_amount"],
                    "date_label": (MON[int(month[5:7]) - 1] if month else "") + (" · никто не продавал" if side == "buy" else " · никто не покупал"),
                    "month": month[:7],
                })
    return sorted(cards, key=lambda c: -abs(c["amount_rub"]))[:2]


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
    cards = []
    end = today + timedelta(days=SEASON_DAYS)
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
        med = statistics.median(v for _, v in yrs)
        cards.append({
            "kind": "season", "id": f"season:{secid}", "secid": secid, "sectype": sectype, "name": name,
            "section": "Сезонность",
            "signal": f"Следующие 3 месяца {('чаще рос' if rising else 'чаще падал')}" if name != "Индекс МосБиржи"
                      else f"Следующие 3 месяца индекс {('чаще рос' if rising else 'чаще падал')}",
            "hits": f"{'рост' if rising else 'падение'} в {up if rising else len(yrs) - up} из {len(yrs)} лет",
            "date_label": f"{_day(today)} → {_day(end)}", "median": round(med, 1),
            "compare": {"label": "Те же даты после 2022, %", "chips": [
                {"text": ("+" if v > 0 else "−" if v < 0 else "") + f"{abs(v):.0f}%", "cls": "up" if v > 0 else "dn",
                 "sub": str(y)[2:]} for y, v in yrs if y >= 2022]},
        })
    return cards


# ── Вся витрина ─────────────────────────────────────────────────────────────

def compute_hot(db, user, today: Optional[date] = None) -> Dict[str, Any]:
    today = today or date.today()
    out: Dict[str, Any] = {"generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"), "cards": []}
    for name, fn in (("positions", lambda: scan_positions(db)), ("funds", lambda: scan_funds(user, today)),
                     ("trades", lambda: scan_trades(db, user)), ("season", lambda: scan_season(db, today))):
        try:
            out["cards"].extend(fn())
        except Exception as e:  # noqa: BLE001 — один раздел не роняет страницу
            log.exception(f"hot: раздел {name} упал: {e}")
            out.setdefault("errors", []).append(name)
            try:
                db.rollback()
            except Exception:  # noqa: BLE001
                pass
    oi_dates = [c["date"] for c in out["cards"] if c["kind"] == "oi"]
    out["as_of"] = max(oi_dates) if oi_dates else today.isoformat()
    return out
