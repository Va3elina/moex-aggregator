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

import math
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


def _pct(v: float) -> str:
    return f"{'+' if v > 0 else '−' if v < 0 else ''}{abs(v):.0f}%"


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


def word_of(kind: str, period: str) -> str:
    return ("Макс " if kind == "high" else "Мин ") + PERIOD_WORD[period]


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


PAST_MAX = 10                        # строк в списке прошлых случаев


OI_HORIZONS = (("через день", 1), ("через 2 нед", 14))      # периоды сигналов позиций: день и 14 торговых дней
FUND_HORIZONS = (("через месяц", 21), ("через 3 мес", 63))


def past_case(px: Optional[Sequence[Tuple[date, float]]], d: date, label: str,
              horizons: Sequence[Tuple[str, int]] = FUND_HORIZONS, **extra: Any) -> Dict[str, Any]:
    """Прошлый случай для списка под графиком: дата, что было, изменение цены через каждый период (%)."""
    nxt = d + timedelta(days=1)                  # сигнал известен после закрытия дня
    return {"date": d.isoformat(), "label": label,
            "r": [price_after(px, nxt, n) if px else None for _, n in horizons], **extra}


def base_up(px: Optional[Sequence[Tuple[date, float]]], since: date, n: int = 21) -> Optional[int]:
    """Сравнение «в обычный день»: доля дней с ростом цены через n торговых дней, %."""
    if not px:
        return None
    ch = [px[i + n][1] / px[i][1] - 1 for i in range(len(px) - n) if px[i][0] >= since and px[i][1] > 0]
    return round(sum(c > 0 for c in ch) / len(ch) * 100) if len(ch) >= 50 else None


def past_episodes(hits: Sequence[int], last: int, gap: int) -> List[int]:
    """Первые дни прошлых эпизодов; нынешний эпизод (тот, что тянется до сегодня) — не прошлый случай."""
    firsts = cluster_firsts(hits, gap)
    if hits and firsts and last - hits[-1] <= gap:
        firsts = firsts[:-1]
    return firsts


def _positions(db, sectypes: Sequence[str]) -> Dict[str, List[tuple]]:
    """Дневные ряды физлиц (последний бар дня, будни) за всю историю — в формате _bulk_series скринера:
    (tradedate, net, npart, oi, pos_long, pos_short) + число людей в лонге и в шорте."""
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
        out.setdefault(s, []).append((d, L + S, int(nL or 0) + int(nS or 0), float(oi or 0), L, S, int(nL or 0), int(nS or 0)))
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


# Стороны позиций физлиц: объём лонгов и шортов и число людей в них (как у завода постов: LEGS в detect.py).
LEG_IX = {"long": 4, "short": 5, "nl": 6, "ns": 7}
LEG_WORD = {"long": "Лонг", "short": "Шорт", "nl": "Людей в лонге", "ns": "Людей в шорте"}
MOVE_WINDOWS = ((14, "2 недели", "за 2 недели"), (21, "месяц", "за месяц"))   # сдвиг стороны против своей нормы
MOVE_RATIO = 3.0
EP_GAPS = {"record": 40, "streak": 10, "reversal": 10, "divergence": 10, "move": 14}


def leg_verb(leg: str, up: bool) -> str:
    """Слова скринера для стороны: рост лонга — «набрали», рост шорта — «нарастили»."""
    if leg in ("long", "nl"):
        return "Физлица набрали лонг" if up else "Физлица сократили лонг"
    return "Физлица нарастили шорт" if up else "Физлица сократили шорт"


def thousands(v: float) -> str:
    a = abs(v) / 1000
    return (f"{a:.0f}" if a >= 10 else f"{a:.1f}".replace(".", ",")) + " тыс"


def since_years(vals: Sequence[float], dates: Sequence[date], i: int, higher: bool) -> Tuple[float, Optional[date]]:
    """Сколько лет значение не было таким высоким (низким) — «максимум с …», как record() завода.
    Возвращает (лет, дата прежнего такого значения или None — за всю историю)."""
    v = vals[i]
    for k in range(i - 1, -1, -1):
        if (vals[k] >= v) if higher else (vals[k] <= v):
            return (dates[i] - dates[k]).days / 365, dates[k]
    return (dates[i] - dates[0]).days / 365, None


def weekly_streak(vals: Sequence[float], dates: Sequence[date], i: int) -> int:
    """Сколько закрытых недель подряд сторона росла (по пятничным закрытиям, незакрытая неделя не в счёт)."""
    wk: Dict[Tuple[int, int], float] = {}
    for k in range(max(0, i - 120), i + 1):
        wk[dates[k].isocalendar()[:2]] = vals[k]
    keys = sorted(wk)
    if keys and dates[i].weekday() < 4:            # неделя сигнального дня ещё не закрыта
        keys = keys[:-1]
    n = 0
    for a, b in zip(keys[::-1][1:], keys[::-1]):
        if wk[b] > wk[a]:
            n += 1
        else:
            break
    return n


def expiry_days(dates: Sequence[date], n: int) -> set:
    """День экспирации квартальных фьючерсов (3-й четверг марта/июня/сентября/декабря) и n−1 дней после."""
    marks, ds = set(), list(dates)
    for y in range(ds[0].year, ds[-1].year + 1):
        for m in (3, 6, 9, 12):
            first = date(y, m, 1)
            exp = first + timedelta(days=(3 - first.weekday()) % 7 + 14)
            j = next((k for k, d in enumerate(ds) if d >= exp), None)
            if j is not None:
                marks.update(ds[j:j + n])
    return marks


class Asset:
    """Ряды одного актива и детекторы завода на любой день i (для сегодня и для «Истории»)."""

    def __init__(self, s: str, pts: List[tuple], px: Optional[List[Tuple[date, float]]]):
        self.s, self.pts = s, pts
        self.dates = [p[0] for p in pts]
        self.legs = {leg: [abs(float(p[ix])) for p in pts] for leg, ix in LEG_IX.items()}
        self.net = [p[1] for p in pts]
        self.npart = [p[2] for p in pts]
        self.price = dict(px) if px else {}
        self.px = px
        quarterly = not (len(s) >= 5 and s.endswith("F"))
        self.exp12 = expiry_days(self.dates, 12) if quarterly else set()

    def record(self, i: int) -> List[Dict[str, Any]]:
        out = []
        for leg in LEG_IX:
            v = self.legs[leg]
            if v[i] <= 0:
                continue
            for higher in (True, False):
                if not higher and (leg not in ("long", "nl") or self.dates[i] in self.exp12):
                    continue               # минимум — только у лонга и не в недели после экспирации (как у завода)
                yrs, since = since_years(v, self.dates, i, higher)
                if yrs < 1 or (self.dates[i] - self.dates[0]).days < 365:
                    continue
                when = "за всё время" if since is None else f"с {MON[since.month - 1]} {since.year}"
                out.append({"type": "record", "leg": leg, "up": higher, "score": 4 + min(yrs, 10) * 0.6 + (1 if since is None else 0),
                            "tag": f"{LEG_WORD[leg]}: {'максимум' if higher else 'минимум'} {when}",
                            "what": f"{'максимум' if higher else 'минимум'} {when}",
                            "zone_from": since or self.dates[0], "zone": "вся история" if since is None else when})
        return out

    def streak(self, i: int) -> List[Dict[str, Any]]:
        out = []
        for leg in ("long", "short"):
            k = weekly_streak(self.legs[leg], self.dates, i)
            if k >= 2:
                out.append({"type": "streak", "leg": leg, "up": True, "score": min(2.5 + k * 0.8, 7),
                            "tag": f"{LEG_WORD[leg]} растёт {k}-ю неделю подряд", "what": f"{k}-ю неделю подряд", "zone_from": self.dates[i] - timedelta(days=7 * k + 3),
                            "zone": f"{k} нед подряд"})
        return out

    def _main_leg(self, i: int, j: int) -> Tuple[str, bool]:
        """Сторона, которая сдвинулась сильнее за окно [j, i] (в долях своего уровня)."""
        ch = {leg: (self.legs[leg][i] - self.legs[leg][j]) / max(self.legs[leg][j], 1) for leg in ("long", "short")}
        leg = max(ch, key=lambda q: abs(ch[q]))
        return leg, ch[leg] > 0

    def reversal(self, i: int) -> List[Dict[str, Any]]:
        if i < 255:
            return []
        v, v5 = self.net[i], self.net[i - 5]
        med = statistics.median([abs(x) for x in self.net[i - 250:i]]) or 1
        if (v > 0) == (v5 > 0) or v == 0 or v5 == 0 or min(abs(v), abs(v5)) < 0.2 * med:
            return []
        side = lambda z: "лонг" if z > 0 else "шорт"  # noqa: E731
        leg, up = self._main_leg(i, i - 5)
        return [{"type": "reversal", "leg": leg, "up": up, "score": 6.0, "signal": f"Физлица развернулись из {side(v5)}а в {side(v)}",
                 "tag": "разворот за неделю", "zone_from": self.dates[i - 5], "zone": "неделя", "dir": "up" if v > 0 else "down"}]

    def divergence(self, i: int) -> List[Dict[str, Any]]:
        if i < 255 or not self.price:
            return []
        p, p5 = self.price.get(self.dates[i]), self.price.get(self.dates[i - 5])
        if not p or not p5:
            return []
        v, v5 = self.net[i], self.net[i - 5]
        med = statistics.median([abs(x) for x in self.net[i - 250:i]]) or 1
        rp, rx = p / p5 - 1, (v - v5) / max(abs(v5), med)
        if not ((rp <= -0.02 and rx >= 0.05) or (rp >= 0.02 and rx <= -0.05)):
            return []
        what = "покупают на падении" if rp < 0 else "продают на росте"
        leg, up = self._main_leg(i, i - 5)
        return [{"type": "divergence", "leg": leg, "up": up, "score": 4.5 + min(abs(rp), 0.1) * 20, "signal": f"Физлица {what}",
                 "tag": f"цена {'+' if rp > 0 else '−'}{abs(rp) * 100:.1f}% за неделю".replace(".", ","),
                 "zone_from": self.dates[i - 5], "zone": "неделя", "dir": "up" if rp < 0 else "down"}]

    def move(self, i: int) -> List[Dict[str, Any]]:
        out = []
        for leg in ("long", "short"):
            v = self.legs[leg]
            for n, zone, note in MOVE_WINDOWS:
                if i < n + 60 or self.npart[i] < scr.ATR_MIN_PART:
                    continue
                mv = v[i] - v[i - n]
                base = [abs(v[k] - v[k - 1]) for k in range(i - n - 59, i - n + 1)]
                atr = statistics.fmean(base)
                if atr <= 0 or atr < scr.ATR_FLOOR_REL * max(v[i], 1):
                    continue
                ratio = abs(mv) / (atr * math.sqrt(n))
                if ratio >= MOVE_RATIO and abs(mv) / max(v[i], 1) >= scr.ATR_MIN_REL * math.sqrt(n):
                    out.append({"type": "move", "leg": leg, "up": mv > 0, "score": 3 + min(ratio, 10) * 0.5, "n": n,
                                "tag": f"×{ratio:.1f}".replace(".", ","), "note": note, "ratio": ratio,
                                "what": f"×{ratio:.1f} {note}".replace(".", ","),
                                "zone_from": self.dates[i - n], "zone": zone, "start": self.dates[i - n]})
        return out

    def signals(self, i: int) -> List[Dict[str, Any]]:
        if self.npart[i] < scr.ATR_MIN_PART:
            return []
        return self.record(i) + self.streak(i) + self.reversal(i) + self.divergence(i) + self.move(i)


def _sig_key(g: Dict[str, Any]) -> Tuple:
    """Что считать «таким же» случаем в Истории: тип, сторона, направление (и окно сдвига)."""
    return g["type"], g["leg"], g["up"], g.get("n"), g.get("dir")


def scan_positions(db) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """Позиции физлиц — типы находок завода постов (signals/insights/detect.py) по лонгам, шортам и числу людей
    в них: рекорд стороны (за год и больше), серия недель роста, разворот чистой позиции за неделю, расхождение
    с ценой за неделю; плюс резкий сдвиг стороны за 2 недели и за месяц (×3 к своей норме). Дневных сдвигов
    нет (Вадим 08.10: шум). На актив — одна карточка по сильнейшей находке, остальные — плашками."""
    short = scr.compute_screener(db, "FIZ", "short")
    rows = {r["sectype"]: r for r in short["rows"]
            if r["status"] != "illiquid" and r.get("group") in ALLOWED_GROUPS and r["sectype"] not in FOREIGN}
    series = _positions(db, list(rows))
    px = _prices(db, list(rows))
    cands = []
    for s, r in rows.items():
        pts = series.get(s, [])
        if len(pts) < 300:
            continue
        a = Asset(s, pts, px.get(s))
        sig = a.signals(len(pts) - 1)
        if sig:
            sig.sort(key=lambda g: -g["score"])
            cands.append({"s": s, "row": r, "a": a, "sig": sig, "score": sig[0]["score"]})
    priced = lambda c: c["s"] in PRICE_INDEX or c["row"].get("group") == "Акции"  # noqa: E731
    cands.sort(key=lambda c: (-c["score"], not priced(c)))
    cards = []
    for c in cands[:MAX_OI_CARDS]:
        s, r, a = c["s"], c["row"], c["a"]
        main = c["sig"][0]
        last = len(a.dates) - 1
        leg, vals, dates = main["leg"], a.legs[main["leg"]], a.dates
        # Заголовок — одна фраза: «Физлица нарастили шорт — максимум за всё время». Ниже — одна тихая строка:
        # до двух находок, которые подтверждают главную (та же сторона, то же направление) или дают контекст цены.
        # Противоречивые (растёт и лонг, и шорт) не показываем — как завод, который пишет про одну тему.
        if main.get("signal"):
            signal = main["signal"]
        elif leg in ("nl", "ns"):                  # число людей — своими словами: «Людей в шорте — максимум за всё время»
            signal = f"{LEG_WORD[leg]} — {main['what']}"
        else:
            signal = leg_verb(leg, main["up"]) + " — " + main["what"]
        side = lambda g: "L" if g["leg"] in ("long", "nl") else "S"  # noqa: E731
        tags: List[Dict[str, Any]] = []
        if main["type"] == "divergence":
            tags.append({"tone": "muted", "text": main["tag"]})
        seen = {main["tag"]}
        for g in c["sig"][1:]:
            if len(tags) >= 2 or g["tag"] in seen:
                continue
            agrees = g["type"] in ("divergence", "reversal") or (side(g) == side(main) and g["up"] == main["up"])
            if not agrees:
                continue
            seen.add(g["tag"])
            what = g.get("what") or g["tag"]
            if g["type"] in ("divergence", "reversal"):
                text = what
            elif main["type"] in ("divergence", "reversal"):    # у заголовка без стороны — называем сторону
                text = g["tag"][0].lower() + g["tag"][1:]
            elif g["leg"] == leg:
                text = what
            else:
                text = f"{LEG_WORD[g['leg']].lower()} — {what}"
            tags.append({"tone": "muted", "text": text})
        # История: такие же находки этой стороны за 3 года, по одной на эпизод, нынешний не в счёт
        key, far = _sig_key(main), max(300, last - 750)
        hits, info = [], {}
        detector = getattr(a, main["type"])
        for i in range(far, last + 1):
            for g in detector(i):
                if _sig_key(g) == key:
                    hits.append(i)
                    info[i] = g
                    break
        past = {"title": "История", "horizons": [h for h, _ in OI_HORIZONS],
                "cases": [past_case(a.px, dates[k], info[k]["tag"] if main["type"] != "move" else
                                    leg_verb(leg, main["up"]).replace("Физлица ", "").capitalize() + " " + info[k]["tag"],
                                    OI_HORIZONS, **{"from": max(info[k]["zone_from"], dates[0]).isoformat(),
                                                    "zone": info[k]["zone"]})
                          for k in past_episodes(hits, last, EP_GAPS[main["type"]])][::-1][:PAST_MAX]}
        # окно графика: зона находки с запасом; у рекорда — от прежнего такого значения; плюс прошлые случаи
        zf = main["zone_from"]
        span = max((dates[last] - zf).days, 30)
        start = zf - timedelta(days=max(span // 3, 90 if main["type"] != "record" else 120))
        if past["cases"]:
            start = min(start, date.fromisoformat(past["cases"][-1]["date"]) - timedelta(days=20))
        start = max(start, dates[0])
        chart: Dict[str, Any] = {"type": "legs", "leg": leg}
        if main["zone"] != "вся история":          # зона на весь график ничего не выделяет — рекорд и так в заголовке
            chart["zone"] = {"from": max(zf, dates[0]).isoformat(), "label": main["zone"]}
        if main.get("start"):
            chart["start"] = {"date": main["start"].isoformat(), "value": round(vals[dates.index(main["start"])])}
        daily_from = start if past["cases"] or (dates[last] - start).days <= 760 else dates[last] - timedelta(days=760)
        idx = [i for i in _thin(dates, daily_from) if dates[i] >= start]
        sel = [dates[i] for i in idx]
        chart["series"] = [[dates[i].isoformat(), round(vals[i])] for i in idx]
        if s in px:
            chart["price"] = [_r(v, 4) for v in _price_on(px[s], sel)]
        chart["now"] = {"date": dates[last].isoformat(), "value": round(vals[last])}
        card = {"kind": "oi", "id": f"oi:{s}", "sectype": s, "name": r["name"], "signal": signal, "tags": tags,
                "date": dates[last].isoformat(), "chart": chart, "past": past}
        past["base_up"] = base_up(a.px, date(2022, 3, 1), OI_HORIZONS[-1][1])
        cards.append(card)
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


FUND_ASSET = {"stocks": ("IMOEX", "индекс"), "money_market": ("IMOEX", "индекс"), "bonds": ("RGBI", "индекс ОФЗ"),
              "gold": ("GLDRUB_TOM", "золото"), "yuan": ("CNYRUB_TOM", "юань")}


def _case_key(c: Dict[str, Any]) -> Tuple[str, str, bool]:
    """Один случай — одна строка: серия считается по её началу, остальное — по подсвеченному столбику."""
    ch = c["chart"]
    anchor = (ch.get("run") or {}).get("from") if c["case"] == "streak" else ch.get("hl")
    return c["case"], anchor or "", c["amount"] > 0


def fund_past(months, weeks, today: date, cur: Dict[str, Any], px, start: date = date(2022, 3, 7)) -> List[Tuple[date, Dict[str, Any]]]:
    """Прошлые такие же случаи категории: тот же тип и та же сторона (приток/отток). Перематываем по неделям,
    день случая — когда он впервые показался бы на витрине; нынешний случай не в счёт."""
    seen, out = set(), []
    t = start
    while t < today:
        c = fund_case(months, weeks, t)
        if c and c["case"] == cur["case"] and (c["amount"] > 0) == (cur["amount"] > 0):
            k = _case_key(c)
            if k not in seen and k != _case_key(cur):
                seen.add(k)
                out.append((t, c))
        t += timedelta(days=7)
    return out


def scan_funds(user, today: date, db=None) -> List[Dict[str, Any]]:
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
            card = {"kind": "flows", "id": f"flows:{cat}", "category": cat, "name": name, **case}
            if db is not None:
                secid, what = FUND_ASSET[cat]
                px = [(r[0], float(r[1])) for r in db.execute(text(
                    "SELECT trade_date, close FROM index_data WHERE secid = :x AND close > 0 ORDER BY 1"), {"x": secid}).fetchall()]
                rows = [past_case(px, t - timedelta(days=1), c["signal"], hl=c["chart"]["hl"], run=c["chart"].get("run"))
                        for t, c in fund_past(months, weeks, today, case, px)]
                card["past"] = {"title": "История", "asset": what, "cases": rows[::-1][:PAST_MAX],
                                "horizons": [h for h, _ in FUND_HORIZONS],
                                "base_up": base_up(px, date(2022, 3, 1))}
            cards.append(card)
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
        # история по годам: тот же отрезок в прошлые годы и путь года целиком (по наведению — вместо текущего)
        by_year: Dict[int, List[Tuple[date, float]]] = {}
        for d, v in closes:
            by_year.setdefault(d.year, []).append((d, v))
        hist = []
        for y, v in sorted(yrs, reverse=True)[:PAST_MAX]:
            row = by_year.get(y, [])
            if len(row) < 100:
                continue
            base = row[0][1]
            hist.append({"date": f"{y}-{today.month:02d}-{today.day:02d}", "label": str(y), "r": [v],
                         "curve": [[k, round((c / base - 1) * 100, 2)] for k, (_, c) in enumerate(row) if k % 2 == 0]})
        cards.append({
            "kind": "season", "id": f"season:{secid}", "secid": secid, "sectype": sectype, "name": name,
            "signal": "Следующие 3 месяца " + ("чаще рос" if rising else "чаще падал"),
            "hits": f"{'рост' if rising else 'падение'} в {up if rising else len(yrs) - up} из {len(yrs)} лет",
            "date_label": f"{_day(today)} → {_day(today + timedelta(days=SEASON_DAYS))}",
            "chart": {"type": "season", "avg": avg, "cur": cur, "today": today_td,
                      "zone_to": min(today_td + 63, avg[-1][0]), "rising": rising},
            "past": {"title": "История", "horizons": ["за 3 месяца"], "cases": hist, "base_up": None},
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
                cards = scan_funds(user, today, db)
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
