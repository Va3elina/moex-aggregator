#!/usr/bin/env python3
"""Движок инсайтов, шаг 4: карточка находки → бриф писателю и график.

Карточка — всё, что писателю можно сказать о находке, посчитанное кодом НА ДАТУ
(данные не позже as_of): главная цифра, с какого времени такого не было, что было с
ценой после прошлых таких же эпизодов, ближайшая аналогия, ряд для графика.

В Шаге В история и аналогии были запрещены («никаких прошлых аналогий», «годовые
сравнения не идут») — и именно их заводу не хватало: у канала отсылка к истории в 49%
постов от данных, у завода в 17%, рекорд — 60% против 0%, вывод автора — 26% против 0%.
Здесь они не запрещены, а выданы: посчитаны по нашим рядам, каждую цифру можно проверить.

Три типа: рекорд позиций физлиц, потоки в фонды, сезонность.

    python3 cards.py positions MX short 2026-08-12     # бриф в консоль, график в step4/
"""
import os
import re
import sys
from functools import lru_cache

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
from signals.insights import data as dbdata, detect as det  # noqa: E402  ряды и сезонная кривая — те же, что у детекторов

GEN = ["января", "февраля", "марта", "апреля", "мая", "июня", "июля", "августа",
       "сентября", "октября", "ноября", "декабря"]
PREP = ["январе", "феврале", "марте", "апреле", "мае", "июне", "июле", "августе",
        "сентябре", "октябре", "ноябре", "декабре"]
# нога → (кратко, единица)
LEG = {"long": ("покупки физлиц", "контрактов"), "short": ("шорт физлиц", "контрактов"),
       "nl": ("число физлиц в лонге", "человек"), "ns": ("число физлиц в шорте", "человек"),
       "net": ("чистый лонг физлиц", "контрактов")}
# фьючерс → (именительный, дательный): «по фьючерсу на индекс Мосбиржи»
HUMAN = {"MX": ("фьючерс на индекс Мосбиржи", "фьючерсу на индекс Мосбиржи"),
         "IMOEXF": ("вечный фьючерс на индекс Мосбиржи", "вечному фьючерсу на индекс Мосбиржи"),
         "MM": ("мини-фьючерс на индекс Мосбиржи", "мини-фьючерсу на индекс Мосбиржи"),
         "RI": ("фьючерс на индекс РТС", "фьючерсу на индекс РТС"),
         "Si": ("фьючерс на доллар", "фьючерсу на доллар"),
         "USDRUBF": ("вечный фьючерс на доллар", "вечному фьючерсу на доллар"),
         "CR": ("фьючерс на юань", "фьючерсу на юань"),
         "CNYRUBF": ("вечный фьючерс на юань", "вечному фьючерсу на юань"),
         "Eu": ("фьючерс на евро", "фьючерсу на евро"),
         "GD": ("фьючерс на золото", "фьючерсу на золото"),
         "GLDRUBF": ("вечный фьючерс на золото", "вечному фьючерсу на золото"),
         "BR": ("фьючерс на нефть Brent", "фьючерсу на нефть Brent"),
         "RB": ("фьючерс на индекс гособлигаций", "фьючерсу на индекс гособлигаций")}
FUND = {"bonds": ("фонды облигаций", "фондах облигаций", "фондов облигаций"),
        "stocks": ("фонды акций", "фондах акций", "фондов акций"),
        "money_market": ("фонды денежного рынка", "фондах денежного рынка", "фондов денежного рынка"),
        "gold": ("фонды золота", "фондах золота", "фондов золота"),
        "yuan": ("юаневые фонды", "юаневых фондах", "юаневых фондов"),
        "all": ("все биржевые фонды", "биржевых фондах", "биржевых фондов")}
HASHTAG = {"positions": "#открытыепозиции", "funds": "#деньгивфондах", "seasonality": "#сезонность"}
# forecast_backtest.py, 13.09: 2646 эпизодов в 72 фьючерсах — с 2023 года рекорды позиций угадывают
# направление цены в 51% случаев, сезонность индекса хуже «всегда вверх». Вадим: прогнозы цены не делать.
NO_FORECAST = ("прогноз цены не давать: на истории такие сигналы с 2023 года угадывают направление в половине "
               "случаев; «что было после» - это история, а не обещание")


# ── числа и даты по-русски: бриф отдаёт их круглыми, писатель переносит как есть ─────
def d_ru(d, ref=None) -> str:
    d = pd.Timestamp(d)
    s = f"{d.day} {GEN[d.month - 1]}"
    return s if ref is not None and pd.Timestamp(ref).year == d.year else f"{s} {d.year} года"


def m_ru(p, ref=None) -> str:
    p = pd.Timestamp(p)
    s = f"в {PREP[p.month - 1]}"
    return s if ref is not None and pd.Timestamp(ref).year == p.year else f"{s} {p.year} года"


def mn_ru(p, ref=None) -> str:
    """Месяц в именительном: «март», «март 2025»."""
    p = pd.Timestamp(p)
    return det.MONTHS[p.month - 1] + ("" if ref is not None and pd.Timestamp(ref).year == p.year else f" {p.year}")


def dm_ru(doy: int) -> str:
    d = pd.Timestamp(2001, 1, 1) + pd.Timedelta(days=int(doy))
    return f"{d.day} {GEN[d.month - 1]}"


def n_ru(v) -> str:
    """Кругло: 1,3 трлн; 69 млрд; 4,6 млн; 97 тыс.; 4 073. Десятки и больше — без дробной части."""
    a = abs(v)
    for lim, div, word in ((1e12, 1e12, " трлн"), (1e9, 1e9, " млрд"), (1e6, 1e6, " млн"), (1e4, 1e3, " тыс.")):
        if a >= lim:
            x = v / div
            s = (f"{x:.0f}" if abs(x) >= 10 else f"{x:.1f}").replace(".", ",")
            return (s[:-2] if s.endswith(",0") else s) + word
    return f"{v:,.0f}".replace(",", " ")


UNITS = {"контрактов": ("контракт", "контракта", "контрактов"), "человек": ("человек", "человека", "человек")}


def plural(n, forms) -> str:
    n = abs(int(round(n)))
    if n % 10 == 1 and n % 100 != 11:
        return forms[0]
    if 2 <= n % 10 <= 4 and not 12 <= n % 100 <= 14:
        return forms[1]
    return forms[2]


def q_ru(v, unit) -> str:
    """Количество с единицей в нужном падеже: «97 тыс. контрактов», «4 073 человека»."""
    s = n_ru(v)
    forms = UNITS.get(unit, (unit, unit, unit))
    return f"{s} {plural(v, forms) if s[-1].isdigit() else forms[2]}"


def p_ru(x, sign=True) -> str:
    v = x * 100
    if abs(v) < 0.05:
        return "0%"
    s = (f"{v:+.0f}" if abs(v) >= 1 else f"{v:+.1f}").replace(".", ",") + "%"
    return s if sign else s.lstrip("+-")


def x_ru(r) -> str:
    s = f"{r:.1f}".replace(".", ",")
    return f"в {s[:-2] if s.endswith(',0') else s} раза"


def span_ru(years) -> str:
    if years >= 2:
        return f"больше {int(years)} лет"
    if years >= 1.4:
        return "полтора года"
    if years >= 1:
        return "больше года"
    m = max(int(round(years * 12)), 1)
    return f"{m} {'месяца' if 2 <= m <= 4 else 'месяцев'}"


def px_ru(v, label) -> str:
    if label in ("доллар", "юань", "евро"):
        return f"{v:.2f}".replace(".", ",") + " ₽"
    if label.startswith("индекс"):
        return f"{v:,.0f}".replace(",", " ")
    if label == "золото":
        return f"{v:,.0f}".replace(",", " ") + " ₽ за грамм"
    return (f"{v:,.0f}" if v >= 100 else f"{v:,.2f}").replace(",", " ").replace(".", ",") + " ₽"


# ── ряды ────────────────────────────────────────────────────────────────────────
@lru_cache(None)
def data():
    P = det.load_positions()
    names, groups, to_stock = det.instruments()
    idx, stk, perp = det.prices()
    return P, names, groups, to_stock, idx, stk, perp, det.usd_series(idx, perp)


@lru_cache(None)
def funds_data():
    f = dbdata.read("funds")
    fd = dbdata.read("fund_data", parse_dates=["trade_date"])
    fd = fd.merge(f[["fund_id", "category"]], on="fund_id").sort_values(["fund_id", "trade_date"])
    g = fd.groupby("fund_id")
    pn, pp = g.nav.shift(1), g.pay.shift(1)
    fd["flow"] = (fd.nav - pn) - pn * (fd.pay - pp) / pp
    daily = fd.dropna(subset=["flow"]).pivot_table(index="trade_date", columns="category", values="flow",
                                                   aggfunc="sum").fillna(0)
    daily["all"] = daily.sum(axis=1)
    # активы: фонды отчитываются не каждый день — без протяжки сумма скачет от пропусков
    navp = fd.pivot_table(index="trade_date", columns="fund_id", values="nav").ffill(limit=7)
    cat = f.set_index("fund_id")["category"].reindex(navp.columns).values
    nav = navp.T.groupby(cat).sum().T
    nav["all"] = nav.sum(axis=1)
    return daily, nav


def upto(s, t):
    return s[s.index <= pd.Timestamp(t)].dropna()


def since(arr, dates, i, higher):
    u = det.since_date(arr, dates, i, higher)
    return u, (dates[i] - (u if u is not None else dates[0])).days / 365


def status_ru(u, years, dates, higher, ref, word=None) -> str | None:
    w = word or ("максимум" if higher else "минимум")
    if u is None:
        return f"{w} за всё время наших данных (они начинаются в {dates[0].year} году)"
    if years >= 0.3:
        return f"{w} с {d_ru(u, ref)} - такого не было {span_ru(years)}"
    return None


def human_name(sec) -> tuple:
    if sec in HUMAN:
        return HUMAN[sec]
    P, names, groups, *_ = data()
    raw = str(names.get(sec, sec))
    n = raw.replace(" (вечн)", "").replace(" (мини)", "")
    kind = "вечный фьючерс" if "(вечн)" in raw else "фьючерс"
    kind_d = "вечному фьючерсу" if "(вечн)" in raw else "фьючерсу"
    if groups.get(sec) == "Акции":
        return f"{kind} на акции «{n}»", f"{kind_d} на акции «{n}»"
    return f"{kind} {n}", f"{kind_d} {n}"


def price_for(sec):
    P, names, groups, to_stock, idx, stk, perp, usd = data()
    code = det.CODE.get(sec)
    m = {"MIX": ("индекс Мосбиржи", idx.get("IMOEX")), "RI": ("индекс РТС", idx.get("RTSI")),
         "Si": ("доллар", usd), "CNY": ("юань", perp.get("CNYRUBF")), "Eu": ("евро", perp.get("EURRUBF")),
         "GOLD": ("золото", perp.get("GLDRUBF")), "RGBI": ("индекс гособлигаций", idx.get("RGBI"))}
    if code in m and m[code][1] is not None:
        return m[code][0], m[code][1].dropna()
    t = to_stock.get(sec)
    if t in stk.columns:
        return f"акции «{str(names.get(sec)).replace(' (вечн)', '')}»", stk[t].dropna()
    return None, None


def fwd(pser, date, k):
    """Изменение цены за k торговых дней после даты — только если оно уже известно."""
    j = pser.index.searchsorted(pd.Timestamp(date))
    if j >= len(pser) or j + k >= len(pser):
        return None
    return float(pser.iloc[j + k] / pser.iloc[j] - 1)


def episodes(arr, i, gap=40, hist=250) -> list:
    """Прошлые пики ряда: дни на максимуме за год и больше. Дни ближе gap торговых
    дней друг к другу — один эпизод; дата эпизода — день его вершины."""
    prev_max = pd.Series(arr[:i + 1]).shift(1).rolling(hist, min_periods=200).max().values
    eps = []
    for j in np.flatnonzero(arr[:i + 1] >= prev_max):
        if eps and j - eps[-1]["end"] <= gap:
            eps[-1]["end"] = j
            if arr[j] >= arr[eps[-1]["top"]]:
                eps[-1]["top"] = j
        else:
            eps.append({"start": j, "end": j, "top": j})
    return eps


# ── карточка: рекорд позиций ─────────────────────────────────────────────────────
def positions_card(sec, leg, as_of) -> dict:
    P, *_ = data()
    raw = upto(P[leg if leg != "net" else "net"][sec], as_of)
    lab, unit = LEG[leg]
    sign = 1
    if leg == "net" and raw.iloc[-1] < 0:
        sign, lab = -1, "чистый шорт физлиц"
    ser = raw * sign
    arr, dates = ser.values.astype(float), ser.index
    i, t, v = len(arr) - 1, ser.index[-1], float(arr[-1])
    nom, dat = human_name(sec)
    plabel, pfull = price_for(sec)
    pser = upto(pfull, t) if pfull is not None else None
    were = "были" if (plabel or "").startswith("акции") else "был"
    u, years = since(arr, dates, i, True)
    st = status_ru(u, years, dates, True, t)

    facts = [f"{lab} по {dat} - {q_ru(v, unit)} на закрытие {d_ru(t, t)}"]
    if st:
        facts.append(f"это {st}")
    eps = episodes(arr, i)
    cur = eps[-1] if eps and i - eps[-1]["end"] <= 40 else None
    start = cur["start"] if cur else i
    if start > 0:
        j = int(np.argmax(arr[:start]))
        prev, pdate = float(arr[j]), dates[j]
        if prev > 0 and v > prev:
            r = v / prev
            facts.append(f"прежний пик - {q_ru(prev, unit)} {d_ru(pdate, t)}; сейчас выше "
                         + (x_ru(r) if r >= 1.5 else f"на {p_ru(r - 1, False)}"))
    ch = []
    for k, w in ((1, "за день"), (5, "за неделю"), (20, "за месяц")):
        if i >= k and arr[i - k] > 0 and leg != "net":
            ch.append(f"{w} {p_ru(v / arr[i - k] - 1)}")
        elif i >= k and leg == "net":
            ch.append(f"{w} {'+' if v - arr[i - k] >= 0 else '-'}{q_ru(abs(v - arr[i - k]), unit)}")
    if ch:
        facts.append("изменение: " + ", ".join(ch))
    if cur and i - cur["start"] >= 5:
        facts.append(f"на максимумах за год и больше с {d_ru(dates[cur['start']], t)}; с тех пор "
                     f"{p_ru(v / arr[cur['start']] - 1)}" if arr[cur['start']] > 0 else "")

    # что было после прошлых пиков
    past = [e for e in eps if e is not cur and e["end"] < start]
    rows, r20s, r60s = [], [], []
    for e in past[-6:]:
        td, tv = dates[e["top"]], float(arr[e["top"]])
        r20 = fwd(pser, td, 20) if pser is not None else None
        r60 = fwd(pser, td, 60) if pser is not None else None
        if r20 is not None:
            r20s.append(r20)
        if r60 is not None:
            r60s.append(r60)
        tail = []
        if r20 is not None:
            tail.append(f"через месяц {plabel} {p_ru(r20)}")
        if r60 is not None:
            tail.append(f"через три месяца {p_ru(r60)}")
        rows.append({"date": td, "value": tv, "r20": r20, "r60": r60,
                     "line": f"пик {d_ru(td, t)}: {q_ru(tv, unit)}" + (" - " + ", ".join(tail) if tail else "")})
    after = [r["line"] for r in rows]
    if len(r20s) >= 2:
        k = sum(r > 0 for r in r20s)
        after.append(f"итого после {len(r20s)} прошлых пиков {plabel} через месяц {were} выше в {k} "
                     f"{plural(k, ('случае', 'случаях', 'случаях'))} из {len(r20s)}, медиана "
                     f"{p_ru(float(np.median(r20s)))}")
    analogy = []
    if rows:
        best = min(rows, key=lambda r: abs(np.log(max(r["value"], 1) / max(v, 1))))
        if best["r20"] is not None:
            analogy.append(f"ближе всего по масштабу - пик {d_ru(best['date'], t)} ({q_ru(best['value'], unit)}): "
                           f"за следующий месяц {plabel} {p_ru(best['r20'])}"
                           + (f", за три - {p_ru(best['r60'])}" if best["r60"] is not None else ""))

    price = []
    if pser is not None and len(pser) > 25:
        pa, pdts = pser.values, pser.index
        pl = [f"{plabel} - {px_ru(pa[-1], plabel)} на {d_ru(pdts[-1], t)}",
              f"за неделю {p_ru(pa[-1] / pa[-6] - 1)}, за месяц {p_ru(pa[-1] / pa[-21] - 1)}"]
        for hi in (False, True):
            pu, py = since(pa, pdts, len(pa) - 1, hi)
            s = status_ru(pu, py, pdts, hi, t)
            if s and py >= 0.5:
                pl.append(f"{plabel} на {s}".replace("на максимум", "на максимуме").replace("на минимум", "на минимуме"))
        price = ["; ".join(pl[:2])] + pl[2:]

    context = []
    for L in ("long", "short", "nl", "ns", "net"):
        if L == leg:
            continue
        s2 = upto(P[L][sec], t)
        if len(s2) < 260:
            continue
        a2, d2 = s2.values.astype(float), s2.index
        l2, u2 = LEG[L]
        if L == "net":
            l2 = "чистый лонг физлиц" if a2[-1] >= 0 else "чистый шорт физлиц"
            a2 = a2 if a2[-1] >= 0 else -a2
        best_s = None
        for hi in (True, False):
            uu, yy = since(a2, d2, len(a2) - 1, hi)
            if yy >= 0.5:
                best_s = status_ru(uu, yy, d2, hi, t)
                break
        if best_s:
            context.append(f"{l2} - {q_ru(a2[-1], u2)}, {best_s}")
        elif L != "net" and a2[-21] > 0 and abs(a2[-1] / a2[-21] - 1) >= 0.15:
            context.append(f"{l2} - {q_ru(a2[-1], u2)}, за месяц {p_ru(a2[-1] / a2[-21] - 1)}")

    limits = [f"данные дневные, на закрытие торгов {d_ru(t, t)}; что было внутри дня, не видно",
              f"данные по {dat} начинаются в {dates[0].year} году",
              NO_FORECAST]
    if len(r20s) < 4:
        limits.append(f"прошлых пиков с известным продолжением всего {len(r20s)} - это история, "
                      f"а не закономерность: не обобщай")
    win = ser[ser.index >= t - pd.Timedelta(days=3 * 365)]
    chart = {"type": "line2", "title": f"{lab.capitalize()}, {nom}", "x": win.index, "y": win.values,
             "y_label": unit, "marks": [(r["date"], r["value"]) for r in rows if r["date"] >= win.index[0]]
             + [(t, v)]}
    if pser is not None:
        pw = pser[pser.index >= win.index[0]]
        chart.update({"x2": pw.index, "y2": pw.values, "y2_label": plabel})
    return {"kind": "positions", "spec": {"sec": sec, "leg": leg}, "as_of": t,
            "headline": f"{lab.capitalize()} по {dat} - {q_ru(v, unit)}" + (f", {st}" if st else ""),
            "facts": [f for f in facts if f], "after": after, "analogy": analogy, "price": price,
            "context": context, "limits": limits, "chart": chart,
            "chart_note": [f"на графике - {lab} по {dat} за три года (оранжевая линия) и {plabel} "
                           f"(серая); точками отмечены прошлые пики и текущее значение"],
            "hashtag": HASHTAG["positions"]}


# ── карточка: потоки в фонды ─────────────────────────────────────────────────────
def funds_card(cat, as_of) -> dict:
    P, names, groups, to_stock, idx, stk, perp, usd = data()
    daily, nav = funds_data()
    d = upto(daily[cat], as_of)
    t = d.index[-1]
    nom, prep, gen = FUND[cat]
    word = lambda x: "приток" if x > 0 else "отток"  # noqa: E731
    mtd = float(d[d.index.to_period("M") == t.to_period("M")].sum())
    mo = d.resample("ME").sum()
    past = mo[mo.index < t.to_period("M").start_time]
    dv = d.values
    facts = [f"{d_ru(t, t)} {nom}: {word(dv[-1])} {n_ru(abs(dv[-1]))} ₽ за день"]
    # разворот: «бегство из облигаций» автор увидел по первым дням оттока после притока —
    # 24.06 был уже второй день, и счётчик «первого дня» его пропускал
    sg, run = np.sign(dv[-1]), 1
    while run < len(dv) and np.sign(dv[-1 - run]) == sg:
        run += 1
    opp = 0
    while run + opp < len(dv) and np.sign(dv[-1 - run - opp]) == -sg:
        opp += 1
    reversal = None
    if run <= 3 and opp >= 5:
        reversal = (f"{word(dv[-1])} {('первый', 'второй', 'третий')[run - 1]} день подряд после {opp} "
                    f"{plural(opp, ('торгового дня', 'торговых дней', 'торговых дней'))} {word(-dv[-1])}а")
        facts.append(reversal)
    rec = None
    for n in (5, 20):
        s = d.rolling(n).sum().dropna()
        a, ds = s.values, s.index
        hi = a[-1] > 0
        uu, yy = since(a, ds, len(a) - 1, hi)
        stx = status_ru(uu, yy, ds, hi, t, word="рекорд")
        line_n = f"{word(a[-1])} за {n} торговых дней - {n_ru(abs(a[-1]))} ₽" + (f", {stx}" if stx else "")
        facts.append(line_n)
        if stx and (uu is None or yy >= 1) and rec is None:
            rec = line_n
    mname = GEN[t.month - 1]
    line = f"{word(mtd)} с начала {mname} - {n_ru(abs(mtd))} ₽"
    mtd_rec = len(past) >= 12 and ((mtd < 0 and mtd <= past.min()) or (mtd > 0 and mtd >= past.max()))
    if mtd_rec:
        first = past.index[0]
        line += f"; это уже больше, чем в любой полный месяц наших данных (с {GEN[first.month - 1]} {first.year} года)"
    facts.append(line)
    head = f"{nom.capitalize()}: " + (line if mtd_rec else (reversal or rec or line))
    signs = np.sign(past.values)
    k = 0
    for sg in signs[::-1]:
        if sg != signs[-1]:
            break
        k += 1
    if np.sign(mtd) == signs[-1]:
        facts.append(f"{word(mtd)} идёт {k + 1}-й месяц подряд, считая текущий")
    elif k >= 3:
        facts.append(f"до этого {k} месяцев подряд был {word(signs[-1])}")
    aum = upto(nav[cat], t)
    a_now = float(aum.iloc[-1])
    a_1y = aum[aum.index <= t - pd.Timedelta(days=365)]
    facts.append(f"в {prep} сейчас {n_ru(a_now)} ₽" + (f"; год назад было {n_ru(float(a_1y.iloc[-1]))} ₽"
                                                         if len(a_1y) else ""))
    if a_now > 0:
        facts.append(f"с начала месяца {'ушло' if mtd < 0 else 'пришло'} {p_ru(abs(mtd) / a_now, False)} "
                     f"от активов {gen}")
    last6 = past.iloc[-6:]
    history = ["помесячно, последние полные месяцы: "
               + "; ".join(f"{mn_ru(m, t)} {'+' if x > 0 else '-'}{n_ru(abs(x))} ₽" for m, x in last6.items())]
    if len(past):
        mx, mn = past.idxmax(), past.idxmin()
        history.append(f"самый большой приток за месяц в наших данных - {m_ru(mx, t)} ({n_ru(past.max())} ₽), "
                       f"самый большой отток - {m_ru(mn, t)} ({n_ru(abs(past.min()))} ₽)")
    rel = {"bonds": ("индекс гособлигаций", idx.get("RGBI")), "stocks": ("индекс Мосбиржи", idx.get("IMOEX")),
           "gold": ("золото", perp.get("GLDRUBF")), "yuan": ("юань", perp.get("CNYRUBF"))}.get(cat)
    price, after, analogy, weak = [], [], [], None
    if rel and rel[1] is not None:
        rl, rs = rel[0], upto(rel[1], t)
        ra = rs.values
        price.append(f"{rl} - {px_ru(ra[-1], rl)}; за неделю {p_ru(ra[-1] / ra[-6] - 1)}, за месяц "
                     f"{p_ru(ra[-1] / ra[-21] - 1)}; от максимума за год {p_ru(ra[-1] / ra[-250:].max() - 1)}")
        me = rs.resample("ME").last()
        same = past[np.sign(past) == np.sign(mtd)] if mtd != 0 else past.iloc[0:0]
        big = same.reindex(same.abs().sort_values(ascending=False).index)[:3]
        if len(big) and big.abs().max() < 0.3 * abs(mtd):
            weak = (f"прошлые месяцы с {word(mtd)}ом были намного меньше нынешнего (крупнейший - "
                    f"{n_ru(big.abs().max())} ₽): это не аналогия по масштабу, вывод на них не строить")
        for m, x in big.sort_index().items():
            j = me.index.searchsorted(m)
            if 1 <= j < len(me):
                r_in = me.iloc[j] / me.iloc[j - 1] - 1
                r_nx = me.iloc[j + 1] / me.iloc[j] - 1 if j + 1 < len(me) and me.index[j + 1] <= t else None
                after.append(f"{m_ru(m, t)}: {word(x)} {n_ru(abs(x))} ₽ - {rl} за тот месяц {p_ru(r_in)}"
                             + (f", за следующий {p_ru(r_nx)}" if r_nx is not None else ""))
    limits = [f"наша выборка биржевых фондов, данные с {d.index[0].year} года; у других источников "
              f"суммы могут быть больше - другой охват фондов",
              "поток = изменение активов минус изменение цены пая; дневные данные по дате отчёта фондов"]
    if weak:
        limits.append(weak)
    months = mo[mo.index >= t - pd.Timedelta(days=30 * 30)]
    chart = {"type": "bars", "title": f"{nom.capitalize()}: приток и отток по месяцам, млрд ₽",
             "x": months.index, "y": months.values / 1e9, "current": True}
    return {"kind": "funds", "spec": {"cat": cat}, "as_of": t,
            "headline": head,
            "facts": facts, "history": history, "after": after, "analogy": analogy, "price": price,
            "context": [], "limits": limits, "chart": chart,
            "chart_note": [f"на графике - приток и отток в {prep} по месяцам за два с половиной года; "
                           f"последний столбец - текущий месяц на {d_ru(t, t)}"],
            "hashtag": HASHTAG["funds"]}


# ── карточка: сезонность ─────────────────────────────────────────────────────────
def seasonality_card(code, as_of) -> dict:
    P, names, groups, to_stock, idx, stk, perp, usd = data()
    label, full = ("доллар", usd) if code == "Si" else ("индекс Мосбиржи", idx["IMOEX"].dropna())
    s = upto(full, as_of)
    t = s.index[-1]
    C = det.seasonal_curve(s, t.year)
    tps = det.turning_points(C)
    d0 = t.dayofyear - 1
    lo, hi = int(np.argmin(C)), int(np.argmax(C))
    y0, y1 = t.year - 10, t.year - 1
    facts = [f"сезонный путь {'доллара' if code == 'Si' else 'индекса Мосбиржи'} по годам {y0}-{y1} "
             f"(средний, без годового тренда): дно года - около {dm_ru(lo)}, пик - около {dm_ru(hi)}, "
             f"размах {p_ru(C[hi] - C[lo], False)}"]
    ahead = min(tps, key=lambda tp: (tp[0] - d0) % 366)
    behind = min(tps, key=lambda tp: (d0 - tp[0]) % 366)
    KIND = {"дно": ("сезонное дно", "сезонного дна"), "пик": ("сезонный пик", "сезонного пика")}
    days = lambda n: f"{n} {plural(n, ('день', 'дня', 'дней'))}"  # noqa: E731
    grow = lambda m: "растёт" if m > 0 else "снижается"  # noqa: E731
    d_b, kind_b, nd_b, move_b = behind
    d_, kind, nd, move = ahead
    fwd_move = float(C[d_] - C[d0])
    days_to, back = (d_ - d0) % 366, (d0 - d_b) % 366
    # фаза года одной строкой — она же заголовок карточки
    if back <= 2:
        phase = (f"{KIND[kind_b][0]} приходится на эти дни (около {dm_ru(d_b)}); дальше до ~{dm_ru(nd_b)} "
                 f"кривая в среднем {grow(move_b)} на {p_ru(abs(move_b), False)}")
    elif days_to <= 45:
        phase = (f"до {KIND[kind][1]} (около {dm_ru(d_)}) - {days(days_to)}; до него кривая в среднем "
                 f"{grow(fwd_move)} на {p_ru(abs(fwd_move), False)}, после - {'рост' if move > 0 else 'снижение'} "
                 f"на {p_ru(abs(move), False)} до ~{dm_ru(nd)}")
    elif back <= 30:
        phase = (f"{KIND[kind_b][0]} (около {dm_ru(d_b)}) было {days(back)} назад; дальше до ~{dm_ru(nd_b)} "
                 f"кривая в среднем {grow(move_b)} на {p_ru(abs(move_b), False)}")
    else:
        phase = (f"сейчас отрезок сезонного {'роста' if fwd_move > 0 else 'снижения'}: от {KIND[kind_b][1]} "
                 f"(около {dm_ru(d_b)}) до {KIND[kind][1]} (около {dm_ru(d_)}); до его конца кривая в среднем "
                 f"{grow(fwd_move)} ещё на {p_ru(abs(fwd_move), False)}")
    facts.append(phase)
    # по годам: от этого календарного дня до даты следующей сезонной точки
    target = ahead if days_to >= 10 else (ahead[2], None, None, None)
    tdoy = target[0]
    outs = []
    for y in range(y0, y1 + 1):
        a = pd.Timestamp(y, 1, 1) + pd.Timedelta(days=d0)
        b = pd.Timestamp(y, 1, 1) + pd.Timedelta(days=tdoy) + (pd.DateOffset(years=1) if tdoy <= d0 else pd.Timedelta(0))
        ja, jb = full.index.searchsorted(a), full.index.searchsorted(b)
        if jb < len(full) and full.index[jb] <= t and ja < jb:
            outs.append((y, float(full.iloc[jb] / full.iloc[ja] - 1)))
    after = []
    if outs:
        k = sum(r > 0 for _, r in outs)
        after.append(f"от {dm_ru(d0)} до {dm_ru(tdoy)} по годам: "
                     + ", ".join(f"{y}: {p_ru(r)}" for y, r in outs))
        after.append(f"рост в {k} годах из {len(outs)}, медиана {p_ru(float(np.median([r for _, r in outs])))} "
                     f"(это без снятия тренда - как видел бы инвестор)")
    for n in (20, 40):
        rets = []
        for y in range(1, 11):
            a = t - pd.DateOffset(years=y)
            j = full.index.searchsorted(a)
            if j + n < len(full) and full.index[j + n] <= t:
                rets.append(full.iloc[j + n] / full.iloc[j] - 1)
        if len(rets) >= 8:
            k = int(sum(r > 0 for r in rets))
            after.append(f"следующие {n} торговых дней: {label} рос в {k} годах из {len(rets)}, медиана "
                         f"{p_ru(float(np.median(rets)))}")
    ytd = s[s.index.year == t.year]
    price = [f"{label} - {px_ru(s.iloc[-1], label)} на {d_ru(t, t)}; с начала года {p_ru(ytd.iloc[-1] / ytd.iloc[0] - 1)}"]
    if len(ytd) >= 60:
        corr = float(np.corrcoef(np.log(ytd / ytd.iloc[0]).values, C[ytd.index.dayofyear.values - 1])[0, 1])
        if corr >= 0.5:
            price.append(f"путь этого года похож на сезонный: корреляция {f'{corr:.2f}'.replace('.', ',')} "
                         f"(1 - полное совпадение формы)")
    for hi_ in (False, True):
        pu, py = since(s.values, s.index, len(s) - 1, hi_)
        stx = status_ru(pu, py, s.index, hi_, t)
        if stx and py >= 0.5:
            price.append(f"{label} на {stx}".replace("на максимум", "на максимуме").replace("на минимум", "на минимуме"))
    y52 = s[s.index > t - pd.Timedelta(days=365)]
    price.append(f"за год: минимум {px_ru(y52.min(), label)} ({d_ru(y52.idxmin(), t)}), максимум "
                 f"{px_ru(y52.max(), label)} ({d_ru(y52.idxmax(), t)})")
    limits = [NO_FORECAST,
              "сезонность - среднее за 10 лет: в отдельные годы было иначе, это видно в строке по годам",
              "сезонная кривая без годового тренда: у доллара тренд за 10 лет вверх, поэтому по годам "
              "рост встречается чаще, чем по кривой" if code == "Si" else
              "сезонная кривая без годового тренда; строки по годам - с трендом"]
    xs = pd.date_range(pd.Timestamp(t.year, 1, 1), periods=366, freq="D")
    chart = {"type": "season", "title": f"Сезонность: {label}, средний путь {y0}-{y1} и {t.year} год",
             "x": xs, "y": C * 100, "x2": ytd.index, "y2": (ytd / ytd.iloc[0] - 1).values * 100,
             "now": t, "y_label": "средний путь, %", "y2_label": f"{t.year}, % с начала года",
             "marks": [(xs[d_], C[d_] * 100) for d_, *_ in tps]}
    return {"kind": "seasonality", "spec": {"code": code}, "as_of": t,
            "headline": f"Сезонность: {label} - {phase}",
            "facts": facts, "after": after, "analogy": [], "price": price, "context": [],
            "limits": limits, "chart": chart,
            "chart_note": [f"на графике - средний сезонный путь ({y0}-{y1}, оранжевая линия, без тренда) "
                           f"и путь {t.year} года с начала года (серая); точки - сезонные дно и пик"],
            "hashtag": HASHTAG["seasonality"]}


def build_card(spec: dict, as_of) -> dict:
    kind = spec["kind"]
    if kind == "positions":
        return positions_card(spec["sec"], spec["leg"], as_of)
    if kind == "funds":
        return funds_card(spec["cat"], as_of)
    return seasonality_card(spec["code"], as_of)


SECTIONS = (("ЦИФРЫ", "facts"), ("ИСТОРИЯ РЯДА", "history"), ("ЧТО БЫЛО ПОСЛЕ ПРОШЛЫХ ЭПИЗОДОВ", "after"),
            ("АНАЛОГИЯ", "analogy"), ("ЦЕНА И ФОН", "price"), ("ДРУГИЕ СТОРОНЫ ПОЗИЦИИ", "context"),
            ("ГРАФИК К ПОСТУ", "chart_note"), ("ОГРАНИЧЕНИЯ - чего не утверждать", "limits"))


def focus_lines(card: dict) -> list:
    """Итерация 2: одна история вместо всей карточки — находка, одна аналогия и опора для
    вывода. В первой итерации писатель пересказывал карточку целиком (2,66 числа на 100
    знаков против 0,48 у автора), и автор был интереснее в 13 парах из 15."""
    k = card["kind"]
    after, analogy, facts = card.get("after") or [], card.get("analogy") or [], card.get("facts") or []
    weak = any("не аналогия по масштабу" in x for x in card.get("limits") or [])
    if k == "positions":
        story = analogy[0] if analogy else (after[-2] if len(after) >= 2 else None)
        stat = after[-1] if after and after[-1].startswith("итого") else None
    elif k == "funds":
        story = (card.get("history") or [None])[-1] if weak or not after else after[0]
        stat = next((x for x in facts if "сейчас" in x), None)
    else:   # у сезонности находка — уже фаза года; история — похож ли на неё этот год
        story = next((x for x in card.get("price") or [] if "корреляция" in x), None) \
            or (after[2] if len(after) > 2 else None)
        stat = after[1] if len(after) > 1 else None
    return [f"находка: {card['headline']}"] + [f"одна история: {story}"] * bool(story) \
        + [f"опора для вывода: {stat}"] * bool(stat)


# ── контекст мира (итерация 3): срез сервиса, ставка, новости недели, второй мозг ─────
# Вадим 13.09: «второй мозг и был тем самым контекстом мира, плюс данные сервиса не только
# с ОИ». После двух итераций автор был интереснее 13/15 и 15/15 — тем, чего в карточке нет.
NEWS_RX = {"MIX": r"индекс|IMOEX|Мосбирж|рынок акций|ЦБ|ставк|санкц|переговор|бюджет",
           "Si": r"рубл|доллар|юан|валют|курс|ЦБ|ставк|нефт|экспорт|санкц|девальвац",
           "bonds": r"ОФЗ|облигац|ставк|ЦБ|инфляц|Минфин|доходност|бюджет",
           "stocks": r"фонд|БПИФ|акци|индекс|ЦБ|ставк"}
NEWS_RX.update({"RI": NEWS_RX["MIX"], "CNY": NEWS_RX["Si"], "Eu": NEWS_RX["Si"]})


@lru_cache(None)
def ctx_data():
    def rd(f, **kw):
        return dbdata.read(f.replace(".csv", ""), **kw)
    news = rd("news_ctx.csv")
    if news is not None:
        news["posted_at"] = pd.to_datetime(news.posted_at, utc=True).dt.tz_localize(None)
    rate = rd("key_rate.csv", parse_dates=["valid_from"])
    brain = rd("brain_ctx.csv")
    if brain is not None:
        brain["ts"] = pd.to_datetime(brain.ts, utc=True).dt.tz_localize(None)
        brain["comps"] = brain["comps"].fillna("")
    b = dbdata.read("breadth", parse_dates=["trade_date"])
    b = b[b.universe == "imoex"].pivot_table(index="trade_date", columns="ema_period", values="percent_above").sort_index()
    m = dbdata.read("macro", parse_dates=["period_date"])
    cap = m[m.indicator == "MARKET_CAP_TOTAL"].set_index("period_date")["value"].sort_index()
    gdp = m[m.indicator == "GDP_QUARTERLY"].set_index("period_date")["value"].sort_index()
    buff = (cap / gdp.rolling(4).sum().reindex(cap.index, method="ffill")).dropna() * 100
    return news, rate, brain, b, buff


def market_lines(t) -> list:
    """Срез рынка на дату по индикаторам сервиса — не только позиции."""
    P, names, groups, to_stock, idx, stk, perp, usd = data()
    news, rate, brain, br, buff = ctx_data()
    a, u = upto(idx["IMOEX"], t).values, upto(usd, t).values
    out = [f"индекс Мосбиржи - {px_ru(a[-1], 'индекс')}: за месяц {p_ru(a[-1] / a[-21] - 1)}, от максимума за год "
           f"{p_ru(a[-1] / a[-250:].max() - 1)}",
           f"доллар - {px_ru(u[-1], 'доллар')}, за месяц {p_ru(u[-1] / u[-21] - 1)}"]
    bb = br[br.index <= t]
    if len(bb):
        row = bb.iloc[-1]
        out.append(f"широта: выше 200-дневной средней {row.get(200, np.nan):.0f}% акций индекса, выше 50-дневной "
                   f"{row.get(50, np.nan):.0f}%")
    bf = buff[buff.index <= t]
    if len(bf):
        out.append(f"индикатор Баффетта, капитализация к ВВП - {bf.iloc[-1]:.0f}%")
    daily, nav = funds_data()
    parts = []
    for c in ("bonds", "stocks", "money_market"):
        d = upto(daily[c], t)
        mtd = float(d[d.index.to_period("M") == pd.Timestamp(t).to_period("M")].sum())
        parts.append(f"{FUND[c][0]} {'+' if mtd >= 0 else '-'}{n_ru(abs(mtd))} ₽")
    out.append(f"потоки в фонды с начала {GEN[pd.Timestamp(t).month - 1]}: " + ", ".join(parts))
    if rate is not None:
        r = rate[rate.valid_from <= t]
        num = lambda s: (re.search(r"(\d+(?:[.,]\d+)?)\s*%", str(s)) or [None, "?"])[1]  # noqa: E731
        if len(r):
            line = f"ключевая ставка ЦБ - {num(r.iloc[-1].statement)}% с {d_ru(r.iloc[-1].valid_from, t)}"
            out.append(line + (f"; до этого {num(r.iloc[-2].statement)}%" if len(r) > 1 else ""))
    return out


def news_lines(spec, t, cutoff, k=6) -> list:
    """Новости недели ДО поста: самые просматриваемые по теме находки, без повторов."""
    news = ctx_data()[0]
    if news is None:
        return []
    code = spec.get("code") or det.CODE.get(spec.get("sec")) or spec.get("cat")
    rx, case = NEWS_RX.get(code), False
    if rx is None and spec.get("sec"):
        # акция: тикер или имя С ЗАГЛАВНОЙ — иначе «Самолет» ловил «самолетов Boeing»
        P, names, groups, to_stock, *_ = data()
        nm = str(names.get(spec["sec"], "")).replace(" (вечн)", "").split(" ")[0]
        rx, case = "|".join(x for x in (re.escape(nm) if nm else "", to_stock.get(spec["sec"]) or "") if x), True
    if not rx:
        return []
    w = news[(news.posted_at >= pd.Timestamp(t) - pd.Timedelta(days=7)) & (news.posted_at <= cutoff)]
    w = w[w.text.str.contains(rx, case=case, regex=True, na=False)]
    # зарубежное без связи с Россией и служебный шум — не контекст нашего рынка
    foreign = w.text.str.contains(r"#сша|🇺🇸|#иран|🇮🇷|🇮🇱|#израил|🇨🇳|Boeing|#BA\b|Бессент|ФРС", regex=True, na=False)
    ours = w.text.str.contains(r"росси|РФ|рубл|ЦБ|Мосбирж|IMOEX|ОФЗ|Минфин", case=False, regex=True, na=False)
    noise = w.text.str.contains(r"официальн\w* курс|на выходные", case=False, regex=True, na=False)
    w = w[~(foreign & ~ours) & ~noise].sort_values("views", ascending=False)
    out, seen = [], set()
    for _, r in w.iterrows():
        s = re.split(r"(?<=[.!?])\s", str(r.text).strip())[0][:170]
        if s[:50].lower() in seen:
            continue
        seen.add(s[:50].lower())
        out.append(f"{d_ru(r.posted_at, t)}: {s}")
        if len(out) >= k:
            break
    return out


def brain_lines(spec, t, k=4) -> list:
    """Второй мозг за полтора месяца до даты: события по бумаге, для индекса и фондов —
    сделки фондов и смены состава индекса."""
    brain = ctx_data()[2]
    if brain is None:
        return []
    w = brain[(brain.ts <= t) & (brain.ts >= pd.Timestamp(t) - pd.Timedelta(days=45))]
    P, names, groups, to_stock, *_ = data()
    tk = to_stock.get(spec.get("sec")) if spec.get("sec") else None
    if tk:
        w = w[w.comps.str.contains(f"company:{tk}", regex=False)]
    elif spec["kind"] == "funds" or det.CODE.get(spec.get("sec")) in ("MIX", "RI") or spec.get("code") == "MIX":
        w = w[w.kind.isin(["fund_event", "index_event"])]
    else:
        return []
    return [f"{d_ru(r.ts, t)}: {r.title}" for _, r in w.sort_values("ts", ascending=False).head(k).iterrows()]


def context_for(card, spec, post_ts=None) -> dict:
    t = pd.Timestamp(card["as_of"])
    cutoff = pd.to_datetime(post_ts, utc=True).tz_convert(None) if post_ts else t + pd.Timedelta(hours=9)
    return {"срез рынка по данным сервиса": market_lines(t), "новости недели до поста": news_lines(spec, t, cutoff),
            "второй мозг, события за полтора месяца": brain_lines(spec, t)}


def brief_text(card: dict, focus: bool = False, context: dict | None = None) -> str:
    out = [f"НАХОДКА: {card['headline']}", f"ДАТА ДАННЫХ: {d_ru(card['as_of'])}", ""]
    if focus:
        out += ["ГЛАВНОЕ - строй пост вокруг этого:"] + [f"- {x}" for x in focus_lines(card)] + [""]
    if context:
        out.append("КОНТЕКСТ - что было в мире до поста; опора для позиции канала, но не причина движения:")
        for name, lines in context.items():
            if lines:
                out += [f"{name}:"] + [f"- {x}" for x in lines]
        out.append("")
    if focus:
        out += ["ФОН - отсюда не больше двух фактов, и только если они усиливают историю.", ""]
    for title, key in SECTIONS:
        items = [x for x in card.get(key) or [] if x]
        if items:
            out += [f"{title}:"] + [f"- {x}" for x in items] + [""]
    out.append(f"ХЭШТЕГ РУБРИКИ: {card['hashtag']}")
    return "\n".join(out)


def draw_chart(card: dict, path: str):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.dates as mdates
    import matplotlib.pyplot as plt
    ACC, INK, GREY = "#FF5C2B", "#1d1d1f", "#8e8e93"
    ch = card["chart"]
    fig, ax = plt.subplots(figsize=(10, 5.6), dpi=130)
    for sp in ("top", "right"):
        ax.spines[sp].set_visible(False)
    if ch["type"] == "bars":
        x, y = ch["x"], ch["y"]
        cols = [ACC if v < 0 else "#2f7d6d" for v in y]
        bars = ax.bar(x, y, width=20, color=cols, alpha=0.85)
        bars[-1].set_edgecolor(INK)
        bars[-1].set_linewidth(1.6)
        ax.axhline(0, color=INK, lw=0.8)
        ax.set_ylabel("млрд ₽")
        ax.annotate("текущий месяц", (x[-1], y[-1]), textcoords="offset points",
                    xytext=(-10, -14 if y[-1] < 0 else 8), ha="right", fontsize=9, color=INK)
    else:
        ax.plot(ch["x"], ch["y"], color=ACC, lw=2.2, label=ch.get("y_label"))
        for mx, my in ch.get("marks", []):
            ax.scatter([mx], [my], s=36, color=ACC, edgecolor=INK, zorder=5)
        ax.set_ylabel(ch.get("y_label", ""))
        if "x2" in ch:
            ax2 = ax.twinx()
            ax2.plot(ch["x2"], ch["y2"], color=GREY, lw=1.3, label=ch.get("y2_label"))
            ax2.set_ylabel(ch.get("y2_label", ""), color=GREY)
            ax2.spines["top"].set_visible(False)
        if ch["type"] == "season":
            ax.axvline(pd.Timestamp(ch["now"]).replace(year=pd.Timestamp(ch["x"][0]).year), color=INK, lw=0.8,
                       ls="--")
            short = ["янв", "фев", "мар", "апр", "май", "июн", "июл", "авг", "сен", "окт", "ноя", "дек"]
            ax.xaxis.set_major_locator(mdates.MonthLocator())
            ax.xaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(
                lambda v, _: short[mdates.num2date(v).month - 1]))
        else:
            ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: n_ru(v)))
    ax.set_title(ch["title"], loc="left", fontsize=13, color=INK)
    src = "Данные: раскрытие управляющих компаний" if card["kind"] == "funds" else "Данные: Мосбиржа"
    fig.text(0.01, 0.005, src, fontsize=8, color=GREY)
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)


if __name__ == "__main__":
    kind = sys.argv[1]
    spec = {"positions": lambda a: {"kind": "positions", "sec": a[0], "leg": a[1]},
            "funds": lambda a: {"kind": "funds", "cat": a[0]},
            "seasonality": lambda a: {"kind": "seasonality", "code": a[0]}}[kind](sys.argv[2:-1])
    c = build_card(spec, sys.argv[-1])
    print(brief_text(c))
    os.makedirs(os.path.join(HERE, "step4"), exist_ok=True)
    p = os.path.join(HERE, "step4", f"sample_{kind}.png")
    draw_chart(c, p)
    print("график:", p)
