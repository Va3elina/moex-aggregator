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
from api.services.fund_reorg import correct_flows  # noqa: E402  поправки на реорганизации фондов — те же, что у сайта
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
HASHTAG = {"positions": "#открытыепозиции", "funds": "#деньгивфондах", "seasonality": "#сезонность",
           "fund_trades": "#сделкифондов", "macro": "#открытыепозиции"}
# forecast_backtest.py, 13.09: 2646 эпизодов в 72 фьючерсах — с 2023 года рекорды позиций угадывают
# направление цены в 51% случаев, сезонность индекса хуже «всегда вверх». Вадим: прогнозы цены не делать.
# «Исторический» у нас — после 2022 года (Вадим, 14.09): до этого другой рынок, с нерезидентами.
# Пик шорта 2013 года — не аналогия для нынешней толпы; «максимум с 2014 года» читатель канала
# понимает как «такого не было после 2022-го». Цены не трогаем: «минимум с 2009 года» — приём канала.
ERA_START = pd.Timestamp("2022-03-01")
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
    if not s.endswith(",0"):
        return f"в {s} раза"                     # дробное: «в 2,5 раза»
    n = int(round(r))                            # целое: «в 2 раза», но «в 5 раз», «в 21 раз»
    word = "раза" if n % 10 in (2, 3, 4) and n % 100 not in (12, 13, 14) else "раз"
    return f"в {n} {word}"


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
    fd = fd.merge(f[["fund_id", "category", "ticker"]], on="fund_id").sort_values(["fund_id", "trade_date"])
    g = fd.groupby("fund_id")
    pn, pp = g.nav.shift(1), g.pay.shift(1)
    fd["flow"] = (fd.nav - pn) - pn * (fd.pay - pp) / pp
    fd = correct_flows(fd)      # слияния и ликвидации фондов — как на графике сайта
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


def era_status(u, years, dates, higher, ref) -> str | None:
    """Рекорд позиции в рамках нынешнего рынка: если значение держится с дат до 2022 года, это
    «максимум за всё время после 2022 года», а не «с 2014 года»."""
    if dates[0] >= ERA_START or (u is not None and u >= ERA_START):
        return status_ru(u, years, dates, higher, ref)
    w = "максимум" if higher else "минимум"
    if u is None:
        return f"исторический {w} - за всё время наших данных, с {dates[0].year} года"
    return f"исторический {w}: такого не было за всё время после 2022 года"


def older_peak(arr, dates, i, unit, ref) -> str:
    """Старый пик другого рынка — одной оговоркой, не аналогией."""
    old = dates[:i] < ERA_START
    if not old.any() or arr[:i][old].max() <= arr[i]:
        return ""
    j = int(np.flatnonzero(old)[np.argmax(arr[:i][old])])
    return (f"для понимания, в пост не выносить: до 2022 года, в другом рынке с нерезидентами, бывало и "
            f"больше - {q_ru(arr[j], unit)} {d_ru(dates[j], ref)}")


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


def trend_lines(arr, dates, pser, t, lab, plabel, were, W=40, MIN_PX=0.05) -> list:
    """Тренд вместо пика. Вадим к #2124 (Самолёт, 15.09) и #2216 (АФК, 16.09): «важен не предыдущий
    исторический пик, а тренд: последние месяцы чистая позиция снижается вместе с ценой; в октябре-ноябре
    2024 шорт рос, цена падала, и только когда объём шорта начал снижаться, акция перешла к росту».

    Тренд — знак изменения позиции и цены за W торговых дней (цена — не меньше MIN_PX). Прошлые эпизоды —
    отрезки после 2022 года с тем же сочетанием знаков; «разворот» — первый день после отрезка, когда
    позиция за 10 торговых дней пошла в обратную сторону, и от него — цена через месяц и три."""
    if pser is None or len(arr) < W + 30:
        return []
    s = pd.Series(arr, index=dates)
    ps = pser.reindex(dates).ffill()
    dpos, dpx = s.diff(W), ps / ps.shift(W) - 1
    cp, cx = dpos.iloc[-1], dpx.iloc[-1]
    if not (np.isfinite(cp) and np.isfinite(cx)) or cp == 0 or abs(cx) < MIN_PX:
        return []
    sp, sx = np.sign(cp), np.sign(cx)
    many = lab.startswith("покупки")
    one = lab.startswith("число")
    now_v = ("растут" if many else "растёт") if sp > 0 else ("снижаются" if many else "снижается")
    past_v = (("росли" if many else "росло" if one else "рос") if sp > 0
              else ("снижались" if many else "снижалось" if one else "снижался"))
    turn_v = (("начали" if many else "начало" if one else "начал") + (" снижаться" if sp > 0 else " расти"))
    ok = ((np.sign(dpos) == sp) & (np.sign(dpx) == sx) & (dpx.abs() >= MIN_PX)).values & (dates >= ERA_START)
    runs = []
    for i in np.flatnonzero(ok):
        if runs and i - runs[-1][1] <= 5:
            runs[-1][1] = i
        else:
            runs.append([i, i])
    n = len(arr) - 1
    cur = runs[-1] if runs and runs[-1][1] >= n - 5 else None
    past = [r for r in runs if r is not cur and r[1] - r[0] >= 10]
    d10 = s.diff(10)
    out = [f"главное - тренд, а не прошлый пик: за последние два месяца {lab} {now_v}, а {plabel} {p_ru(cx)}"
           + (f"; так идёт с {d_ru(dates[max(cur[0] - W, 0)], t)}" if cur else "")]
    ups = n_after = 0
    for i0, i1 in past[-4:]:
        j = next((k for k in range(i1 + 1, n + 1) if np.sign(d10.iloc[k]) == -sp), None)
        if j is None:
            continue
        a = max(i0 - W, 0)
        r20, r60 = fwd(pser, dates[j], 20), fwd(pser, dates[j], 60)
        tail = ([f"через месяц {p_ru(r20)}"] if r20 is not None else []) + \
               ([f"через три {p_ru(r60)}"] if r60 is not None else [])
        out.append(f"так же было {d_ru(dates[a], t)} - {d_ru(dates[i1], t)}: {lab} {past_v}, {plabel} "
                   f"{p_ru(ps.iloc[i1] / ps.iloc[a] - 1)}; {d_ru(dates[j], t)} {lab} {turn_v} - "
                   + (f"после этого {plabel} " + ", ".join(tail) if tail else "что было после, ещё не известно"))
        if r20 is not None:
            n_after += 1
            ups += r20 > 0
    if n_after >= 2:
        out.append(f"итого: когда такой тренд позиции разворачивался, {plabel} через месяц {were} выше в {ups} "
                   f"{plural(ups, ('случае', 'случаях', 'случаях'))} из {n_after}")
    elif len(out) == 1:
        out.append("похожих эпизодов такого тренда после 2022 года не было - прошлые пики ниже только фон")
    return out


INTRADAY_NOTE, INTRADAY_SKIP = -0.10, -0.20   # откат к утру: оговорка / находка не идёт в пост


_OI5_ASOF = """SELECT sectype, tradedate, tradetime, pos_long, pos_short, pos_long_num, pos_short_num FROM open_interest
                WHERE interval = 5 AND clgroup = 'FIZ' AND sectype = :s AND tradedate = :d AND tradetime <= :t
                ORDER BY tradetime DESC LIMIT 1"""


_OI5_ALL = """SELECT DISTINCT ON (sectype) sectype, tradedate, tradetime, pos_long, pos_short, pos_long_num,
                     pos_short_num
                FROM open_interest WHERE interval = 5 AND clgroup = 'FIZ' AND tradedate = :d AND tradetime <= :t
               ORDER BY sectype, tradetime DESC"""


def intraday_snapshots(now=None) -> pd.DataFrame:
    """Последний 5-минутный срез позиций физлиц по каждому фьючерсу за сегодня, не позже «сейчас» (индекс — код
    фьючерса; long/short — объём сторон, шорт со знаком плюс; nl/ns — люди)."""
    now = pd.Timestamp(now if now is not None else NOW if NOW is not None
                       else pd.Timestamp.now(tz="Europe/Moscow").tz_localize(None))
    df = _sql(_OI5_ALL, {"d": now.date(), "t": now.time()})
    if df.empty:
        return df
    df["tradedate"] = pd.to_datetime(df.tradedate)
    df["long"], df["short"] = df.pos_long.astype(float), -df.pos_short.astype(float)
    df["nl"], df["ns"] = df.pos_long_num.astype(float), df.pos_short_num.astype(float)
    return df.set_index("sectype")


def intraday_now(sec, leg, as_of):
    """Последнее утреннее значение позиции (5-минутные данные, время МСК), если оно новее закрытия as_of."""
    try:
        if NOW is not None:        # прогон на прошлом: срез того дня, не позже «сейчас»
            df = _sql(_OI5_ASOF, {"s": sec, "d": pd.Timestamp(NOW).date(), "t": pd.Timestamp(NOW).time()})
            df["tradedate"] = pd.to_datetime(df["tradedate"])
        else:
            df = dbdata.read("oi_intraday_last", parse_dates=["tradedate"])
    except Exception:  # noqa: BLE001 — без интрадея карточка та же, что раньше
        return None
    r = df[df.sectype == sec]
    if r.empty or r.tradedate.iloc[0] <= pd.Timestamp(as_of):
        return None
    r = r.iloc[0]
    val = {"long": r.pos_long, "short": -r.pos_short, "net": r.pos_long + r.pos_short,
           "nl": r.pos_long_num, "ns": r.pos_short_num}.get(leg)
    return None if val is None or pd.isna(val) else (float(val), r.tradedate, r.tradetime)


def price_lines(pser, t, plabel) -> list:
    """Цена бумаги к находке: уровень, неделя и месяц, многолетний максимум или минимум."""
    if pser is None or len(pser) <= 25:
        return []
    pa, pdts = pser.values, pser.index
    pl = [f"{plabel} - {px_ru(pa[-1], plabel)} на {d_ru(pdts[-1], t)}",
          f"за неделю {p_ru(pa[-1] / pa[-6] - 1)}, за месяц {p_ru(pa[-1] / pa[-21] - 1)}"]
    for hi in (False, True):
        pu, py = since(pa, pdts, len(pa) - 1, hi)
        s = status_ru(pu, py, pdts, hi, t)
        if s and py >= 0.5:
            pl.append(f"{plabel} на {s}".replace("на максимум", "на максимуме").replace("на минимум", "на минимуме"))
    return ["; ".join(pl[:2])] + pl[2:]


def other_legs(P, sec, leg, t) -> list:
    """Другие стороны позиции физлиц по тому же фьючерсу: многолетний уровень или сдвиг за месяц от 15%."""
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
                best_s = era_status(uu, yy, d2, hi, t)
                break
        if best_s:
            context.append(f"{l2} - {q_ru(a2[-1], u2)}, {best_s}")
        elif L != "net" and a2[-21] > 0 and abs(a2[-1] / a2[-21] - 1) >= 0.15:
            context.append(f"{l2} - {q_ru(a2[-1], u2)}, за месяц {p_ru(a2[-1] / a2[-21] - 1)}")
    return context


# ── повороты: новые типы постов по позициям ─────────────────────────────────────
# Вадим 28.09: трендовые посты нужны как есть, новыми типами — разбавить. Поворот — то, с чего автор канала начинает
# пост, а завод пропускал (реплей 27.09, слепая оценка: «идёт по шаблону рекорд → тренд → эпизод → 3 из 4 и пропускает
# повод и поворот»). Карточка тренда поворот не показывает; пост нового типа — отдельная находка дня (insight_scan.pick,
# spec["angle"]), у его карточки своё ГЛАВНОЕ. Число контрактов не называется (R02) — только кратности и доли.
ANGLE_MIN = {"концентрация": 3.0, "повод": 4.0, "доля": 0.15, "рывок": 8.0}
# порядок важности для канала: повод дня и узкий круг — сильнее доли (28.09: две «доли», одна по какао, обошли повод
# Самолёта −18% за два дня); доля — только у индекса и валют, как в посте «Шортистов больше, чем когда-либо»;
# рывок по ходу дня — живое событие, он первым
ANGLE_PRIORITY = {"рывок": 0, "повод": 1, "концентрация": 2, "доля": 3}
# Рывок по ходу дня (Вадим 28.09: «сканер находок — на интрадей, хотя бы каждый час»): 5-минутный срез позиции против
# закрытия — не меньше ANGLE_MIN["рывок"] обычных дневных ходов (медиана модуля дневного изменения за 60 дней) и не
# меньше 10%. Калибровка на 42 днях (28.09): 1–2 бумаги в день, в 24 днях из 39 — хоть одна; среди них посты канала
# «Магнит шортов» (22.09: шорт +106% к 20:00) и «Самолёт падает, толпа докупает» (15.09: лонг +41% уже к 12:00), Русал
# 25.09 (+77%, крупнейший прирост покупок с 2012 года) и 28.09 (+19% к 12:00 при +4 покупателях).
SURGE_MIN_PCT = 0.10
SHARE_CODES = ("MIX", "RI", "Si", "CNY", "Eu")


_LIVE = """SELECT begin_time AS t, close FROM candles
             WHERE secid = :s AND type = 'stock' AND interval = 5 AND begin_time >= :d AND begin_time <= :now
             ORDER BY begin_time DESC LIMIT 1"""


NOW = None      # «сейчас» для прогона на прошлом (trigger_scan --at): живая цена и утренний срез — не позже него


def live_price(ticker, after, now=None):
    """Последняя 5-минутная цена акции за день ПОСЛЕ `after` (сегодня), не позже now: «повод дня» тем же вечером —
    пост канала «Самолёт падает, толпа докупает» (15.09, 21:27) про −12% «сегодня». → (цена, время МСК) или None."""
    if not ticker:
        return None
    now = pd.Timestamp(now if now is not None else NOW if NOW is not None
                       else pd.Timestamp.now(tz="Europe/Moscow").tz_localize(None))
    try:
        df = _sql(_LIVE, {"s": ticker, "d": pd.Timestamp(after).normalize() + pd.Timedelta(days=1), "now": now})
    except Exception:  # noqa: BLE001 — без живой цены повод считается по закрытиям
        return None
    if df.empty:
        return None
    return float(df.close.iloc[0]), pd.Timestamp(df.t.iloc[0])


def crowd_result(gross, pser, leg, plabel) -> str | None:
    """Итог толпы по валовой стороне за месяц: цена в дни прироста позиции против нынешней (как в positions_card)."""
    arr, dates = gross.values.astype(float), gross.index
    i = len(arr) - 1
    if pser is None or i < 21:
        return None
    pw = pser.reindex(dates[i - 20:i + 1]).ffill().values
    add = np.clip(np.diff(arr[i - 21:i + 1]), 0, None)
    ok = ~np.isnan(pw) & (add > 0)
    if ok.sum() < 3 or add[ok].sum() < 0.2 * arr[i]:
        return None
    vw, now = float((add[ok] * pw[ok]).sum() / add[ok].sum()), float(pser.iloc[-1])
    res = (now / vw - 1) * (-1 if leg == "short" else 1)
    if abs(res) < 0.03:
        return None
    bet = "ставка на падение" if leg == "short" else "ставка на рост"
    return (f"{bet} {'в плюсе' if res > 0 else 'в минусе'} примерно на {p_ru(abs(res), False)}: набирали в среднем по "
            f"{px_ru(vw, plabel)}, сейчас {px_ru(now, plabel)}")


def position_angles(P, sec, leg, lab, dat, plabel, arr, dates, pser, past, t) -> list:
    """[{type, strength, line}] по силе:
      • концентрация — объём позиции за месяц растёт, а людей в ней меньше: «Магнит шортов» (23.09) — объём шорта ×6,7
        при числе шортистов −15%, «кто-то один крупный». Проверяется и вторая сторона: если покупки тоже на максимуме, а
        чистая позиция мала, узкий круг может держать спред, а не ставку против (слепая оценка 28.09);
      • повод — резкий ход цены за день-два против обычного хода за тот же срок, а толпа на рекорде: «Самолёт падает,
        толпа докупает» (15.09); с итогом толпы и экспирацией как усилителем («лонг растёт за два дня до экспирации,
        когда его обычно режут» — приём канала);
      • доля — число физлиц в шорте (лонге) как доля всех с позицией: за месяц (толпа переходит из лонга в шорт) или
        против прошлого пика; посылка «трейдеров больше» — только если их правда больше (евро 28.09: было наоборот)."""
    out = []
    i, v = len(arr) - 1, float(arr[-1])
    col = lambda name: upto(P[name][sec], t).reindex(dates).ffill()  # noqa: E731
    cnt_leg = {"long": "nl", "short": "ns"}.get(leg)
    if cnt_leg and i >= 61 and v > 0:
        a = surge_angle(sec, leg, lab, dat, plabel, arr, dates, pser, t, col)
        if a:
            out.append(a)
    if cnt_leg and i >= 20 and arr[i - 20] > 0:
        cs = col(cnt_leg)
        c_now, c_20 = float(cs.iloc[-1]), float(cs.iloc[-21])
        if c_now > 0 and c_20 > 0 and np.isfinite(c_now) and np.isfinite(c_20):
            k = (v / c_now) / (arr[i - 20] / c_20)
            if k >= 2 and v / arr[i - 20] >= 1.5:
                who = "шорте" if leg == "short" else "лонге"
                line = (f"поворот - концентрация: {lab} по {dat} за месяц вырос {x_ru(v / arr[i - 20])}, а число "
                        f"физлиц в {who} {p_ru(c_now / c_20 - 1)}; на одного в среднем {x_ru(k)} больше, чем месяц назад - "
                        f"позицию набирает узкий круг, а не толпа; кто именно - данные не говорят")
                opp = "long" if leg == "short" else "short"
                u_o, y_o, net = None, 0.0, None
                if opp in P and sec in P[opp]:
                    u_o, y_o = since(col(opp).abs().values.astype(float), dates, i, True)
                if "net" in P and sec in P["net"]:
                    net = float(col("net").iloc[-1])
                if y_o >= 0.5 or (net is not None and abs(net) < 0.3 * v):
                    other = "покупки физлиц" if opp == "long" else "шорт физлиц"
                    line += (f"; ВТОРАЯ СТОРОНА: {other} тоже " + (f"на {era_status(u_o, y_o, dates, True, t)}"
                                                                    if y_o >= 0.5 else "велики")
                             + (f", чистая позиция - всего {p_ru(abs(net) / v, False)} от объёма" if net is not None else "")
                             + " - узкий круг может держать обе стороны (спред), а не ставку против: «ставят против» "
                               "не утверждай, «шортят единицы» - перебор")
                out.append({"type": "концентрация", "strength": float(k), "line": line})
    if pser is not None and len(pser) > 62 and leg in ("long", "short", "net"):
        pa, pd_ = pser.values.astype(float), pser.index
        typ1 = float(np.median(np.abs(pa[-60:] / pa[-61:-1] - 1)))
        typ2 = float(np.median(np.abs(pa[-60:] / pa[-62:-2] - 1)))
        cand = [(pa[-1] / pa[-2] - 1, "за день", typ1), (pa[-1] / pa[-3] - 1, "за два дня", typ2)]
        try:
            stock = data()[3].get(sec)
        except Exception:  # noqa: BLE001 — нет справочника (тест без базы): повод по закрытиям
            stock = None
        live = live_price(stock, pd_[-1])
        if live:
            # сегодня, по 5-минутной цене: вечерний проход (signals/trigger_scan.py) ловит повод в тот же день
            lp, lt = live
            cand += [(lp / pa[-1] - 1, f"сегодня к {lt:%H:%M}", typ1), (lp / pa[-2] - 1, "за два дня с сегодняшним", typ2)]
        mv, span, typ = max(cand, key=lambda z: abs(z[0]) / z[2] if z[2] > 0 else 0)
        if typ > 0 and abs(mv) >= max(0.05, 3 * typ):
            move = "падает" if mv < 0 else "растёт"
            side = {"long": "покупки физлиц", "short": "шорт физлиц", "net": "чистая позиция физлиц"}[leg]
            today = span.startswith("сегодня") or span.endswith("с сегодняшним")
            when = f"{span} (торги ещё идут)" if today else f"{span} (к {d_ru(pd_[-1], t)})"
            line = (f"повод дня: {plabel} {when} {p_ru(mv)} - {x_ru(abs(mv) / typ)} больше обычного хода "
                    f"{'за два дня' if 'два дня' in span else 'за день'}; бумага {move}, а {side} - на рекорде (на "
                    f"закрытие {d_ru(t, t)}); объяснение хода - в КОНТЕКСТЕ («что было у компании»): «на рынке "
                    f"связывают», «на фоне», не «из-за»")
            gleg = leg if leg in ("long", "short") else ("long" if v >= 0 else "short")
            if gleg in P:
                cr = crowd_result(col(gleg).abs(), pser, gleg, plabel)
                if cr:
                    line += f"; итог толпы: {cr} - оценка по средней цене в дни прироста позиции"
            from signals.insights.expiry import next_expiry
            ex = pd.Timestamp(next_expiry(t))
            left = (ex - pd.Timestamp(t).normalize()).days
            if 0 < left <= 5 and i >= 2 and arr[i] > arr[i - 2] and "вечн" not in dat:
                line += (f"; и это за {left} {plural(left, ('день', 'дня', 'дней'))} до квартальной экспирации "
                         f"{d_ru(ex, t)}, когда позиции обычно сокращают - толпа идёт против привычки")
            out.append({"type": "повод", "strength": abs(mv) / typ, "line": line, "live": today})
    other = {"ns": "nl", "nl": "ns"}.get(leg)
    if other and i >= 20 and det.CODE.get(sec) in SHARE_CODES:
        os_ = col(other)
        n_now, o_now = v, float(os_.iloc[-1])
        n_20, o_20 = float(arr[i - 20]), float(os_.iloc[-21])
        if n_now + o_now > 0 and n_20 + o_20 > 0:
            sh, sh20 = n_now / (n_now + o_now), n_20 / (n_20 + o_20)
            who, opp_who = ("шорте", "лонге") if leg == "ns" else ("лонге", "шорте")
            best = None
            if abs(sh - sh20) >= 0.10:
                ch_n = p_ru(n_now / n_20 - 1) if n_20 else "н/д"
                ch_o = p_ru(o_now / o_20 - 1) if o_20 else "н/д"
                shift = ""
                if sh > sh20:
                    shift = " - толпа переходит из лонга в шорт" if leg == "ns" else " - толпа переходит из шорта в лонг"
                best = (abs(sh - sh20), f"поворот - доля: за месяц доля физлиц в {who} среди всех с позицией по {dat} "
                                        f"{'выросла' if sh > sh20 else 'снизилась'} с {p_ru(sh20, False)} до "
                                        f"{p_ru(sh, False)}: число в {who} {ch_n}, в {opp_who} {ch_o}{shift}")
            if past:
                j = past[-1]["top"]
                n0, o0 = float(arr[j]), float(os_.iloc[j])
                if n0 + o0 > 0 and np.isfinite(o0) and best is not None:
                    # сдвиг за месяц — главное, прошлый пик — фоном (евро 16.09: 15% → 35% за месяц важнее пика 2023)
                    best = (best[0], best[1] + f"; на прошлом пике {d_ru(dates[j], t)} доля была {p_ru(n0 / (n0 + o0), False)}"
                                     + ("" if (n_now + o_now) > (n0 + o0) else ", но трейдеров с позицией тогда было "
                                        "больше - «их стало больше» не пиши"))
                elif n0 + o0 > 0 and np.isfinite(o0):
                    sh0 = n0 / (n0 + o0)
                    if abs(sh - sh0) >= 0.10:
                        more = (n_now + o_now) > (n0 + o0)
                        best = (abs(sh - sh0), f"поворот - доля: в {who} {p_ru(sh, False)} всех физлиц с позицией по "
                                               f"{dat}; на прошлом пике {d_ru(dates[j], t)} было {p_ru(sh0, False)}; "
                                               + ("трейдеров с позицией теперь больше - число людей растёт вместе с рынком, "
                                                  "сравнивай долю" if more else
                                                  "трейдеров с позицией теперь меньше, чем тогда - «их стало больше» не пиши"))
            if best:
                out.append({"type": "доля", "strength": best[0], "line": best[1]})
    return sorted(out, key=lambda a: (ANGLE_PRIORITY[a["type"]], -a["strength"] / ANGLE_MIN[a["type"]]))

def surge_angle(sec, leg, lab, dat, plabel, arr, dates, pser, t, col) -> dict | None:
    """Рывок по ходу дня: срез позиции (5 минут) против закрытия t в долях обычного дневного хода; люди той же стороны
    (те же добирают — узкий круг, пришли новые — толпа), рекорд по срезу, живая цена акции. Экспирация между закрытием
    и срезом или день экспирации — переход в следующий контракт, а не рывок."""
    now_ = intraday_now(sec, leg, t)
    if not now_:
        return None
    iv, iday, itime = now_
    from signals.insights.expiry import near_expiry, next_expiry
    if near_expiry(iday) or (pd.Timestamp(next_expiry(t)) <= pd.Timestamp(iday) and "вечн" not in dat):
        return None
    v = float(arr[-1])
    typ = float(np.median(np.abs(np.diff(arr[-61:]))))
    if typ <= 0 or v <= 0 or not np.isfinite(iv):
        return None
    k, pct = abs(iv - v) / typ, iv / v - 1
    if k < ANGLE_MIN["рывок"] or abs(pct) < SURGE_MIN_PCT:
        return None
    hm = str(itime)[:5]
    line = (f"рывок по ходу дня: {lab} по {dat} к {hm} {d_ru(iday, t)} {p_ru(pct)} к закрытию {d_ru(t, t)} - "
            f"{x_ru(k)} больше обычного дневного хода")
    cnt = {"long": "nl", "short": "ns"}[leg]
    c_now, c0 = intraday_now(sec, cnt, t), float(col(cnt).iloc[-1])
    if c_now and c0 > 0 and np.isfinite(c0) and c_now[0] > 0:
        c1 = float(c_now[0])
        dn, cp = c1 - c0, c1 / c0 - 1
        line += (f"; число физлиц в {'лонге' if leg == 'long' else 'шорте'} {p_ru(cp)} "
                 f"({'+' if dn >= 0 else '-'}{abs(dn):.0f} {plural(dn, ('человек', 'человека', 'человек'))})")
        if pct > 0 and cp < pct / 3:
            line += (f" - позицию добирают те же люди: на одного в среднем {p_ru((iv / c1) / (v / c0) - 1)}, это узкий "
                     f"круг, а не толпа")
        elif pct > 0 and cp >= pct / 2:
            line += " - пришли новые люди: это толпа, а не узкий круг"
    d2 = dates.append(pd.DatetimeIndex([pd.Timestamp(iday)]))
    u, yrs = since(np.append(arr, iv), d2, len(arr), pct > 0)
    st = era_status(u, yrs, d2, pct > 0, t)
    if st and (u is None or yrs >= 1):
        line += f"; по срезу это {st} (дневные данные за {d_ru(iday, t)} будут вечером)"
    try:
        stock = data()[3].get(sec)
    except Exception:  # noqa: BLE001 — нет справочника (тест без базы): без живой цены
        stock = None
    live = live_price(stock, t) if stock else None
    if live and pser is not None and len(pser):
        line += f"; {plabel} к {live[1]:%H:%M} {p_ru(live[0] / float(pser.iloc[-1]) - 1)} к закрытию"
    line += (f"; день не закончен: пиши «к {hm}», а не «за день» и не «на закрытие»; причина - только из КОНТЕКСТА, "
             f"«на фоне», не «из-за»")
    return {"type": "рывок", "strength": float(k), "line": line, "live": True,
            "point": (pd.Timestamp(iday), float(iv), hm)}


ANGLE_NOTE = {
    "рывок": "ТИП ПОСТА: РЫВОК ПО ХОДУ ДНЯ - не трендовый пост, событие идёт прямо сейчас. Начни с того, что случилось "
             "с позицией физлиц к указанному времени: на сколько и во сколько раз сильнее обычного дня, те же люди "
             "добирают или пришли новые, что с ценой. Рекорд - если он есть в строке. Тренд и статистика прошлых "
             "разворотов - не нужны или одной фразой.",
    "концентрация": "ТИП ПОСТА: КОНЦЕНТРАЦИЯ - не трендовый пост. Мысль одна: позицию набирает узкий круг, а не толпа. "
                    "Тренд, прошлые эпизоды и «в скольких случаях из скольких» - не нужны или одной фразой.",
    "повод": "ТИП ПОСТА: ПОВОД ДНЯ - не трендовый пост. Начни с хода цены этих дней и с того, что толпа стоит на другой "
             "стороне (или удваивает ставку); что говорят о причине - из КОНТЕКСТА, «на рынке связывают». Тренд и "
             "статистика прошлых разворотов - не нужны или одной фразой.",
    "доля": "ТИП ПОСТА: ДОЛЯ - не трендовый пост. Не число людей, а доля в шорте (лонге) среди всех с позицией - за "
            "месяц или против прошлого пика, как в строке поворота; скажи, что из этого следует. Тренд и статистика "
            "разворотов - не нужны или одной фразой.",
}


def angle_card(card: dict, angle: str) -> dict:
    """Карточка поста нового типа из карточки позиций: своё ГЛАВНОЕ (поворот, находка, итог толпы, цена) и пометка типа
    для писателя. Цифры те же — карточка тренда остаётся нетронутой."""
    a = next((x for x in card.get("angles") or [] if x["type"] == angle), None)
    if a is None:
        raise ValueError(f"у находки нет поворота «{angle}»")
    crowd = next((f for f in card.get("facts") or [] if f.startswith("итог толпы")), None)
    focus = [ANGLE_NOTE[angle], a["line"], f"находка: {card['headline']}"] + \
        [crowd] * bool(crowd and "итог толпы" not in a["line"]) + ([f"цена: {card['price'][0]}"] if card.get("price") else [])
    # тренд и «N из M» у поста нового типа не нужны: карточка сама тянула писателя обратно в шаблон (слепая оценка 28.09)
    limits = [x for x in card.get("limits") or [] if not x.startswith("пост строится на блоке ТРЕНД")]
    chart, note = card.get("chart"), list(card.get("chart_note") or [])
    if angle == "рывок" and a.get("point") and chart and chart.get("type") == "line2":
        # последняя точка графика — срез по ходу дня, иначе рывок на картинке не виден
        d1, v1, hm = a["point"]
        chart = {**chart, "x": pd.DatetimeIndex(chart["x"]).append(pd.DatetimeIndex([d1])),
                 "y": np.append(np.asarray(chart["y"], dtype=float), v1), "marks": list(chart.get("marks") or []) + [(d1, v1)]}
        note.append(f"последняя точка - срез {d_ru(d1, card['as_of'])} к {hm} МСК")
        limits = [x for x in limits if not x.startswith("данные дневные")] + \
            [f"дневные данные - на закрытие {d_ru(card['as_of'], card['as_of'])}; последняя точка - 5-минутный срез "
             f"{d_ru(d1, card['as_of'])} к {hm}, торги ещё идут"]
    head = card["headline"]
    if angle == "рывок":
        # свой заголовок: у трендовой карточки он тот же, что у вчерашнего черновика по бумаге (Русал 28.09 снимался
        # проверкой «такой заголовок уже был» — #3049 от 27.09)
        head = "Рывок по ходу дня: " + a["line"].split(";")[0].replace("рывок по ходу дня: ", "")
    return {**card, "headline": head, "focus": focus, "trend": [], "after": [], "analogy": [], "limits": limits,
            "chart": chart, "chart_note": note, "spec": {**card["spec"], "angle": angle}}


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
    st = era_status(u, years, dates, True, t)

    facts = [f"{lab} по {dat} - {q_ru(v, unit)} на закрытие {d_ru(t, t)}"]
    if st:
        facts.append(f"это {st}")
        op = older_peak(arr, dates, i, unit, t)
        if op:
            facts.append(op)
    eps = episodes(arr, i)
    cur = eps[-1] if eps and i - eps[-1]["end"] <= 40 else None
    start = cur["start"] if cur else i
    era = dates[:start] >= ERA_START
    if start > 0 and era.any():
        j = int(np.flatnonzero(era)[np.argmax(arr[:start][era])])
        prev, pdate = float(arr[j]), dates[j]
        if prev > 0 and v > prev:
            r = v / prev
            facts.append(f"прежний пик - {q_ru(prev, unit)} {d_ru(pdate, t)}; сейчас выше "
                         + (x_ru(r) if r >= 1.5 else f"на {p_ru(r - 1, False)} - на {q_ru(v - prev, unit)}"))
    ch = []
    for k, w in ((1, "за день"), (5, "за неделю"), (20, "за месяц")):
        if i >= k and arr[i - k] > 0 and leg != "net":
            ch.append(f"{w} {p_ru(v / arr[i - k] - 1)}")
        elif i >= k and leg == "net":
            ch.append(f"{w} {'+' if v - arr[i - k] >= 0 else '-'}{q_ru(abs(v - arr[i - k]), unit)}")
    if ch:
        facts.append("изменение: " + ", ".join(ch))
    intraday_chg = None
    now = intraday_now(sec, leg, t)
    if now and v > 0:
        iv, iday, itime = now
        iv *= sign
        intraday_chg = iv / v - 1
        from signals.insights.expiry import next_expiry
        ex = next_expiry(t)
        # реплей 27.09: «−38% к утру» через экспирацию 17.09 черновики объявили разворотом (шортисты по индексу)
        crossed = pd.Timestamp(ex) <= pd.Timestamp(iday) and "вечн" not in nom
        facts.append(f"утром {d_ru(iday, t)} к {str(itime)[:5]} МСК {lab} - {q_ru(iv, unit)} "
                     f"({p_ru(intraday_chg)} к закрытию {d_ru(t, t)})"
                     + (f" - между ними квартальная экспирация {d_ru(ex, t)}: изменение механическое, это НЕ разворот "
                        f"и не тема поста" if crossed else
                        " - картина к утру уже развернулась: без этой оговорки пост писать нельзя"
                        if intraday_chg <= INTRADAY_NOTE else ""))
    if leg != "net" and st and i >= 5 and arr[i - 5] > 0 and v > arr[i - 5]:
        # #2124 SMLT: писатель объявил «пик пройден», а позиция ещё росла
        facts.append("позиция всё ещё растёт: нынешний эпизод не закончен - «пик пройден» не пиши; "
                     "прошлые эпизоды ниже - это пики, называй их с датами")
    # итог толпы: цена в дни прироста позиции против нынешней. В посте коллеги к #2104 (Лукойл, 14.09)
    # «шортисты сидят в незафиксированном убытке» — самая сильная строка, и она считается по рядам
    if leg in ("long", "short") and pser is not None and i >= 21:
        pw = pser.reindex(dates[i - 20:i + 1]).ffill().values
        add = np.clip(np.diff(arr[i - 21:i + 1]), 0, None)
        ok = ~np.isnan(pw) & (add > 0)
        if ok.sum() >= 3 and add[ok].sum() >= 0.2 * v:
            vw, now = float((add[ok] * pw[ok]).sum() / add[ok].sum()), float(pser.iloc[-1])
            res = (now / vw - 1) * (-1 if leg == "short" else 1)
            if abs(res) >= 0.03:
                bet = "ставка на падение" if leg == "short" else "ставка на рост"
                cost = "стоили" if were == "были" else "стоил"
                facts.append(f"итог толпы: за месяц позицию наращивали, когда {plabel} {cost} в среднем "
                             f"{px_ru(vw, plabel)}, на {d_ru(pser.index[-1], t)} - {px_ru(now, plabel)}; {bet} "
                             f"{'в плюсе' if res > 0 else 'в минусе'} примерно на {p_ru(abs(res), False)} - "
                             f"оценка по средней цене в дни прироста позиции")
    if cur and i - cur["start"] >= 5:
        facts.append(f"на максимумах за год и больше с {d_ru(dates[cur['start']], t)}; с тех пор "
                     f"{p_ru(v / arr[cur['start']] - 1)}" if arr[cur['start']] > 0 else "")

    # что было после прошлых пиков
    past = [e for e in eps if e is not cur and e["end"] < start and dates[e["top"]] >= ERA_START]
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
                     f"{plural(k, ('случае', 'случаях', 'случаях'))} из {len(r20s)}")
    analogy = []
    if rows:
        best = min(rows, key=lambda r: abs(np.log(max(r["value"], 1) / max(v, 1))))
        if best["r20"] is not None:
            analogy.append(f"ближе всего по масштабу - пик {d_ru(best['date'], t)} ({q_ru(best['value'], unit)}): "
                           f"за следующий месяц {plabel} {p_ru(best['r20'])}"
                           + (f", за три - {p_ru(best['r60'])}" if best["r60"] is not None else ""))

    price = price_lines(pser, t, plabel)
    context = other_legs(P, sec, leg, t)
    angles = position_angles(P, sec, leg, lab, dat, plabel, arr, dates, pser, past, t)

    limits = [f"данные дневные, на закрытие торгов {d_ru(t, t)}; что было внутри дня, не видно",
              f"данные по {dat} начинаются в {dates[0].year} году",
              NO_FORECAST]
    from signals.insights.expiry import expiry_note
    if expiry_note(t):
        limits.append(expiry_note(t))
    if dates[0] < ERA_START:
        limits.append("до 2022 года был другой рынок, с нерезидентами: пики до 2022 года - не аналогия и в "
                      "пост не нужны; рекорд после 2022-го называй «исторический максимум» без оговорок")
    if len(r20s) < 4:
        limits.append(f"прошлых пиков с известным продолжением всего {len(r20s)} - это история, "
                      f"а не закономерность: не обобщай")
    trend = trend_lines(arr, dates, pser, t, lab, plabel, were)
    if trend:
        limits.append("пост строится на блоке ТРЕНД: прошлые пики - фон; «пик пройден», сравнение с прошлым пиком "
                      "и число контрактов в пост не нужны")
    win = ser[ser.index >= t - pd.Timedelta(days=3 * 365)]
    chart = {"type": "line2", "title": f"{lab.capitalize()}, {nom}", "x": win.index, "y": win.values,
             "y_label": unit, "marks": [(r["date"], r["value"]) for r in rows if r["date"] >= win.index[0]]
             + [(t, v)]}
    if pser is not None:
        pw = pser[pser.index >= win.index[0]]
        chart.update({"x2": pw.index, "y2": pw.values, "y2_label": plabel})
    return {"kind": "positions", "spec": {"sec": sec, "leg": leg}, "as_of": t,
            "headline": f"{lab.capitalize()} по {dat} - {q_ru(v, unit)}" + (f", {st}" if st else ""),
            "facts": [f for f in facts if f], "trend": trend, "after": after, "analogy": analogy, "price": price,
            "intraday_change": intraday_chg, "angles": angles,
            "context": context, "limits": limits, "chart": chart,
            "chart_note": [f"на графике - {lab} по {dat} за три года (оранжевая линия) и {plabel} "
                           f"(серая); точками отмечены прошлые пики и текущее значение"],
            "hashtag": HASHTAG["positions"]}


# ── карточка: потоки в фонды ─────────────────────────────────────────────────────
def stall_episodes(past, me, t) -> list:
    """Месяцы, когда приток остановился: поток меньше четверти среднего за прошлые 12 месяцев при
    притоке в среднем. Пост Вадима про золото (14.09): «небольшие оттоки или замедление притоков» —
    оба раза золото в тот момент стояло у минимумов, после которых начинался рост. Соседние месяцы
    (через один) — один эпизод; только после 2022 года."""
    avg = past.shift(1).rolling(12, min_periods=6).mean()
    m = past[(past.index >= ERA_START) & (avg > 0) & (past < 0.25 * avg)]
    eps = []
    for d in m.index:
        if eps and (d.to_period("M") - eps[-1][-1].to_period("M")).n <= 2:
            eps[-1].append(d)
        else:
            eps.append([d])
    out = []
    for e in eps:
        if (t.to_period("M") - e[-1].to_period("M")).n <= 2:
            continue        # примыкает к текущему месяцу — это нынешний эпизод, не прошлый
        j0, j1 = me.index.searchsorted(e[0]), me.index.searchsorted(e[-1])
        if j0 < 1 or j1 >= len(me):
            continue
        r3 = me.iloc[j1 + 3] / me.iloc[j1] - 1 if j1 + 3 < len(me) and me.index[j1 + 3] <= t else None
        out.append({"months": e, "flow": float(past[e].sum()), "r_in": float(me.iloc[j1] / me.iloc[j0 - 1] - 1),
                    "r3": None if r3 is None else float(r3)})
    return out


# ── карточка: позиция на минимуме ────────────────────────────────────────────────
def positions_low_card(sec, base, as_of) -> dict:
    """Позиция на минимуме — зеркало positions_card: «покупки физлиц по доллару — минимум с апреля 2025» (пост канала
    22.09 «Спекулянты уходят из валюты»). Детектор такие находки давал (нога *_low), а карточки не было — KeyError
    long_low, и завод их не писал (реплей 27.09). Дно вместо пика, «ещё снижается» вместо «ещё растёт», что было после
    прошлых минимумов — пиков зеркального ряда."""
    P, *_ = data()
    ser = upto(P[base][sec], as_of)
    lab, unit = LEG[base]
    arr, dates = ser.values.astype(float), ser.index
    i, t, v = len(arr) - 1, dates[-1], float(arr[-1])
    nom, dat = human_name(sec)
    plabel, pfull = price_for(sec)
    pser = upto(pfull, t) if pfull is not None else None
    were = "были" if (plabel or "").startswith("акции") else "был"
    u, years = since(arr, dates, i, False)
    st = era_status(u, years, dates, False, t)
    facts = [f"{lab} по {dat} - {q_ru(v, unit)} на закрытие {d_ru(t, t)}"]
    if st:
        facts.append(f"это {st}")
    lo = max(0, i - 250)
    j = lo + int(np.argmax(arr[lo:i + 1]))
    if arr[j] > 0 and v < arr[j]:
        facts.append(f"максимум за год - {q_ru(arr[j], unit)} {d_ru(dates[j], t)}; с тех пор {p_ru(v / arr[j] - 1)}")
    ch = [f"{w} {p_ru(v / arr[i - k] - 1)}" for k, w in ((1, "за день"), (5, "за неделю"), (20, "за месяц"))
          if i >= k and arr[i - k] > 0]
    if ch:
        facts.append("изменение: " + ", ".join(ch))
    intraday_chg = None
    now = intraday_now(sec, base, t)
    if now and v > 0:
        iv, iday, itime = now
        intraday_chg = iv / v - 1
        facts.append(f"утром {d_ru(iday, t)} к {str(itime)[:5]} МСК {lab} - {q_ru(iv, unit)} "
                     f"({p_ru(intraday_chg)} к закрытию {d_ru(t, t)})")
    if st and i >= 5 and v < arr[i - 5]:
        facts.append("позиция всё ещё снижается: дно не пройдено - «разворот» и «дно пройдено» не пиши")
    eps = episodes(-arr, i)
    cur = eps[-1] if eps and i - eps[-1]["end"] <= 40 else None
    start = cur["start"] if cur else i
    past = [e for e in eps if e is not cur and e["end"] < start and dates[e["top"]] >= ERA_START]
    after, r20s, marks = [], [], []
    for e in past[-6:]:
        td, tv = dates[e["top"]], float(arr[e["top"]])
        marks.append((td, tv))
        r20 = fwd(pser, td, 20) if pser is not None else None
        r60 = fwd(pser, td, 60) if pser is not None else None
        if r20 is not None:
            r20s.append(r20)
        tail = ([f"через месяц {plabel} {p_ru(r20)}"] if r20 is not None else []) + \
               ([f"через три месяца {p_ru(r60)}"] if r60 is not None else [])
        after.append(f"дно {d_ru(td, t)}: {q_ru(tv, unit)}" + (" - " + ", ".join(tail) if tail else ""))
    if len(r20s) >= 2:
        k = sum(r > 0 for r in r20s)
        after.append(f"итого после {len(r20s)} прошлых минимумов {plabel} через месяц {were} выше в {k} "
                     f"{plural(k, ('случае', 'случаях', 'случаях'))} из {len(r20s)}")
    trend = trend_lines(arr, dates, pser, t, lab, plabel, were)
    limits = [f"данные дневные, на закрытие торгов {d_ru(t, t)}; что было внутри дня, не видно",
              f"данные по {dat} начинаются в {dates[0].year} году", NO_FORECAST,
              "минимум позиции - не прогноз цены: «толпа ушла» не значит, что цена развернётся"]
    from signals.insights.expiry import expiry_note
    # у вечного фьючерса экспирации нет: оговорка «перед экспирацией позиции снижаются сами» к нему не относится
    if expiry_note(t) and "вечн" not in nom:
        limits.append(expiry_note(t))
    if 0 < len(r20s) < 4:
        limits.append(f"прошлых минимумов с известным продолжением всего {len(r20s)} - это история, "
                      f"а не закономерность: не обобщай")
    win = ser[ser.index >= t - pd.Timedelta(days=3 * 365)]
    chart = {"type": "line2", "title": f"{lab.capitalize()}, {nom}", "x": win.index, "y": win.values,
             "y_label": unit, "marks": [m for m in marks if m[0] >= win.index[0]] + [(t, v)]}
    if pser is not None:
        pw = pser[pser.index >= win.index[0]]
        chart.update({"x2": pw.index, "y2": pw.values, "y2_label": plabel})
    return {"kind": "positions", "spec": {"sec": sec, "leg": base + "_low"}, "as_of": t,
            "headline": f"{lab.capitalize()} по {dat} - {q_ru(v, unit)}" + (f", {st}" if st else ""),
            "facts": facts, "trend": trend, "after": after, "analogy": [], "price": price_lines(pser, t, plabel),
            "intraday_change": intraday_chg, "context": other_legs(P, sec, base, t), "limits": limits,
            "chart": chart,
            "chart_note": [f"на графике - {lab} по {dat} за три года (оранжевая линия) и {plabel} (серая); точками "
                           f"отмечены прошлые минимумы и текущее значение"] if plabel else [],
            "hashtag": HASHTAG["positions"]}


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
    era = past[past.index >= ERA_START]     # рекорды и аналогии — в рынке после 2022 года
    dv = d.values
    day_rec = det.day_record(dv, len(dv) - 1)
    facts = [f"{d_ru(t, t)} {nom}: {word(dv[-1])} {n_ru(abs(dv[-1]))} ₽ за день"
             + ("; это рекорд одного дня за всё время наших данных" if day_rec else "")]
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
    mtd_rec = len(era) >= 12 and ((mtd < 0 and mtd <= era.min()) or (mtd > 0 and mtd >= era.max()))
    if mtd_rec:
        line += "; это уже больше, чем в любой полный месяц за всю историю наблюдений - с 2022 года"
    facts.append(line)
    day_line = f"{word(dv[-1])} за один день - {n_ru(abs(dv[-1]))} ₽, рекорд за всё время наших данных"
    head = f"{nom.capitalize()}: " + (day_line if day_rec else line if mtd_rec else (reversal or rec or line))
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
    if len(era):
        mx, mn = era.idxmax(), era.idxmin()
        history.append(f"самый большой приток за месяц после 2022 года - {m_ru(mx, t)} ({n_ru(era.max())} ₽), "
                       f"самый большой отток - {m_ru(mn, t)} ({n_ru(abs(era.min()))} ₽)")
        if cat == "money_market" and "RUSFAR3M" in idx:
            # #2127: рекорд 210 млрд был при ставке ~21%, сейчас ~14% и +16 млрд — несравнимые периоды
            rf = idx["RUSFAR3M"].dropna()
            now_r, then_r = upto(rf, t), rf[rf.index <= mx]
            if len(now_r) and len(then_r):
                pc = lambda x: f"{x:.1f}".replace(".", ",") + "%"  # noqa: E731
                history.append(f"ставка RUSFAR на 3 месяца сейчас {pc(now_r.iloc[-1])}, в месяц самого большого "
                               f"притока ({m_ru(mx, t)}) была {pc(then_r.iloc[-1])}: при другой ставке суммы "
                               f"потоков несравнимы - сопоставляй только вместе со ставкой")
    # спот, как на графике сайта: у вечного фьючерса на золото цена только с июля 2023 — с ним
    # «что было после» прошлых оттоков выходило пустым
    spot = lambda code, alt: idx[code].dropna() if code in idx else perp.get(alt)  # noqa: E731
    rel = {"bonds": ("индекс гособлигаций", idx.get("RGBI")), "stocks": ("индекс Мосбиржи", idx.get("IMOEX")),
           "gold": ("золото", spot("GLDRUB_TOM", "GLDRUBF")), "yuan": ("юань", spot("CNYRUB_TOM", "CNYRUBF"))}.get(cat)
    price, after, analogy, weak, stalls = [], [], [], None, []
    if rel and rel[1] is not None:
        rl, rs = rel[0], upto(rel[1], t)
        ra = rs.values
        price.append(f"{rl} - {px_ru(ra[-1], rl)}; за неделю {p_ru(ra[-1] / ra[-6] - 1)}, за месяц "
                     f"{p_ru(ra[-1] / ra[-21] - 1)}, за три месяца {p_ru(ra[-1] / ra[-63] - 1)}; "
                     f"от максимума за год {p_ru(ra[-1] / ra[-250:].max() - 1)}")
        # ставка рядом с потоками (#2127, #2217): приток в юаневые фонды в конце 2024 остановила ставка RUSFAR в
        # юанях — 20% до ноября, в декабре около нуля и ниже; без ставки эпизод выглядит аналогией
        rate = {"money_market": ("RUSFAR3M", "ставка RUSFAR на 3 месяца"),
                "yuan": ("RUSFARCNY", "ставка RUSFAR в юанях")}.get(cat)
        rf_m = None
        if rate and rate[0] in idx:
            rf = upto(idx[rate[0]].dropna(), t)
            rf_m = rf.resample("ME").median()
            pc = lambda x: f"{x:.1f}".replace(".", ",") + "%"  # noqa: E731
            if len(rf) > 63:
                price.append(f"{rate[1]} сейчас {pc(rf.iloc[-20:].median())} (медиана за 20 торговых дней), три месяца "
                             f"назад {pc(rf.iloc[-83:-63].median())}")
        me = rs.resample("ME").last()
        avg12 = float(past.iloc[-12:].mean()) if len(past) >= 6 else 0.0
        pace = mtd * t.days_in_month / t.day      # месяц не закончился: темп, а не сумма
        stalls = stall_episodes(past, me, t) if avg12 > 0 and pace < 0.25 * avg12 else []
        for e in stalls[-3:]:
            m0, m1 = e["months"][0], e["months"][-1]
            span = mn_ru(m1, t) if m0 == m1 else (f"{det.MONTHS[m0.month - 1]} - {mn_ru(m1, t)}" if m0.year == m1.year
                                                  else f"{mn_ru(m0, t)} - {mn_ru(m1, t)}")
            note = ""
            if rf_m is not None:
                # до эпизода — максимум из медиан двух месяцев: в ноябре 2024 ставка уже падала (медиана 9%),
                # а держатели юаневых фондов до конца ноября получали ~20% (Вадим к #2217)
                r0 = rf_m[rf_m.index < m0].iloc[-2:].max() if (rf_m.index < m0).any() else None
                r1 = rf_m[rf_m.index <= m1].iloc[-1] if (rf_m.index <= m1).any() else None
                if r0 is not None and r1 is not None and abs(r1 - r0) >= 5:
                    note = (f"; {rate[1]} за это время {'упала' if r1 < r0 else 'выросла'} с {pc(r0)} до {pc(r1)} - "
                            f"поток тогда определяла ставка: если сейчас ставка стабильна, это не аналогия")
            after.append(f"похожий эпизод - приток остановился, {span}: за это время {'+' if e['flow'] > 0 else '-'}"
                         f"{n_ru(abs(e['flow']))} ₽, {rl} {p_ru(e['r_in'])}"
                         + (f"; за три месяца после {p_ru(e['r3'])}" if e["r3"] is not None else "") + note)
        if stalls:
            analogy.append(f"сейчас приток тоже {'остановился' if mtd <= 0 else 'почти остановился'}: средний приток за прошлый год - {n_ru(avg12)} ₽ в месяц, "
                           f"с начала {mname} - {word(mtd)} {n_ru(abs(mtd))} ₽; похожих эпизодов после 2022 года - {len(stalls)}")
        same = era[np.sign(era) == np.sign(mtd)] if mtd != 0 else era.iloc[0:0]
        big = same.reindex(same.abs().sort_values(ascending=False).index)[:3 - min(len(stalls), 2)]
        if len(big) and big.abs().max() < 0.3 * abs(mtd):
            weak = (f"прошлые месяцы с {word(mtd)}ом были намного меньше нынешнего (крупнейший - "
                    f"{n_ru(big.abs().max())} ₽): по масштабу это не аналогия"
                    + (", но режим «приток остановился» сравнивать можно" if stalls else ", вывод на них не строить"))
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
    if 0 < len(stalls) < 4:
        limits.append(f"похожих эпизодов всего {len(stalls)}: можно сказать, что было в тех случаях, но прямо "
                      f"назови, что их {len(stalls)} - это сопутствующий сигнал, а не закономерность")
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


def _sql(query: str, params: dict) -> pd.DataFrame:
    """Разовый запрос с параметрами (у data.read — только именованные выгрузки без параметров)."""
    from sqlalchemy import text
    from api.database import SessionLocal
    db = SessionLocal()
    try:
        r = db.execute(text(query), params)
        return pd.DataFrame(r.fetchall(), columns=list(r.keys()))
    finally:
        db.close()


def rub_ru(v) -> str:
    v = abs(float(v))
    return (f"{v / 1e9:.1f}".replace(".", ",") + " млрд ₽") if v >= 1e9 else f"{v / 1e6:.0f} млн ₽"


# ── карточка: сделки фондов за месяц ─────────────────────────────────────────────
# Посты канала 22.09 «Фокус смещается на сырьё» (Лукойл обошёл Сбербанк в портфеле фондов) и 25.09 «Кого фонды
# продают в убыток?». Движок находил «Сделки фондов за август» (detect_fund_trades), но карточки не было — завод их
# не писал (реплей 27.09). Цифры — те же, что на странице «Что покупают фонды»: её API, срез без задержки.
FUND_TRADES_API = os.environ.get("FUND_TRADES_API", "https://framedata.ru/api/fund-trades")
_SECTORS = """
    SELECT DISTINCT ON (sr.isin) sr.isin, sr.secid, s.title AS sector, c.title AS company
      FROM securities_ref sr
      JOIN brain_ticker_map m ON m.ticker = sr.secid
      JOIN brain_nodes c ON c.id = m.company_id
      LEFT JOIN brain_edges e ON e.src = m.company_id AND e.kind = 'в_секторе'
      LEFT JOIN brain_nodes s ON s.id = e.dst
     WHERE sr.isin = ANY(CAST(:isins AS text[]))"""


def _ft_get(path: str, **params) -> dict:
    import json
    import urllib.parse
    import urllib.request
    q = urllib.parse.urlencode({k: v for k, v in params.items() if v is not None})
    with urllib.request.urlopen(f"{FUND_TRADES_API}/{path}?{q}", timeout=180) as r:
        return json.load(r)


def fund_trades_card(as_of, month=None) -> dict:
    t = pd.Timestamp(as_of)
    mv = _ft_get("movers", period="1m", sort="amount", limit=8, scope="portfolio",
                 as_of=str((pd.Timestamp(month) + pd.offsets.MonthEnd(0)).date()) if month else None)
    m0 = pd.Timestamp(mv["resolved_month"])
    end, prev_end = (m0 + pd.offsets.MonthEnd(0)).date(), (m0 - pd.Timedelta(days=1)).date()
    mname = det.MONTHS[m0.month - 1]
    pf = _ft_get("portfolio", as_of=str(end))
    pp = _ft_get("portfolio", as_of=str(prev_end))
    rank = lambda p: sorted(p.get("holdings") or [], key=lambda h: -(h.get("value_rub") or 0))  # noqa: E731
    now, before = rank(pf), rank(pp)
    pos_before = {h.get("akey"): k for k, h in enumerate(before, 1)}
    isin_of = lambda h: h.get("isin") or h.get("akey")   # noqa: E731 — у строк покупок ISIN лежит в akey
    isins = [isin_of(h) for h in now[:6] + mv["top_accumulated"][:5] + mv["top_reduced"][:5] if isin_of(h)]
    sec = _sql(_SECTORS, {"isins": isins}) if isins else pd.DataFrame(columns=["isin", "secid", "sector", "company"])
    sector = {k: v for k, v in zip(sec["isin"], sec["sector"]) if v}   # sec.isin — метод DataFrame, не колонка
    secid = dict(zip(sec["isin"], sec["secid"]))
    company = {k: v for k, v in zip(sec["isin"], sec["company"]) if v}
    # «КЦ ИКС 5», «Татнфт 3ао» — биржевые сокращения; в пост — имя компании из мозга, у привилегированных — «-п»
    nm = lambda h: (company.get(isin_of(h), h["asset_name"]) +  # noqa: E731
                    ("-п" if company.get(isin_of(h)) and re.search(r"\bап\b|-п\b|прив", h["asset_name"]) else ""))
    total = pf.get("total_value_rub") or sum(h.get("value_rub") or 0 for h in now)
    buys = [f"{nm(i)}: купили на {rub_ru(i['total_delta_amount'])} (покупали {i['funds_buying']} фондов, "
            f"продавали {i['funds_selling']})" for i in mv["top_accumulated"][:5]]
    sells = [f"{nm(i)}: продали на {rub_ru(i['total_delta_amount'])} (продавали {i['funds_selling']} фондов, "
             f"покупали {i['funds_buying']})" for i in mv["top_reduced"][:5]]
    top = []
    for k, h in enumerate(now[:5], 1):
        was = pos_before.get(h.get("akey"))
        sh = f" ({sector[isin_of(h)]})" if isin_of(h) in sector else ""
        top.append(f"{k}. {nm(h)}{sh} - {rub_ru(h.get('value_rub') or 0)}, "
                   f"{p_ru((h.get('value_rub') or 0) / total, False) if total else ''} портфеля"
                   + (f"; месяцем раньше - {was}-е место" if was and was != k else "; месяцем раньше - то же место"
                      if was else "; месяцем раньше не было в портфеле"))
    focus = ["ТИП ПОСТА: СДЕЛКИ ФОНДОВ - что покупали и продавали фонды акций за месяц и что из этого следует; одна "
             "мысль, не список бумаг",
             f"находка: сделки фондов акций за {mname}: больше всего купили {nm(mv['top_accumulated'][0])} "
             f"(+{rub_ru(mv['top_accumulated'][0]['total_delta_amount'])}), больше всего продали "
             f"{nm(mv['top_reduced'][0])} (-{rub_ru(mv['top_reduced'][0]['total_delta_amount'])})"]
    leader_changed = now and before and now[0].get("akey") != before[0].get("akey")
    if leader_changed:
        # как давно прежний лидер держал первое место — по срезам назад, пока лидер не другой
        streak, d = 1, prev_end
        for _ in range(18):
            d = (pd.Timestamp(d).replace(day=1) - pd.Timedelta(days=1)).date()
            try:
                lead = rank(_ft_get("portfolio", as_of=str(d)))
            except Exception:  # noqa: BLE001 — нет среза: считаем по тому, что есть
                break
            if not lead or lead[0].get("akey") != before[0].get("akey"):
                break
            streak += 1
        span = f"{streak} {plural(streak, ('месяц', 'месяца', 'месяцев'))}"
        focus.append(f"смена лидера в портфеле фондов: на первом месте {nm(now[0])} "
                     f"({rub_ru(now[0].get('value_rub') or 0)}) - "
                     + ("впервые за все месяцы наших срезов" if streak >= 19 else f"впервые за {span}")
                     + f"; {nm(before[0])} был первым {span} подряд и опустился на второе место; «впервые» без срока "
                       f"не пиши - раньше лидер мог быть тем же")
    oil = [h for h in now[:4] if "нефт" in (sector.get(isin_of(h)) or "").lower()]
    if len(oil) >= 2:
        focus.append(f"в первой четвёрке портфеля {len(oil)} компании нефти и газа: " + ", ".join(nm(h) for h in oil))
    P, names, groups, to_stock, idx, stk, perp, usd = data()
    price = []
    for i in (mv["top_accumulated"][:2] + mv["top_reduced"][:2]):
        tk = secid.get(isin_of(i))
        if tk in stk.columns:
            ps = upto(stk[tk].dropna(), t)
            pm = ps[ps.index <= pd.Timestamp(end)]
            pb = ps[ps.index <= pd.Timestamp(prev_end)]
            if len(pm) and len(pb) and len(ps) > 60:
                price.append(f"{nm(i)}: за {mname} {p_ru(pm.iloc[-1] / pb.iloc[-1] - 1)}, за три месяца до "
                             f"{d_ru(ps.index[-1], t)} {p_ru(ps.iloc[-1] / ps.iloc[-61] - 1)}")
    return {"kind": "fund_trades", "spec": {"month": str(m0.date())}, "as_of": pd.Timestamp(end),
            "headline": (lambda h: h[:1].upper() + h[1:])(focus[1].replace("находка: ", "")),
            "focus": focus, "facts": ["больше всего купили: " + "; ".join(buys[:3]),
                                      "больше всего продали: " + "; ".join(sells[:3])] + buys[3:] + sells[3:],
            "history": [f"портфель фондов на конец {GEN[m0.month - 1]}: " + (f"{rub_ru(total)}, " if total else "")
                        + f"{pf.get('num_funds')} фондов акций"] + top,
            "price": price, "context": [], "after": [], "analogy": [], "trend": [],
            "limits": ["сделки фондов - из ежемесячных отчётов о составе (СЧА): сделки внутри месяца не видны, только "
                       "итог к концу месяца; сумма - оценка по изменению числа бумаг и их цене, сплиты учтены",
                       f"отчёты выходят с задержкой: это данные на конец {GEN[m0.month - 1]} - пиши "
                       f"«в {MONTHS_IN[m0.month - 1]}», не «сейчас»", "мотив фондов не утверждать: «фиксируют убыток», «ставят на нефть» - только "
                       "«похоже»; цены покупки фондов у нас нет", NO_FORECAST],
            "chart": {"type": "hbars", "title": f"Сделки фондов акций за {mname}, млрд ₽",
                      "labels": [nm(i) for i in mv["top_accumulated"][:5] + mv["top_reduced"][:5]],
                      "values": [round(i["total_delta_amount"] / 1e9, 2)
                                 for i in mv["top_accumulated"][:5] + mv["top_reduced"][:5]]},
            "chart_note": [f"на графике - пять самых крупных покупок и продаж фондов акций за {mname}, млрд ₽"],
            "hashtag": HASHTAG["fund_trades"]}


MONTHS_IN = ["январе", "феврале", "марте", "апреле", "мае", "июне", "июле", "августе", "сентябре", "октябре", "ноябре",
             "декабре"]


# ── карточка: макро-новость — как отреагировали наши данные ─────────────────────
# Новость важности 5 без компании («адские санкции», перемирие) Шаг А отбрасывал: у завода не было пути для макро
# (реплей 27.09: 13 таких новостей за две недели — ни одного поста). Здесь пост — не пересказ новости, а реакция наших
# данных: цена фьючерсов и 5-минутные позиции физлиц за два часа после новости и к концу дня, против обычного хода.
_CANDLES5 = """SELECT begin_time AS t, close FROM candles
                WHERE secid = :s AND type = :ty AND interval = 5 AND begin_time >= :a AND begin_time <= :b
                ORDER BY begin_time"""
_OI5 = """SELECT tradedate + tradetime AS t, pos_long + pos_short AS net, pos_long, pos_short, pos_long_num AS nl,
                 pos_short_num AS ns
            FROM open_interest WHERE interval = 5 AND clgroup = 'FIZ' AND sectype = :s
             AND tradedate BETWEEN :a AND :b ORDER BY tradedate, tradetime"""
MACRO_INSTR = {"IMOEXF": ("фьючерс на индекс Мосбиржи", "futures"), "USDRUBF": ("фьючерс на доллар", "futures"),
               "LKOH": ("акции Лукойла", "stock"), "ROSN": ("акции Роснефти", "stock"), "SBER": ("акции Сбербанка", "stock")}
MACRO_EXTRA = {"санкции": ("LKOH", "ROSN"), "нефть_газ": ("LKOH", "ROSN"), "геополитика": ("SBER",)}


def _window(df, e, col, hours=2):
    """Значение до новости, через hours часов торгов после неё и в конце её дня. До — последний снимок раньше новости
    (ночная новость — закрытие прошлого вечера, и разрыв на открытии тоже реакция)."""
    before, after = df[df.t < e], df[df.t >= e]
    if before.empty or after.empty:
        return None
    t0 = after.t.iloc[0]
    v0 = float(before[col].iloc[-1])
    in2 = after[after.t <= t0 + pd.Timedelta(hours=hours)]
    day = after[after.t.dt.normalize() == t0.normalize()]
    return {"t0": t0, "v0": v0, "v2": float(in2[col].iloc[-1]), "t2": in2.t.iloc[-1],
            "vd": float(day[col].iloc[-1]), "td": day.t.iloc[-1]}


def _end_label(w, t) -> str:
    """«к концу торгов 17 сентября», если день закончился, иначе «к 09:25»: пост посреди дня не должен звать утро
    концом торгов (холостой прогон 17.09 в 09:30)."""
    return f"к концу торгов {d_ru(w['td'], t)}" if w["td"].hour >= 23 else f"к {w['td']:%H:%M}"


def _typical(df, col, hours=2, pct=True):
    """Обычный ход за hours часов: медиана модуля изменения внутри дня за прошлые 60 торговых дней."""
    s = df.set_index("t")[col].resample("1h").last().dropna()
    ch = (s / s.shift(hours) - 1) if pct else (s - s.shift(hours))
    same_day = s.index.normalize() == s.index.to_series().shift(hours).dt.normalize()
    ch = ch[same_day].abs().dropna()
    return float(ch.median()) if len(ch) >= 50 else None


def macro_card(event, headline, theme, as_of) -> dict:
    e = pd.Timestamp(event)                      # время новости, МСК без пояса
    t = pd.Timestamp(as_of)
    a, b = e - pd.Timedelta(days=90), min(t, e.normalize() + pd.Timedelta(days=1, hours=23, minutes=59))
    facts, context, ratios, chart = [], [], [], None
    focus = ["ТИП ПОСТА: РЕАКЦИЯ НА МАКРО-НОВОСТЬ - не пересказ новости, а что в первые часы сделали цена и толпа "
             "(физлица) и насколько это сильнее обычного дня; саму новость - одной фразой",
             f"новость: {d_ru(e, t)} в {e:%H:%M} МСК - «{headline}»"]
    for sid in ("IMOEXF", "USDRUBF") + MACRO_EXTRA.get(theme, ()):
        name, ty = MACRO_INSTR[sid]
        df = _sql(_CANDLES5, {"s": sid, "ty": ty, "a": a, "b": b})
        if df.empty:
            continue
        df["t"] = pd.to_datetime(df.t)
        df["close"] = df.close.astype(float)
        w = _window(df, e, "close")
        if not w:
            continue
        typ = _typical(df[df.t < e], "close")
        r2, rd = w["v2"] / w["v0"] - 1, w["vd"] / w["v0"] - 1
        k = abs(r2) / typ if typ else None
        if k and sid == "IMOEXF":
            ratios.append(k)
            day = df[(df.t >= e - pd.Timedelta(hours=3)) & (df.t <= w["td"])]
            chart = {"type": "intraday", "title": f"Фьючерс на индекс Мосбиржи и позиции физлиц, {d_ru(e, t)}",
                     "x": day.t.values, "y": day.close.values, "y_label": "фьючерс на индекс", "event": e}
        line = (f"{name}: за два часа после новости {p_ru(r2)} (к {w['t2']:%H:%M}), {_end_label(w, t)} "
                f"{p_ru(rd)}" + (f"; обычный двухчасовой ход - {p_ru(typ, False)}, сейчас в "
                                 f"{f'{k:.1f}'.replace('.', ',')} раза больше" if k and k >= 1.5 else ""))
        facts.append(line)
        if sid == "IMOEXF":
            focus.append(f"цена: {line}")
    for sid, name in (("IMOEXF", "вечном фьючерсе на индекс Мосбиржи"), ("USDRUBF", "вечном фьючерсе на доллар")):
        oi = _sql(_OI5, {"s": sid, "a": a.date(), "b": b.date()})
        if oi.empty:
            continue
        oi["t"] = pd.to_datetime(oi.t)
        for c in ("net", "pos_long", "pos_short", "nl", "ns"):
            oi[c] = oi[c].astype(float)
        w = _window(oi, e, "net")
        if not w:
            continue
        typ = _typical(oi[oi.t < e], "net", pct=False)
        d2, dd = w["v2"] - w["v0"], w["vd"] - w["v0"]
        side = "чистый лонг" if w["v0"] >= 0 else "чистый шорт"
        base = abs(w["v0"]) or 1.0
        # число контрактов не называем (R02) — доля позиции и кратность против обычного
        line = (f"физлица в {name}: {side} за два часа после новости {p_ru(d2 / base * (1 if w['v0'] >= 0 else -1))}, "
                f"{_end_label(w, t).replace('к концу торгов', 'к концу дня')} "
                f"{p_ru(dd / base * (1 if w['v0'] >= 0 else -1))}"
                + (f"; обычно за два часа позиция меняется на {p_ru(typ / base, False)}, сейчас {x_ru(abs(d2) / typ)} "
                   f"сильнее" if typ and abs(d2) >= 1.5 * typ else ""))
        if typ and sid == "IMOEXF":
            ratios.append(abs(d2) / typ)
            if chart is not None:
                day = oi[(oi.t >= e - pd.Timedelta(hours=3)) & (oi.t <= w["td"])]
                chart.update({"x2": day.t.values, "y2": day.net.values, "y2_label": "чистая позиция физлиц"})
        ns = _window(oi, e, "ns")
        if ns and ns["v0"] > 0 and abs(ns["vd"] / ns["v0"] - 1) >= 0.03:
            line += f"; число физлиц в шорте к концу дня {p_ru(ns['vd'] / ns['v0'] - 1)}"
        context.append(line)
        if sid == "IMOEXF":
            focus.append(f"толпа: {line}")
    ix = _sql("SELECT trade_date, close FROM index_data WHERE secid = 'IMOEX' AND trade_date BETWEEN :a AND :b "
              "ORDER BY trade_date", {"a": (e - pd.Timedelta(days=10)).date(), "b": b.date()})
    if len(ix) >= 2:
        ix["close"] = ix.close.astype(float)
        day = ix[pd.to_datetime(ix.trade_date) >= e.normalize()]
        prev = ix[pd.to_datetime(ix.trade_date) < e.normalize()]
        if len(day) and len(prev):
            close = f"{day.close.iloc[0]:,.0f}".replace(",", " ")
            facts.append(f"индекс Мосбиржи за {d_ru(pd.Timestamp(day.trade_date.iloc[0]), t)}: "
                         f"{p_ru(day.close.iloc[0] / prev.close.iloc[-1] - 1)} (закрытие {close})")
    return {"kind": "macro", "spec": {"event": str(e), "theme": theme}, "as_of": t,
            "headline": f"Реакция наших данных на новость {d_ru(e, t)} в {e:%H:%M} МСК: «"
                        + (headline if len(headline) <= 110 else re.sub(r"\s+\S*$", "", headline[:110]) + "…") + "»",
            "focus": focus, "facts": facts, "context": context, "history": [], "price": [], "after": [], "analogy": [],
            "trend": [],
            "limits": ["совпадение по времени, а не доказанная причина: в эти часы выходили и другие новости - «на "
                       "фоне», «в первые часы после», но не «из-за»",
                       "позиции физлиц - 5-минутные снимки биржи по группе «физлица»; это не число людей и не деньги",
                       "пост - о реакции наших данных, а не пересказ новости: саму новость - одной фразой", NO_FORECAST],
            "chart": chart, "strength": max(ratios) if ratios else 0.0,
            "chart_note": ["на графике - фьючерс на индекс Мосбиржи (оранжевая линия) и чистая позиция физлиц (серая) по "
                           "5 минутам; вертикальная черта - время новости"] if chart else [],
            "hashtag": HASHTAG["macro"]}


def build_card(spec: dict, as_of) -> dict:
    kind = spec["kind"]
    if kind == "positions":
        if str(spec["leg"]).endswith("_low"):
            return positions_low_card(spec["sec"], spec["leg"][:-4], as_of)
        card = positions_card(spec["sec"], spec["leg"], as_of)
        return angle_card(card, spec["angle"]) if spec.get("angle") else card
    if kind == "funds":
        return funds_card(spec["cat"], as_of)
    if kind == "fund_trades":
        return fund_trades_card(as_of, spec.get("month"))
    if kind == "macro":
        return macro_card(spec["event"], spec["headline"], spec.get("theme", ""), as_of)
    return seasonality_card(spec["code"], as_of)


SECTIONS = (("ЦИФРЫ", "facts"), ("ТРЕНД - главное для поста", "trend"), ("ИСТОРИЯ РЯДА", "history"), ("ЧТО БЫЛО ПОСЛЕ ПРОШЛЫХ ЭПИЗОДОВ", "after"),
            ("АНАЛОГИЯ", "analogy"), ("ЦЕНА И ФОН", "price"), ("ДРУГИЕ СТОРОНЫ ПОЗИЦИИ", "context"),
            ("ГРАФИК К ПОСТУ", "chart_note"), ("ОГРАНИЧЕНИЯ - чего не утверждать", "limits"))


def focus_lines(card: dict) -> list:
    """Итерация 2: одна история вместо всей карточки — находка, одна аналогия и опора для
    вывода. В первой итерации писатель пересказывал карточку целиком (2,66 числа на 100
    знаков против 0,48 у автора), и автор был интереснее в 13 парах из 15."""
    if card.get("focus"):       # новые виды карточек (сделки фондов, макро) задают «главное» сами
        return list(card["focus"])
    k = card["kind"]
    after, analogy, facts = card.get("after") or [], card.get("analogy") or [], card.get("facts") or []
    weak = any("не аналогия по масштабу" in x for x in card.get("limits") or [])
    if k == "positions" and card.get("trend"):
        trend = card["trend"]
        story = next((x for x in trend if x.startswith("так же было")), None)
        stat = next((x for x in trend if x.startswith("итого")), None)
        return ([f"находка: {card['headline']}", f"тренд: {trend[0]}"] + [f"одна история: {story}"] * bool(story)
                + [f"опора для вывода: {stat}"] * bool(stat))
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
        out.append("КОНТЕКСТ - что было в мире до поста; повод, а не доказанная причина: «на фоне», «в те же дни» - можно, «из-за» - нельзя:")
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
    if ch["type"] == "intraday":
        # макро: цена фьючерса и чистая позиция физлиц по 5 минутам, черта — время новости
        ax.plot(ch["x"], ch["y"], color=ACC, lw=2.0)
        ax.set_ylabel(ch.get("y_label", ""))
        ax.axvline(pd.Timestamp(ch["event"]), color=INK, lw=1.0, ls="--")
        ax.annotate("новость", (pd.Timestamp(ch["event"]), float(np.nanmax(ch["y"]))), textcoords="offset points",
                    xytext=(4, -10), fontsize=9, color=INK)
        if "x2" in ch:
            ax2 = ax.twinx()
            ax2.plot(ch["x2"], ch["y2"], color=GREY, lw=1.3)
            ax2.set_ylabel(ch.get("y2_label", ""), color=GREY)
            ax2.spines["top"].set_visible(False)
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    elif ch["type"] == "hbars":
        # сделки фондов: покупки (зелёные) и продажи (оранжевые) месяца, млрд ₽
        labels, vals = ch["labels"][::-1], ch["values"][::-1]
        ax.barh(range(len(vals)), vals, color=[ACC if v < 0 else "#2f7d6d" for v in vals], alpha=0.85)
        ax.set_yticks(range(len(vals)), labels)
        ax.axvline(0, color=INK, lw=0.8)
        ax.set_xlabel("млрд ₽")
        for k, v in enumerate(vals):
            ax.annotate(f"{v:+.1f}".replace(".", ","), (v, k), textcoords="offset points",
                        xytext=(6 if v >= 0 else -6, -3), ha="left" if v >= 0 else "right", fontsize=9, color=INK)
    elif ch["type"] == "bars":
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
    src = ("Данные: раскрытие управляющих компаний" if card["kind"] == "funds" else
           "Данные: отчёты фондов о составе (СЧА), страница «Что покупают фонды»" if card["kind"] == "fund_trades" else
           "Данные: Мосбиржа")
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
