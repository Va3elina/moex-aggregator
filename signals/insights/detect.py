#!/usr/bin/env python3
"""Движок инсайтов, шаг 2: находки по нашим рядам — на каждый день (версия 3).

Вход — выгрузки с прода в data/: дневные позиции физлиц по фьючерсам (ноги и
число участников), NAV фондов, широта рынка, капитализация и ВВП, цены, события
фондов из мозга. Выход — insights.json: {date, family, instrument, type, score,
title, facts}. Семейства и типы совпадают с разметкой каталога.

Версия 2 (после первой проверки на истории, охват FRAME был 28%):
  • НОГИ ПОЗИЦИИ. Канал пишет «рекордные покупки», «рекордный шорт» — это объём
    длинной или короткой стороны, а не нетто. Шорт физлиц по индексу 12.08.2026
    был рекордом с 2012 года, а нетто — ничем не примечательно;
  • «РЯДОМ С РЕКОРДОМ» держится несколько дней: автор пишет не в день экстремума
    (Баффетт — минимум 19.07, пост 28.07);
  • ПОТОКИ ФОНДОВ — по скользящим пяти дням, а не по календарным неделям: пост
    24.06 про «бегство из облигаций» опережал пятничную метку недели;
  • СЕЗОННОСТЬ — окно на 20–40 торговых дней вперёд, а не календарный месяц;
  • ЦЕНА ИНДЕКСА — минимумы «с такого-то года», серии недель, просадка от пика.

Версия 3 (охват FRAME был 67%, топ-10 — 44%):
  • ВСЕЛЕННАЯ. Вечные и мини-контракты сведены к своему инструменту (IMOEXF → MIX,
    USDRUBF → Si). Зарубежные индексы и экзотика сырья весят 0,3: Nikkei, пшеница и
    сахар занимали топ дня, а канал о них не пишет;
  • РЕЗКОЕ ИЗМЕНЕНИЕ — за день или два, по всем ногам, включая число участников
    («число шортистов выросло в разы за пару дней»), без дней экспирации: в них
    квартальная нога проседает сама собой;
  • МИНИМУМЫ НОГ И ЧИСТАЯ ПОЗИЦИЯ — «впервые за полтора года физлица продают»;
  • СВОДКИ ПО РЫНКУ: рекорд шорта или покупки на падении сразу во многих бумагах —
    канал пишет «шортят даже гигантов», а не про одну бумагу;
  • СЕЗОННАЯ КРИВАЯ года без тренда: окно сезонного дна или пика и сильнейшие
    сезонные месяцы («валюта идёт по расписанию» — дно доллара ~23.06, пик ~22.03);
  • ЦЕНА ДОЛЛАРА и длительность снижения: «10 недель падения» у автора — это десятая
    неделя от максимума, а не десять красных недель подряд;
  • ШИРОТА в середине диапазона: пересечение 50% и уровни «с такой-то даты»;
  • РЕЙТИНГ: разнообразие семейств в топе дня и свежесть рекорда, который обновляется.

    python3 detect.py --since 2025-08-25 --until 2026-08-31
"""
import argparse
import datetime as dtm
import json
import os
from collections import Counter, defaultdict

import numpy as np
import pandas as pd

from api.services.fund_reorg import correct_flows  # поправки на реорганизации фондов — те же, что у сайта
from signals.insights import data

HERE = os.path.dirname(os.path.abspath(__file__))
D = os.path.join(HERE, "data")

WEIGHT = {"MX": 1.4, "IMOEXF": 1.3, "MM": 1.0, "RI": 1.1, "RM": 0.8,
          "Si": 1.4, "USDRUBF": 1.3, "UM": 0.8, "CR": 1.3, "CNYRUBF": 1.2, "Eu": 1.0, "EURRUBF": 0.9,
          "BR": 1.0, "BM": 0.8, "GD": 1.0, "GLDRUBF": 0.9, "GL": 0.8, "NG": 0.8, "SV": 0.8,
          "RB": 1.1, "RGBIF": 1.0,
          "SR": 1.2, "SBERF": 1.1, "GZ": 1.1, "GAZPF": 1.0, "LK": 0.9, "RN": 0.9, "VB": 0.9}
STOCK_WEIGHT = 0.7     # фьючерсы на акции: канал разбирает и малые бумаги, когда там рекорд
OTHER_WEIGHT = 0.3     # зарубежные индексы, экзотика сырья и валют — канал о них не пишет
# Рыночные ряды редки и канал пишет о них часто — им множитель при ранжировании.
FAMILY_MULT = {"широта": 1.3, "баффетт": 1.3, "сезонность": 1.2, "фонды": 1.2,
               "сделки_фондов": 1.0, "позиции": 1.0, "цена": 1.0}
CODE = {"MX": "MIX", "IMOEXF": "MIX", "MM": "MIX", "RI": "RI", "RM": "RI",
        "Si": "Si", "USDRUBF": "Si", "UM": "Si", "CR": "CNY", "CNYRUBF": "CNY", "Eu": "Eu", "EURRUBF": "Eu",
        "GD": "GOLD", "GLDRUBF": "GOLD", "GL": "GOLD", "BR": "BR", "BM": "BR", "RB": "RGBI", "RGBIF": "RGBI"}
FX = ["Si", "Eu", "CR"]
MONTHS = ["январь", "февраль", "март", "апрель", "май", "июнь", "июль", "август",
          "сентябрь", "октябрь", "ноябрь", "декабрь"]
MIN_PART = 50
DIVERSITY = 0.9        # k-я находка одного семейства в дне весит DIVERSITY ** k
# Нога → подпись и глаголы в нужном роде: «покупки выросли», «шорт вырос», «число выросло».
LEGS = {"long": ("покупки физлиц (длинная сторона)", "выросли", "сократились", "растут"),
        "short": ("шорт физлиц (короткая сторона)", "вырос", "сократился", "растёт"),
        "ns": ("число физлиц в шорте", "выросло", "сократилось", "растёт"),
        "nl": ("число физлиц в лонге", "выросло", "сократилось", "растёт")}


def ru(d) -> str:
    return pd.Timestamp(d).strftime("%d.%m.%Y")


def num(v) -> str:
    return f"{v:,.0f}".replace(",", " ")


class Out:
    def __init__(self, since, until):
        self.best, self.since, self.until = {}, pd.Timestamp(since), pd.Timestamp(until)
        self.ctx = {}   # из какого ряда находка (фьючерс, категория фондов) — карточке нужен сам ряд

    def add(self, date, family, instrument, typ, score, title, **facts):
        facts = {**self.ctx, **facts}
        date = pd.Timestamp(date).normalize()
        if not (self.since <= date <= self.until):
            return
        # одна лучшая находка на (семейство, инструмент, тип, нога) в день
        key = (date, family, instrument, typ, facts.get("leg"))
        if key not in self.best or self.best[key]["score"] < score:
            self.best[key] = {"date": date.date().isoformat(), "family": family,
                              "instrument": instrument, "type": typ,
                              "score": round(float(score), 2), "title": title, "facts": facts}

    @property
    def items(self):
        return list(self.best.values())


def since_date(arr, dates, i, higher=True):
    """Когда в последний раз было не хуже текущего. None — впервые за всю историю."""
    prev = arr[:i]
    idx = np.flatnonzero(prev >= arr[i] if higher else prev <= arr[i])
    return dates[idx[-1]] if len(idx) else None


def record(out, t, family, code, arr, dates, i, higher, w, what, unit="", persist=True,
           fmt=num, word=None, **facts) -> float:
    """Рекорд «с даты» + «рядом с рекордом» (держится несколько дней после).
    Возвращает, сколько лет не было такого значения."""
    u = since_date(arr, dates, i, higher)
    years = (t - (u if u is not None else dates[0])).days / 365
    v = arr[i]
    wd = word or ("максимум" if higher else "минимум")
    if years >= 1:
        when = f"за всё время наших данных (с {dates[0].year})" if u is None else f"с {ru(u)}"
        out.add(t, family, code, "рекорд_или_экстремум", (4 + min(years, 10) * 0.6 + (1 if u is None else 0)) * w,
                f"{what} {fmt(v)}{unit} — {wd} {when}", since=None if u is None else str(u.date()),
                years=round(years, 1), value=float(v), hi=bool(higher), **facts)
        return years
    if years >= 0.3:
        when = "за всё время наших данных" if u is None else f"с {ru(u)}"
        out.add(t, family, code, "уровень_к_истории", (2.5 + years * 3) * w,
                f"{what} {fmt(v)}{unit} — {wd} {when}", since=None if u is None else str(u.date()),
                years=round(years, 2), value=float(v), hi=bool(higher), **facts)
    if persist:
        # рядом с многолетним экстремумом: в пределах 3% от максимума за 3 года
        win = arr[max(0, i - 750):i + 1]
        ext = win.max() if higher else win.min()
        if ext != 0 and ((higher and v >= 0.97 * ext) or (not higher and v <= 1.03 * ext)) and i >= 250:
            out.add(t, family, code, "уровень_к_истории", 4 * w,
                    f"{what} держится у многолетнего {'максимума' if higher else 'минимума'}",
                    value=float(v), hi=bool(higher), **facts)
    return years


def weekly_streak(series: pd.Series, t) -> int:
    """Сколько ЗАКРЫТЫХ недель подряд ряд рос. Незакрытая неделя не считается: иначе
    один слабый вторник обнулял серию, которую автор видит по пятничным закрытиям."""
    wk = series.loc[:t].resample("W-FRI").last().dropna()
    if len(wk) and wk.index[-1] > pd.Timestamp(t):
        wk = wk.iloc[:-1]
    d = np.sign(wk.diff().values[1:])
    n = 0
    for s in d[::-1]:
        if s <= 0:
            break
        n += 1
    return n


def after_expiry(dates, n) -> set:
    """День экспирации квартальных фьючерсов (третий четверг марта, июня, сентября,
    декабря) и n−1 торговых дней после: квартальная нога в них проседает сама собой —
    часть физлиц не перекладывается в следующий контракт."""
    marks = set()
    for y in range(dates[0].year, dates[-1].year + 1):
        for m in (3, 6, 9, 12):
            first = pd.Timestamp(y, m, 1)
            j = dates.searchsorted(first + pd.Timedelta(days=(3 - first.weekday()) % 7 + 14))
            marks.update(dates[j:j + n])
    return marks


# ── позиции физлиц ─────────────────────────────────────────────────────────────
def load_positions():
    oi = data.read("oi_daily", parse_dates=["tradedate"])
    oi = oi[oi.clgroup == "FIZ"].copy()
    oi["net"] = oi.pos_long + oi.pos_short           # шорт хранится со знаком минус
    oi["long"] = oi.pos_long
    oi["short"] = -oi.pos_short
    oi["nl"] = oi.pos_long_num
    oi["ns"] = oi.pos_short_num
    oi["npart"] = oi.pos_long_num.fillna(0) + oi.pos_short_num.fillna(0)
    return {c: _after_last_gap(oi.pivot_table(index="tradedate", columns="sectype", values=c,
                                              aggfunc="last").sort_index())
            for c in ("net", "long", "short", "nl", "ns", "npart")}


GAP_DAYS = 60


def _after_last_gap(df):
    """Ряд — только после последнего перерыва торгов дольше GAP_DAYS. У мини-фьючерса на Полюс (PX)
    дыра 19.11.2024 → 04.09.2025 (перезапуск контракта): склеенный ряд дал «исторический максимум за всё
    время, с 2019 года» (#2417), а на сайте данные только с сентября 2025 (Вадим 18.09)."""
    for c in df.columns:
        s = df[c].dropna()
        if len(s) < 2:
            continue
        gaps = s.index.to_series().diff() > pd.Timedelta(days=GAP_DAYS)
        if gaps.any():
            df.loc[df.index < gaps[gaps].index[-1], c] = np.nan
    return df


def instruments():
    ins = data.read("instruments")
    fut = ins[ins.type == "futures"].drop_duplicates("sectype").set_index("sectype")
    stk = ins[ins.type == "stock"].drop_duplicates("name").set_index("name")["sec_id"]
    names, groups = fut["name"].to_dict(), fut["group"].to_dict()
    # «Газпром (вечн)» → та же акция, что «Газпром»
    to_stock = {s: stk.get(str(n).replace(" (вечн)", "").replace(" (мини)", ""))
                for s, n in names.items() if groups.get(s) == "Акции"}
    return names, groups, to_stock


def prices():
    idx = data.read("index_data", parse_dates=["trade_date"])
    idx = idx.pivot_table(index="trade_date", columns="secid", values="close").sort_index()
    stk = data.read("candles_stocks", parse_dates=["d"])
    stk = stk.pivot_table(index="d", columns="secid", values="close").sort_index()
    perp = data.read("candles_perp", parse_dates=["d"])
    perp = perp.pivot_table(index="d", columns="secid", values="close").sort_index()
    return idx, stk, {c: perp[c].dropna() for c in perp.columns}


def usd_series(idx, perp) -> pd.Series:
    """Доллар: IMOEX/RTSI — без дыр с 2012 года (РТС — это IMOEX в долларах), в
    масштабе вечного USDRUBF, чтобы в заголовке стояли рубли."""
    proxy = (idx["IMOEX"] / idx["RTSI"]).dropna()
    if "USDRUBF" in perp:
        both = pd.concat([proxy, perp["USDRUBF"]], axis=1, join="inner").dropna()
        if len(both) > 50:
            return proxy * float((both.iloc[:, 1] / both.iloc[:, 0]).median())
    return proxy


def detect_positions(out, P, names, groups, to_stock, idx, stk, perp):
    net, part = P["net"], P["npart"]
    price_by_code = {"MIX": idx.get("IMOEX"), "RI": idx.get("RTSI"), "Si": usd_series(idx, perp),
                     "RGBI": idx.get("RGBI"), "CNY": perp.get("CNYRUBF"), "Eu": perp.get("EURRUBF"),
                     "GOLD": perp.get("GLDRUBF")}
    lo = out.since - pd.Timedelta(days=10)
    liquid = [s for s in net.columns
              if part[s].loc[part.index >= out.since - pd.Timedelta(days=365)].median() >= MIN_PART
              and groups.get(s) in ("Акции", "Валюта", "Индексы", "Сырьё")]
    leg_rec = defaultdict(lambda: defaultdict(list))   # день → нога → бумаги с рекордом за год и больше
    div = defaultdict(lambda: defaultdict(list))       # день → «покупают на падении» → бумаги
    for s in liquid:
        out.ctx = {"sec": s}
        g = groups.get(s)
        w = WEIGHT.get(s, STOCK_WEIGHT if g == "Акции" else OTHER_WEIGHT)
        # Рекорд — редкость, вес бумаги её только смягчает: рекорд за всю историю по VK
        # канал разбирал наравне с индексом, а полный вес бумаги прятал его на 45-е место.
        wr = 0.75 + 0.25 * w if g == "Акции" else w
        name = names.get(s, s)
        code = CODE.get(s) or to_stock.get(s) or s
        ticker = to_stock.get(s) or name
        quarterly = not (len(s) >= 5 and s.endswith("F"))     # у вечных контрактов экспирации нет
        x = net[s].dropna()
        if len(x) < 300:
            continue
        p = price_by_code.get(code)
        if p is None and to_stock.get(s) in stk.columns:
            p = stk[to_stock[s]].dropna()
        for leg, (label, up, down, grows) in LEGS.items():
            ser = P[leg][s].dropna()
            if len(ser) < 300:
                continue
            arr, dates = ser.values.astype(float), ser.index
            dif = np.abs(np.diff(arr))
            exp3 = after_expiry(dates, 3) if quarterly else set()
            exp12 = after_expiry(dates, 12) if quarterly else set()
            lw = 1.0 if leg in ("long", "short") else 0.8
            for t in dates[(dates >= lo) & (dates <= out.until)]:
                i = dates.get_loc(t)
                if i < 260:
                    continue
                yrs = record(out, t, "позиции", code, arr, dates, i, True, wr * lw, f"{name}: {label}", leg=leg)
                if yrs >= 1 and g == "Акции" and leg in ("long", "short"):
                    leg_rec[t][leg].append(ticker)
                # минимум ноги: «впервые за полтора года физлица продают валюту» — кроме
                # недель после экспирации, когда квартальная нога проседает сама собой
                if leg in ("long", "nl") and t not in exp12:
                    record(out, t, "позиции", code, arr, dates, i, False, wr * lw * 0.8, f"{name}: {label}",
                           persist=False, leg=leg + "_low")
                # резкое изменение за день или два: «шорты сократились на треть за день»
                atr = np.median(dif[i - 21:i - 1])
                best = None
                for n in (1, 2):
                    if atr <= 0 or any(dates[i - k] in exp3 for k in range(n)):
                        continue
                    last = arr[i] - arr[i - n]
                    ratio, rel = abs(last) / (atr * np.sqrt(n)), abs(last) / max(abs(arr[i - n]), 1)
                    # медиана дневного хода ниже среднего: порог 4 и 8% — иначе 17 тысяч
                    # «резких» находок за год, по сорок с лишним в день
                    if ratio >= 4 and rel >= 0.08:
                        sc = (2.5 + min(ratio, 8) * 0.35 + min(rel, 0.5) * 5) * w * lw
                        if best is None or sc > best[0]:
                            best = (sc, n, last, rel, ratio)
                if best:
                    sc, n, last, rel, ratio = best
                    out.add(t, "позиции", code, "резкое_изменение", sc,
                            f"{name}: {label} {up if last > 0 else down} на {rel:.0%} за "
                            f"{'день' if n == 1 else 'два дня'} — в {ratio:.1f} раза сильнее обычного", leg=leg)
                if leg in ("long", "short"):
                    k = weekly_streak(ser, t)
                    if k >= 2:
                        out.add(t, "позиции", code, "серия", min(2.5 + k * 0.8, 7) * w,
                                f"{name}: {label} {grows} {k}-ю неделю подряд", leg=leg, weeks=k)
        # нетто: рекорд чистой позиции, резкий сдвиг за день, разворот, расхождение с ценой
        arr, dates = x.values.astype(float), x.index
        neg = -arr
        exp3 = after_expiry(dates, 3) if quarterly else set()
        for t in dates[(dates >= lo) & (dates <= out.until)]:
            i = dates.get_loc(t)
            if i < 260:
                continue
            v, v1, v5 = arr[i], arr[i - 1], arr[i - 5]
            med = np.median(np.abs(arr[max(0, i - 250):i])) or 1
            if v > 0:
                record(out, t, "позиции", code, arr, dates, i, True, wr * 0.9, f"{name}: чистый лонг физлиц",
                       persist=False, leg="net")
            elif v < 0:
                record(out, t, "позиции", code, neg, dates, i, True, wr * 0.9, f"{name}: чистый шорт физлиц",
                       persist=False, leg="net")
            d1, base = v - v1, max(abs(v1), med)
            usual = np.median(np.abs(np.diff(arr[i - 21:i])))
            if t not in exp3 and usual > 0 and abs(d1) >= 0.3 * base and abs(d1) >= 4 * usual:
                out.add(t, "позиции", code, "резкое_изменение", (4 + min(abs(d1) / base, 1) * 4) * w,
                        f"{name}: чистая позиция физлиц за день {num(v1)} → {num(v)} ({d1 / base:+.0%})", leg="net")
            side = lambda z: "лонг" if z > 0 else "шорт"  # noqa: E731
            if np.sign(v) != np.sign(v5) and min(abs(v), abs(v5)) >= 0.2 * med:
                out.add(t, "позиции", code, "разворот", 6 * w,
                        f"{name}: физлица развернулись из {side(v5)}а в {side(v)} за неделю")
            if p is not None and t in p.index:
                j = p.index.get_loc(t)
                if j >= 5:
                    rp = p.iloc[j] / p.iloc[j - 5] - 1
                    rx = (v - v5) / max(abs(v5), med)
                    if (rp <= -0.02 and rx >= 0.05) or (rp >= 0.02 and rx <= -0.05):
                        what = "покупают на падении" if rp < 0 else "продают на росте"
                        out.add(t, "позиции", code, "расхождение_цены_и_позиций", (4.5 + min(abs(rp), 0.1) * 20) * w,
                                f"{name}: физлица {what} — цена {rp:+.1%} за неделю, позиция {rx:+.0%}")
                        if g == "Акции":
                            div[t][what].append(ticker)

    out.ctx = {}
    # сводки по рынку: канал пишет «рекордный шорт сразу в пяти бумагах», «толпа
    # усредняет по десяти фьючерсам» — одна находка на весь рынок
    for t, by_leg in leg_rec.items():
        for leg, tick in by_leg.items():
            tick = list(dict.fromkeys(tick))
            if len(tick) >= 3:
                out.add(t, "позиции", "STOCKS_ALL", "рекорд_или_экстремум", 6 + min(len(tick), 10) * 0.4,
                        f"{'Шорт' if leg == 'short' else 'Покупки'} физлиц на максимумах за год и больше сразу "
                        f"в {len(tick)} бумагах: {', '.join(tick[:6])}", leg=leg, tickers=tick)
    for t, by_what in div.items():
        for what, tick in by_what.items():
            tick = list(dict.fromkeys(tick))
            if len(tick) >= 4:
                out.add(t, "позиции", "STOCKS_ALL", "расхождение_цены_и_позиций", 5.5 + min(len(tick), 10) * 0.3,
                        f"Физлица {what} сразу в {len(tick)} бумагах: {', '.join(tick[:6])}", leg=what, tickers=tick)

    # вся валюта сразу
    for t in net.index[(net.index >= lo) & (net.index <= out.until)]:
        moves = []
        for s in FX:
            sh = P["short"][s].dropna()
            if t not in sh.index:
                break
            i = sh.index.get_loc(t)
            d5 = sh.diff(5)
            hist = d5.iloc[max(0, i - 250):i].dropna()
            if len(hist) < 50 or hist.std() == 0:
                break
            moves.append((d5.iloc[i] - hist.mean()) / hist.std())
        if len(moves) == len(FX) and (all(m >= 1.0 for m in moves) or all(m <= -1.0 for m in moves)):
            verb = "наращивают шорт" if moves[0] > 0 else "сокращают шорт"
            out.add(t, "позиции", "FX_ALL", "резкое_изменение", 7,
                    f"Физлица синхронно {verb} по доллару, евро и юаню за неделю")
        grow = [weekly_streak(P["short"][s].dropna(), t) for s in FX]
        if min(grow) >= 2:
            out.add(t, "позиции", "FX_ALL", "серия", 6.5,
                    f"Шорт физлиц растёт по всей валюте {min(grow)}-ю неделю подряд", weeks=min(grow))

    # шорт растёт сразу во многих бумагах
    stock_fut = [s for s in liquid if groups.get(s) == "Акции"]
    for t in net.index[(net.index >= lo) & (net.index <= out.until)]:
        hit = []
        for s in stock_fut:
            sh = P["short"][s].dropna()
            if t not in sh.index:
                continue
            i = sh.index.get_loc(t)
            if i < 20:
                continue
            a = sh.values
            atr = np.abs(np.diff(a[i - 15:i])).mean()
            if atr > 0 and (a[i] - a[i - 5]) / (atr * np.sqrt(5)) >= 2:
                hit.append(to_stock.get(s) or names.get(s, s))
        hit = list(dict.fromkeys(hit))
        if len(hit) >= 4:
            out.add(t, "позиции", "STOCKS_ALL", "резкое_изменение", 5 + min(len(hit), 10) * 0.3,
                    f"Шорт физлиц за неделю заметно вырос сразу в {len(hit)} бумагах: {', '.join(hit[:6])}",
                    tickers=hit)


# ── потоки в фонды ──────────────────────────────────────────────────────────────
def detect_funds(out):
    f = data.read("funds")
    fd = data.read("fund_data", parse_dates=["trade_date"])
    fd = fd.merge(f[["fund_id", "category", "ticker"]], on="fund_id").sort_values(["fund_id", "trade_date"])
    g = fd.groupby("fund_id")
    prev_nav, prev_pay = g.nav.shift(1), g.pay.shift(1)
    fd["flow"] = (fd.nav - prev_nav) - prev_nav * (fd.pay - prev_pay) / prev_pay
    fd = correct_flows(fd)      # слияния и ликвидации фондов — как на графике сайта
    daily = fd.dropna(subset=["flow"]).pivot_table(index="trade_date", columns="category", values="flow",
                                                   aggfunc="sum").fillna(0)
    daily["all"] = daily.sum(axis=1)
    label = {"bonds": "фонды облигаций", "stocks": "фонды акций", "money_market": "фонды денежного рынка",
             "gold": "фонды золота", "yuan": "юаневые фонды", "all": "все фонды"}
    bn = lambda v: f"{abs(v) / 1e9:.1f} млрд ₽"  # noqa: E731
    for c in daily.columns:
        out.ctx = {"cat": c}
        # первый день оттока после серии притоков (и наоборот): «бегство» видно раньше,
        # чем его покажет даже пятидневная сумма
        dd = daily[c]
        dv = dd.values
        for t in dd.index[(dd.index >= out.since) & (dd.index <= out.until)]:
            i = dd.index.get_loc(t)
            prev = dv[max(0, i - 10):i]
            if len(prev) == 10 and dv[i] != 0 and np.all(np.sign(prev) == -np.sign(dv[i])) \
                    and abs(dv[i]) >= 0.5 * np.abs(prev).mean():
                word = "отток" if dv[i] < 0 else "приток"
                out.add(t, "фонды", f"funds:{c}", "разворот", 6.5,
                        f"{label[c].capitalize()}: первый день — {word} {bn(dv[i])} после двух недель в обратную сторону",
                        leg="1d")
        # 20 дней — «отток идёт с начала июля»: пятидневка такой волны не видит
        for n in (5, 20):
            s = daily[c].rolling(n).sum().dropna()
            arr, dates = s.values, s.index
            for t in dates[(dates >= out.since) & (dates <= out.until)]:
                i = dates.get_loc(t)
                if i < 120:
                    continue
                v = arr[i]
                word = "приток" if v > 0 else "отток"
                record(out, t, "фонды", f"funds:{c}", arr, dates, i, v > 0, 1.2,
                       f"{label[c].capitalize()}: {word} за {n} дней", persist=False, fmt=bn, word="рекорд",
                       leg=f"{n}d")
        s5 = daily[c].rolling(5).sum().dropna()
        mtd = daily[c].groupby(daily.index.to_period("M")).cumsum()
        mo = daily[c].resample("ME").sum()
        arr, dates = s5.values, s5.index
        for t in dates[(dates >= out.since) & (dates <= out.until)]:
            i = dates.get_loc(t)
            if i < 120:
                continue
            v = arr[i]
            word = "приток" if v > 0 else "отток"
            hist = arr[max(0, i - 250):i]
            if hist.std() > 0 and abs((v - hist.mean()) / hist.std()) >= 2.2:
                out.add(t, "фонды", f"funds:{c}", "резкое_изменение", 5.5,
                        f"{label[c].capitalize()}: необычный {word} за 5 дней — {bn(v)}")
            prev = arr[max(0, i - 20):i]
            if len(prev) == 20 and np.all(np.sign(prev) == -np.sign(v)) and abs(v) >= 0.3 * np.abs(prev).mean():
                out.add(t, "фонды", f"funds:{c}", "разворот", 6,
                        f"{label[c].capitalize()}: {word} после месяца в обратную сторону — {bn(v)} за 5 дней")
            # месяц с начала: рекорд оттока/притока среди месяцев
            m = mtd.loc[t] if t in mtd.index else None
            past = mo[mo.index < t.to_period("M").start_time]
            if m is not None and len(past) >= 12:
                if (m < 0 and m <= past.min()) or (m > 0 and m >= past.max()):
                    out.add(t, "фонды", f"funds:{c}", "рекорд_или_экстремум", 7.5,
                            f"{label[c].capitalize()}: {'отток' if m < 0 else 'приток'} с начала месяца {bn(m)} — рекорд за всё время наших данных",
                            leg="mtd", value=float(m), hi=bool(m > 0))
                signs = np.sign(past.values)
                n = 0
                for sg in signs[::-1]:
                    if sg != signs[-1]:
                        break
                    n += 1
                if n >= 3 and np.sign(m) == -signs[-1] and abs(m) >= 0.3 * abs(past.values[-3:]).mean():
                    out.add(t, "фонды", f"funds:{c}", "разворот", 6.5,
                            f"{label[c].capitalize()}: месяц идёт в {'отток' if m < 0 else 'приток'} после {n} месяцев подряд наоборот",
                            leg="mo")
                if n >= 3 and np.sign(m) == signs[-1]:
                    out.add(t, "фонды", f"funds:{c}", "серия", min(3 + n * 0.25, 7),
                            f"{label[c].capitalize()}: {'приток' if m > 0 else 'отток'} {n + 1}-й месяц подряд", months=int(n + 1))


# ── широта, Баффетт, сезонность ─────────────────────────────────────────────────
def detect_breadth(out):
    b = data.read("breadth", parse_dates=["trade_date"])
    b = b[b.universe == "imoex"]
    for period in (200, 100, 50, 20):
        s = b[b.ema_period == period].set_index("trade_date")["percent_above"].sort_index()
        arr, dates = s.values, s.index
        for t in dates[(dates >= out.since) & (dates <= out.until)]:
            i = dates.get_loc(t)
            v = arr[i]
            if v <= 15 or v >= 85:
                u = since_date(arr, dates, i, higher=v >= 85)
                years = (t - (u if u is not None else dates[0])).days / 365
                mood = "почти все акции выше" if v >= 85 else "почти все акции ниже"
                out.add(t, "широта", "STOCKS_ALL", "широта", 4.5 + min(years, 5) * 0.6 + abs(v - 50) / 25,
                        f"Широта рынка: {v:.0f}% акций индекса выше {period}-дневной средней — {mood}"
                        + (f"; такого не было с {ru(u)}" if u is not None and years >= 0.5 else ""),
                        percent=round(float(v), 1), period=period)
            # середина диапазона: «самая низкая широта с марта» и пересечение половины
            if i >= 250:
                record(out, t, "широта", "STOCKS_ALL", arr, dates, i, v >= 50, 1.0,
                       f"Широта рынка: доля акций выше {period}-дневной средней", unit="%", persist=False,
                       period=period, leg=f"lvl{period}")
            if period in (200, 100) and i >= 1 and (arr[i] >= 50) != (arr[i - 1] >= 50):
                side = arr[i] >= 50
                prev = np.flatnonzero((arr[:i - 1] >= 50) == side)
                u = dates[prev[-1]] if len(prev) else None
                years = (t - (u if u is not None else dates[0])).days / 365
                if years >= 0.25:
                    out.add(t, "широта", "STOCKS_ALL", "широта", 5 + min(years, 3),
                            f"Широта рынка: {'больше' if side else 'меньше'} половины акций выше {period}-дневной "
                            f"средней — впервые с {ru(u) if u is not None else 'начала данных'}",
                            period=period, leg=f"x{period}")


def detect_buffett(out):
    m = data.read("macro", parse_dates=["period_date"])
    cap = m[m.indicator == "MARKET_CAP_TOTAL"].set_index("period_date")["value"].sort_index()
    gdp = m[m.indicator == "GDP_QUARTERLY"].set_index("period_date")["value"].sort_index()
    ratio = (cap / gdp.rolling(4).sum().reindex(cap.index, method="ffill")).dropna() * 100
    ratio = ratio[ratio.index >= "2015-01-01"]
    arr, dates = ratio.values, ratio.index
    for t in dates[(dates >= out.since) & (dates <= out.until)]:
        i = dates.get_loc(t)
        record(out, t, "баффетт", "STOCKS_ALL", arr, dates, i, False, 1.2,
               "Индикатор Баффетта", unit="%", persist=True, fmt=lambda z: f"{z:.1f}")
        pct = (arr[:i + 1] <= arr[i]).mean()
        if pct <= 0.03:
            out.add(t, "баффетт", "STOCKS_ALL", "оценка_рынка", 6.5,
                    f"Индикатор Баффетта {arr[i]:.0f}% — рынок в самых дешёвых 3% своей истории с 2015 года",
                    ratio=round(float(arr[i]), 1))


def detect_seasonality(out, idx, usd):
    series = {"MIX": ("индекс Мосбиржи", idx["IMOEX"].dropna()), "Si": ("доллар", usd)}
    days = pd.date_range(out.since, out.until, freq="B")
    for code, (label, s) in series.items():
        for t in days:
            for n in (20, 40):
                rets = []
                for y in range(1, 11):
                    start = t - pd.DateOffset(years=y)
                    j = s.index.searchsorted(start)
                    if j + n >= len(s) or s.index[j] > start + pd.Timedelta(days=7):
                        continue
                    rets.append(s.iloc[j + n] / s.iloc[j] - 1)
                if len(rets) < 8:
                    continue
                share, med = np.mean(np.array(rets) > 0), float(np.median(rets))
                if (share >= 0.75 or share <= 0.25) and abs(med) >= 0.01:
                    verb = "рос" if share >= 0.75 else "падал"
                    k = int(round(share * len(rets))) if share >= 0.75 else int(round((1 - share) * len(rets)))
                    out.add(t, "сезонность", code, "сезонность", 4 + abs(share - 0.5) * 6 + min(abs(med), 0.08) * 20,
                            f"Сезонность: в ближайшие ~{n} торговых дней {label} исторически {verb} в {k} из {len(rets)} лет (медиана {med:+.1%})",
                            horizon=n, share=round(float(share), 2), median=round(med, 4))


def seasonal_curve(s: pd.Series, year: int):
    """Средний путь года без тренда по десяти прошлым годам: 366 значений, [0] — 1 января.
    Тренд снят у каждого года отдельно (год начинается и кончается в нуле), иначе
    девальвация 2014–2024 превращает любое окно в «доллар обычно растёт»."""
    curves = []
    for y in range(year - 10, year):
        p = s[s.index.year == y]
        if len(p) < 200:
            continue
        lr = np.log(p / p.iloc[0]).values
        doy = p.index.dayofyear.values
        det = lr - (doy / 365.0) * lr[-1]
        curves.append(pd.Series(det, index=doy).groupby(level=0).last()
                      .reindex(range(1, 367)).interpolate().bfill().ffill())
    if len(curves) < 8:
        return None
    c = pd.concat(curves, axis=1).mean(axis=1).values
    ext = np.concatenate([c[-30:], c, c[:30]])            # декабрь переходит в январь
    return pd.Series(ext).rolling(15, center=True, min_periods=1).mean().values[30:-30]


def turning_points(c, half=45, min_move=0.025) -> list:
    """Сезонные дно и пик: экстремум в окне ±half дней, до следующего противоположного
    экстремума кривая проходит не меньше min_move."""
    n = len(c)
    pts = []
    for d in range(n):
        win = c[[(d + k) % n for k in range(-half, half + 1)]]
        if c[d] == win.min():
            pts.append((d, "дно"))
        elif c[d] == win.max():
            pts.append((d, "пик"))
    res = []
    for j, (d, kind) in enumerate(pts):
        nd, nk = pts[(j + 1) % len(pts)]
        if nk != kind and abs(c[nd] - c[d]) >= min_move:
            res.append((d, kind, nd, float(c[nd] - c[d])))
    return res


def detect_seasonal_curve(out, idx, usd):
    series = {"Si": ("доллар", usd), "MIX": ("индекс Мосбиржи", idx["IMOEX"].dropna())}
    days = pd.date_range(out.since, out.until, freq="B")
    on = lambda d: (pd.Timestamp(2001, 1, 1) + pd.Timedelta(days=int(d))).strftime("%d.%m")  # noqa: E731
    for code, (label, s) in series.items():
        cache = {}
        for t in days:
            if t.year not in cache:
                c = seasonal_curve(s, t.year)
                if c is None:
                    cache[t.year] = None
                else:
                    fwd = np.array([c[(k + 30) % 366] - c[k] for k in range(366)])
                    cache[t.year] = (c, turning_points(c), fwd, float(np.quantile(np.abs(fwd), 0.85)))
            if cache[t.year] is None:
                continue
            c, tps, fwd, q85 = cache[t.year]
            d0 = t.dayofyear - 1
            ytd = s[(s.index.year == t.year) & (s.index <= t)]
            corr = 0.0
            if len(ytd) >= 60:
                corr = float(np.corrcoef(np.log(ytd / ytd.iloc[0]).values, c[ytd.index.dayofyear.values - 1])[0, 1])
            cs = f"; путь этого года повторяет сезонный (корреляция {corr:.2f})" if corr >= 0.7 else ""
            for d, kind, nd, move in tps:
                delta = (d - d0 + 183) % 366 - 183
                if -10 <= delta <= 20:
                    out.add(t, "сезонность", code, "сезонность", 4.5 + min(abs(move), 0.1) * 30 + max(corr, 0) * 1.5,
                            f"Сезонность: {label} в окне сезонного {'дна' if kind == 'дно' else 'пика'} "
                            f"(исторически ~{on(d)}); дальше в среднем {'рост' if move > 0 else 'снижение'} "
                            f"на {abs(move):.0%} до ~{on(nd)}{cs}", anchor=f"{kind}{d}", leg="curve", corr=round(corr, 2))
            f30 = fwd[d0]
            if abs(f30) >= 0.02 and abs(f30) >= q85:
                out.add(t, "сезонность", code, "сезонность", 4 + min(abs(f30), 0.06) * 50 + max(corr, 0),
                        f"Сезонность: впереди один из сильнейших сезонных месяцев — {label} за 30 дней в среднем "
                        f"{'растёт' if f30 > 0 else 'снижается'} на {abs(f30):.1%}{cs}", anchor="seg", leg="curve",
                        corr=round(corr, 2))


# ── сделки фондов и цена ────────────────────────────────────────────────────────
def detect_fund_trades(out):
    ev = data.read("brain_events")
    ev["ts"] = pd.to_datetime(ev.ts, utc=True).dt.tz_localize(None)
    ev = ev[ev.kind == "fund_event"]
    for snap, grp in ev.groupby(ev.ts.dt.to_period("M")):
        start = grp.ts.max()
        for day in pd.date_range(start, start + pd.Timedelta(days=35), freq="D"):
            out.add(day, "сделки_фондов", None, "сделки_фондов", 4 + min(len(grp), 60) / 30,
                    f"Сделки фондов за {MONTHS[snap.month - 1]}: {len(grp)} заметных изменений позиций",
                    events=len(grp))


def detect_prices(out, idx, stk, perp, usd):
    lows = defaultdict(list)
    px = lambda z: f"{z:,.2f}".replace(",", " ")  # noqa: E731
    for sec in stk.columns:
        s = stk[sec].dropna()
        r = s.pct_change()
        arr, dates = s.values, s.index
        for t in dates[(dates >= out.since) & (dates <= out.until)]:
            i = dates.get_loc(t)
            v = r.iloc[i]
            if abs(v) >= 0.07:
                out.add(t, "цена", sec, "движение_цены", 3 + min(abs(v), 0.4) * 20, f"{sec}: {v:+.0%} за день")
            if i >= 250:
                if record(out, t, "цена", sec, arr, dates, i, False, 0.9, f"{sec}: цена", persist=False, fmt=px) >= 1:
                    lows[t].append(sec)
    for t, secs in lows.items():
        if len(secs) >= 5:
            out.add(t, "цена", "STOCKS_ALL", "рекорд_или_экстремум", 5 + min(len(secs), 30) * 0.1,
                    f"{len(secs)} акций на минимумах за год и больше: {', '.join(secs[:8])}", tickers=secs)
    rub = lambda z: f"{z:.2f} ₽"  # noqa: E731
    for s, code, label, w, mv, fmt in ((idx["IMOEX"].dropna(), "MIX", "Индекс Мосбиржи", 1.3, 0.03, num),
                                       (idx["RTSI"].dropna(), "RI", "Индекс РТС", 1.0, 0.03, num),
                                       (usd, "Si", "Доллар", 1.3, 0.025, rub),
                                       (perp.get("GLDRUBF"), "GOLD", "Золото", 0.8, 0.04, num)):
        if s is None or len(s) < 300:
            continue
        arr, dates = s.values, s.index
        # сколько недель от максимума (минимума) за полгода: «10 недель падения» у автора
        # — это десятая неделя от пика, а не десять красных недель подряд. Окно полгода,
        # не год: от годового пика доллар «снижался» 88 недель подряд, хотя уже рос.
        hi, lo_ = s.rolling(126, min_periods=60).max(), s.rolling(126, min_periods=60).min()
        last_hi = pd.Series(s.index.where(s.values >= hi.values * 0.999), index=s.index).ffill()
        last_lo = pd.Series(s.index.where(s.values <= lo_.values * 1.001), index=s.index).ffill()
        dur_dn = ((s.index - pd.DatetimeIndex(last_hi.values)) / pd.Timedelta(days=1)).values
        dur_up = ((s.index - pd.DatetimeIndex(last_lo.values)) / pd.Timedelta(days=1)).values
        for t in dates[(dates >= out.since) & (dates <= out.until)]:
            i = dates.get_loc(t)
            if i < 260:
                continue
            record(out, t, "цена", code, arr, dates, i, False, w, label, persist=True, fmt=fmt, leg="low")
            record(out, t, "цена", code, arr, dates, i, True, w, label, persist=False, fmt=fmt, leg="high")
            r1 = arr[i] / arr[i - 1] - 1
            if code == "Si" and abs(r1) >= 0.015:
                out.add(t, "цена", code, "движение_цены", (4 + min(abs(r1), 0.06) * 60) * w,
                        f"{label} {r1:+.1%} за день ({rub(arr[i])})", leg="1d")
            r5 = arr[i] / arr[i - 5] - 1
            if abs(r5) >= mv:
                out.add(t, "цена", code, "движение_цены", (4 + min(abs(r5), 0.12) * 30) * w,
                        f"{label} {r5:+.1%} за неделю", leg="5d")
            peak, trough = arr[max(0, i - 250):i + 1].max(), arr[max(0, i - 250):i + 1].min()
            dd, ru_ = arr[i] / peak - 1, arr[i] / trough - 1
            if dd <= -0.15:
                out.add(t, "цена", code, "уровень_к_истории", (4 + min(-dd, 0.4) * 10) * w,
                        f"{label}: {dd:.0%} от максимума за год", leg="dd")
            if ru_ >= (0.08 if code == "Si" else 0.15):
                out.add(t, "цена", code, "уровень_к_истории", (4 + min(ru_, 0.4) * 10) * w,
                        f"{label}: {ru_:+.0%} от минимума за год", leg="ru")
            wk = s.loc[:t].resample("W-FRI").last().dropna().diff().values[1:]
            for sign, word in ((-1, "падает"), (1, "растёт")):
                n = 0
                for d in wk[::-1]:
                    if np.sign(d) != sign:
                        break
                    n += 1
                if n >= 3:
                    out.add(t, "цена", code, "серия", min(3 + n * 0.6, 8) * w,
                            f"{label} {word} {n}-ю неделю подряд", weeks=n, leg="wk")
            for dur, ref, down in ((dur_dn, last_hi, True), (dur_up, last_lo, False)):
                if np.isnan(dur[i]):
                    continue
                n = int(dur[i] // 7)
                chg = arr[i] / s.loc[ref.iloc[i]] - 1
                # больше полугода — уже тренд, а не «N-я неделя падения»: без потолка индекс
                # 80 недель подряд «снижался от максимума» и стоял первым в топе каждый день
                if 6 <= n <= 26 and (chg <= -0.05 if down else chg >= 0.05):
                    u = since_date(dur, dates, i, True)
                    years = (t - (u if u is not None else dates[0])).days / 365
                    what = "снижения" if down else "роста"
                    out.add(t, "цена", code, "серия", (4 + min(n, 20) * 0.15 + min(years, 10) * 0.4) * w,
                            f"{label}: {n}-я неделя {what} от {'максимума' if down else 'минимума'} "
                            f"{ru(ref.iloc[i])} ({chg:+.0%})"
                            + (f"; такого долгого {what} не было с {ru(u)}" if u is not None and years >= 1 else ""),
                            weeks=n, leg="dur_dn" if down else "dur_up")


def rank(items: list) -> list:
    """Баллы → рейтинг дня: новизна, вес ряда, разнообразие семейств.

    Новизна — главное: рекорд в день рекорда весит больше, чем «держится у рекорда»
    на пятый день. Находка считается продолжающейся, если та же (семейство,
    инструмент, тип, нога) была в один из трёх предыдущих календарных дней — чтобы
    выходные не обнуляли серию. Рекорд, который продолжает обновляться (отток с
    начала месяца растёт каждый день), — развивающаяся история, а не повтор."""
    def nkey(x):
        # Длина серии, дата рекорда и сезонная точка — часть «новости»: третья неделя
        # роста — новое событие после второй, а не повтор.
        f = x.get("facts") or {}
        return (x["family"], x["instrument"], x["type"], f.get("leg"), f.get("weeks"), f.get("months"),
                f.get("since"), f.get("anchor"), f.get("period"))

    seen = defaultdict(dict)
    for x in items:
        seen[nkey(x)][x["date"]] = (x.get("facts") or {}).get("value")
    for x in items:
        k, f = nkey(x), x.get("facts") or {}
        d = dtm.date.fromisoformat(x["date"])
        run, cur, first_prev = 0, d, None
        while run < 10:
            prev = [cur - dtm.timedelta(days=b) for b in (1, 2, 3)]
            hit = next((p for p in prev if p.isoformat() in seen[k]), None)
            if not hit:
                break
            first_prev = first_prev or hit
            run, cur = run + 1, hit
        novelty = 1.35 if run == 0 else (1.0 if run <= 2 else 0.7)
        if x["family"] == "сделки_фондов":      # месячная сводка свежа около двух недель
            novelty = 1.2 if run <= 10 else 0.7
        if x["type"] == "рекорд_или_экстремум" and run > 2 and f.get("value") is not None:
            pv = seen[k].get(first_prev.isoformat())
            v = f["value"]
            if pv is not None and (v > pv + 0.005 * abs(pv) if f.get("hi", True) else v < pv - 0.005 * abs(pv)):
                novelty = 1.0
        mult = FAMILY_MULT.get(x["family"], 1.0)
        if x["family"] == "цена" and x["instrument"] not in ("MIX", "RI", "Si", "GOLD", "STOCKS_ALL") \
                and x["type"] in ("рекорд_или_экстремум", "уровень_к_истории"):
            mult *= 0.5                         # минимумы отдельных акций: в плохой день их сотня
        x["score"] = round(x["score"] * novelty * mult, 2)
        x["run_days"] = run
    grouped = defaultdict(list)
    for x in items:
        # не больше трёх на (семейство, инструмент): общий лимит на инструмент выдавливал
        # сезонность доллара ценой и позициями по тому же доллару
        grouped[(x["date"], x["family"], x["instrument"] or x["family"])].append(x)
    out = []
    for g in grouped.values():
        g.sort(key=lambda z: -z["score"])
        out += g[:3]
    # разнообразие: без него топ дня — десять рекордов ног по разным фьючерсам, а
    # широта, фонды и сезонность, о которых канал пишет чаще всего, — за его краем
    by_day = defaultdict(list)
    for x in out:
        by_day[x["date"]].append(x)
    for g in by_day.values():
        g.sort(key=lambda z: -z["score"])
        cnt = Counter()
        for x in g:
            x["score"] = round(x["score"] * DIVERSITY ** cnt[x["family"]], 2)
            cnt[x["family"]] += 1
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default="2025-08-25")
    ap.add_argument("--until", default="2026-08-31")
    a = ap.parse_args()
    out = Out(a.since, a.until)
    P = load_positions()
    names, groups, to_stock = instruments()
    idx, stk, perp = prices()
    usd = usd_series(idx, perp)
    detect_positions(out, P, names, groups, to_stock, idx, stk, perp)
    detect_funds(out)
    out.ctx = {}
    detect_breadth(out)
    detect_buffett(out)
    detect_seasonality(out, idx, usd)
    detect_seasonal_curve(out, idx, usd)
    detect_fund_trades(out)
    detect_prices(out, idx, stk, perp, usd)
    items = rank(out.items)
    json.dump(items, open(os.path.join(HERE, "insights.json"), "w", encoding="utf-8"),
              ensure_ascii=False, indent=0, default=str)
    days = Counter(x["date"] for x in items)
    print(f"находок {len(items)}; по семействам {Counter(x['family'] for x in items)}")
    print(f"по типам {Counter(x['type'] for x in items).most_common()}")
    print(f"дней {len(days)}, находок в день медиана {sorted(days.values())[len(days) // 2]}")


if __name__ == "__main__":
    main()
