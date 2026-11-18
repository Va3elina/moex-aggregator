"""Картинки к сигналам «Денег в фондах» и сезонности (Вадим 18.11.2026): график сайта с разметкой — движок
frame-chart-render, как у рекордов позиций (signals/record_card.py).

  • рекорд потока (funds_flow_notify) — столбики притоков-оттоков среза и индекс категории, столбик рекорда в рамке
    или кружке, прежний рекорд — кружок с месяцем и годом;
  • серия (funds_flow_streak) — рамка вокруг столбиков серии и сумма за серию;
  • сезонность (season_notify) — годовой график сезонности, рамка на период сигнала.

Период графика — самый короткий из периодов страницы, где сигнал ЧИТАЕТСЯ (Вадим 18.11: «надо вглядываться»):
столбик сигнала не тоньше нескольких пикселей и не теряется рядом с самыми большими столбиками. Прежний рекорд, если
он в такой период не попал, не рисуем — окно рекорда («за всё время») говорит подпись. Таймфрейм не меняем: месячная
сумма смешала бы рекордную неделю с соседними и могла бы сменить знак.

Любой сбой → None, бот шлёт текст без картинки.
"""
from __future__ import annotations

import datetime as dt
import os

import requests

from signals.record_card import MS, render

SITE = "https://framedata.ru"
UA = {"User-Agent": "frame-signal-card"}
# периоды графика притоков-оттоков на таймфрейме (FundsMoneyPage FLOW_MIN_PERIODS) → подпись и длина в днях
PERIODS = {"1d": (("1М", "1m", 31), ("1Г", "1y", 365), ("3Г", "3y", 1095), ("Всё", "all", None)),
           "1w": (("1Г", "1y", 365), ("3Г", "3y", 1095), ("Всё", "all", None)),
           "1m": (("1Г", "1y", 365), ("3Г", "3y", 1095), ("Всё", "all", None))}
TF_LABEL = {"1d": "День", "1w": "Неделя", "1m": "Месяц"}
GENITIVE = {"money_market": "денежного рынка", "stocks": "акций", "bonds": "облигаций", "gold": "золота",
            "yuan": "юаня"}
INDEX = {"money_market": "RUSFAR3M", "stocks": "IMOEX", "bonds": "RGBITR", "gold": "GLDRUB_TOM", "yuan": "CNYRUB_TOM"}
COLOR = {"in": "--funds-flow-positive", "out": "--funds-flow-negative"}
MAX_BARS = 160          # больше — столбик уже палочка шириной в пару пикселей
MIN_SHARE = 0.15        # столбик сигнала не ниже 15% самого большого в кадре — иначе теряется
# «21 неделя оттока подряд»: именительный падеж
UNIT = {"1d": ("рабочий день", "рабочих дня", "рабочих дней"), "1w": ("неделя", "недели", "недель"),
        "1m": ("месяц", "месяца", "месяцев")}
UNIT_GEN = {"1d": ("рабочего дня", "рабочих дней"), "1w": ("недели", "недель"), "1m": ("месяца", "месяцев")}


def plural(n: int, forms: tuple) -> str:
    if n % 10 == 1 and n % 100 != 11:
        return forms[0]
    if len(forms) == 3 and 2 <= n % 10 <= 4 and not 12 <= n % 100 <= 14:
        return forms[1]
    return forms[-1]


def money(v: float) -> str:
    """−5,3 млрд / +12 млрд / −285 млрд — как подписи на канале."""
    a = abs(v)
    s = f"{a:.1f}" if a < 10 else f"{a:.0f}"
    return f"{'−' if v < 0 else '+'}{s.replace('.', ',')} млрд"


def month(d: str) -> str:
    return f"{MS[int(d[5:7]) - 1]} {d[:4]}"


def _view(bars: list, tf: str, need_from: str | None, sig: tuple[int, int], value: float | None):
    """Самый короткий период страницы, где сигнал читается: столбики сигнала (вся серия) в кадре, столбиков не больше
    MAX_BARS, столбик сигнала (value) не теряется рядом с самыми большими. Из читаемых — первый, где виден и прежний
    рекорд (кадр начинается не позже need_from); такого нет — просто первый читаемый. → (подпись, код, первый столбик)."""
    end = dt.date.fromisoformat(bars[-1]["period_end"])
    opts = []
    for label, code, days in PERIODS[tf]:
        cut = None if days is None else (end - dt.timedelta(days=days)).isoformat()
        start = 0 if cut is None else next((i for i, b in enumerate(bars) if b["period_end"] >= cut), 0)
        if start > sig[0]:
            continue
        view = bars[start:]
        top = max(abs(b["flow"]) for b in view) or 1.0
        readable = len(view) <= MAX_BARS and (value is None or abs(value) >= MIN_SHARE * top)
        covers = need_from is None or bars[start]["period_end"] <= need_from
        opts.append(((label, code, start), readable, covers))
    if not opts:
        return PERIODS[tf][-1][0], PERIODS[tf][-1][1], 0
    for pick in ([o for o in opts if o[1] and o[2]], [o for o in opts if o[1]], opts):
        if pick:
            return pick[0][0]
    return opts[0][0]


def _index(cat: str, since: str, until: str) -> list:
    """Линия индекса категории за кадр гистограммы (у сайта — тот же /api/funds/chart)."""
    d = requests.get(f"{SITE}/api/funds/chart", params={"category": cat, "period": "all"}, timeout=30, headers=UA).json()
    return [p for p in (d.get("index") or {}).get("data") or [] if since <= p["date"] <= until]


def _flows_card(x: dict, bars: list, start: int, label: str, code: str, signal: dict) -> dict:
    view = bars[start:]
    cat = x["cat"]
    name = x["name"].split(" · ")
    sub = name[1] if len(name) > 1 and name[1] != "все фонды" else None
    return {
        "kind": "flows",
        "flows": {"category": cat, "timeframe": x["tf"], "period": code, "rolling": None, "flows": view},
        "flowTitle": f"Чистые притоки и оттоки из фондов {GENITIVE.get(cat, '')} (млрд ₽)",
        "index": _index(cat, view[0]["period_start"] or view[0]["period_end"], view[-1]["period_end"]) or None,
        "indexLabel": INDEX.get(cat),
        "meta": {"title": "Деньги в фондах",
                 "details": [" · ".join([name[0]] + ([sub] if sub else [])), "Притоки-Оттоки", label, TF_LABEL[x["tf"]]]},
        "signal": signal,
    }


def flow_record(x: dict, bars: list) -> dict | None:
    """Рекорд последнего столбика (funds_flow_notify.records): x — рекорд (cat, name, tf, side, window, flow),
    bars — все столбики среза на этом таймфрейме (ff.slot_flows)."""
    if len(bars) < 2:
        return None
    sgn = 1 if x["side"] == "in" else -1
    v = bars[-1]["flow"]
    n = len(bars)
    prev = next((i for i in range(n - 2, -1, -1) if sgn * bars[i]["flow"] >= sgn * v), None)
    label, code, start = _view(bars, x["tf"], bars[prev]["period_end"] if prev is not None else bars[0]["period_end"],
                               (n - 1, n - 1), v)
    word = "приток" if x["side"] == "in" else "отток"
    sig = {"headline": f"рекордный {word} {x['window']}", "colorVar": COLOR[x["side"]],
           "dir": "high" if sgn > 0 else "low", "hl": [n - 1 - start, n - 1 - start], "amount": money(v)}
    if prev is not None and prev >= start:
        sig["prev"] = prev - start
        sig["prevLabel"] = month(bars[prev]["period_end"])
    return _flows_card(x, bars, start, label, code, sig)


def flow_streak(x: dict, bars: list) -> dict | None:
    """Серия или разворот (funds_flow_streak.events): x — событие (i, n, kind, side, tf, run_sum), bars — закрытые
    столбики среза, на которых оно найдено."""
    i, k = x["i"], x["n"]
    if x["kind"] == "break":                             # первый столбик другого знака после серии — он в рамке
        lo, word = i - k, "приток" if x["side"] == "in" else "отток"
        opp = "оттока" if x["side"] == "in" else "притока"
        head = f"{word} после {k} {plural(k, UNIT_GEN[x['tf']])} {opp}"
        amount, side = money(bars[i]["flow"]), x["side"]
    else:
        lo = i - k + 1
        head = f"{k} {plural(k, UNIT[x['tf']])} {'притока' if x['side'] == 'in' else 'оттока'} подряд"
        amount, side = money(x.get("run_sum", sum(b["flow"] for b in bars[lo:i + 1]))), x["side"]
    bars = bars[:i + 1]
    # серия читается, если влезла целиком; высоту отдельного столбика не требуем — смысл в длине серии
    label, code, start = _view(bars, x["tf"], None, (lo, i), None)
    first = i if x["kind"] == "break" else lo
    sig = {"headline": head, "colorVar": COLOR[side], "dir": "high" if side == "in" else "low",
           "hl": [first - start, i - start], "amount": amount}
    return _flows_card(x, bars, start, label, code, sig)


def season(p: dict, start: dt.date) -> dict | None:
    """Сезонность (season_notify): p — сигнал (code, name, dir, days, hits, years), start — день входа."""
    y = requests.get(f"{SITE}/api/seasonality/yearly", params={"secid": p["code"]}, timeout=30, headers=UA).json()
    if not y.get("average"):
        return None
    mx = max(1, (y.get("max_trading_days") or 252) - 1)
    cur = [c for c in y.get("current") or [] if c["date"] <= start.isoformat()]
    td = cur[-1]["td"] if cur else round((start.timetuple().tm_yday / 365) * mx)
    a = td / mx
    b = a + p["days"] * 252 / 365 / mx
    rng = (y.get("years_range") or "").replace("-", "–")
    word = "рост" if p["dir"] == "up" else "падение"
    return {"kind": "season", "yearly": y,
            "meta": {"title": f"Сезонность по {p['name']}", "ticker": p["code"],
                     "details": ["Годовая"] + ([rng] if rng else [])},
            "signal": {"headline": f"{word} в {p['hits']} из {p['years']} лет",
                       "colorVar": "--accent" if p["dir"] == "up" else "--funds-flow-negative",
                       "from": a, "to": b, "dir": p["dir"]}}


def picture(kind: str, *args) -> bytes | None:
    """PNG к сигналу или None: kind — flow_record | flow_streak | season, args — как у сборщика."""
    try:
        card = {"flow_record": flow_record, "flow_streak": flow_streak, "season": season}[kind](*args)
    except Exception as e:  # noqa: BLE001 — без картинки лучше, чем без сигнала
        print(f"[funds_card] {kind}: данные графика: {type(e).__name__}: {e}")
        return None
    return render(card) if card else None


def dry(png: bytes | None, name: str) -> None:
    """Прогон без отправки (--dry-run): картинка в папку CARD_DIR, если задана, — посмотреть глазами."""
    d = os.environ.get("CARD_DIR")
    if png and d:
        os.makedirs(d, exist_ok=True)
        with open(os.path.join(d, name + ".png"), "wb") as fh:
            fh.write(png)
        print(f"  картинка: {os.path.join(d, name + '.png')}")
    elif d:
        print("  картинки нет")
