"""Серии притоков и оттоков «Денег в фондах» → @framesignalbot: «Отток идёт 6 месяцев подряд» (Вадим 10.11.2026).

Столбцы и срезы те же, что у рекордов потоков (signals/funds_flow_notify.py): график притоков-оттоков сайта
(api/services/fund_flows, с поправками FLOW_REORG_CORRECTIONS), категория целиком и каждая подкатегория. Порога по
величине нет (Вадим 10.11: «без порога СЧА»): серия — столбцы одного знака подряд, нулевой столбец её рвёт.

Только закрытые столбцы: неделя и месяц — когда у среза уже есть данные следующего слота (данные фондов приходят с
лагом 1–2 дня), день — все, кроме последнего дня данных (свежий день почти всегда неполный).

События на столбце:
  • отметка — длина серии дошла до круглого числа MILESTONES («10 недель подряд»);
  • рекорд — серия стала самой длинной в эту сторону за всю историю графика (с 2020 или min_date категории),
    не короче первой отметки; совпала с отметкой — одно сообщение;
  • разворот — первый столбец другого знака после серии не короче BREAK_MIN.
Несколько горизонтов одного среза и стороны в одном прогоне — одно сообщение по старшему (месяц > неделя > день).

Состояние — STATE_DIR/funds_flow_streak.state.json: последний разобранный закрытый столбец по срезу и горизонту.
Новый срез или горизонт (и первый прогон) запоминается молча, без сообщений задним числом.

    python3 -m signals.funds_flow_streak --dry-run    # показать, ничего не слать
    python3 -m signals.funds_flow_streak --backtest   # все события на истории: сколько и какие
"""
import os

_url = os.environ.get("DB_URL", "")
if "@db:" in _url:
    os.environ["DB_URL"] = _url.replace("@db:", "@127.0.0.1:")

import argparse  # noqa: E402
import html  # noqa: E402
import json  # noqa: E402
import time  # noqa: E402
import urllib.parse  # noqa: E402
from collections import Counter  # noqa: E402
from datetime import date  # noqa: E402

from api.services import fund_flows as ff  # noqa: E402
from signals.framesignal import SITE, send  # noqa: E402
from signals.insights import fresh  # noqa: E402

NAME = "funds_flow_streak"
STATE_FILE = os.path.join(fresh.STATE_DIR, f"{NAME}.state.json")
HORIZONS = ("1d", "1w", "1m")                         # младший → старший
MILESTONES = {"1d": (10, 20, 30, 50, 100), "1w": (5, 10, 20, 30, 50), "1m": (3, 6, 12, 24)}
BREAK_MIN = {"1d": 20, "1w": 10, "1m": 6}             # разворот после серии не короче (вторая отметка)
MIN_HISTORY_DAYS = 365                                # «рекорд за всю историю» на короткой истории ничего не значит
# винительный падеж: «идёт 1 месяц / 2 месяца / 5 месяцев»
UNIT = {"1d": ("рабочий день", "рабочих дня", "рабочих дней"), "1w": ("неделю", "недели", "недель"),
        "1m": ("месяц", "месяца", "месяцев")}
# родительный: «после 8 месяцев оттока», «после 21 недели»
UNIT_GEN = {"1d": ("рабочего дня", "рабочих дней"), "1w": ("недели", "недель"), "1m": ("месяца", "месяцев")}
SIDE = {1: "in", -1: "out"}
WORD = {"in": "Приток", "out": "Отток"}
WORD_GEN = {"in": "притока", "out": "оттока"}


def plural(n: int, forms: tuple) -> str:
    if n % 10 == 1 and n % 100 != 11:
        return forms[0]
    if 2 <= n % 10 <= 4 and not 12 <= n % 100 <= 14:
        return forms[1]
    return forms[2]


def span(n: int, tf: str) -> str:
    return f"{n} {plural(n, UNIT[tf])}"


def span_gen(n: int, tf: str) -> str:
    one, many = UNIT_GEN[tf]
    return f"{n} {one if n % 10 == 1 and n % 100 != 11 else many}"


def sign(v: float) -> int:
    return 1 if v > 0 else -1 if v < 0 else 0


def closed(bars: list, tf: str) -> list:
    """Закрытые столбцы: день — без последнего дня данных, неделя/месяц — без последнего (текущего) слота."""
    return bars[:-1]


def events(bars: list, tf: str) -> list:
    """Все события серий на закрытых столбцах среза: [{i, day, side, kind, n, record}] по возрастанию i.
    kind="streak" — серия длиной n на столбце i (отметка и/или рекорд), kind="break" — разворот на i после
    серии длиной n в другую сторону (side — сторона нового столбца)."""
    if not bars:
        return []
    first = date.fromisoformat(bars[0]["period_end"])
    out, run, prev_s = [], 0, 0
    best = {1: 0, -1: 0}                          # самая длинная завершённая серия в каждую сторону
    for i, b in enumerate(bars):
        s = sign(b["flow"])
        day = date.fromisoformat(b["period_end"])
        if s != prev_s:
            if prev_s and run >= BREAK_MIN[tf] and s:
                out.append({"i": i, "day": day, "side": SIDE[s], "kind": "break", "n": run})
            if prev_s:
                best[prev_s] = max(best[prev_s], run)
            run = 1 if s else 0
        else:
            run = run + 1 if s else 0
        prev_s = s
        if not s:
            continue
        mile = run in MILESTONES[tf]
        rec = (run == best[s] + 1 and best[s] > 0 and run >= MILESTONES[tf][0]
               and (day - first).days >= MIN_HISTORY_DAYS)
        if mile or rec:
            out.append({"i": i, "day": day, "side": SIDE[s], "kind": "streak", "n": run, "record": rec,
                        "since": first.year})
    return out


def _load():
    from signals.funds_flow_notify import _load as load
    return load()


def slice_bars(cats: dict, raw: list):
    """[(срез, tf, закрытые столбцы)] — те же срезы и столбцы, что у рекордов потоков."""
    from signals.funds_flow_notify import slices
    for s in slices(cats):
        min_date = ff.CATEGORY_INDEX_MAP.get(s["cat"], {}).get("min_date")
        date_from = max(ff.ALL_FROM, date.fromisoformat(min_date)) if min_date else ff.ALL_FROM
        series = ff.fill_series(raw, s["ids"], date_from)
        if not series:
            continue
        corr = ff.corrections(s["cat"], cats[s["cat"]]["funds"], series)
        for tf in HORIZONS:
            yield s, tf, closed(ff.slot_flows(series, corr, tf), tf)


def chart_url(x: dict) -> str:
    """Диплинк на /funds-money: срез, «Притоки-Оттоки» на горизонте серии и период, где серия видна целиком."""
    days = x["n"] * {"1d": 1.5, "1w": 7, "1m": 31}[x["tf"]]
    period = "1y" if days <= 300 else "3y" if days <= 1000 else "all"
    if x.get("record") and x["tf"] != "1d":
        period = "all"
    p = [("category", x["cat"]), ("funds", ",".join(str(i) for i in x["ids"])), ("tf", x["tf"]), ("period", period)]
    return f"{SITE}/funds-money?" + urllib.parse.urlencode(p, safe=",")


def message(x: dict) -> str:
    head = f"{'🔥 ' if x.get('record') else ''}<b>{html.escape(x['name'])}</b>\n"
    if x["kind"] == "break":
        opp = "out" if x["side"] == "in" else "in"
        body = f"{WORD[x['side']]} после {span_gen(x['n'], x['tf'])} {WORD_GEN[opp]}\n"
    else:
        body = f"{WORD[x['side']]} идёт {span(x['n'], x['tf'])} подряд\n"
        if x.get("record"):
            body += f"Самая долгая серия {WORD_GEN[x['side']]} с {x['since']} года\n"
    return head + body + f'<a href="{html.escape(chart_url(x))}">График →</a>'


def senior_only(new: list) -> list:
    """Один срез и сторона на нескольких горизонтах в одном прогоне — одно сообщение по старшему горизонту."""
    best: dict = {}
    for x in new:
        k = (x["cat"], x["sub"], x["side"])
        if k not in best or HORIZONS.index(x["tf"]) > HORIZONS.index(best[k]["tf"]):
            best[k] = x
    return [x for x in new if x is best[(x["cat"], x["sub"], x["side"])]]


def _key(s: dict, tf: str) -> str:
    return f"{s['cat']}|{s['sub'] or '*'}|{tf}"


def _load_state() -> dict:
    try:
        with open(STATE_FILE, encoding="utf-8") as fh:
            return json.load(fh)
    except FileNotFoundError:
        return {}


def _save_state(state: dict) -> None:
    os.makedirs(fresh.STATE_DIR, exist_ok=True)
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(dict(sorted(state.items())), fh, ensure_ascii=False)
    os.replace(tmp, STATE_FILE)


def pending(cats: dict, raw: list, state: dict) -> tuple[list, dict]:
    """События на закрытых столбцах после последнего разобранного (state) и новое состояние. Срез без состояния
    запоминается на последнем закрытом столбце без событий."""
    new, upd = [], {}
    for s, tf, bars in slice_bars(cats, raw):
        if not bars:
            continue
        k = _key(s, tf)
        last = bars[-1]["period_end"]
        upd[k] = last
        seen = state.get(k)
        if seen is None:
            continue
        for e in events(bars, tf):
            if bars[e["i"]]["period_end"] > seen:
                new.append({**s, **e, "tf": tf})
    return new, upd


def run_once(dry_run: bool = False) -> dict:
    state = _load_state()
    new, upd = pending(*_load(), state)
    out = senior_only(new)
    out.sort(key=lambda x: (x["day"], x["cat"], x["sub"] or "", x["side"]))
    summary = {"events": len(new), "new": len(out), "sent": 0, "failed": 0, "seeded": len(set(upd) - set(state))}
    ok = True
    for x in out:
        msg = message(x)
        if dry_run:
            print(msg + "\n")
            continue
        if send(msg, NAME, bot="signal"):
            summary["sent"] += 1
        else:
            summary["failed"] += 1
            ok = False
        time.sleep(1.1)
    # недоставленное не повторяем по кругу каждый прогон, но и не теряем молча: при сбое отправки состояние не
    # двигаем — следующий прогон пришлёт снова
    if not dry_run and ok:
        _save_state({**state, **upd})
    return summary


def backtest(recent_from: str | None = None) -> None:
    """Все события на всей истории: число по горизонтам и видам, в среднем за месяц, и лента после recent_from."""
    cats, raw = _load()
    allev = []
    for s, tf, bars in slice_bars(cats, raw):
        for e in events(bars, tf):
            allev.append({**s, **e, "tf": tf})
    if not allev:
        print("событий нет")
        return
    last = max(e["day"] for e in allev)
    for label, since in (("вся история", None), ("последние 12 мес", date(last.year - 1, last.month, last.day))):
        ev = [e for e in allev if since is None or e["day"] > since]
        months = max(1, ((last - (since or min(e["day"] for e in allev))).days) / 30.4)
        c = Counter((e["tf"], e["kind"] + ("+rec" if e.get("record") else "")) for e in ev)
        print(f"== {label}: {len(ev)} событий, {len(ev) / months:.1f} в месяц")
        for tf in HORIZONS:
            parts = ", ".join(f"{k}={v}" for (t, k), v in sorted(c.items()) if t == tf)
            n = sum(v for (t, _), v in c.items() if t == tf)
            print(f"   {tf}: {n} ({n / months:.1f}/мес)  {parts}")
    if recent_from:
        print(f"\n== лента с {recent_from}")
        for e in sorted((e for e in allev if e["day"].isoformat() >= recent_from),
                        key=lambda e: (e["day"], e["cat"], e["sub"] or "")):
            print(f"-- {e['day']} [{e['tf']}]")
            print(message(e).split("\n<a")[0].replace("<b>", "").replace("</b>", "") + "\n")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="показать сообщения, ничего не слать")
    ap.add_argument("--backtest", action="store_true", help="события на всей истории")
    ap.add_argument("--recent", default=None, help="с --backtest: лента событий с даты YYYY-MM-DD")
    a = ap.parse_args()
    if a.backtest:
        backtest(a.recent)
    else:
        print(f"[{NAME}] {run_once(dry_run=a.dry_run)}")
