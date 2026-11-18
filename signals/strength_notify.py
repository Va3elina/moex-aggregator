"""«Сила рынка» → @framesignalbot (Вадим 18.11.2026): доля акций индекса МосБиржи выше EMA200 — как на /strength
(вселенная IMOEX, рубли, EMA200, гистограмма).

События на закрытом дне (вчера и раньше — сегодняшний столбик ещё меняется):
  • край — доля опустилась до LOW% и ниже или поднялась до HIGH% и выше; повторно — только после выхода из края
    (LOW+5 / HIGH−5) и не чаще раза в PAUSE_DAYS; со звуком и оранжевым кружком;
  • смена режима — доля поднялась до 60% после того, как была ≤40%, или опустилась до 40% после ≥60%; без звука.
На истории с 2021 года: край ~3 раза в год, смена режима ~2 раза в год.

Картинка — две панели страницы (индекс и доля выше EMA200) с разметкой: пунктир порога, кружок на дне сигнала,
прошлые такие же эпизоды кружками с месяцем и годом, крупная подпись. Период — 1 год, если прошлый такой эпизод в
него попал, иначе 5 лет (периоды страницы).

Состояние — STATE_DIR/strength_notify.state.json (последний разобранный день, режим, зона, день последнего края).
Первый прогон запоминает состояние молча.

    python3 -m signals.strength_notify --dry-run          # показать, ничего не слать (CARD_DIR — сохранить картинки)
    python3 -m signals.strength_notify --backtest         # все события на истории
"""
from __future__ import annotations

import argparse
import datetime as dt
import html
import json
import os
import time

import requests

from signals import signal_marks
from signals.framesignal import SITE, send, send_photo
from signals.record_card import MS, render

NAME = "strength_notify"
# та же папка состояний, что у остальных сигналов (signals/insights/fresh.STATE_DIR) — без подключения к БД
STATE_DIR = os.environ.get("SCAN_STATE_DIR", "/opt/frame/data/scan_state")
STATE_FILE = os.path.join(STATE_DIR, f"{NAME}.state.json")
EMA = 200
LOW, HIGH = 15.0, 85.0             # края
REG_LO, REG_HI = 40.0, 60.0        # смена режима с запасом: шум вокруг 50% не сигналит
PAUSE_DAYS = 30
UA = {"User-Agent": "frame-signal-card"}


def load(days: int = 9000) -> list:
    """[(дата, доля %, IMOEX)] — те же ряды, что у страницы /strength (история силы и индекс)."""
    d = requests.get(f"{SITE}/api/breadth/history", params={"ema_period": EMA, "days": days, "universe": "imoex"},
                     timeout=60, headers=UA).json()
    ix = {p["date"]: p["close"] for p in d.get("imoex") or []}
    return [(p["date"], float(p["percent_above"]), ix[p["date"]]) for p in d.get("data") or []
            if p.get("percent_above") is not None and p["date"] in ix]


def step(st: dict, day: str, v: float) -> list:
    """Один закрытый день: обновляет состояние st, возвращает события [{kind, side, day, value}]."""
    out = []
    zone = "low" if v <= LOW else "high" if v >= HIGH else None
    if st.get("zone") and (st["zone"] == "low" and v > LOW + 5 or st["zone"] == "high" and v < HIGH - 5):
        st["zone"] = None                                    # вышли из края — следующий заход снова событие
    if zone and st.get("zone") != zone:
        last = st.get("edge_day")
        if not last or (dt.date.fromisoformat(day) - dt.date.fromisoformat(last)).days > PAUSE_DAYS:
            out.append({"kind": "edge", "side": zone, "day": day, "value": v})
            st["edge_day"] = day
        st["zone"] = zone
    reg = "up" if v >= REG_HI else "down" if v <= REG_LO else None
    if reg and st.get("regime") and reg != st["regime"]:
        out.append({"kind": "regime", "side": reg, "day": day, "value": v})
    if reg:
        st["regime"] = reg
    st["day"] = day
    return out


def episodes(rows: list, e: dict) -> list:
    """Прошлые такие же события (те же правила) до дня e — для кружков на картинке и строки «прошлый раз»."""
    st: dict = {}
    out = []
    for d, v, _ in rows:
        if d >= e["day"]:
            break
        out += [x for x in step(st, d, v) if x["kind"] == e["kind"] and x["side"] == e["side"]]
    return out


def month(d: str) -> str:
    return f"{MS[int(d[5:7]) - 1]} {d[:4]}"


def headline(e: dict) -> str:
    v = round(e["value"])
    if e["kind"] == "edge":
        return f"только {v}% акций выше EMA{EMA}" if e["side"] == "low" else f"{v}% акций выше EMA{EMA}"
    return f"{v}% акций снова выше EMA{EMA}" if e["side"] == "up" else f"лишь {v}% акций выше EMA{EMA}"


def card(rows: list, e: dict) -> dict:
    """Карточка для движка: две панели страницы за 1 год или 5 лет, разметка сигнала."""
    upto = [r for r in rows if r[0] <= e["day"]]
    end = dt.date.fromisoformat(e["day"])
    prev = episodes(rows, e)
    period, days = "1 год", 365
    if prev and prev[-1]["day"] < (end - dt.timedelta(days=365)).isoformat():
        period, days = "5 лет", 1825
    cut = (end - dt.timedelta(days=days)).isoformat()
    view = [r for r in upto if r[0] >= cut]
    idx = {r[0]: i for i, r in enumerate(view)}
    peaks = [{"i": idx[p["day"]], "label": month(p["day"])} for p in prev if p["day"] in idx][-2:]
    low = e["side"] in ("low", "down")
    level = (LOW if e["side"] == "low" else HIGH) if e["kind"] == "edge" else (REG_LO if low else REG_HI)
    return {
        "kind": "strength", "ema": EMA, "indexLabel": "Индекс МосБиржи",
        "synced": [{"time": d, "breadth": v, "imoex": ix} for d, v, ix in view],
        "meta": {"title": "Сила рынка", "asset": "Индекс МосБиржи", "details": [f"EMA{EMA}", period, "RUB", "Гистограмма"]},
        "signal": {"headline": headline(e), "colorVar": "--funds-flow-negative" if low else "--funds-flow-positive",
                   "level": level, "peaks": peaks},
    }


def message(rows: list, e: dict) -> str:
    v = round(e["value"])
    if e["kind"] == "edge":
        what = (f"Выше EMA{EMA} только {v}% акций индекса" if e["side"] == "low"
                else f"Выше EMA{EMA} уже {v}% акций индекса")
        head = f"{signal_marks.PULSE} <b>Сила рынка</b>\n"
    else:
        what = (f"Выше EMA{EMA} снова больше половины акций индекса: {v}%" if e["side"] == "up"
                else f"Выше EMA{EMA} снова меньше половины акций индекса: {v}%")
        head = "<b>Сила рынка</b>\n"
    prev = episodes(rows, e)
    tail = f"\nПрошлый раз — {month(prev[-1]['day'])}" if prev else ""
    return f"{head}{html.escape(what)}{tail}"


def picture(rows: list, e: dict) -> bytes | None:
    try:
        return render(card(rows, e))
    except Exception as ex:  # noqa: BLE001 — без картинки лучше, чем без сигнала
        print(f"[{NAME}] картинка: {type(ex).__name__}: {ex}")
        return None


def _load_state() -> dict:
    try:
        with open(STATE_FILE, encoding="utf-8") as fh:
            return json.load(fh)
    except FileNotFoundError:
        return {}


def _save_state(st: dict) -> None:
    os.makedirs(STATE_DIR, exist_ok=True)
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(st, fh, ensure_ascii=False)
    os.replace(tmp, STATE_FILE)


def run_once(dry_run: bool = False, today: dt.date | None = None) -> dict:
    today = today or dt.date.today()
    rows = load()
    closed = [r for r in rows if r[0] < today.isoformat()]
    st = _load_state()
    summary = {"day": closed[-1][0] if closed else None, "events": 0, "sent": 0, "failed": 0}
    if not closed:
        return summary
    if not st:                                               # первый прогон: состояние по истории, без сообщений
        for d, v, _ in closed:
            step(st, d, v)
        if not dry_run:
            _save_state(st)
        summary["seeded"] = True
        return summary
    new = []
    for d, v, _ in closed:
        if d > st["day"]:
            new += step(st, d, v)
    summary["events"] = len(new)
    ok = True
    for k, e in enumerate(new):
        msg = message(rows, e)
        png = picture(rows, e)
        if dry_run:
            print(msg)
            from signals.funds_card import dry
            dry(png, f"{NAME}_{k}")
            print()
            continue
        quiet = e["kind"] != "edge"
        kw = {"bot": "signal", "silent": quiet, "meta": {"type": "strength"}, "chart_url": f"{SITE}/strength"}
        if send_photo(png, msg, NAME, **kw) if png else send(msg, NAME, **kw):
            summary["sent"] += 1
        else:
            summary["failed"] += 1
            ok = False
        time.sleep(1.1)
    if not dry_run and ok:
        _save_state(st)
    return summary


def backtest() -> None:
    st: dict = {}
    for d, v, _ in load():
        for e in step(st, d, v):
            if d >= "2021":
                print(d, e["kind"], e["side"], round(v, 1))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--backtest", action="store_true")
    a = ap.parse_args()
    if a.backtest:
        backtest()
    else:
        print(f"[{NAME}] {run_once(dry_run=a.dry_run)}")
