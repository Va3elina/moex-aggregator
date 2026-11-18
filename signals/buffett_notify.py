"""Индикатор Баффетта → @framesignalbot (Вадим 18.11.2026): капитализация рынка к ВВП — как на /buffett (Кап/ВВП,
месяц, без сглаживания).

Событие — закрытый месяц, на котором отношение стало минимумом или максимумом за год, 2, 3, 5 лет или за всё время
(«за N лет» — с прошлого раза, когда было ниже/выше). Помесячные рекорды идут сериями, поэтому та же или более
короткая глубина повторяется не раньше чем через PAUSE_DAYS; более длинная — сразу (год → 5 лет — новость).
На истории с 2016 года — около 2,5 сигнала в год. Значок и звук — как у рекордов позиций (signals/signal_marks).

Картинка — график страницы (SimpleChart: капитализация слева, Кап/ВВП справа) с разметкой рекордов позиций:
кружок на месяце сигнала, прошлый раз ниже/выше — кружок с месяцем и годом и пунктир его уровня, крупная подпись.
Период — самый короткий из периодов страницы (5, 10, 20 лет, всё), где виден прошлый раз.

Состояние — STATE_DIR/buffett_notify.state.json: последний разобранный месяц и что отправлено по стороне.
Первый прогон запоминает месяц молча.

    python3 -m signals.buffett_notify --dry-run      # показать, ничего не слать (CARD_DIR — сохранить картинки)
    python3 -m signals.buffett_notify --backtest     # все события на истории
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

NAME = "buffett_notify"
STATE_DIR = os.environ.get("SCAN_STATE_DIR", "/opt/frame/data/scan_state")
STATE_FILE = os.path.join(STATE_DIR, f"{NAME}.state.json")
WINDOWS = ((1825, "за 5 лет"), (1095, "за 3 года"), (730, "за 2 года"), (365, "за год"))
RANK = {"за всё время": 0, **{w: i for i, (_, w) in enumerate(WINDOWS, 1)}}
PAUSE_DAYS = 90
PERIODS = (("5Л", 1825), ("10Л", 3650), ("20Л", 7300))     # периоды страницы; дальше — «Всё»
UA = {"User-Agent": "frame-signal-card"}


def load() -> list:
    """[(конец месяца, Кап/ВВП %, капитализация трлн ₽)] — ряд страницы /buffett (месяц, без сглаживания)."""
    d = requests.get(f"{SITE}/api/buffett/cap-gdp", params={"period": "all", "timeframe": "1m", "smooth": "false"},
                     timeout=60, headers=UA).json()
    return [(p["date"], float(p["buffett"]), float(p["cap"])) for p in d.get("data") or [] if p.get("buffett") is not None]


def closed(rows: list, today: dt.date) -> list:
    """Только закрытые месяцы: точка текущего месяца ещё меняется."""
    first = today.replace(day=1).isoformat()
    return [r for r in rows if r[0] < first]


def record(rows: list, i: int) -> list:
    """Рекорды месяца i против всех прежних: [(сторона, окно, индекс прошлого раза или None)]."""
    d, v, _ = rows[i]
    out = []
    for side, sg in (("min", -1), ("max", 1)):
        prev = [k for k in range(i) if sg * rows[k][1] >= sg * v]
        if not prev:
            out.append((side, "за всё время", None))
            continue
        gap = (dt.date.fromisoformat(d) - dt.date.fromisoformat(rows[prev[-1]][0])).days
        w = next((w for n, w in WINDOWS if gap >= n), None)
        if w:
            out.append((side, w, prev[-1]))
    return out


def due(side: str, w: str, day: str, sent: dict) -> bool:
    p = sent.get(side)
    if p is None or RANK[w] < p["rank"]:
        return True
    return (dt.date.fromisoformat(day) - dt.date.fromisoformat(p["day"])).days >= PAUSE_DAYS


def phrase(rows: list, i: int, side: str, w: str, ref: int | None) -> str:
    """«минимум за год»; прошлый раз давнее 5 лет — «минимум с 2001 года» (окно «за 5 лет» тогда преуменьшает)."""
    word = "минимум" if side == "min" else "максимум"
    if w == "за 5 лет" and ref is not None:
        gap = (dt.date.fromisoformat(rows[i][0]) - dt.date.fromisoformat(rows[ref][0])).days
        if gap >= 7 * 365:
            return f"{word} с {rows[ref][0][:4]} года"
    return f"{word} {w}"


def month(d: str) -> str:
    return f"{MS[int(d[5:7]) - 1]} {d[:4]}"


def pct(v: float) -> str:
    return f"{v:.1f}".replace(".", ",") + "%"


def card(rows: list, i: int, side: str, w: str, ref: int | None) -> dict:
    upto = rows[:i + 1]
    end = dt.date.fromisoformat(upto[-1][0])
    label, start = "Всё", 0
    if ref is not None:
        need = (end - dt.date.fromisoformat(upto[ref][0])).days * 1.15 + 60
        for lab, days in PERIODS:
            if days >= need:
                cut = (end - dt.timedelta(days=days)).isoformat()
                label, start = lab, next(k for k, r in enumerate(upto) if r[0] >= cut)
                break
    view = upto[start:]
    peaks = [ref - start] if ref is not None else []
    if ref is None:                                          # за всё время — главный прошлый экстремум
        sg = -1 if side == "min" else 1
        body = view[:-12]
        if body:
            peaks = [max(range(len(body)), key=lambda k: sg * body[k][1])]
    return {
        "asset": "Капитализация (трлн ₽)", "lineLabel": "Капитализация / ВВП",
        "price": [{"time": d, "value": c} for d, _, c in view],
        "line": [{"time": d, "value": v} for d, v, _ in view],
        "chartProps": {"primaryColor": "var(--accent-secondary)", "secondaryColor": "var(--accent)", "reverseLegend": True,
                       "chartPadding": {"left": 100, "right": 100}, "hideTime": True,
                       "primaryLabel": "Капитализация (трлн ₽)", "secondaryLabel": "Капитализация / ВВП"},
        "fmt": {"value": "trln", "primaryAxis": "int", "secondaryValue": "pct2", "secondaryAxis": "pct1"},
        "meta": {"title": "Индикатор Баффетта", "asset": "Капитализация / ВВП", "details": [label, "1 месяц"]},
        "signal": {"headline": phrase(rows, i, side, w, ref), "colorVar": "--accent",
                   "dir": "low" if side == "min" else "high",
                   "peaks": [{"date": view[k][0], "value": view[k][1], "label": month(view[k][0])} for k in peaks]},
    }


def message(rows: list, i: int, side: str, w: str, ref: int | None) -> str:
    d, v, _ = rows[i]
    head = f"{signal_marks.mark(w)}<b>Индикатор Баффетта</b>\n"
    body = f"Капитализация / ВВП — {pct(v)}: {phrase(rows, i, side, w, ref)}\n"
    if ref is not None:
        body += f"{'Ниже' if side == 'min' else 'Выше'} было в {month(rows[ref][0])}: {pct(rows[ref][1])}\n"
    return head + html.escape(body.rstrip("\n")) + history(rows, i, side, w)


def history(rows: list, i: int, side: str, w: str) -> str:
    """Прошлые месяцы с таким же или более глубоким рекордом в ту же сторону и индекс МосБиржи через 3 и 12 месяцев."""
    from signals import signal_history as sh
    try:
        hits = [k for k in range(12, i) if any(sd == side and RANK[ww] <= RANK[w] for sd, ww, _ in record(rows, k))]
        eps = sh.episodes(hits, i, 3)
        if not eps:
            return ""
        db = sh.session()
        try:
            px = sh.imoex(db)
        finally:
            db.close()
        out = [(rows[k][0], [sh.price_after(px, dt.date.fromisoformat(rows[k][0]) + dt.timedelta(days=1), n) for n in (63, 252)])
               for k in eps]
        up = sh.base_up(px, dt.date(2005, 1, 1), 63)
        return sh.block(sh.cases_title(len(out)), ["индекс через 3 мес", "через год"], out,
                        f"В обычный день индекс через 3 месяца рос в {up}% случаев" if up is not None else None)
    except Exception as e:  # noqa: BLE001
        print(f"[{NAME}] история: {type(e).__name__}: {e}")
        return ""


def picture(rows: list, i: int, side: str, w: str, ref: int | None) -> bytes | None:
    try:
        return render(card(rows, i, side, w, ref))
    except Exception as e:  # noqa: BLE001 — без картинки лучше, чем без сигнала
        print(f"[{NAME}] картинка: {type(e).__name__}: {e}")
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
    rows = closed(load(), today or dt.date.today())
    st = _load_state()
    summary = {"month": rows[-1][0] if rows else None, "events": 0, "sent": 0, "failed": 0}
    if not rows:
        return summary
    if not st:
        if not dry_run:
            _save_state({"month": rows[-1][0], "sent": {}})
        summary["seeded"] = True
        return summary
    if rows[-1][0] <= st["month"]:
        return summary
    i = len(rows) - 1
    sent = st.get("sent", {})
    ok = True
    for k, (side, w, ref) in enumerate(record(rows, i)):
        if not due(side, w, rows[i][0], sent):
            continue
        summary["events"] += 1
        msg, png = message(rows, i, side, w, ref), picture(rows, i, side, w, ref)
        if dry_run:
            print(msg)
            from signals.funds_card import dry
            dry(png, f"{NAME}_{k}")
            print()
            continue
        quiet = signal_marks.silent(w)
        kw = {"bot": "signal", "silent": quiet, "meta": {"type": "buffett", "window": w}, "chart_url": f"{SITE}/buffett"}
        if send_photo(png, msg, NAME, **kw) if png else send(msg, NAME, **kw):
            summary["sent"] += 1
            sent[side] = {"day": rows[i][0], "rank": RANK[w]}
        else:
            summary["failed"] += 1
            ok = False
        time.sleep(1.1)
    if not dry_run and ok:
        _save_state({"month": rows[-1][0], "sent": sent})
    return summary


def backtest(since: str = "2016") -> list:
    rows = closed(load(), dt.date.today())
    sent: dict = {}
    out = []
    for i in range(1, len(rows)):
        for side, w, ref in record(rows, i):
            if due(side, w, rows[i][0], sent):
                sent[side] = {"day": rows[i][0], "rank": RANK[w]}
                if rows[i][0] >= since:
                    out.append((rows[i][0], side, w, round(rows[i][1], 1)))
    return out


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--backtest", action="store_true")
    a = ap.parse_args()
    if a.backtest:
        for e in backtest():
            print(*e)
    else:
        print(f"[{NAME}] {run_once(dry_run=a.dry_run)}")
