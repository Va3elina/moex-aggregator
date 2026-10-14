"""Рекорды позиций физлиц → отдельное сообщение на каждый в @framesignalbot (личка админа).

Ряды те же, что у движка находок завода постов (signals/insights/detect.load_positions): дневные позиции физлиц
по фьючерсам. Четыре показателя — объём лонга, объём шорта, число физлиц в лонге, число физлиц в шорте. Рекорд —
максимум показателя, какого не было 3 месяца, год или за всё время (у завода нижний порог ~4 месяца, поэтому окна
считаются здесь). Вселенная и фильтры как у завода: группы Акции/Валюта/Индексы/Сырьё, медиана участников за год
≥ MIN_PART, история от года, без малоактивных контрактов скринера и без дней экспирации ±1. В сообщении — только
актив, показатель, окно рекорда и ссылка на график, без цифр (Вадим 13.10: «цифры не нужны»).

Два режима, общее состояние отправленного (STATE_DIR/record_notify.sent.json: день|актив|показатель → окно):
  • --live (крон каждые 5 минут, 9:00–23:55 МСК) — последний 5-минутный срез дня против истории дневных значений:
    рекорд уходит в момент пробоя (Вадим 14.10: «почему не онлайн, а пачкой в 10:45»). Повторно — только если
    за день окно выросло (3 месяца → год → всё время);
  • по закрытию (крон каждый час, `45 7-17 * * *`, ждёт дневные позиции, signals/insights/fresh.py) — догон того,
    что срезы не поймали; в сообщении пометка «по закрытию».

    python3 -m signals.record_notify --live --dry-run   # онлайн: показать, ничего не слать
    python3 -m signals.record_notify --dry-run          # по закрытию: показать, ничего не слать
    python3 -m signals.record_notify --force            # по закрытию сейчас, не дожидаясь свежего дня
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
from datetime import timedelta  # noqa: E402

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

from api.database import SessionLocal  # noqa: E402
from signals.framesignal import SITE, send  # noqa: E402
from signals.insights import data, fresh  # noqa: E402
from signals.insights import detect as det  # noqa: E402
from signals.insights.expiry import near_expiry  # noqa: E402

NAME = "record_notify"
SENT_FILE = os.path.join(fresh.STATE_DIR, f"{NAME}.sent.json")
GROUPS = ("Акции", "Валюта", "Индексы", "Сырьё")
MIN_HISTORY = 260                     # торговых дней: на короткой истории «за всё время» ничего не значит
# показатель → подпись; цифр в сообщении нет (Вадим 13.10: «цифры не нужны»)
LEGS = {"long": "Объём лонга физлиц", "short": "Объём шорта физлиц",
        "nl": "Число физлиц в лонге", "ns": "Число физлиц в шорте"}
# поле среза open_interest для показателя; шорт хранится со знаком минус
SNAP = {"long": ("pos_long", 1), "short": ("pos_short", -1), "nl": ("pos_long_num", 1), "ns": ("pos_short_num", 1)}
# окно рекорда: сколько дней не было такого значения → подпись; «всё время» — значение выше всей истории
WINDOWS = ((365, "за год"), (91, "за 3 месяца"))
RANK = {"за всё время": 0, "за год": 1, "за 3 месяца": 2}


def _low_activity() -> set:
    try:
        from api.services.oi_screener import low_activity_set
        db = SessionLocal()
        try:
            return low_activity_set(db)
        finally:
            db.close()
    except Exception as e:  # noqa: BLE001 — без фильтра лучше, чем без рекордов
        print(f"[{NAME}] нет фильтра малоактивных: {type(e).__name__}: {e}")
        return set()


def window(hist: np.ndarray, dates, v: float, t) -> str | None:
    """Окно рекорда значения v на день t против истории строго до t; None — не рекорд."""
    if v <= 0:
        return None
    idx = np.flatnonzero(hist >= v)
    if not len(idx):
        return "за всё время"
    gap = (pd.Timestamp(t) - dates[idx[-1]]).days
    return next((w for d, w in WINDOWS if gap >= d), None)


def _universe(P: dict, groups: dict, low: set, last: pd.Timestamp) -> list:
    part = P["npart"]
    year = part.loc[part.index >= last - pd.Timedelta(days=365)]
    return [s for s in part.columns
            if groups.get(s) in GROUPS and s not in low and year[s].median() >= det.MIN_PART]


def records_close() -> tuple[pd.Timestamp, list]:
    """Рекорды по закрытию последнего дня дневных данных."""
    P = det.load_positions()
    names, groups, _ = det.instruments()
    until = P["npart"].index.max()
    if near_expiry(until):
        return until, []
    out = []
    for s in _universe(P, groups, _low_activity(), until):
        for leg in LEGS:
            ser = P[leg][s].dropna()
            if len(ser) < MIN_HISTORY + 1 or ser.index[-1] != until:
                continue
            w = window(ser.values[:-1].astype(float), ser.index[:-1], float(ser.values[-1]), until)
            if w:
                out.append({"sec": s, "name": names.get(s, s), "leg": leg, "window": w})
    return until, out


def records_live() -> tuple[pd.Timestamp | None, list]:
    """Рекорды последнего 5-минутного среза дня против дневной истории до него."""
    snap = data.read("oi_intraday_last", parse_dates=["tradedate"])
    if not len(snap):
        return None, []
    day = snap.tradedate.max()
    P = det.load_positions()
    last = P["npart"].index.max()
    if day <= last or near_expiry(day):       # срез за день, по которому уже есть закрытие, или экспирация
        return day, []
    names, groups, _ = det.instruments()
    snap = snap[snap.tradedate == day].set_index("sectype")
    out = []
    for s in _universe(P, groups, _low_activity(), last):
        if s not in snap.index:
            continue
        for leg, (col, sign) in SNAP.items():
            ser = P[leg][s].dropna()
            v = snap.at[s, col]
            if len(ser) < MIN_HISTORY or ser.index[-1] != last or pd.isna(v):
                continue
            w = window(ser.values.astype(float), ser.index, sign * float(v), day)
            if w:
                out.append({"sec": s, "name": names.get(s, s), "leg": leg, "window": w})
    return day, out


def chart_url(x: dict) -> str:
    """Диплинк на /oi (frontend/src/utils/oiDeepLink.ts): физлица, дневной график; объём — позиции с нужной
    ногой, число физлиц — режим участников. Периода короче года на странице нет — 3 месяца открываем на году."""
    p = [("instrument", x["sec"]), ("clgroup", "FIZ"), ("interval", "24"),
         ("period", "all" if x["window"] == "за всё время" else "1y")]
    if x["leg"] in ("long", "short"):
        p += [("mode", "positions"), ("variant", x["leg"])]
    else:
        p.append(("mode", "participants"))
    return f"{SITE}/oi?" + urllib.parse.urlencode(p)


def message(x: dict, close_day=None) -> str:
    tail = f" · по закрытию {pd.Timestamp(close_day):%d.%m}" if close_day is not None else ""
    return (f"🔺 <b>{html.escape(x['name'])}</b>\n{LEGS[x['leg']]}\nРекорд {x['window']}{tail}\n"
            f'<a href="{html.escape(chart_url(x))}">График →</a>')


def _key(day, x: dict) -> str:
    return f"{pd.Timestamp(day).date()}|{x['sec']}|{x['leg']}"


def _load_sent() -> dict:
    """день|актив|показатель → ранг отправленного окна (0 — за всё время)."""
    try:
        with open(SENT_FILE, encoding="utf-8") as fh:
            raw = json.load(fh)
    except FileNotFoundError:
        return {}
    return {k: 0 for k in raw} if isinstance(raw, list) else raw    # список — прежний формат, без окна


def _save_sent(sent: dict, since: str) -> None:
    os.makedirs(fresh.STATE_DIR, exist_ok=True)
    with open(SENT_FILE, "w", encoding="utf-8") as fh:
        json.dump({k: v for k, v in sorted(sent.items()) if k[:10] >= since}, fh, ensure_ascii=False)


def run_once(live: bool, dry_run: bool = False) -> dict:
    day, recs = records_live() if live else records_close()
    summary = {"mode": "live" if live else "close", "day": str(day.date()) if day is not None else None,
               "records": len(recs), "new": 0, "sent": 0, "failed": 0}
    if day is None:
        return summary
    sent = _load_sent()
    new = [x for x in recs if RANK[x["window"]] < sent.get(_key(day, x), 99)]
    new.sort(key=lambda x: (RANK[x["window"]], x["name"], list(LEGS).index(x["leg"])))
    summary["new"] = len(new)
    for x in new:
        msg = message(x, None if live else day)
        if dry_run:
            print(msg + "\n")
            continue
        if send(msg, NAME):
            summary["sent"] += 1
            sent[_key(day, x)] = RANK[x["window"]]
        else:
            summary["failed"] += 1
        time.sleep(1.1)                   # лимит Telegram — около сообщения в секунду на чат
    if not dry_run and new:
        _save_sent(sent, (day - timedelta(days=14)).date().isoformat())
    return summary


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--live", action="store_true", help="онлайн: последний 5-минутный срез против дневной истории")
    ap.add_argument("--dry-run", action="store_true", help="показать сообщения, ничего не слать")
    ap.add_argument("--force", action="store_true", help="по закрытию сейчас: не ждать свежий день и не смотреть отметку")
    a = ap.parse_args()
    why = None if (a.live or a.dry_run or a.force) else fresh.wait_reason(NAME)
    if why:
        print(f"[{NAME}] пропуск: {why}")
    else:
        s = run_once(a.live, dry_run=a.dry_run)
        if not (a.live or a.dry_run) and not s["failed"]:
            fresh.mark_done(NAME, s["day"])
        if not a.live or s["new"]:
            print(f"[{NAME}] {s}")
