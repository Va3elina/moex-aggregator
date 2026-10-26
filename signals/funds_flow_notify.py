"""Рекордные притоки и оттоки «Денег в фондах» → отдельное сообщение на каждый рекорд в @framesignalbot (личка админа).

Модель та же, что у рекордов позиций физлиц (signals/record_notify.py): рекорд — поток, какого не было 3 месяца,
полгода, год, 2, 3, 5 лет или за всё время (окна, повторы и пауза — оттуда же: window, due, COOLDOWN_DAYS).

Всё, что в сообщении, читатель видит на сайте (Вадим 26.10):
  • столбцы — ровно графика притоков-оттоков (api/services/fund_flows: тот же код, что /api/funds/flows, с поправками
    FLOW_REORG_CORRECTIONS); горизонт — таймфрейм графика: день, неделя, месяц (календарные, последний слот может быть
    неполным — так он и нарисован). «За всё время» — период графика «Всё»: с 2020 года, не раньше min_date категории;
  • срезы — категория целиком (все фонды) и каждая её подкатегория, как группы в таблице и выборе фондов на странице
    (состав — load_fund_categories, тот же, что у сайта);
  • ссылка открывает этот срез (?category=…&funds=…), «Притоки-Оттоки» на этом таймфрейме (?tf=) и период, где
    рекорд виден (?period=).
Цифр нет (Вадим 13.10: «цифры не нужны»): срез, приток или отток, горизонт, окно рекорда и ссылка на график.

Данные фондов приходят с лагом 1–2 дня и обновляются в 09:30/14:30/18:00/19:10 МСК — крон после каждого обновления
(signals/funds_flow_notify.sh). Повторы помнит STATE_DIR/funds_flow_notify.sent.json (срез|сторона|горизонт).

    python3 -m signals.funds_flow_notify --dry-run   # показать, ничего не слать
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
from datetime import date, timedelta  # noqa: E402

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

from api.services import fund_flows as ff  # noqa: E402
from signals.framesignal import SITE, send  # noqa: E402
from signals.insights import fresh  # noqa: E402
from signals.record_notify import COOLDOWN_DAYS, RANK, WINDOWS, due, window  # noqa: E402

NAME = "funds_flow_notify"
SENT_FILE = os.path.join(fresh.STATE_DIR, f"{NAME}.sent.json")
HORIZONS = {"1d": "за день", "1w": "за неделю", "1m": "за месяц"}
SIDES = {"in": "Приток", "out": "Отток"}
MIN_HISTORY_DAYS = 365                # на короткой истории «за всё время» ничего не значит
# подписи групп как в таблице фондов (FundsTable: «Управляемые фонды акций» → «Управляемые фонды»)
SUBCAT_LABEL = {"Управляемые фонды акций": "Управляемые фонды"}
# период графика под окно: у притоков-оттоков на дневном/недельном/месячном ТФ есть 1y, 3y и all
CHART_PERIOD = {"за всё время": "all", "за 5 лет": "all", "за 3 года": "3y", "за 2 года": "3y"}


def mark(w: str) -> str:
    """Метка окна, как в record_notify (Вадим 26.10): 3 месяца — без эмодзи, дальше по красному кружку на каждое
    следующее окно (полгода — один, 5 лет — пять), за всё время — огонёк."""
    if w == "за всё время":
        return "🔥 "
    n = len(WINDOWS) - RANK[w]
    return "🔴" * n + " " if n else ""


def slices(categories: dict) -> list:
    """Срезы страницы: категория целиком и каждая непустая подкатегория (группа в таблице фондов)."""
    out = []
    for cat, conf in categories.items():
        funds = conf["funds"]
        out.append({"cat": cat, "sub": None, "name": f"{conf['name']} · все фонды", "ids": sorted(funds)})
        subs: dict = {}
        for fid, f in funds.items():
            if f.get("subcategory"):
                subs.setdefault(f["subcategory"], []).append(fid)
        for sub, ids in subs.items():
            label = SUBCAT_LABEL.get(sub, sub)
            out.append({"cat": cat, "sub": sub, "name": f"{conf['name']} · {label}", "ids": sorted(ids)})
    return out


def bar_records(bars: list) -> list:
    """Рекорды последнего столбца против всех прежних: [(сторона, окно)]. Приток — максимум потока, отток — максимум
    −поток; дата столбца — его конец (period_end)."""
    if len(bars) < 2:
        return []
    ends = pd.to_datetime([b["period_end"] for b in bars])
    if (ends[-1] - ends[0]).days < MIN_HISTORY_DAYS:
        return []
    v = np.array([b["flow"] for b in bars], dtype=float)
    out = []
    for side, sgn in (("in", 1.0), ("out", -1.0)):
        w = window(sgn * v[:-1], ends[:-1], float(sgn * v[-1]), ends[-1])
        if w:
            out.append((side, w))
    return out


def _load():
    from sqlalchemy import text
    from api.database import SessionLocal
    cats = ff.load_fund_categories()
    ids = [fid for c in cats.values() for fid in c["funds"]]
    db = SessionLocal()
    try:
        raw = db.execute(text("SELECT fund_id, trade_date, nav, pay FROM fund_data WHERE fund_id = ANY(:ids) "
                              "ORDER BY fund_id, trade_date"), {"ids": ids}).fetchall()
    finally:
        db.close()
    return cats, [(r[0], r[1], None if r[2] is None else float(r[2]), None if r[3] is None else float(r[3]))
                  for r in raw]


def records(cats: dict, raw: list) -> tuple[date | None, list]:
    """Рекорды последних столбцов по всем срезам и горизонтам. День — последний день данных фондов."""
    out, last = [], None
    for s in slices(cats):
        min_date = ff.CATEGORY_INDEX_MAP.get(s["cat"], {}).get("min_date")
        date_from = max(ff.ALL_FROM, date.fromisoformat(min_date)) if min_date else ff.ALL_FROM
        series = ff.fill_series(raw, s["ids"], date_from)
        if not series:
            continue
        corr = ff.corrections(s["cat"], cats[s["cat"]]["funds"], series)
        for tf in HORIZONS:
            bars = ff.slot_flows(series, corr, tf)
            if not bars:
                continue
            end = date.fromisoformat(bars[-1]["period_end"])
            last = end if last is None or end > last else last
            for side, w in bar_records(bars):
                out.append({**s, "tf": tf, "side": side, "window": w, "day": end})
    return last, out


def chart_url(x: dict) -> str:
    """Диплинк на /funds-money (FundsMoneyPage): раздел, фонды среза, «Притоки-Оттоки» на горизонте рекорда и период,
    где он виден (окна до года — на годе)."""
    p = [("category", x["cat"]), ("funds", ",".join(str(i) for i in x["ids"])), ("tf", x["tf"]),
         ("period", CHART_PERIOD.get(x["window"], "1y"))]
    return f"{SITE}/funds-money?" + urllib.parse.urlencode(p, safe=",")


def message(x: dict) -> str:
    return (f"{mark(x['window'])}<b>{html.escape(x['name'])}</b>\n{SIDES[x['side']]} {HORIZONS[x['tf']]}\n"
            f"Рекорд {x['window']}\n"
            f'<a href="{html.escape(chart_url(x))}">График →</a>')


def _key(x: dict) -> str:
    return f"{x['cat']}|{x['sub'] or '*'}|{x['side']}|{x['tf']}"


def _load_sent() -> dict:
    try:
        with open(SENT_FILE, encoding="utf-8") as fh:
            return json.load(fh)
    except FileNotFoundError:
        return {}


def _save_sent(sent: dict, since: str) -> None:
    os.makedirs(fresh.STATE_DIR, exist_ok=True)
    with open(SENT_FILE, "w", encoding="utf-8") as fh:
        json.dump({k: v for k, v in sorted(sent.items()) if v["day"] >= since}, fh, ensure_ascii=False)


def run_once(dry_run: bool = False) -> dict:
    day, recs = records(*_load())
    summary = {"day": str(day) if day else None, "records": len(recs), "new": 0, "sent": 0, "failed": 0}
    if day is None:
        return summary
    sent = _load_sent()
    new = [x for x in recs if due(x, sent.get(_key(x)), x["day"])]
    new.sort(key=lambda x: (RANK[x["window"]], x["cat"], x["sub"] or "", list(HORIZONS).index(x["tf"]), x["side"]))
    summary["new"] = len(new)
    for x in new:
        msg = message(x)
        if dry_run:
            print(msg + "\n")
            continue
        if send(msg, NAME):
            summary["sent"] += 1
            sent[_key(x)] = {"day": x["day"].isoformat(), "rank": RANK[x["window"]]}
        else:
            summary["failed"] += 1
        time.sleep(1.1)                   # лимит Telegram — около сообщения в секунду на чат
    if not dry_run and new:
        _save_sent(sent, (day - timedelta(days=COOLDOWN_DAYS + 7)).isoformat())
    return summary


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="показать сообщения, ничего не слать")
    a = ap.parse_args()
    print(f"[{NAME}] {run_once(dry_run=a.dry_run)}")
