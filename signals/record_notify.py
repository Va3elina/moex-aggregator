"""Рекорды позиций физлиц → отдельное сообщение на каждый в @framesignalbot (личка админа).

Ряды те же, что у движка находок завода постов (signals/insights/detect.load_positions): дневные позиции физлиц
по фьючерсам. Четыре показателя — объём лонга, объём шорта, число физлиц в лонге, число физлиц в шорте. Рекорд —
максимум показателя, какого не было 3 месяца, год или за всё время (та же since_date, что у record в detect.py;
у завода нижний порог ~4 месяца, поэтому окна считаются здесь). Вселенная и фильтры как у завода: группы
Акции/Валюта/Индексы/Сырьё, медиана участников за год ≥ MIN_PART, история от года, без малоактивных контрактов
скринера и без дней экспирации ±1. В сообщении — только актив, показатель и окно рекорда, без цифр
(Вадим 13.10: «цифры не нужны»).

Крон каждый час, как insight_scan (`45 7-17 * * *`); сканер ждёт дневные позиции за последний торговый день
(signals/insights/fresh.py). Отправленное помнит файл в STATE_DIR: рекорд уходит один раз.

    python3 -m signals.record_notify --dry-run    # показать сообщения, ничего не слать
    python3 -m signals.record_notify --force      # прогнать сейчас, не дожидаясь свежего дня
"""
import os

_url = os.environ.get("DB_URL", "")
if "@db:" in _url:
    os.environ["DB_URL"] = _url.replace("@db:", "@127.0.0.1:")

import argparse  # noqa: E402
import html  # noqa: E402
import json  # noqa: E402
import time  # noqa: E402
from datetime import timedelta  # noqa: E402

import pandas as pd  # noqa: E402
import requests  # noqa: E402
from dotenv import load_dotenv  # noqa: E402

from api.database import SessionLocal  # noqa: E402
from signals.insights import detect as det  # noqa: E402
from signals.insights import fresh  # noqa: E402
from signals.insights.expiry import near_expiry  # noqa: E402

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(_ROOT, ".env"))

NAME = "record_notify"
SENT_FILE = os.path.join(fresh.STATE_DIR, f"{NAME}.sent.json")
GROUPS = ("Акции", "Валюта", "Индексы", "Сырьё")
MIN_HISTORY = 260                     # торговых дней: на короткой истории «за всё время» ничего не значит
# показатель → подпись; цифр в сообщении нет (Вадим 13.10: «цифры не нужны»)
LEGS = {"long": "Объём лонга физлиц", "short": "Объём шорта физлиц",
        "nl": "Число физлиц в лонге", "ns": "Число физлиц в шорте"}
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


def records() -> tuple[pd.Timestamp, list]:
    """Рекорды на последний день данных: [{sec, name, leg, window, value}]."""
    P = det.load_positions()
    names, groups, _ = det.instruments()
    until = P["npart"].index.max()
    if near_expiry(until):
        return until, []
    low = _low_activity()
    part = P["npart"]
    year = part.loc[part.index >= until - pd.Timedelta(days=365)]
    out = []
    for s in part.columns:
        if groups.get(s) not in GROUPS or s in low or not (year[s].median() >= det.MIN_PART):
            continue
        for leg in LEGS:
            ser = P[leg][s].dropna()
            if len(ser) < MIN_HISTORY or ser.index[-1] != until:
                continue
            arr, dates = ser.values.astype(float), ser.index
            i = len(arr) - 1
            if arr[i] <= 0:
                continue
            u = det.since_date(arr, dates, i, True)
            if u is None:
                window = "за всё время"
            else:
                gap = (until - u).days
                window = next((w for d, w in WINDOWS if gap >= d), None)
            if window:
                out.append({"sec": s, "name": names.get(s, s), "leg": leg, "window": window,
                            "value": float(arr[i])})
    out.sort(key=lambda x: (RANK[x["window"]], x["name"], list(LEGS).index(x["leg"])))
    return until, out


def _key(until, x: dict) -> str:
    return f"{until.date()}|{x['sec']}|{x['leg']}"


def message(x: dict) -> str:
    return f"🔺 <b>{html.escape(x['name'])}</b>\n{LEGS[x['leg']]}\nРекорд {x['window']}"


def _load_sent() -> set:
    try:
        with open(SENT_FILE, encoding="utf-8") as fh:
            return set(json.load(fh))
    except FileNotFoundError:
        return set()


def _save_sent(keys: set, since: str) -> None:
    os.makedirs(fresh.STATE_DIR, exist_ok=True)
    with open(SENT_FILE, "w", encoding="utf-8") as fh:
        json.dump(sorted(k for k in keys if k[:10] >= since), fh, ensure_ascii=False)


def send(text: str) -> bool:
    tok = os.environ.get("ALERT_BOT_TOKEN")
    chats = [c.strip() for c in os.environ.get("ADMIN_CHAT_ID", "").split(",") if c.strip()]
    if not tok or not chats:
        print(f"[{NAME}] нет ALERT_BOT_TOKEN или ADMIN_CHAT_ID — не отправлено")
        return False
    root = os.environ.get("TELEGRAM_API_ROOT", "https://api.telegram.org").rstrip("/")
    ok = False
    for chat in chats:
        try:
            r = requests.post(f"{root}/bot{tok}/sendMessage", timeout=20, data={
                "chat_id": chat, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True})
            if r.json().get("ok"):
                ok = True
            else:
                print(f"[{NAME}] чат {chat}: не принято: {r.text[:200].replace(tok, '<REDACTED>')}")
        except requests.RequestException as e:
            print(f"[{NAME}] чат {chat}: {str(e).replace(tok, '<REDACTED>')}")
    return ok


def run_once(dry_run: bool = False) -> dict:
    until, recs = records()
    sent = _load_sent()
    new = [x for x in recs if _key(until, x) not in sent]
    summary = {"data_until": str(until.date()), "records": len(recs), "new": len(new), "sent": 0, "failed": 0}
    for x in new:
        msg = message(x)
        if dry_run:
            print(msg + "\n")
            continue
        if send(msg):
            summary["sent"] += 1
            sent.add(_key(until, x))
        else:
            summary["failed"] += 1
        time.sleep(1.1)                   # лимит Telegram — около сообщения в секунду на чат
    if not dry_run:
        _save_sent(sent, (until - timedelta(days=14)).date().isoformat())
    return summary


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="показать сообщения, ничего не слать")
    ap.add_argument("--force", action="store_true", help="прогнать сейчас: не ждать свежий день и не смотреть отметку")
    a = ap.parse_args()
    why = None if (a.dry_run or a.force) else fresh.wait_reason(NAME)
    if why:
        print(f"[{NAME}] пропуск: {why}")
    else:
        s = run_once(dry_run=a.dry_run)
        if not a.dry_run and not s["failed"]:
            fresh.mark_done(NAME, s["data_until"])
        print(f"[{NAME}] {s}")
