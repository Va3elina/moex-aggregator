"""Рекорды движка находок → короткая сводка в @framesignalbot (личка админа).

Те же находки, что видит завод постов (signals/insight_scan.detections — общий кэш на день данных), но без отбора
в посты: каждая «рекорд_или_экстремум» — значение, какого не было год и больше (функция record в
signals/insights/detect.py): ноги позиций физлиц (покупки, шорт, число физлиц в лонге и шорте), чистая позиция,
цена индексов, доллара, золота и минимумы акций, потоки фондов, широта, Баффетт. Фильтры завода те же: малоактивные
контракты и дни экспирации отсеяны.

Крон каждый час, как insight_scan (`45 7-17 * * *`); сканер ждёт дневные позиции за последний торговый день
(signals/insights/fresh.py). Отправленное помнит файл в STATE_DIR: рекорд за день уходит один раз, а запоздавшие
находки (потоки фондов приходят на день-два позже) догоняются следующим днём данных.

    python3 -m signals.record_notify --dry-run    # показать сообщение, ничего не слать
    python3 -m signals.record_notify --force      # прогнать сейчас, не дожидаясь свежего дня
"""
import os

_url = os.environ.get("DB_URL", "")
if "@db:" in _url:
    os.environ["DB_URL"] = _url.replace("@db:", "@127.0.0.1:")

import argparse  # noqa: E402
import html  # noqa: E402
import json  # noqa: E402
from datetime import timedelta  # noqa: E402

import pandas as pd  # noqa: E402
import requests  # noqa: E402
from dotenv import load_dotenv  # noqa: E402

from signals.insights import data, fresh  # noqa: E402
from signals.insight_scan import detections, drop_expiry_days, drop_low_activity  # noqa: E402

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(_ROOT, ".env"))

NAME = "record_notify"
SENT_FILE = os.path.join(fresh.STATE_DIR, f"{NAME}.sent.json")
LOOKBACK_DAYS = 7          # запоздавшие находки (фонды) старше недели уже не новость
FAMILY_ORDER = ["позиции", "цена", "фонды", "сделки_фондов", "широта", "баффетт", "сезонность"]
FAMILY_LABEL = {"позиции": "Позиции физлиц", "цена": "Цена", "фонды": "Деньги в фондах",
                "сделки_фондов": "Сделки фондов", "широта": "Широта рынка", "баффетт": "Индикатор Баффетта",
                "сезонность": "Сезонность"}
MAX_LEN = 4000


def _key(x: dict) -> str:
    f = x.get("facts") or {}
    return "|".join(str(v) for v in (x["date"], x["family"], x["instrument"], f.get("leg"), f.get("hi")))


def _load_sent() -> set | None:
    try:
        with open(SENT_FILE, encoding="utf-8") as fh:
            return set(json.load(fh))
    except FileNotFoundError:
        return None


def _save_sent(keys: set, since: str) -> None:
    os.makedirs(fresh.STATE_DIR, exist_ok=True)
    keep = sorted(k for k in keys if k[:10] >= since)
    with open(SENT_FILE, "w", encoding="utf-8") as fh:
        json.dump(keep, fh, ensure_ascii=False)


def records(until: pd.Timestamp) -> list:
    items = drop_expiry_days(drop_low_activity(detections(until)))
    since = (until - timedelta(days=LOOKBACK_DAYS)).date().isoformat()
    return [x for x in items if x["type"] == "рекорд_или_экстремум" and x["date"] >= since]


def _line(x: dict, until_iso: str) -> str:
    f = x.get("facts") or {}
    mark = "🔺" if f.get("hi", True) else "🔻"
    late = "" if x["date"] == until_iso else f" <i>({pd.Timestamp(x['date']):%d.%m})</i>"
    return f"{mark} {html.escape(x['title'])}{late}"


def messages(recs: list, until: pd.Timestamp) -> list:
    until_iso = until.date().isoformat()
    head = f"<b>Рекорды за {until:%d.%m.%Y}</b> · {len(recs)}"
    order = {f: i for i, f in enumerate(FAMILY_ORDER)}
    blocks = []
    for fam in sorted({x["family"] for x in recs}, key=lambda f: order.get(f, 99)):
        rows = sorted((x for x in recs if x["family"] == fam), key=lambda x: -x["score"])
        blocks.append([f"\n<b>{FAMILY_LABEL.get(fam, fam)}</b>"] + [_line(x, until_iso) for x in rows])
    out, cur = [], head
    for b in blocks:
        for ln in b:
            if len(cur) + len(ln) + 1 > MAX_LEN:
                out.append(cur)
                cur = head + " (продолжение)"
            cur += "\n" + ln
    out.append(cur)
    return out


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
    oi = data.read("oi_daily", parse_dates=["tradedate"])
    until = oi.tradedate.max()
    recs = records(until)
    sent = _load_sent()
    if sent is None:                      # первый запуск: прошлые дни не досылаем, только последний день данных
        sent = {_key(x) for x in recs if x["date"] < until.date().isoformat()}
    new = [x for x in recs if _key(x) not in sent]
    summary = {"data_until": str(until.date()), "records": len(recs), "new": len(new), "sent": 0}
    if not new:
        if not dry_run:
            _save_sent(sent, (until - timedelta(days=LOOKBACK_DAYS + 7)).date().isoformat())
        return summary
    for msg in messages(new, until):
        if dry_run:
            print(msg + "\n")
        elif send(msg):
            summary["sent"] += 1
    if not dry_run and summary["sent"]:
        sent |= {_key(x) for x in new}
        _save_sent(sent, (until - timedelta(days=LOOKBACK_DAYS + 7)).date().isoformat())
    return summary


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="показать сообщение, ничего не слать")
    ap.add_argument("--force", action="store_true", help="прогнать сейчас: не ждать свежий день и не смотреть отметку")
    a = ap.parse_args()
    why = None if (a.dry_run or a.force) else fresh.wait_reason(NAME)
    if why:
        print(f"[{NAME}] пропуск: {why}")
    else:
        s = run_once(dry_run=a.dry_run)
        if not a.dry_run and (s["sent"] or not s["new"]):
            fresh.mark_done(NAME, s["data_until"])
        print(f"[{NAME}] {s}")
