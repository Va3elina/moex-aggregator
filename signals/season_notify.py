"""Сезонность → уведомление в @framesignalbot в день, когда у актива начинается сильный сезонный период.

Правило — то же, что у витрины /hot и сезонности завода (signals/insights/season.py): следующие 3 месяца (91 день
от той же даты) шли в одну сторону минимум в 75% лет (лет не меньше 10), медиана хода по модулю от 3%. Уведомление —
в день, когда актив ВХОДИТ в условие (накануне не выполнялось), и не чаще раза в 30 дней на актив. Всё, что в
сообщении, читатель видит на сайте. Прежние периоды «от сезонного дна до пика» строились по средней кривой года без
тренда, которой на сайте нет, — убраны (Вадим 26.10).

Активы — индексы, валюты, золото и акции со связью с фьючерсом (season.assets). В сообщении — актив, сторона, конец
периода, «рост (падение) в N из M лет» и ссылка на сезонность актива (/seasonality?ticker=…&chart=yearly).

Крон раз в день (`0 7 * * *`, 10:00 МСК); вход в условие за последние LOOKBACK_DAYS дней, ещё не отправленный,
догоняется. Отправленное помнит STATE_DIR/season_notify.sent.json.

    python3 -m signals.season_notify --list       # входы с начала года
    python3 -m signals.season_notify --dry-run    # что ушло бы сегодня
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
from datetime import date, datetime, timedelta, timezone  # noqa: E402

import pandas as pd  # noqa: E402

from signals.framesignal import SITE, send  # noqa: E402
from signals.insights import fresh, season  # noqa: E402

NAME = "season_notify"
SENT_FILE = os.path.join(fresh.STATE_DIR, f"{NAME}.sent.json")
LOOKBACK_DAYS = 3
MSK = timezone(timedelta(hours=3))


def entries(since: date, until: date) -> list:
    """Входы в условие /hot по всем активам за [since, until]: (период, дата начала). Пауза между входами считается
    с запасом до окна — как у детектора завода (detect.detect_seasonality_all)."""
    out = []
    lo = pd.Timestamp(since) - pd.Timedelta(days=4 * season.COOLDOWN)
    for code, name, s in season.assets():
        for d, c in season.entries(s, lo, pd.Timestamp(until)):
            if d.date() < since:
                continue
            out.append(({"code": code, "name": name, "dir": "up" if c["rising"] else "down", "days": season.DAYS,
                         "hits": c["hits"], "years": c["n"], "median": round(c["med"] / 100, 4)}, d.date()))
    return sorted(out, key=lambda e: (e[1], e[0]["code"]))


def _plural(n: int, one: str, few: str, many: str) -> str:
    if n % 10 == 1 and n % 100 != 11:
        return one
    if 2 <= n % 10 <= 4 and not 12 <= n % 100 <= 14:
        return few
    return many


def span(days: int) -> str:
    """«Следующие 3 месяца», «Следующие 3 недели»."""
    if days >= 45:
        n = max(2, round(days / 30.44))
        return f"Следующие {n} {_plural(n, 'месяц', 'месяца', 'месяцев')}"
    n = max(2, round(days / 7))
    return f"Следующие {n} {_plural(n, 'неделя', 'недели', 'недель')}"


def chart_url(p: dict) -> str:
    return f"{SITE}/seasonality?" + urllib.parse.urlencode({"ticker": p["code"], "chart": "yearly"})


def message(p: dict, start: date, today: date) -> str:
    up = p["dir"] == "up"
    end = start + timedelta(days=p["days"])
    head = "📅"                      # сезонность — календарь, сторона словами (Вадим 26.10)
    what = "Сезонность: время для покупки" if up else "Сезонность: период снижения"
    hits = f"{'Рост' if up else 'Падение'} в {p['hits']} из {p['years']} лет"
    return (f"{head} <b>{html.escape(p['name'])}</b>\n{what}\n{span((end - today).days)}, до {end:%d.%m}\n{hits}\n"
            f'<a href="{html.escape(chart_url(p))}">График →</a>')


def _key(p: dict, start: date) -> str:
    return f"{start}|{p['code']}|{p['dir']}"


def due(today: date) -> list:
    """Входы за последние LOOKBACK_DAYS дней, у которых до конца периода ещё не меньше двух недель."""
    return [(p, st) for p, st in entries(today - timedelta(days=LOOKBACK_DAYS), today)
            if (st + timedelta(days=p["days"]) - today).days >= 14]


def _load_sent() -> set:
    try:
        with open(SENT_FILE, encoding="utf-8") as fh:
            return set(json.load(fh))
    except FileNotFoundError:
        return set()


def _save_sent(sent: set, today: date) -> None:
    os.makedirs(fresh.STATE_DIR, exist_ok=True)
    keep = sorted(k for k in sent if k[:10] >= str(today - timedelta(days=400)))
    with open(SENT_FILE, "w", encoding="utf-8") as fh:
        json.dump(keep, fh, ensure_ascii=False)


def run_once(dry_run: bool = False, today: date | None = None) -> dict:
    today = today or datetime.now(MSK).date()
    sent = _load_sent()
    new = [(p, st) for p, st in due(today) if _key(p, st) not in sent]
    summary = {"today": str(today), "new": len(new), "sent": 0, "failed": 0}
    for p, st in new:
        msg = message(p, st, today)
        if dry_run:
            print(msg + "\n")
            continue
        if send(msg, NAME):
            summary["sent"] += 1
            sent.add(_key(p, st))
        else:
            summary["failed"] += 1
        time.sleep(1.1)
    if not dry_run and new:
        _save_sent(sent, today)
    return summary


def print_list(today: date) -> None:
    es = entries(date(today.year, 1, 1), today)
    for p, st in es:
        print(f"{p['name']:<28} {'рост   ' if p['dir'] == 'up' else 'падение'} {st:%d.%m} → "
              f"{st + timedelta(days=p['days']):%d.%m} {p['hits']}/{p['years']} медиана {p['median']:+.1%}")
    print(f"всего {len(es)}: рост {sum(p['dir'] == 'up' for p, _ in es)}, "
          f"падение {sum(p['dir'] == 'down' for p, _ in es)}, активов {len({p['code'] for p, _ in es})}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="показать, что ушло бы сегодня, ничего не слать")
    ap.add_argument("--list", action="store_true", help="входы в условие с начала года")
    ap.add_argument("--today", help="дата прогона ГГГГ-ММ-ДД (проверка)")
    a = ap.parse_args()
    t = date.fromisoformat(a.today) if a.today else None
    if a.list:
        print_list(t or datetime.now(MSK).date())
    else:
        print(f"[{NAME}] {run_once(dry_run=a.dry_run, today=t)}")
