"""Сезонность → уведомление в @framesignalbot в день, когда у актива начинается сильный сезонный период.

Периоды считаются заранее, раз в год, и лежат файлом (SEASON_PERIODS_DIR/season_periods_<год>.json): сезонность от
дня к дню не меняется (Вадим 14.10: «можно сделать всё заранее и пользоваться понятными периодами»). Период — от
сезонного дна до пика (рост) или от пика до дна (снижение) средней кривой года без тренда по 10 прошлым годам
(signals/insights/detect.seasonal_curve и turning_points, как у сезонного детектора завода). Активы — индексы, валюты,
золото и акции со связью с фьючерсом (signals/insights/season.assets, правило витрины /hot).

Период берётся, если в те же календарные даты цена шла в его сторону минимум в MIN_SHARE лет выборки (лет не меньше
MIN_YEARS) и медиана хода по модулю от MIN_MOVE: на истории прода за 2016–2025 это ~50 периодов на ~35 активов за год,
около одного уведомления в неделю. В сообщении — актив, сторона, длина периода и его конец, «рост (падение) в N из M
последних лет» и ссылка на сезонность актива (/seasonality?ticker=…&chart=yearly).

Крон раз в день (`0 7 * * *`, 10:00 МСК); период, начавшийся в последние LOOKBACK_DAYS дней и ещё не отправленный,
догоняется. Отправленное помнит STATE_DIR/season_notify.sent.json.

    python3 -m signals.season_notify --list       # все периоды года
    python3 -m signals.season_notify --dry-run    # что ушло бы сегодня
    python3 -m signals.season_notify --rebuild    # пересчитать периоды года заново
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

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

from signals.framesignal import SITE, send  # noqa: E402
from signals.insights import detect as det  # noqa: E402
from signals.insights import fresh, season  # noqa: E402

NAME = "season_notify"
PERIODS_DIR = os.environ.get("SEASON_PERIODS_DIR", "/opt/frame/data")
SENT_FILE = os.path.join(fresh.STATE_DIR, f"{NAME}.sent.json")
SAMPLE_YEARS = 10                    # выборка — 10 прошлых лет, как у seasonal_curve
MIN_YEARS = 8                        # лет с ценой на весь период — не меньше
MIN_SHARE = 0.8                      # доля лет в сторону периода: 8 из 10
MIN_MOVE = 0.03                      # медиана хода по модулю
MIN_DAYS = 20                        # короче — не период, а шум кривой
LOOKBACK_DAYS = 3
MSK = timezone(timedelta(hours=3))


def _years_moves(s: pd.Series, start: int, length: int, year: int) -> list:
    """Ход цены от дня start (0 — 1 января) на length календарных дней в каждом году выборки."""
    out = []
    for y in range(year - SAMPLE_YEARS, year):
        d0 = pd.Timestamp(y, 1, 1) + pd.Timedelta(days=start)
        d1 = d0 + pd.Timedelta(days=length)
        if d0 < s.index[0] or d1 > s.index[-1]:
            continue
        i0 = s.index.searchsorted(d0, "right") - 1
        i1 = s.index.searchsorted(d1, "right") - 1
        out.append(float(s.iloc[i1] / s.iloc[i0] - 1))
    return out


def build(year: int) -> list:
    """Сильные сезонные периоды года по всем активам."""
    res = []
    for code, name, s in season.assets():
        c = det.seasonal_curve(s, year)
        if c is None:
            continue
        for d, kind, nd, _move in det.turning_points(c):
            up = kind == "дно"
            length = int((nd - d) % 366)
            if length < MIN_DAYS:
                continue
            moves = _years_moves(s, int(d), length, year)
            if len(moves) < MIN_YEARS:
                continue
            hits = sum((m > 0) if up else (m < 0) for m in moves)
            med = float(np.median(moves))
            if hits / len(moves) < MIN_SHARE or abs(med) < MIN_MOVE or (med > 0) != up:
                continue
            res.append({"code": code, "name": name, "dir": "up" if up else "down", "start": int(d), "days": length,
                        "hits": hits, "years": len(moves), "median": round(med, 4)})
    return sorted(res, key=lambda p: (p["start"], p["code"]))


def periods(year: int, rebuild: bool = False) -> list:
    path = os.path.join(PERIODS_DIR, f"season_periods_{year}.json")
    if not rebuild and os.path.exists(path):
        with open(path, encoding="utf-8") as fh:
            return json.load(fh)["periods"]
    ps = build(year)
    os.makedirs(PERIODS_DIR, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump({"year": year, "built_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                   "periods": ps}, fh, ensure_ascii=False, indent=1)
    return ps


def start_date(p: dict, year: int) -> date:
    return date(year, 1, 1) + timedelta(days=p["start"])


def _plural(n: int, one: str, few: str, many: str) -> str:
    if n % 10 == 1 and n % 100 != 11:
        return one
    if 2 <= n % 10 <= 4 and not 12 <= n % 100 <= 14:
        return few
    return many


def span(days: int) -> str:
    """«Следующие 4 месяца», «Следующие 3 недели»."""
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
    head = "🟢" if up else "🔻"
    what = "Сезонность: время для покупки" if up else "Сезонность: период снижения"
    hits = f"{'Рост' if up else 'Падение'} в {p['hits']} из {p['years']} последних лет"
    return (f"{head} <b>{html.escape(p['name'])}</b>\n{what}\n{span((end - today).days)}, до {end:%d.%m}\n{hits}\n"
            f'<a href="{html.escape(chart_url(p))}">График →</a>')


def _key(p: dict, start: date) -> str:
    return f"{start}|{p['code']}|{p['dir']}"


def due(ps: list, today: date) -> list:
    """Периоды, начавшиеся в последние LOOKBACK_DAYS дней: (период, дата начала). Начало в конце прошлого года
    тоже ловится — список прошлого года здесь не нужен, у периода тот же календарный день."""
    out = []
    for p in ps:
        for y in (today.year, today.year - 1):
            st = start_date(p, y)
            if 0 <= (today - st).days <= LOOKBACK_DAYS and (st + timedelta(days=p["days"]) - today).days >= 14:
                out.append((p, st))
    return out


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
    ps = periods(today.year)
    sent = _load_sent()
    new = [(p, st) for p, st in due(ps, today) if _key(p, st) not in sent]
    summary = {"today": str(today), "periods": len(ps), "new": len(new), "sent": 0, "failed": 0}
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


def print_list(year: int, rebuild: bool) -> None:
    ps = periods(year, rebuild)
    for p in ps:
        st = start_date(p, year)
        print(f"{p['name']:<28} {'рост   ' if p['dir'] == 'up' else 'падение'} {st:%d.%m} → "
              f"{st + timedelta(days=p['days']):%d.%m} ({p['days']} дн) {p['hits']}/{p['years']} "
              f"медиана {p['median']:+.1%}")
    print(f"всего {len(ps)}: рост {sum(p['dir'] == 'up' for p in ps)}, падение {sum(p['dir'] == 'down' for p in ps)}, "
          f"активов {len({p['code'] for p in ps})}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="показать, что ушло бы сегодня, ничего не слать")
    ap.add_argument("--list", action="store_true", help="все периоды года")
    ap.add_argument("--rebuild", action="store_true", help="пересчитать периоды года заново")
    ap.add_argument("--today", help="дата прогона ГГГГ-ММ-ДД (проверка)")
    a = ap.parse_args()
    t = date.fromisoformat(a.today) if a.today else None
    if a.list:
        print_list((t or datetime.now(MSK).date()).year, a.rebuild)
    else:
        if a.rebuild:
            periods((t or datetime.now(MSK).date()).year, rebuild=True)
        print(f"[{NAME}] {run_once(dry_run=a.dry_run, today=t)}")
