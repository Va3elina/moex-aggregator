"""Итоги дня всех роботов одним сообщением (root, cron раз в день в 21:00 МСК).

По каждому роботу: результат закрытых сегодня сделок, сколько открыто, итог с начала.
Рынок РФ — журналы гибридных роботов (/opt/hybrid-bot/state_*/journal.csv, рубли, песочница Т-Инвестиций);
крипта — журнал Kamaz (/opt/kamaz-bot/bot/state/journal/*.jsonl, события «кампания закрыта», доллары, демо Bybit).
"""
from __future__ import annotations

import csv
import datetime as dt
import json
import pathlib
import sys
from zoneinfo import ZoneInfo

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import robots_tg as TG  # noqa: E402

MSK = ZoneInfo("Europe/Moscow")
RU = [("Валюты", pathlib.Path("/opt/hybrid-bot/state_fx/journal.csv")), ("Пятёрка", pathlib.Path("/opt/hybrid-bot/state_five1/journal.csv"))]
CRYPTO = pathlib.Path("/opt/kamaz-bot/bot/state/journal")
CRYPTO_STATUS = pathlib.Path("/opt/kamaz-bot/bot/state/status.txt")


def ddmm(iso: str) -> str:
    return dt.date.fromisoformat(iso[:10]).strftime("%d.%m")


def msk_date(t: str) -> dt.date:
    return dt.datetime.fromisoformat(str(t)[:19]).replace(tzinfo=dt.timezone.utc).astimezone(MSK).date()


def n_deals(n: int) -> str:
    return TG.plural(n, "сделка", "сделки", "сделок")


def short(x: float, cur: str) -> str:
    return TG.signed(x) if cur == "₽" else TG.usd(x)


def pct(x: float, base: float | None) -> str:
    return f" {TG.pct(100 * x / base)}" if base else ""


def fnum(x) -> float | None:
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def ru_line(name: str, path: pathlib.Path, day: dt.date) -> str:
    """День — % от капитала на начало дня; всего — % от стартового капитала (капитал в день первого входа)."""
    if not path.exists():
        return f"<b>{name}</b> журнала нет"
    rows = list(csv.DictReader(path.open()))
    eqs = [e for e in (fnum(r.get("equity")) for r in rows) if e]
    start = eqs[0] if eqs else None
    closed = [r for r in rows if r.get("pnl_rub") not in (None, "")]
    total = sum(float(r["pnl_rub"]) for r in closed)
    today = [r for r in closed if r.get("d_out") == day.isoformat()]
    t = sum(float(r["pnl_rub"]) for r in today)
    eq_day = max((fnum(r.get("equity")) or 0 for r in today), default=0) or None
    part = f"{TG.rub(t)}{pct(t, eq_day)}, " if today else ""
    return f"{TG.dot(t) if today else TG.icon('нет сделок')} <b>{name}</b> {part}всего {TG.rub(total)}{pct(total, start)}"


def crypto_line(day: dt.date) -> str:
    ev = []
    for f in sorted(CRYPTO.glob("*.jsonl")):
        for line in f.read_text().splitlines():
            if '"кампания закрыта"' in line:
                try:
                    ev.append(json.loads(line))
                except Exception:
                    pass
    closed = [e for e in ev if e.get("kind") == "кампания закрыта" and e.get("src") == "реал"]
    total = sum(float(e.get("result") or 0) for e in closed)
    today = [float(e.get("result") or 0) for e in closed if msk_date(e.get("at")) == day]
    b = None
    try:
        import re
        m = re.search(r"бюджет \$(\d+)", CRYPTO_STATUS.read_text().splitlines()[0])
        b = float(m.group(1)) if m else None
    except Exception:
        pass
    part = f"{TG.usd(sum(today))}{pct(sum(today), b)}, " if today else ""
    return f"{TG.dot(sum(today)) if today else TG.icon('нет сделок')} <b>Крипта</b> {part}всего {TG.usd(total)}{pct(total, b)}"


def text(day: dt.date) -> str:
    L = [f"{TG.icon('итоги')} <b>Итоги {day:%d.%m}</b>"]
    for name, path in RU:
        L.append(ru_line(name, path, day))
    L.append(crypto_line(day))
    return "\n".join(L)


if __name__ == "__main__":
    day = dt.datetime.now(MSK).date()
    if len(sys.argv) > 1 and sys.argv[1] == "--print":
        print(text(day))
    else:
        TG.send(text(day))
