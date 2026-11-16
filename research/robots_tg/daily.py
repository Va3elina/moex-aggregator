"""Итоги дня всех роботов одним сообщением (root, cron раз в день в 21:00 МСК).

По каждому роботу: результат закрытых сегодня сделок, сколько открыто, итог с начала.
Рынок РФ — журналы гибридных роботов (/opt/hybrid-bot/state_*/journal.csv, рубли, песочница Т-Инвестиций);
крипта — журналы трёх стратегий Kamaz (/opt/kamaz-bot/bot/state*/journal/*.jsonl, события «кампания закрыта», доллары,
у каждой свой демо-счёт Bybit) и строка сравнения трёх с общего старта (первый демо-старт soft3/hold);
Kamaz бумага — бумажные книги A (state) и B (state_b) с первого старта B, события «цель»/«таймер» с src=бумага.
Скринер VB — робот robots/vb_bot.py (/opt/vb-bot/state: journal.csv вход/выход по ногам, state.json открытые ноги),
песочница Т-Инвестиций, 10 млн, плечо ×3; результат ноги = (выход − вход) × бумаг × сторона − комиссия 0,05 % за сторону.
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
CRYPTO_FEEDS = [("Крипта A", pathlib.Path("/opt/kamaz-bot/bot/state")), ("Крипта soft3", pathlib.Path("/opt/kamaz-bot/bot/state_soft3")),
                ("Крипта hold", pathlib.Path("/opt/kamaz-bot/bot/state_hold"))]
CRYPTO = CRYPTO_FEEDS[0][1] / "journal"               # стратегия A (для строки бумажных книг A/B)
PAPER_B = pathlib.Path("/opt/kamaz-bot/bot/state_b")  # бумажная книга варианта B (служба kamaz-bot-paper-b)
PAPER_CAP = 100_000  # бумажные книги: 10 ячеек по $10 000
VB = pathlib.Path("/opt/vb-bot/state")   # робот скринера VB
VB_CAP = 10_000_000


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


def vb_line(day: dt.date) -> str:
    """Скринер VB: закрытые сегодня ноги, итог с запуска робота (% от 10 млн), сколько открыто."""
    j = VB / "journal.csv"
    st = json.loads((VB / "state.json").read_text()) if (VB / "state.json").exists() else {}
    n_open = len(st.get("legs", {}))
    pnl = []                                                      # (день выхода, ₽)
    if j.exists():
        rows = list(csv.DictReader(j.open(encoding="utf-8")))
        ins = {r["key"]: r for r in rows if r["action"] == "вход"}
        for r in rows:
            a = ins.get(r["key"])
            if r["action"] != "выход" or not a or not fnum(r["price"]) or not fnum(a["price"]):
                continue
            q, side, pi, po = float(a["qty"]), int(a["side"]), float(a["price"]), float(r["price"])
            pnl.append((dt.date.fromisoformat(r["ts"][:10]), side * q * (po - pi) - 0.0005 * q * (pi + po)))
    total = sum(x for _, x in pnl)
    today = [x for d, x in pnl if d == day]
    t = sum(today)
    part = f"{TG.rub(t)}{pct(t, VB_CAP)}, " if today else ""
    op = f", открыто {n_open}" if n_open else ""
    since = f" с {ddmm(st['start'])}" if st.get("start") else ""
    return f"{TG.dot(t) if today else TG.icon('нет сделок')} <b>Скринер VB ×3</b> {part}всего{since} {TG.rub(total)}{pct(total, VB_CAP)}{op}"


def crypto_closed(d: pathlib.Path) -> list[dict]:
    ev = []
    for f in sorted((d / "journal").glob("*.jsonl")):
        for line in f.read_text().splitlines():
            if '"кампания закрыта"' in line or '"kind": "старт"' in line:
                try:
                    ev.append(json.loads(line))
                except Exception:
                    pass
    return ev


def crypto_status(d: pathlib.Path) -> tuple[float | None, int]:
    """Бюджет и сколько настоящих кампаний открыто — из статуса бота."""
    import re
    try:
        s = (d / "status.txt").read_text()
        m = re.search(r"бюджет \$(\d+)", s.splitlines()[0]); n = re.search(r"открыто кампаний (\d+)", s)
        return (float(m.group(1)) if m else None, int(n.group(1)) if n else 0)
    except Exception:
        return None, 0


def crypto_line(name: str, d: pathlib.Path, day: dt.date) -> str:
    closed = [e for e in crypto_closed(d) if e.get("kind") == "кампания закрыта" and e.get("src") == "реал"]
    total = sum(float(e.get("result") or 0) for e in closed)
    today = [float(e.get("result") or 0) for e in closed if msk_date(e.get("at")) == day]
    b, n_open = crypto_status(d)
    part = f"{TG.usd(sum(today))}{pct(sum(today), b)}, " if today else ""
    op = f", открыто {n_open}" if n_open else ""
    return f"{TG.dot(sum(today)) if today else TG.icon('нет сделок')} <b>{name}</b> {part}всего {TG.usd(total)}{pct(total, b)}{op}"


def crypto_compare_line() -> str:
    """Три стратегии с общего старта (первый демо-старт soft3/hold): итог закрытых в $ и % от бюджета."""
    starts = [e["at"] for _, d in CRYPTO_FEEDS[1:] for e in crypto_closed(d) if e.get("kind") == "старт" and e.get("mode") == "demo" and e.get("at")]
    if not starts:
        return ""
    t0 = min(starts)
    parts = []
    for name, d in CRYPTO_FEEDS:
        cl = [e for e in crypto_closed(d) if e.get("kind") == "кампания закрыта" and e.get("src") == "реал" and ts(e.get("at")) >= ts(t0)]
        s = sum(float(e.get("result") or 0) for e in cl); b, _ = crypto_status(d)
        parts.append(f"{name.split()[-1]} {TG.usd(s)}{pct(s, b)} ({len(cl)})")
    return f"{TG.icon('итоги')} <b>Kamaz три стратегии</b> с {ddmm(t0)}: " + ", ".join(parts)


def ts(t) -> dt.datetime:
    return dt.datetime.fromisoformat(str(t)[:19].replace(" ", "T"))


def paper_lines(d: pathlib.Path) -> list[dict]:
    rows = []
    for f in sorted((d / "journal").glob("*.jsonl")):
        for line in f.read_text().splitlines():
            try:
                rows.append(json.loads(line))
            except Exception:
                pass
    return rows


def paper_ab_line(day: dt.date) -> str:
    """Бумажные книги Kamaz: рабочая A и вариант B (вход внутри минутки, цель 3.9/3.0) — с первого старта B, ячейки по $10 000."""
    if not (PAPER_B / "journal").exists():
        return ""
    B = paper_lines(PAPER_B)
    t0 = min((r["at"] for r in B if r.get("kind") == "старт" and r.get("at")), default=None)
    if not t0:
        return ""
    res = {}
    for name, rows in (("A", paper_lines(CRYPTO.parent)), ("B", B)):
        cl = [r for r in rows if r.get("src") == "бумага" and r.get("kind") in ("цель", "таймер") and ts(r.get("t")) >= ts(t0)]
        res[name] = (sum(float(r.get("pnl") or 0) for r in cl if msk_date(r["t"]) == day), sum(float(r.get("pnl") or 0) for r in cl),
                     sum(1 for r in cl if msk_date(r["t"]) == day))
    (at, att, an), (bt, btt, bn) = res["A"], res["B"]
    day_part = f"A {TG.usd(at)} · B {TG.usd(bt)} (B−A {TG.usd(bt - at)}), " if an or bn else ""
    head = TG.dot(bt) if bn else TG.icon("нет сделок")
    return (f"{head} <b>Kamaz бумага</b> {day_part}"
            f"с {ddmm(t0)} A {TG.usd(att)}{pct(att, PAPER_CAP)} · B {TG.usd(btt)}{pct(btt, PAPER_CAP)}")


def text(day: dt.date) -> str:
    L = [f"{TG.icon('итоги')} <b>Итоги {day:%d.%m}</b>"]
    for name, path in RU:
        L.append(ru_line(name, path, day))
    try:
        L.append(vb_line(day))
    except Exception as e:  # новый робот не должен ронять сводку
        L.append(f"<b>Скринер VB</b>: ошибка {TG.esc(e)}")
    for name, d in CRYPTO_FEEDS:
        try:
            L.append(crypto_line(name, d, day))
        except Exception as e:  # одна стратегия не должна ронять сводку
            L.append(f"<b>{name}</b>: ошибка {TG.esc(e)}")
    try:
        cmp = crypto_compare_line()
    except Exception as e:
        cmp = f"Kamaz три стратегии: ошибка {TG.esc(e)}"
    if cmp:
        L.append(cmp)
    try:
        ab = paper_ab_line(day)
    except Exception as e:  # строка сравнения не должна ронять сводку
        ab = f"Kamaz бумага A/B: ошибка {TG.esc(e)}"
    if ab:
        L.append(ab)
    return "\n".join(L)


if __name__ == "__main__":
    day = dt.datetime.now(MSK).date()
    if len(sys.argv) > 1 and sys.argv[1] == "--print":
        print(text(day))
    else:
        TG.send(text(day))
