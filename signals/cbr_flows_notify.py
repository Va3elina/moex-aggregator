"""«Потоки капитала» (ОРФР Банка России, /cbr-flows) → @framesignalbot (Вадим 18.11.2026) — в формате поста
t.me/FrameTool/205: карусель «Потоки капитала · месяц» (обложка, «Кто покупал акции», «Кто покупал ОФЗ»,
«Главное за месяц») и выводы текстом.

Повод — в базе появился новый месяц (cbr_flows, period_kind='month'; данные заливаются вручную из обзора ЦБ —
скилл moex-cbr-flows). Сайт ЦБ сам не опрашиваем (правило «без автоскачивания»).

  • строки акций и ОФЗ — покупки минус продажи групп участников за месяц, мелко — прошлый месяц (как в посте);
  • «Главное за месяц» — наши индексы за тот же месяц: индекс МосБиржи, лучшая и худшая отрасль, доллар, индекс
    гособлигаций (в посте были цифры из PDF обзора — их машина не читает, поэтому заголовок честный: не «из обзора»);
  • выводы — правилами по самим потокам: физлица в акциях к прошлому месяцу и крупнейшие покупки/продажи, с отметкой
    «больше, чем за любой месяц за N», если это так.
Валютный раздел ЦБ заморожен с апреля 2026 — не берём.

Состояние — STATE_DIR/cbr_flows_notify.state.json (последний отправленный месяц). Первый прогон запоминает молча.

    python3 -m signals.cbr_flows_notify --dry-run                # показать (CARD_DIR — картинки)
    python3 -m signals.cbr_flows_notify --month 2026-10 --dry-run
"""
from __future__ import annotations

import os

_url = os.environ.get("DB_URL", "")
if "@db:" in _url:
    os.environ["DB_URL"] = _url.replace("@db:", "@127.0.0.1:")

import argparse  # noqa: E402
import datetime as dt  # noqa: E402
import json  # noqa: E402

from sqlalchemy import text  # noqa: E402

from signals.framesignal import SITE, send, send_slides  # noqa: E402
from signals.record_card import render  # noqa: E402

NAME = "cbr_flows_notify"
STATE_DIR = os.environ.get("SCAN_STATE_DIR", "/opt/frame/data/scan_state")
STATE_FILE = os.path.join(STATE_DIR, f"{NAME}.state.json")
NOM = ["январь", "февраль", "март", "апрель", "май", "июнь", "июль", "август", "сентябрь", "октябрь", "ноябрь", "декабрь"]
PREP = ["январе", "феврале", "марте", "апреле", "мае", "июне", "июле", "августе", "сентябре", "октябре", "ноябре", "декабре"]
# группы участников — короткие имена поста 205
SHORT = {"Физические лица": "Физлица", "Нефинансовые организации": "Компании", "Прочие Банки": "Прочие банки",
         "Нерезиденты": "Нерезиденты", "НФО": "НФО", "СЗКО": "Системные банки", "Доверительное управление": "Управляющие"}
INSTR = {"stocks": "акции", "ofz": "ОФЗ"}
INSTR_ACC = {"stocks": "акций", "ofz": "ОФЗ"}
SECTORS = {"MOEXOG": "Нефть и газ", "MOEXFN": "Финансы", "MOEXMM": "Металлы и добыча", "MOEXEU": "Электроэнергетика",
           "MOEXTL": "Телекомы", "MOEXCN": "Потребительский сектор", "MOEXCH": "Химия", "MOEXTN": "Транспорт",
           "MOEXRE": "Строители", "MOEXIT": "IT"}


def fmt(v: float, nd: int = 1) -> str:
    v = round(v, nd) + 0.0                      # −0,04 → «0,0», а не «−0,0»
    s = f"{abs(v):.{nd}f}".replace(".", ",")
    return ("+" if v > 0 else "−" if v < 0 else "") + s


def load_flows(db) -> dict:
    """{инструмент: {группа: {дата конца месяца: млрд ₽}}} по акциям и ОФЗ."""
    out: dict = {}
    for t, d, c, v in db.execute(text("""
        SELECT instrument_type, period_end_date, category, value FROM cbr_flows
        WHERE period_kind = 'month' AND instrument_type IN ('stocks', 'ofz') ORDER BY period_end_date""")):
        out.setdefault(t, {}).setdefault(c, {})[d.isoformat()] = float(v)
    return out


def months_since(series: dict, day: str, v: float) -> int | None:
    """Сколько месяцев подряд до day не было значения того же знака больше по модулю (None — за всю историю)."""
    past = sorted(d for d in series if d < day)
    for k, d in enumerate(reversed(past)):
        x = series[d]
        if x * v > 0 and abs(x) >= abs(v):
            return k
    return None


def span(n: int | None) -> str | None:
    if n is None:
        return "за всю историю данных"
    # n — сколько прошлых месяцев подряд были меньше; «за полгода» = максимум из 6 месяцев вместе с текущим
    if n >= 11:
        y = (n + 1) // 12
        return "за последний год" if y < 2 else f"за последние {y} года" if y < 5 else f"за последние {y} лет"
    if n >= 5:
        return "за последние полгода"
    return None


def bullets(F: dict, day: str, prev: str) -> list:
    """Выводы правилами (как пункты поста 205): физлица в акциях к прошлому месяцу; крупнейшие покупатель и продавец
    ОФЗ и акций — с «больше, чем за любой месяц за …», если так."""
    out = []
    fz = F["stocks"].get("Физические лица", {})
    a, b = fz.get(day), fz.get(prev)
    if a is not None and b is not None:
        if a > 0 and b > 0 and b / max(a, 1e-9) >= 2:
            r = b / a
            times = f"в {int(r)} раз{'а' if int(r) < 5 else ''}"
            out.append(f"Физлица сократили покупки акций {'более чем ' if r - int(r) >= 0.15 else ''}{times} — "
                       f"с {fmt(b)[1:]} до {fmt(a)[1:]} млрд ₽.")
        elif a > 0 and b > 0 and a / max(b, 1e-9) >= 2:
            out.append(f"Физлица нарастили покупки акций с {fmt(b)[1:]} до {fmt(a)[1:]} млрд ₽.")
        elif a < 0 <= b:
            out.append(f"Физлица перешли к продажам акций: {fmt(a)} млрд ₽ после {fmt(b)} месяцем раньше.")
        elif a > 0 >= b:
            out.append(f"Физлица снова покупают акции: {fmt(a)} млрд ₽ после {fmt(b)} месяцем раньше.")
        else:
            out.append(f"Физлица: {fmt(a)} млрд ₽ в акциях против {fmt(b)} месяцем раньше.")
    for t in ("ofz", "stocks"):
        vals = {c: s[day] for c, s in F[t].items() if day in s}
        for pick in (max, min):
            c = pick(vals, key=vals.get)
            v = vals[c]
            if (pick is max and v <= 0) or (pick is min and v >= 0) or (t == "stocks" and c == "Физические лица"):
                continue
            verb = "купили" if v > 0 else "продали"
            line = f"{SHORT.get(c, c)} {verb} {INSTR_ACC[t]} на {fmt(v)[1:]} млрд ₽"
            s = span(months_since(F[t][c], day, v))
            out.append(line + (f" — больше, чем за любой месяц {s}." if s else "."))
    # сначала пункты с рекордом, потом остальные; не больше четырёх
    head, rest = out[:1], out[1:]
    rest.sort(key=lambda x: "больше, чем" not in x)
    return (head + rest)[:4]


def rows(F: dict, t: str, day: str, prev: str, prev_m: str) -> list:
    items = sorted(((SHORT.get(c, c), s.get(day), s.get(prev)) for c, s in F[t].items() if day in s), key=lambda x: -x[1])
    return [{"name": n, "value": fmt(a), "color": "up" if a > 0 else "down" if a < 0 else "ink",
             "note": f"в {prev_m} {fmt(b)}" if b is not None else None} for n, a, b in items]


def market(db, day: str) -> list:
    """Изменения за месяц по нашим индексам: (подпись, текст, цвет)."""
    end = dt.date.fromisoformat(day)
    start = end.replace(day=1)

    def chg(secid):
        r = db.execute(text("""
            SELECT (SELECT close FROM index_data WHERE secid = :s AND trade_date < :a ORDER BY trade_date DESC LIMIT 1),
                   (SELECT close FROM index_data WHERE secid = :s AND trade_date <= :b ORDER BY trade_date DESC LIMIT 1)
        """), {"s": secid, "a": start, "b": end}).first()
        return (float(r[1]) / float(r[0]) - 1) * 100 if r and r[0] and r[1] else None

    out = []
    col = lambda v: "up" if v > 0 else "down"  # noqa: E731
    ix = chg("IMOEX")
    if ix is not None:
        out.append(("Индекс МосБиржи", f"{fmt(ix)}%", col(ix)))
    sec = {n: v for s, n in SECTORS.items() if (v := chg(s)) is not None}
    if sec:
        # лучшая отрасль — если выросла, худшая — если упала: без подписи «лучшая/худшая» иначе вводит в заблуждение
        best, worst = max(sec, key=sec.get), min(sec, key=sec.get)
        if sec[best] > 0:
            out.append((best, f"{fmt(sec[best])}%", "up"))
        if sec[worst] < 0:
            out.append((worst, f"{fmt(sec[worst])}%", "down"))
    usd = chg("USD000UTSTOM")
    if usd is not None:
        out.append(("Доллар к рублю", f"{fmt(usd)}%", "ink"))
    rg = chg("RGBI")
    if rg is not None:
        out.append(("Индекс гособлигаций", f"{fmt(rg)}%", col(rg)))
    return out


def build(db, day: str) -> tuple[list, str]:
    F = load_flows(db)
    months = sorted({d for s in F["stocks"].values() for d in s})
    prev = months[months.index(day) - 1] if months.index(day) > 0 else day
    m, pm = int(day[5:7]) - 1, int(prev[5:7]) - 1
    sub = f"Покупки минус продажи на бирже в {PREP[m]}, млрд ₽"
    cards = [{"kind": "cover", "title": ["Потоки", "капитала"], "pill": f"{NOM[m]} {day[:4]}"},
             {"kind": "list", "title": "Кто покупал акции", "sub": sub, "logos": False, "rows": rows(F, "stocks", day, prev, PREP[pm])},
             {"kind": "list", "title": "Кто покупал ОФЗ", "sub": sub, "logos": False, "rows": rows(F, "ofz", day, prev, PREP[pm])}]
    mk = market(db, day)
    if mk:
        cards.append({"kind": "list", "title": f"Главное за {NOM[m]}", "sub": "Что изменилось на рынке за месяц", "logos": False,
                      "rows": [{"name": n, "value": v, "color": c} for n, v, c in mk]})
    pts = bullets(F, day, prev)
    cap = (f"<b>Потоки капитала · {NOM[m]} {day[:4]}</b>\n"
           f"Банк России выпустил обзор рисков финансовых рынков — потоки капитала обновлены за {NOM[m]}.\n\n"
           + "\n".join(f"• {p}" for p in pts)
           + f'\n\n<a href="https://cbr.ru/analytics/fin_stab/orfr/ORFR_{day[:4]}-{int(day[5:7])}/">Обзор на сайте ЦБ →</a>')
    return cards, cap


def _load_state() -> dict:
    try:
        with open(STATE_FILE, encoding="utf-8") as fh:
            return json.load(fh)
    except FileNotFoundError:
        return {}


def _save_state(st: dict) -> None:
    os.makedirs(STATE_DIR, exist_ok=True)
    with open(STATE_FILE, "w", encoding="utf-8") as fh:
        json.dump(st, fh, ensure_ascii=False)


def send_month(db, day: str, dry_run: bool) -> bool:
    cards, cap = build(db, day)
    imgs = []
    for c in cards:
        png = render(c)
        if not png:
            imgs = []
            break
        imgs.append(png)
    if dry_run:
        print(cap)
        from signals.funds_card import dry
        for k, png in enumerate(imgs or [None]):
            dry(png, f"{NAME}_{k}")
        return True
    kw = {"meta": {"type": "cbr_flows"}, "chart_url": f"{SITE}/cbr-flows"}
    return send_slides(imgs, cap, NAME, **kw) if imgs else send(cap, NAME, **kw)


def run_once(dry_run: bool = False) -> dict:
    from api.database import SessionLocal
    db = SessionLocal()
    try:
        last = db.execute(text("SELECT MAX(period_end_date) FROM cbr_flows WHERE period_kind = 'month' "
                               "AND instrument_type IN ('stocks', 'ofz')")).scalar()
        if not last:
            return {"month": None}
        day = last.isoformat()
        st = _load_state()
        if not st:
            if not dry_run:
                _save_state({"month": day})
            return {"month": day, "seeded": True}
        if day <= st["month"]:
            return {"month": day, "new": False}
        ok = send_month(db, day, dry_run)
        if ok and not dry_run:
            _save_state({"month": day})
        return {"month": day, "new": True, "sent": ok}
    finally:
        db.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--month", help="YYYY-MM — собрать карусель за месяц (последний день месяца из базы)")
    a = ap.parse_args()
    if a.month:
        from api.database import SessionLocal
        db = SessionLocal()
        try:
            day = db.execute(text("SELECT MAX(period_end_date) FROM cbr_flows WHERE period_kind = 'month' "
                                  "AND to_char(period_end_date, 'YYYY-MM') = :m"), {"m": a.month}).scalar()
            print(send_month(db, day.isoformat(), a.dry_run) if day else "нет такого месяца")
        finally:
            db.close()
    else:
        print(f"[{NAME}] {run_once(dry_run=a.dry_run)}")
