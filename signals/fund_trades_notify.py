"""«Сделки фондов» → @framesignalbot (Вадим 18.11.2026) — в формате рубрики канала (пост t.me/FrameTool/220).

  • Фонд отчитался — по мере заливки справок СЧА (scha_jobs.status='imported' за свежий месяц): логотип УК,
    название фонда и карточка «Сделки фонда» — 3 крупнейшие покупки и 3 продажи за месяц (млн ₽).
  • Все фонды занесены — когда у каждого фонда раздела (акции; облигации — отдельно), у которого был полный срез
    прошлого месяца, появился срез этого (правило data_release_scan): карусель как пост 220 — обложка, «Фонды
    купили», «Фонды продали» и для акций «Портфель фондов».

Цифры — тем же кодом, что страница /fund-trades (signals/fund_trades_dump.py в api-контейнере, без тирной задержки:
бот служебный). Карточки рисует движок frame-chart-render (frontend/chart-render/cards.ts): имена и логотипы бумаг —
функциями сайта. Рассылка пользователям «данные обновились» (data_release_scan) не трогается.

Состояние — STATE_DIR/fund_trades_notify.state.json: последняя разобранная справка и разделы, по которым итог ушёл.
Первый прогон запоминает всё молча.

    python3 -m signals.fund_trades_notify --dry-run        # показать, ничего не слать (CARD_DIR — картинки)
    python3 -m signals.fund_trades_notify --month stocks 2026-10 --dry-run   # собрать итог за месяц
"""
from __future__ import annotations

import argparse
import datetime as dt
import html
import json
import os
import subprocess
import time

from signals.framesignal import SITE, send, send_photo, send_slides
from signals.record_card import render

NAME = "fund_trades_notify"
STATE_DIR = os.environ.get("SCAN_STATE_DIR", "/opt/frame/data/scan_state")
STATE_FILE = os.path.join(STATE_DIR, f"{NAME}.state.json")
DUMP = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fund_trades_dump.py")
NOM = ["январь", "февраль", "март", "апрель", "май", "июнь", "июль", "август", "сентябрь", "октябрь", "ноябрь", "декабрь"]
PREP = ["январе", "феврале", "марте", "апреле", "мае", "июне", "июле", "августе", "сентябре", "октябре", "ноябре", "декабре"]
GEN = ["января", "февраля", "марта", "апреля", "мая", "июня", "июля", "августа", "сентября", "октября", "ноября", "декабря"]
CAT = {"stocks": ("акций", "Фонды акций"), "bonds": ("облигаций", "Фонды облигаций")}
# кнопки и настройки получателя (signals/signal_prefs): «фонд отчитался» — без звука
FUND_KW = {"silent": True, "meta": {"type": "fund_trades"}, "chart_url": "https://framedata.ru/fund-trades"}
FRESH_MONTHS = 2                    # «фонд отчитался» — только за свежие месяцы (перечитки истории не шлём)


def dump(*args: str):
    """JSON из api-контейнера (код сайта + Redis/БД)."""
    cid = subprocess.run(["docker", "ps", "-q", "-f", "label=com.docker.compose.service=api"],
                         capture_output=True, text=True, timeout=30).stdout.split()
    if not cid:
        raise RuntimeError("api-контейнер не найден")
    with open(DUMP, "rb") as fh:
        r = subprocess.run(["docker", "exec", "-i", cid[0], "python", "-", *args], stdin=fh,
                           capture_output=True, timeout=300)
    if r.returncode != 0:
        raise RuntimeError(f"dump {args[0]}: {r.stderr.decode()[-400:]}")
    return json.loads(r.stdout.decode().strip().splitlines()[-1])


def money(v: float) -> tuple[str, str]:
    """Сумма в рублях → («+684», «млн ₽») / («−1,6», «млрд ₽») — как карточки поста."""
    m = v / 1e6
    sign = "+" if m > 0 else "−"
    if abs(m) >= 1000:
        return f"{sign}{abs(m) / 1000:.1f}".replace(".", ","), "млрд ₽"
    return f"{sign}{abs(m):.0f}", "млн ₽"


def rows(xs: list, color: str) -> list:
    out = []
    for x in xs:
        val, unit = money(x["amount"])
        out.append({"asset": x["asset"], "isin": x.get("isin"), "value": val, "unit": unit, "color": color})
    return out


def month_cards(cat: str, ym: str, d: dict) -> list:
    m = int(ym[5:7]) - 1
    gen, _ = CAT[cat]
    pill = f"{NOM[m]} {ym[:4]}" if cat == "stocks" else f"облигации · {NOM[m]} {ym[:4]}"
    cards = [{"kind": "cover", "title": ["Сделки", "фондов"], "pill": pill},
             {"kind": "list", "title": "Фонды купили", "sub": f"Больше всего денег фонды {gen} вложили в {PREP[m]}",
              "rows": rows(d["buy"], "up")},
             {"kind": "list", "title": "Фонды продали", "sub": f"Из этих бумаг фонды {gen} вывели больше всего в {PREP[m]}",
              "rows": rows(d["sell"], "down")}]
    if d.get("portfolio"):
        cards.append({"kind": "tiles", "title": "Портфель фондов",
                      "sub": f"Крупнейшие доли в общем портфеле фондов {gen} на конец {GEN[m]}",
                      "tiles": [{"asset": p["asset"], "isin": p["isin"], "weight": p["weight"]} for p in d["portfolio"]]})
    return cards


def month_caption(cat: str, ym: str, d: dict) -> str:
    m = int(ym[5:7]) - 1
    gen, _ = CAT[cat]
    return (f"<b>Сделки фондов · {NOM[m]} {ym[:4]}</b>\n"
            f"Фонды {gen} отчитались за {NOM[m]}: сделки залиты по всем {d['funds']} фондам.")


def fund_card(f: dict, ym: str) -> dict:
    m = int(ym[5:7]) - 1
    return {"kind": "list", "title": "Сделки фонда", "sub": f"{f['name']} в {PREP[m]}: больше всего купил и продал",
            "rows": rows(f["buy"], "up") + rows(f["sell"], "down")}


def icon(ticker: str, uk_id) -> str:
    """Логотип УК или фото автора из пака бота (как сводка справок); обычный эмодзи не ставим."""
    try:
        from signals.scha_pipeline import fund_icon
        s = fund_icon(ticker, uk_id)
    except Exception:  # noqa: BLE001
        return ""
    return s + " " if s.startswith("<tg-emoji") else ""


def fund_message(f: dict, ym: str) -> str:
    m = int(ym[5:7]) - 1
    return (f"{icon(f['ticker'], f.get('uk_id'))}<b>{html.escape(f['name'] or f['ticker'])}</b>\n"
            f"Отчитался за {NOM[m]}: сделки на сайте")


def pictures(cards: list) -> list:
    out = []
    for c in cards:
        png = render(c)
        if not png:
            return []
        out.append(png)
    return out


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


def fresh(period: str, today: dt.date) -> bool:
    y, m = int(period[:4]), int(period[5:7])
    return (today.year - y) * 12 + today.month - m <= FRESH_MONTHS


def send_month(cat: str, ym: str, dry_run: bool) -> bool:
    d = dump("month", cat, ym)
    cards, cap = month_cards(cat, ym, d), month_caption(cat, ym, d)
    imgs = pictures(cards)
    if dry_run:
        print(cap)
        from signals.funds_card import dry
        for k, png in enumerate(imgs or [None]):
            dry(png, f"{NAME}_{cat}_{k}")
        print()
        return True
    kw = {"meta": {"type": "fund_trades"}, "chart_url": f"{SITE}/fund-trades?tab=portfolio&month={ym}"}
    return send_slides(imgs, cap, NAME, **kw) if imgs else send(cap, NAME, **kw)


def run_once(dry_run: bool = False, today: dt.date | None = None) -> dict:
    today = today or dt.date.today()
    st = _load_state()
    comp = dump("complete")
    jobs = dump("imported", str(st.get("last_job", 0)))
    summary = {"funds": 0, "months": 0, "sent": 0, "failed": 0}
    if not st:                                          # первый прогон: всё уже залитое — молча
        st = {"last_job": max([j["id"] for j in jobs], default=0),
              "done": {c: v["complete_month"] for c, v in comp.items() if v.get("complete_month")}}
        if not dry_run:
            _save_state(st)
        summary["seeded"] = True
        return summary
    ok = True
    for j in jobs:
        if not fresh(j["period"], today) or j["category"] not in CAT:
            st["last_job"] = j["id"]
            continue
        summary["funds"] += 1
        f = dump("fund", j["ticker"], j["period"])
        f.setdefault("uk_id", j.get("uk_id"))
        msg = fund_message(f, j["period"])
        png = render(fund_card(f, j["period"])) if (f["buy"] or f["sell"]) else None
        if dry_run:
            print(msg)
            from signals.funds_card import dry
            dry(png, f"{NAME}_fund_{j['ticker']}")
            print()
        elif (send_photo(png, msg, NAME, **FUND_KW) if png else send(msg, NAME, **FUND_KW)):
            summary["sent"] += 1
        else:
            summary["failed"] += 1
            ok = False
            break
        st["last_job"] = j["id"]
        time.sleep(1.1)
    for cat, v in comp.items():
        full = v.get("complete_month")
        if cat not in CAT or not full or st.get("done", {}).get(cat, "") >= full:
            continue
        summary["months"] += 1
        if send_month(cat, full, dry_run):
            summary["sent"] += 0 if dry_run else 1
            st.setdefault("done", {})[cat] = full
        else:
            summary["failed"] += 1
            ok = False
    if not dry_run and ok:
        _save_state(st)
    return summary


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--month", nargs=2, metavar=("CAT", "YYYY-MM"), help="собрать итог раздела за месяц")
    a = ap.parse_args()
    if a.month:
        print(send_month(a.month[0], a.month[1], a.dry_run))
    else:
        print(f"[{NAME}] {run_once(dry_run=a.dry_run)}")
