"""Повод дня в тот же день (Вадим 28.09: «продолжай» — после запуска новых типов; «я бы делал всё постоянно — каждый
час или каждый день», поэтому не только вечером, а весь торговый день).

Пост канала «Самолёт падает, толпа докупает» вышел 15.09 в 21:27 — про −12% «сегодня» и рекордный лонг физлиц. Утренний
сканер находок (insight_scan) видит только закрытия по вчерашний день и ловил такой повод наутро. Здесь — проход
по ходу дня: бумага СЕГОДНЯ ушла резко (5-минутная цена против вчерашнего закрытия — cards.live_price), а по ней у
толпы рекорд позиции на последнем закрытии → один черновик «повод дня» (тот же angle_card, что у утренних постов
нового типа).

Раз в день, с TRIGGER_FROM до TRIGGER_TO МСК (первые два часа основной сессии цена ещё не устоялась), не больше одного
черновика; повтор темы — теми же фильтрами, что у
утреннего сканера (канал уже писал, та же находка уже была). Запуск — из дневного прохода связок (combo_scan --mode news,
каждые 20 минут), своего крона нет.

    python3 -m signals.trigger_scan --dry-run [--at 2026-09-15T18:00Z]
"""
import os

_url = os.environ.get("DB_URL", "")
if "@db:" in _url:
    os.environ["DB_URL"] = _url.replace("@db:", "@127.0.0.1:")

import argparse  # noqa: E402
from datetime import datetime, timedelta, timezone  # noqa: E402

import pandas as pd  # noqa: E402
from sqlalchemy import text  # noqa: E402

from api.database import SessionLocal  # noqa: E402
from signals import insight_scan as ins  # noqa: E402
from signals.insights import cards, data  # noqa: E402

TRIGGER_FROM, TRIGGER_TO = 12, 21      # часы МСК: весь торговый день после первых двух часов основной сессии
TRIGGER_POOL = 25                      # сколько лучших рекордов позиций по акциям проверять
MSK = timezone(timedelta(hours=3))
_TODAY = text("""SELECT 1 FROM content_candidates
                  WHERE thread_key LIKE 'insight:trigger:%' AND created_at >= :day LIMIT 1""")


def in_window(now_msk) -> bool:
    return TRIGGER_FROM <= now_msk.hour < TRIGGER_TO and now_msk.weekday() < 5


def pool_of(items: list, stocks: dict) -> list:
    """Рекорды позиций по акциям на последнем закрытии, лучшие первыми, одна бумага — один раз."""
    pos = [x for x in items if x["family"] == "позиции" and x["type"] in ("рекорд_или_экстремум", "уровень_к_истории")
           and (x.get("facts") or {}).get("leg") in ("long", "short", "net")
           and stocks.get((x.get("facts") or {}).get("sec"))]
    if not pos:
        return []
    last = max(x["date"] for x in pos)
    out, seen = [], set()
    for x in sorted((x for x in pos if x["date"] == last), key=lambda z: -z["score"]):
        if x["instrument"] in seen:
            continue
        seen.add(x["instrument"])
        out.append(x)
    return out[:TRIGGER_POOL]


def best_trigger(pool: list, build=cards.build_card, log=print):
    """Самый сильный повод СЕГОДНЯ (по живой цене) среди рекордов пула: (находка, поворот) или None."""
    found = []
    for x in pool:
        f = x["facts"]
        spec = {"kind": "positions", "sec": f["sec"], "leg": f["leg"]}
        try:
            card = build(spec, x["date"])
        except Exception as e:  # noqa: BLE001
            log(f"карточка не собралась: {x['title'][:70]}: {type(e).__name__}: {e}")
            continue
        for a in card.get("angles") or []:
            if a["type"] == "повод" and a.get("live") and a["strength"] >= cards.ANGLE_MIN["повод"]:
                found.append((a["strength"], x, spec, a))
    return max(found, key=lambda z: z[0])[1:] if found else None


def run_once(dry_run: bool = False, at: str | None = None) -> dict:
    now = pd.Timestamp(at) if at else pd.Timestamp(datetime.now(timezone.utc))
    now = now.tz_localize("UTC") if now.tzinfo is None else now
    now_msk = now.tz_convert(MSK)
    if not in_window(now_msk):
        return {"skipped": "вне торговых часов"}
    if at:      # прогон на прошлом: «сейчас» — этот момент; посты канала — только до него
        cards.NOW = now_msk.tz_localize(None)
        _cp = ins.channel_posts
        cut = now.tz_convert("UTC").tz_localize(None)
        ins.channel_posts = lambda as_of, days=ins.REPEAT_DAYS: [(d, t) for d, t in _cp(as_of, days) if d < cut]
    db = SessionLocal()
    try:
        day = now_msk.normalize().tz_convert("UTC")
        if not (dry_run and at) and db.execute(_TODAY, {"day": day}).first():
            return {"skipped": "повод дня сегодня уже был"}
        from signals import combo_scan   # находки дня — из того же кэша, что у связок (раз в торговый день)
        oi = data.read("oi_daily", parse_dates=["tradedate"])
        until = oi.tradedate[oi.tradedate.dt.date < now_msk.date()].max()
        items = ins.drop_expiry_days(ins.drop_low_activity(combo_scan.detections(until), log=lambda *a: None),
                                     log=lambda *a: None)
        pool = pool_of(items, cards.data()[3])
        hit = best_trigger(pool)
        summary = {"pool": len(pool), "created": 0}
        if not hit:
            return summary
        x, spec, a = hit
        rep = ins.repeat_of(spec, x["date"])
        if rep and (pd.Timestamp(x["date"]) - rep["date"]).days <= ins.SKIP_DAYS:
            print(f"[trigger_scan] пропуск, канал писал {rep['date']:%d.%m} «{rep['title'][:50]}»: {x['title'][:70]}")
            return summary
        job = {"kind": "positions", "spec": {**spec, "angle": "повод"}, "date": x["date"],
               "title": f"повод дня: {x['title']}", "repeat": rep, "instrument": x["instrument"],
               "score": round(a["strength"], 1), "thread_key": f"insight:trigger:{x['instrument']}:{now_msk:%Y%m%d}"}
        b = ins.build(job, now.isoformat())
        ins.save_job(db, job, b, 1, dry_run, summary, tag="trigger_scan")
        return summary
    finally:
        db.close()
        if at:
            cards.NOW = None
            ins.channel_posts = _cp


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="показать карточку, ничего не писать")
    ap.add_argument("--at", help="момент в прошлом (ISO, UTC); только с --dry-run")
    a = ap.parse_args()
    if a.at and not a.dry_run:
        ap.error("--at только вместе с --dry-run")
    print(f"[trigger_scan] {run_once(dry_run=a.dry_run, at=a.at)}")
