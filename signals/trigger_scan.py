"""Находки по ходу дня: повод дня и рывок позиции (Вадим 28.09: «продолжай» — после запуска новых типов; «я бы делал
всё постоянно — каждый час или каждый день»; «сканер находок — на интрадей, хотя бы каждый час»).

Пост канала «Самолёт падает, толпа докупает» вышел 15.09 в 21:27 — про −12% «сегодня» и рекордный лонг физлиц. Утренний
сканер находок (insight_scan) видит только закрытия по вчерашний день и ловил такой повод наутро. Здесь — проход
по ходу дня: бумага СЕГОДНЯ ушла резко (5-минутная цена против вчерашнего закрытия — cards.live_price), а по ней у
толпы рекорд позиции на последнем закрытии → один черновик «повод дня» (тот же angle_card, что у утренних постов
нового типа).

Второй сигнал — рывок позиции по ходу дня (Вадим 28.09: «сканер находок — на интрадей, хотя бы каждый час»): 5-минутный
срез позиций физлиц против вчерашнего закрытия по всем фьючерсам, ход в 8+ обычных дневных и от 10% (Русал 28.09: покупки
+19% за первый час сессии при +4 покупателях; по цене повода нет — акция стоит). Карточка — тот же angle_card с поворотом
«рывок» (cards.surge_angle): люди той же стороны, рекорд по срезу, живая цена; на графике — точка среза.

С TRIGGER_FROM до TRIGGER_TO МСК (первые два часа основной сессии цена ещё не устоялась), не больше TRIGGER_PER_DAY
черновиков в день, одна бумага — один раз; повтор темы — теми же фильтрами, что у
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

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from sqlalchemy import text  # noqa: E402

from api.database import SessionLocal  # noqa: E402
from signals import insight_scan as ins  # noqa: E402
from signals.insights import cards, data  # noqa: E402

TRIGGER_FROM, TRIGGER_TO = 12, 21      # часы МСК: весь торговый день после первых двух часов основной сессии
TRIGGER_POOL = 25                      # сколько лучших рекордов позиций по акциям проверять
TRIGGER_PER_DAY = 2                    # поводов и рывков по ходу дня — не больше двух черновиков в день
SURGE_POOL = 6                         # сколько сильнейших рывков по срезу проверять карточкой
# рывок — только наш рынок: акции, индексы МосБиржи и РТС, RGBI, рубль к доллару, юаню и евро, золото и нефть. Какао,
# медь, S&P и биткоин — мимо: реплей 15.09 взял бы шорт меди −45% вместо Самолёта — поста канала того дня
SURGE_CODES = {"MX", "MM", "IMOEXF", "MY", "RI", "RM", "RB", "Si", "USDRUBF", "CR", "CNYRUBF", "Eu", "EURRUBF",
               "GD", "GL", "GLDRUBF", "BR", "BM"}
MSK = timezone(timedelta(hours=3))
_TODAY = text("""SELECT thread_key FROM content_candidates
                  WHERE thread_key LIKE 'insight:trigger:%' AND created_at >= :day""")


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


def surge_allowed(sec, groups: dict) -> bool:
    return groups.get(sec) == "Акции" or sec in SURGE_CODES


def surge_candidates(snap: pd.DataFrame, P: dict, low=(), day=None, groups=None) -> list:
    """Рывки по срезу: объём стороны (long/short) против последнего закрытия до дня среза — в обычных дневных ходах
    (медиана модуля дневного изменения за 60 дней, как в cards.surge_angle), сильнейшие первыми. Малоактивные
    контракты (скрыты на сайте) и дни экспирации — мимо."""
    from signals.insights.expiry import near_expiry
    if snap is None or snap.empty or (day is not None and near_expiry(day)):
        return []
    out = []
    for sec, r in snap.iterrows():
        if sec in low or (groups is not None and not surge_allowed(sec, groups)):
            continue
        for leg in ("long", "short"):
            if leg not in P or sec not in P[leg].columns:
                continue
            ser = P[leg][sec].dropna()
            ser = ser[ser.index < pd.Timestamp(r["tradedate"]).normalize()]
            if len(ser) < 62:
                continue
            v0, v1 = float(ser.iloc[-1]), float(r[leg])
            typ = float(np.median(np.abs(np.diff(ser.values[-61:].astype(float)))))
            if v0 <= 0 or typ <= 0 or not np.isfinite(v1):
                continue
            k, pct = abs(v1 - v0) / typ, v1 / v0 - 1
            if k >= cards.ANGLE_MIN["рывок"] and abs(pct) >= cards.SURGE_MIN_PCT:
                out.append({"sec": sec, "leg": leg, "k": k, "pct": pct, "date": ser.index[-1],
                            "t": str(r["tradetime"])[:5]})
    return sorted(out, key=lambda z: -z["k"])


def best_trigger(pool: list, surges=(), build=cards.build_card, taken=(), log=print):
    """Сильнейшее событие СЕГОДНЯ: повод по живой цене среди рекордов пула или рывок позиции по срезу — (находка,
    спека, поворот) или None. Сила — в долях порога своего типа; бумага, по которой сегодня уже был черновик, — мимо."""
    found = []

    def angles_of(spec, date, title):
        try:
            return build(spec, date).get("angles") or []
        except Exception as e:  # noqa: BLE001
            log(f"карточка не собралась: {title[:70]}: {type(e).__name__}: {e}")
            return []

    for x in pool:
        f = x["facts"]
        if f["sec"] in taken:
            continue
        spec = {"kind": "positions", "sec": f["sec"], "leg": f["leg"]}
        for a in angles_of(spec, x["date"], x["title"]):
            if a["type"] == "повод" and a.get("live") and a["strength"] >= cards.ANGLE_MIN["повод"]:
                found.append((a["strength"] / cards.ANGLE_MIN["повод"], x, spec, a))
    for c in list(surges)[:SURGE_POOL]:
        if c["sec"] in taken:
            continue
        spec = {"kind": "positions", "sec": c["sec"], "leg": c["leg"]}
        title = f"{c['sec']} {c['leg']} {c['pct']:+.0%} к {c['t']}"
        for a in angles_of(spec, c["date"], title):
            if a["type"] == "рывок" and a["strength"] >= cards.ANGLE_MIN["рывок"]:
                x = {"title": a["line"].split(";")[0].replace("рывок по ходу дня: ", ""), "date": c["date"],
                     "instrument": c["sec"], "facts": {"sec": c["sec"], "leg": c["leg"]}}
                found.append((a["strength"] / cards.ANGLE_MIN["рывок"], x, spec, a))
    return max(found, key=lambda z: z[0])[1:] if found else None


def _low_activity(db) -> set:
    try:
        from api.services.oi_screener import low_activity_set
        return set(low_activity_set(db))
    except Exception as e:  # noqa: BLE001 — без фильтра лучше, чем без рывков
        print(f"[trigger_scan] нет фильтра малоактивных: {type(e).__name__}: {e}")
        return set()


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
        done = [] if (dry_run and at) else [r[0] for r in db.execute(_TODAY, {"day": day})]
        if len(done) >= TRIGGER_PER_DAY:
            return {"skipped": f"по ходу дня сегодня уже {len(done)} черновика"}
        taken = {k.split(":")[2] for k in done if k and k.count(":") >= 3}
        from signals import combo_scan   # находки дня — из того же кэша, что у связок (раз в торговый день)
        oi = data.read("oi_daily", parse_dates=["tradedate"])
        until = oi.tradedate[oi.tradedate.dt.date < now_msk.date()].max()
        items = ins.drop_expiry_days(ins.drop_low_activity(combo_scan.detections(until), log=lambda *a: None),
                                     log=lambda *a: None)
        P, _, groups, stocks = cards.data()[:4]
        pool = pool_of(items, stocks)
        surges = surge_candidates(cards.intraday_snapshots(), P, _low_activity(db), now_msk.tz_localize(None), groups)
        hit = best_trigger(pool, surges, taken=taken)
        summary = {"pool": len(pool), "surges": len(surges), "created": 0}
        if not hit:
            return summary
        x, spec, a = hit
        rep = ins.repeat_of(spec, x["date"])
        if rep and (pd.Timestamp(x["date"]) - rep["date"]).days <= ins.SKIP_DAYS:
            print(f"[trigger_scan] пропуск, канал писал {rep['date']:%d.%m} «{rep['title'][:50]}»: {x['title'][:70]}")
            return summary
        job = {"kind": "positions", "spec": {**spec, "angle": a["type"]}, "date": x["date"],
               "title": f"{a['type']} дня: {x['title']}", "repeat": rep, "instrument": spec["sec"],
               "score": round(a["strength"], 1), "thread_key": f"insight:trigger:{spec['sec']}:{now_msk:%Y%m%d}"}
        b = ins.build(job, now.isoformat())
        # своё имя картинки: утренний сканер на ту же дату данных пишет insight_<дата>_positions_a1.png
        ins.save_job(db, job, b, f"t{spec['sec']}{now_msk:%H%M}", dry_run, summary, tag="trigger_scan")
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
