"""Находки движка → кандидаты завода постов: жанр «пост от наших данных», без новости.

Раз в день по будням (cron 30 7 * * 2-6 UTC — данные прошлого торгового дня): детекторы
signals/insights/detect.py по рядам базы → рейтинг → лучшие находки трёх типов (позиции физлиц,
потоки в фонды, сезонность) → фильтры повторов и значимости → карточка находки и график
(signals/insights/cards.py) → content_candidates: source='insight', status='draft_ready' сразу —
Шаги А и Б не нужны, повод и данные поста и есть сама находка. Черновик пишет отдельный Routine
(content_ai.py, TRIGGER_ID_STEP_C_INSIGHT) строго по карточке, проверка — кодом при приёмке
(api/services/insight_check.py), дальше — обычный бот ревью.

Замеры и происхождение — research/content_pipeline_v2/insights/ (HANDOFF.md): на истории 69%
постов FRAME от данных — в топ-10 находок дня; в слепом сравнении итоговый рецепт (карточка +
второй мозг + голос автора + запрет прогноза) интереснее автора в 8 парах из 15.

    python3 -m signals.insight_scan --dry-run     # что создал бы — без записи в базу
    python3 -m signals.insight_scan               # создать кандидатов
"""
import os

# Хост ходит в Postgres через localhost: имя docker-сети «db» с хоста не резолвится
# (тот же приём, что в content_ai.py) — до импорта api.database.
_url = os.environ.get("DB_URL", "")
if "@db:" in _url:
    os.environ["DB_URL"] = _url.replace("@db:", "@127.0.0.1:")

import argparse  # noqa: E402
import re  # noqa: E402
from datetime import datetime, timezone  # noqa: E402

import pandas as pd  # noqa: E402
from sqlalchemy import text  # noqa: E402

from api.database import SessionLocal  # noqa: E402
from signals.insights import cards, data  # noqa: E402
from signals.insights import detect as det  # noqa: E402

PER = {"positions": 3, "funds": 1, "seasonality": 1}     # сколько находок каждого типа в день
FAMILY = {"positions": "позиции", "funds": "фонды", "seasonality": "сезонность"}
HASHTAG = {"positions": r"#открыт\w+", "funds": r"#деньгивфондах", "seasonality": r"#сезонность"}
MEDIA_DIR = os.environ.get("CONTENT_MEDIA_DIR", "/opt/frame/data/content_media")
REPEAT_DAYS, SKIP_DAYS = 14, 3
TOPIC = {"positions": r"шорт|лонг|позици|покупк|продаж|физлиц|физик|толп",
         "funds": r"фонд|приток|отток|БПИФ", "seasonality": r"сезонн"}
FUND_INST = {"bonds": r"облигац|ОФЗ", "stocks": r"фонд\w* акци", "money_market": r"денежн\w* рынк|ликвидност",
             "gold": r"золот", "yuan": r"юан", "all": r"фонд"}

_EXISTS = text("""
    SELECT 1 FROM content_candidates
    WHERE source = 'insight' AND headline = :headline AND created_at > now() - interval '14 days'
    LIMIT 1
""")
_INSERT = text("""
    INSERT INTO content_candidates
        (status, source, headline, raw_text, tickers, futures_ticker, event_type,
         importance_1_5, reasoning, media_filename, thread_key)
    VALUES ('draft_ready', 'insight', :headline, :raw_text, CAST(:tickers AS text[]), :futures_ticker,
            :event_type, 3, :reasoning, :media_filename, :thread_key)
    RETURNING id
""")


def detect_window(until: pd.Timestamp) -> list:
    """Находки за 30 дней до `until`: новизне в рейтинге нужны предыдущие дни."""
    out = det.Out(until - pd.Timedelta(days=30), until)
    P = det.load_positions()
    names, groups, to_stock = det.instruments()
    idx, stk, perp = det.prices()
    usd = det.usd_series(idx, perp)
    det.detect_positions(out, P, names, groups, to_stock, idx, stk, perp)
    det.detect_funds(out)
    out.ctx = {}
    det.detect_breadth(out)
    det.detect_buffett(out)
    det.detect_seasonality(out, idx, usd)
    det.detect_seasonal_curve(out, idx, usd)
    det.detect_fund_trades(out)
    det.detect_prices(out, idx, stk, perp, usd)
    return det.rank(out.items)


def channel_posts(as_of, days=REPEAT_DAYS) -> list:
    t = pd.Timestamp(as_of)
    df = data.read("channel_posts")
    if not len(df):
        return []
    df["d"] = pd.to_datetime(df.posted_at, utc=True).dt.tz_localize(None)
    df = df[(df.d >= t - pd.Timedelta(days=days)) & (df.d <= t + pd.Timedelta(days=1))]
    return [(r.d, r.text or "") for r in df.sort_values("d", ascending=False).itertuples()]


def repeat_of(spec, as_of):
    """Писал ли канал о той же теме недавно: тот же инструмент + тот же тип находки."""
    kind = spec["kind"]
    if kind == "funds":
        inst, case = FUND_INST.get(spec["cat"], r"фонд"), False
    elif kind == "seasonality":
        inst, case = (r"доллар|валют" if spec["code"] == "Si" else r"индекс\w* мосбирж|IMOEX|акци"), False
    else:
        code = det.CODE.get(spec["sec"])
        if code in ("MIX", "RI"):
            inst, case = r"индекс\w* мосбирж|IMOEX|индекс\w* РТС", False
        elif code in ("Si", "CNY", "Eu"):
            inst, case = r"доллар|валют|юан|евро", False
        else:   # акция: имя с заглавной или тикер — «Самолет» не должен ловить «самолетов Boeing»
            P, names, groups, to_stock, *_ = cards.data()
            nm = str(names.get(spec["sec"], "")).replace(" (вечн)", "").split(" ")[0]
            inst = "|".join(x for x in (re.escape(nm) if nm else "", to_stock.get(spec["sec"]) or "") if x)
            case = True
    for d, txt in channel_posts(as_of):
        if inst and re.search(inst, txt, 0 if case else re.I) and re.search(TOPIC[kind], txt, re.I):
            return {"date": d, "title": txt.strip().splitlines()[0][:90] if txt.strip() else ""}
    return None


def fund_significant(cat, as_of) -> bool:
    """Поток с начала месяца или за 20 дней ≥ 3 млрд ₽ или ≥ 3% активов категории:
    «отток 679 млн из фондов золота» (1% активов) — новость для движка, не для читателя."""
    daily, nav = cards.funds_data()
    d = cards.upto(daily[cat], as_of)
    t = d.index[-1]
    mtd = abs(float(d[d.index.to_period("M") == t.to_period("M")].sum()))
    s20 = abs(float(d.iloc[-20:].sum()))
    aum = float(cards.upto(nav[cat], t).iloc[-1])
    return max(mtd, s20) >= (min(3e9, 0.03 * aum) if aum > 0 else 3e9)


def drop_low_activity(items: list, log=print) -> list:
    """Малоактивные контракты на сайте скрыты из открытых позиций — мало физлиц-трейдеров (правило
    low_activity_set скринера). #2126: пост про шорт в Baidu, которого читатель на сервисе не видит."""
    try:
        from api.services.oi_screener import low_activity_set
        db = SessionLocal()
        try:
            low = low_activity_set(db)
        finally:
            db.close()
    except Exception as e:  # noqa: BLE001 — без фильтра лучше, чем без находок
        log(f"нет фильтра малоактивных: {type(e).__name__}: {e}")
        return items
    return [x for x in items if not (x.get("family") == "позиции" and (x.get("facts") or {}).get("sec") in low)]


def drop_expiry_days(items: list, log=print) -> list:
    """Находки по позициям на день экспирации и ±1 торговый день — искажение перехода в следующий
    контракт, а не сигнал (#2419 Мечел 17.09). Цена, фонды и сезонность не трогаются."""
    from signals.insights.expiry import near_expiry
    keep = [x for x in items if not (x.get("family") == "позиции" and near_expiry(x["date"]))]
    if len(keep) < len(items):
        log(f"экспирация: отложено находок по позициям - {len(items) - len(keep)}")
    return keep


def pick(items: list, log=print) -> list:
    """Лучшие находки дня по типам, по разным инструментам, с фильтрами."""
    res = []
    for kind, fam in FAMILY.items():
        pool = [x for x in items if x["family"] == fam]
        if kind == "positions":
            pool = [x for x in pool if x["type"] in ("рекорд_или_экстремум", "уровень_к_истории")
                    and (x.get("facts") or {}).get("leg") in ("long", "short", "nl", "ns", "net")
                    and (x.get("facts") or {}).get("sec")]
        if kind == "funds":
            pool = [x for x in pool if (x.get("facts") or {}).get("cat")]
        if kind == "seasonality":
            pool = [x for x in pool if x["instrument"] in ("Si", "MIX")]
        if not pool:
            continue
        last = max(x["date"] for x in pool)
        seen = set()
        for x in sorted((x for x in pool if x["date"] == last), key=lambda z: -z["score"]):
            if x["instrument"] in seen or len(seen) >= PER[kind]:
                continue
            f = x.get("facts") or {}
            spec = {"positions": {"kind": "positions", "sec": f.get("sec"), "leg": f.get("leg")},
                    "funds": {"kind": "funds", "cat": f.get("cat")},
                    "seasonality": {"kind": "seasonality", "code": x["instrument"]}}[kind]
            if kind == "funds" and not fund_significant(spec["cat"], last):
                log(f"пропуск, мало для читателя: {x['title'][:90]}")
                continue
            rep = repeat_of(spec, last)
            if rep and (pd.Timestamp(last) - rep["date"]).days <= SKIP_DAYS:
                log(f"пропуск, канал писал {rep['date']:%d.%m} «{rep['title'][:50]}»: {x['title'][:70]}")
                continue
            seen.add(x["instrument"])
            res.append({"kind": kind, "spec": spec, "date": last, "title": x["title"], "repeat": rep,
                        "instrument": x["instrument"], "score": x["score"]})
    return res


def _inst_rx(spec):
    """Регэксп инструмента находки и чувствительность к регистру — общий для повторов и
    «что канал уже писал»."""
    kind = spec["kind"]
    if kind == "funds":
        return FUND_INST.get(spec["cat"], r"фонд"), False
    if kind == "seasonality":
        return (r"доллар|валют" if spec["code"] == "Si" else r"индекс\w* мосбирж|IMOEX|акци"), False
    code = det.CODE.get(spec["sec"])
    if code in ("MIX", "RI"):
        return r"индекс\w* мосбирж|IMOEX|индекс\w* РТС", False
    if code in ("Si", "CNY", "Eu"):
        return r"доллар|валют|юан|евро", False
    # акция: имя с заглавной или тикер — «Самолет» не должен ловить «самолетов Boeing»
    P, names, groups, to_stock, *_ = cards.data()
    nm = str(names.get(spec["sec"], "")).replace(" (вечн)", "").split(" ")[0]
    return "|".join(x for x in (re.escape(nm) if nm else "", to_stock.get(spec["sec"]) or "") if x), True


def own_posts(spec, as_of, k=2) -> list:
    """«Что канал уже писал по этому ряду» — посты той же рубрики И о том же инструменте. Одной
    рубрики мало: в карточке «АФК Системы» стояли посты про шорт по индексу и по валюте."""
    inst, case = _inst_rx(spec)
    rows = [(d, t) for d, t in channel_posts(as_of, days=45)
            if re.search(HASHTAG[spec["kind"]], t) and inst and re.search(inst, t, 0 if case else re.I)]
    return [f"{cards.d_ru(d, as_of)}: «{t.strip().splitlines()[0][:90]}»" for d, t in rows[:k]]


def build(job: dict, now_iso: str) -> dict:
    spec, date = job["spec"], job["date"]
    card = cards.build_card(spec, date)
    ctx = cards.context_for(card, spec, now_iso)
    ctx["что канал уже писал по этому ряду"] = own_posts(spec, card["as_of"])
    if job["repeat"]:
        r = job["repeat"]
        ctx["повтор темы - подай как продолжение"] = [
            f"{cards.d_ru(r['date'], card['as_of'])} канал уже писал об этом: «{r['title']}». Начни со ссылки на "
            f"тот пост и скажи, что изменилось с тех пор; не пересказывай его заново"]
    brief = cards.brief_text(card, focus=True, context=ctx)
    return {"card": card, "brief": brief}


def run_once(dry_run: bool = False) -> dict:
    oi = data.read("oi_daily", parse_dates=["tradedate"])
    until = oi.tradedate.max()
    items = drop_expiry_days(drop_low_activity(detect_window(until)))
    jobs = pick(items)
    now_iso = datetime.now(timezone.utc).isoformat()
    summary = {"data_until": str(until.date()), "found": len(items), "picked": len(jobs), "created": 0,
               "skipped_exists": 0}
    db = SessionLocal()
    try:
        for n, job in enumerate(jobs, 1):
            b = build(job, now_iso)
            card = b["card"]
            head = card["headline"][:300]
            if db.execute(_EXISTS, {"headline": head}).first():
                summary["skipped_exists"] += 1
                continue
            spec = job["spec"]
            sec = spec.get("sec")
            tick = (cards.data()[3].get(sec) if sec else None) or job["instrument"] or spec.get("code") or "MIX"
            media = f"insight_{card['as_of']:%Y%m%d}_{spec['kind']}_{n}.png"
            print(f"[insight_scan] {spec['kind']}: {head}" + (" (продолжение темы)" if job["repeat"] else ""))
            if dry_run:
                print(b["brief"][:1500] + "\n")
                continue
            os.makedirs(MEDIA_DIR, exist_ok=True)
            cards.draw_chart(card, os.path.join(MEDIA_DIR, media))
            row = db.execute(_INSERT, {
                "headline": head, "raw_text": b["brief"], "tickers": [str(tick)],
                "futures_ticker": sec, "event_type": f"insight_{spec['kind']}",
                "reasoning": f"движок находок: {job['title'][:200]} (балл {job['score']})",
                "media_filename": media, "thread_key": f"insight:{spec['kind']}:{job['instrument'] or tick}",
            }).first()
            db.commit()
            summary["created"] += 1
            print(f"[insight_scan] кандидат {row[0]} создан")
    finally:
        db.close()
    return summary


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="показать находки и карточки, ничего не писать")
    a = ap.parse_args()
    print(f"[insight_scan] {run_once(dry_run=a.dry_run)}")
