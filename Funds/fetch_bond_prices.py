#!/usr/bin/env python3
"""
Ежедневные цены облигаций из составов фондов (bond_history / bond_ref / bond_events) с ISS МосБиржи.

Историю до 27.10.2026 залила разовая выгрузка research/bonds_iss/backfill.py; этот скрипт держит её
свежей шагом дневного цикла оркестратора (19:10 МСК, рядом со свечами акций).

Что делает за прогон (идемпотентно, всё через UPSERT):
1. Дни: все облигации рынка за каждую дату [последняя дата в bond_history … сегодня] —
   history/engines/stock/markets/bonds/securities.json?date=D, ~35 страниц по 100 строк на день.
   Берём строки только наших бумаг (bond_ref + облигации из bond_maturity). Пропущенный прогон
   догонится сам: окно начинается с последней даты в базе, а не со «вчера».
2. Новые бумаги (облигация в составе фонда, а в bond_ref её нет): вся история бумаги, описание выпуска
   и купоны/амортизации/оферты — до --max-new бумаг за прогон, остаток доберут следующие дни.
3. Справочник и события уже известных непогашенных бумаг — --refresh самых давних за прогон
   (купоны плавающих облигаций становятся известны по ходу, оферты появляются).

Все 50 колонок истории ISS берутся как есть (Вадим: «данные с избытком, а не только цену»).
Цена в % от номинала; цена для фонда = close × facevalue / 100 + accint.
⚠️ Без marketprice_board=1: фильтр отрезает старые режимы (EQOB до 2020), режим — часть ключа.
⚠️ Пауза 1 с между запросами и стоп после 5 сбоев подряд: бан IP сервера в ISS кладёт все
фетчеры МосБиржи разом. Обычный прогон — 70–150 запросов.

Использование:
    python3 Funds/fetch_bond_prices.py --once [--from 2026-10-21] [--max-new 10] [--refresh 40] [--dry-run]
"""
import argparse
import json
import logging
import os
import sys
import time
import urllib.parse
import urllib.request
from datetime import date, timedelta
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

PROJECT_DIR = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_DIR / ".env")
DB_URL = os.getenv("DB_URL")

ISS = "https://iss.moex.com/iss"
_UA = {"User-Agent": "Mozilla/5.0 (frame-bond-prices)"}
PAUSE = 1.0
MAX_FAILS = 5

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("bond_prices")

_last = [0.0]
_fails = [0]


class IssDown(RuntimeError):
    pass


def get(url: str) -> dict:
    """GET к ISS с паузой PAUSE; сбой — до 3 попыток, MAX_FAILS сбоев подряд → IssDown."""
    for attempt in range(3):
        wait = PAUSE - (time.monotonic() - _last[0])
        if wait > 0:
            time.sleep(wait)
        _last[0] = time.monotonic()
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=_UA), timeout=30) as r:
                data = json.load(r)
            _fails[0] = 0
            return data
        except Exception as e:  # noqa: BLE001
            _fails[0] += 1
            log.warning("сбой ISS (%d подряд): %s: %s", _fails[0], type(e).__name__, str(e)[:120])
            if _fails[0] >= MAX_FAILS:
                raise IssDown(url)
            time.sleep(5 * (attempt + 1))
    raise IssDown(url)


def _rows(block: dict | None) -> list[dict]:
    block = block or {}
    return [dict(zip(block.get("columns") or [], r)) for r in block.get("data") or []]


def _clean(v):
    """ISS пишет «нет даты» как 0000-00-00, пустые поля — ''."""
    if v == "" or (isinstance(v, str) and v.startswith("0000-00-00")):
        return None
    return v


def _hist_record(r: dict, isin: str | None) -> dict:
    rec = {k.lower(): _clean(v) for k, v in r.items()}
    rec["trade_date"] = rec.pop("tradedate")
    rec["isin"] = isin
    return rec


def _paged(url: str):
    """Все страницы history (по 100 строк, курсор history.cursor)."""
    start = 0
    while True:
        d = get(f"{url}&start={start}")
        rows = _rows(d.get("history"))
        yield rows
        cur = (_rows(d.get("history.cursor")) or [{}])[0]
        start += cur.get("PAGESIZE") or 100
        if not rows or start >= (cur.get("TOTAL") or 0):
            return


def _upsert(conn, table: str, key: str, records: list[dict]) -> int:
    """UPSERT пачкой одним запросом: jsonb_populate_recordset сам приводит типы колонок таблицы."""
    if not records:
        return 0
    cols = sorted({c for r in records for c in r})
    keys = [k.strip() for k in key.split(",")]
    upd = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c not in keys)
    stamp = "loaded_at" if table == "bond_history" else "updated_at"
    conn.execute(text(f"""
        INSERT INTO {table} ({', '.join(cols)})
        SELECT {', '.join(cols)} FROM jsonb_populate_recordset(NULL::{table}, CAST(:js AS jsonb))
        ON CONFLICT ({key}) DO UPDATE SET {upd}, {stamp} = now()
    """), {"js": json.dumps(records, ensure_ascii=False, default=str)})
    return len(records)


# ── Справочник и события выпуска ──────────────────────────────────────────

def fetch_ref(secid: str, isin: str) -> tuple[dict, list[dict]]:
    """description+boards → строка bond_ref; bondization → купоны/амортизации/оферты."""
    q = urllib.parse.quote(secid)
    d = get(f"{ISS}/securities/{q}.json?iss.meta=off&iss.only=description,boards")
    desc = {r["name"]: r["value"] for r in _rows(d.get("description"))}
    boards = _rows(d.get("boards"))
    flag = lambda k: None if desc.get(k) in (None, "") else desc.get(k) == "1"  # noqa: E731
    ref = {
        "isin": isin, "secid": secid, "shortname": desc.get("SHORTNAME"), "name": desc.get("NAME"),
        "regnumber": desc.get("REGNUMBER"), "emitter_id": desc.get("EMITTER_ID"),
        "issuedate": desc.get("ISSUEDATE"), "matdate": desc.get("MATDATE"),
        "initialfacevalue": desc.get("INITIALFACEVALUE"), "faceunit": desc.get("FACEUNIT"),
        "issuesize": desc.get("ISSUESIZE"), "couponfrequency": desc.get("COUPONFREQUENCY"),
        "bond_type": desc.get("BOND_TYPE"), "bond_subtype": desc.get("BOND_SUBTYPE"),
        "secsubtype": desc.get("SECSUBTYPE"), "coupon_benchmark": desc.get("COUPON_BENCHMARK"),
        "coupon_benchmark_spread": desc.get("COUPON_BENCHMARK_SPREAD"), "listlevel": desc.get("LISTLEVEL"),
        "is_qualified": flag("ISQUALIFIEDINVESTORS"), "is_subord": flag("SUBORDBOND"),
        "has_default": flag("HASDEFAULT"), "has_technical_default": flag("HASTECHNICALDEFAULT"),
        "history_from": min((b["history_from"] for b in boards if b.get("history_from")), default=None),
        "history_till": max((b["history_till"] for b in boards if b.get("history_till")), default=None),
        "description": desc,
    }
    ref = {k: (v if k == "description" else _clean(v)) for k, v in ref.items()}
    bz = get(f"{ISS}/statistics/engines/stock/markets/bonds/bondization/{q}.json?iss.meta=off&limit=unlimited")
    events = []
    for kind, block, dk, vk, pk in (("coupon", "coupons", "coupondate", "value", "valueprc"),
                                    ("amortization", "amortizations", "amortdate", "value", "valueprc"),
                                    ("offer", "offers", "offerdate", "value", "price")):
        seen = set()
        for r in _rows(bz.get(block)):
            dt = _clean(r.get(dk))
            if not dt or dt in seen:
                continue
            seen.add(dt)
            events.append({"secid": secid, "kind": kind, "event_date": dt, "isin": isin,
                           "value": _clean(r.get(vk)), "value_prc": _clean(r.get(pk)),
                           "value_rub": _clean(r.get("value_rub")), "facevalue": _clean(r.get("facevalue")),
                           "raw": r})
    return ref, events


# ── Прогон ────────────────────────────────────────────────────────────────

def universe(conn) -> dict[str, str]:
    """secid → isin: бумаги из разовой выгрузки + облигации, найденные в составах фондов."""
    rows = conn.execute(text("""
        SELECT secid, isin FROM bond_ref
        UNION
        SELECT secid, isin FROM bond_maturity
        WHERE is_bond AND secid IS NOT NULL AND isin NOT IN (SELECT isin FROM bond_ref)
    """)).all()
    return {s: i for s, i in rows}


def run(engine, date_from: date | None, max_new: int, refresh: int, dry: bool) -> dict:
    today = date.today()
    with engine.begin() as conn:
        uni = universe(conn)
        last = conn.execute(text("SELECT max(trade_date) FROM bond_history")).scalar()
        have = set(conn.execute(text("SELECT DISTINCT secid FROM bond_history")).scalars())
        # Бумага из bond_ref без истории = ни разу не торговалась (4 таких после выгрузки) — не «новая»,
        # иначе её спрашивали бы каждый день. Её сделки, если появятся, принесёт проход по дням.
        have |= set(conn.execute(text("SELECT secid FROM bond_ref")).scalars())
        stale = conn.execute(text("""
            SELECT secid FROM bond_ref
            WHERE matdate IS NULL OR matdate >= current_date - 30
            ORDER BY updated_at NULLS FIRST LIMIT :n
        """), {"n": refresh}).scalars().all()
    start = date_from or last or today
    stats = {"бумаг": len(uni), "дни": f"{start}…{today}", "строк_за_дни": 0,
             "новых_бумаг": 0, "строк_новых": 0, "справочник_обновлён": 0}

    def save(table, key, records):
        if dry:
            return len(records)
        with engine.begin() as conn:
            return _upsert(conn, table, key, records)

    try:
        # 1. Дни целиком: один проход по рынку дешевле, чем 1100 запросов по бумагам
        d = start
        while d <= today:
            if d.weekday() < 5 or d == today:
                recs = [_hist_record(r, uni[r["SECID"]])
                        for page in _paged(f"{ISS}/history/engines/stock/markets/bonds/securities.json"
                                           f"?iss.meta=off&date={d}")
                        for r in page if r.get("SECID") in uni]
                n = save("bond_history", "secid, trade_date, boardid", recs)
                stats["строк_за_дни"] += n
                log.info("%s: %d строк наших бумаг", d, n)
            d += timedelta(days=1)

        # 2. Новые бумаги — вся история и описание выпуска
        new = [s for s in uni if s not in have][:max_new]
        for secid in new:
            isin = uni[secid]
            recs = [_hist_record(r, isin)
                    for page in _paged(f"{ISS}/history/engines/stock/markets/bonds/securities/"
                                       f"{urllib.parse.quote(secid)}.json?iss.meta=off")
                    for r in page]
            ref, events = fetch_ref(secid, isin)
            stats["строк_новых"] += save("bond_history", "secid, trade_date, boardid", recs)
            save("bond_ref", "isin", [ref])
            save("bond_events", "secid, kind, event_date", events)
            stats["новых_бумаг"] += 1
            log.info("новая бумага %s (%s): %d дней истории, событий %d", secid, isin, len(recs), len(events))

        # 3. Справочник и купоны известных бумаг — по кругу, самые давние первыми
        for secid in stale:
            if secid in new:
                continue
            ref, events = fetch_ref(secid, uni.get(secid) or "")
            if not ref["isin"]:
                continue
            save("bond_ref", "isin", [ref])
            save("bond_events", "secid, kind, event_date", events)
            stats["справочник_обновлён"] += 1
    except IssDown:
        log.error("ISS не отвечает %d раз подряд — стоп; окно с последней даты догонит следующий прогон", MAX_FAILS)
        stats["остановлен_по_сбоям"] = True
    stats["осталось_новых"] = max(0, len([s for s in uni if s not in have]) - stats["новых_бумаг"])
    return stats


def main():
    ap = argparse.ArgumentParser(description="Ежедневные цены облигаций из составов фондов (ISS)")
    ap.add_argument("--once", action="store_true", help="Один прогон и выход")
    ap.add_argument("--from", dest="date_from", type=date.fromisoformat, help="Начало окна (по умолчанию — последняя дата в базе)")
    ap.add_argument("--max-new", type=int, default=10, help="Новых бумаг за прогон (вся история)")
    ap.add_argument("--refresh", type=int, default=40, help="Обновить справочник/купоны стольким бумагам")
    ap.add_argument("--dry-run", action="store_true", help="Скачать и посчитать, в базу не писать")
    args = ap.parse_args()
    if not args.once:
        ap.print_help()
        return
    if not DB_URL:
        log.error("DB_URL не задан")
        sys.exit(1)
    engine = create_engine(DB_URL, connect_args={"ssl_context": False})
    stats = run(engine, args.date_from, args.max_new, args.refresh, args.dry_run)
    log.info("итог: %s", json.dumps(stats, ensure_ascii=False))
    if stats.get("остановлен_по_сбоям"):
        sys.exit(1)


if __name__ == "__main__":
    main()
