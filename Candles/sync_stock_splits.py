#!/usr/bin/env python3
"""
Синк реестра сплитов акций (stock_splits) со справочником ISS.

Источник: https://iss.moex.com/iss/statistics/engines/stock/splits — полный
список сплитов на фондовом рынке (secid, tradedate, before, after). Берём
только бумаги из instruments (type=stock), ratio = after / before — сколько
новых акций на 1 старую (обратный сплит VTBR 5000→1 даёт 0.0002).

Что НЕ трогаем: price_adjusted и note существующих строк. Признак «биржа
пересчитала цены до сплита» ставится руками; по опыту у всех сплитов из этого
справочника ISS цены задним числом пересчитаны (T, GMKN, TRNFP, VTBR, PLZL),
поэтому новые строки идут с price_adjusted = TRUE. Сплиты вне справочника
(BELU 1:8, конвертация SFIN 1.93) заводятся руками с price_adjusted = FALSE
(db/migrations/104_stock_splits.sql).

Одна строка на secid (PK): если у бумаги несколько сплитов, берётся последний —
у нас таких акций нет, ETF в instruments не входят.

Использование:
    python3 Candles/sync_stock_splits.py --once
"""
import argparse
import json
import logging
import os
import sys
import urllib.request
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

PROJECT_DIR = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_DIR / ".env")
DB_URL = os.getenv("DB_URL")

ISS_SPLITS = "https://iss.moex.com/iss/statistics/engines/stock/splits.json?iss.meta=off"
_UA = {"User-Agent": "Mozilla/5.0 (frame-splits-sync)"}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("stock_splits")


def fetch_iss_splits() -> dict[str, tuple[str, float]]:
    """{secid: (tradedate, ratio)} — по последнему сплиту бумаги."""
    req = urllib.request.Request(ISS_SPLITS, headers=_UA)
    with urllib.request.urlopen(req, timeout=30) as r:
        d = json.load(r)["splits"]
    cols = d["columns"]
    out: dict[str, tuple[str, float]] = {}
    for row in sorted(d["data"], key=lambda x: x[cols.index("tradedate")]):
        rec = dict(zip(cols, row))
        before, after = float(rec["before"]), float(rec["after"])
        if before <= 0 or after <= 0:
            continue
        out[rec["secid"]] = (rec["tradedate"], after / before)
    return out


def sync(engine) -> dict:
    iss = fetch_iss_splits()
    with engine.begin() as conn:
        stocks = {r[0] for r in conn.execute(text(
            "SELECT sec_id FROM instruments WHERE type = 'stock'"))}
        existing = {r[0]: (r[1], float(r[2])) for r in conn.execute(text(
            "SELECT secid, split_date, ratio FROM stock_splits"))}
        new = updated = 0
        for secid, (d, ratio) in iss.items():
            if secid not in stocks:
                continue
            cur = existing.get(secid)
            if cur and cur[0].isoformat() == d and abs(cur[1] - ratio) < 1e-9:
                continue
            conn.execute(text("""
                INSERT INTO stock_splits (secid, split_date, ratio, price_adjusted, note)
                VALUES (:s, :d, :r, TRUE, :note)
                ON CONFLICT (secid) DO UPDATE
                    SET split_date = EXCLUDED.split_date, ratio = EXCLUDED.ratio
            """), {"s": secid, "d": d, "r": ratio,
                   "note": f"из справочника ISS splits, ratio {ratio:g}"})
            if cur:
                updated += 1
                log.info("обновлён %s: %s ratio %g", secid, d, ratio)
            else:
                new += 1
                log.info("новый сплит %s: %s ratio %g", secid, d, ratio)
    return {"новых": new, "обновлено": updated, "в_справочнике": len(iss)}


def main():
    parser = argparse.ArgumentParser(description="Синк stock_splits с ISS")
    parser.add_argument("--once", action="store_true", help="Один прогон и выход")
    args = parser.parse_args()
    if not args.once:
        parser.print_help()
        return
    if not DB_URL:
        log.error("DB_URL не задан")
        sys.exit(1)
    engine = create_engine(DB_URL, connect_args={"ssl_context": False})
    try:
        res = sync(engine)
    finally:
        engine.dispose()
    print(json.dumps(res, ensure_ascii=False))


if __name__ == "__main__":
    main()
