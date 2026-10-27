#!/usr/bin/env python3
"""
Даты погашения облигаций из составов фондов (bond_maturity) из справочника ISS.

Зачем: «Сделки фондов» считают сделку диффом двух месячных справок. Погашенная
облигация из состава просто исчезает, и без даты погашения дифф рисует её как
полную распродажу (РусГидро БО-П06 погашена 15.09.2026 → −720.8 млн ₽ в «Чистых
продажах» за сентябрь). API (api/routers/fund_trades.py) по этой таблице не
считает исчезновение погашенной бумаги сделкой.

Что делает (идемпотентно, fund_holdings_history не трогает):
- Засевает таблицу из bond_ref (разовая выгрузка research/bonds_iss), если она есть.
- Для каждого ISIN из составов, которого ещё нет в bond_maturity, спрашивает ISS
  securities.json?q=ISIN → secid, тип. Облигация → securities/<SECID>.json
  (description) → MATDATE. Не облигация → is_bond = FALSE, больше не спрашиваем.
- Облигации без даты (ISS не знал, бессрочные) переспрашивает раз в неделю.

Новые бумаги в составах появляются раз в месяц с новыми справками, поэтому
обычный прогон делает единицы запросов. Первый прогон дольше: --max ограничивает
число бумаг за прогон, остаток доберут следующие дни.

Использование:
    python3 Funds/sync_bond_maturities.py --once [--max 300]
"""
import argparse
import json
import logging
import os
import re
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

PROJECT_DIR = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_DIR / ".env")
DB_URL = os.getenv("DB_URL")

ISS = "https://iss.moex.com/iss"
_UA = {"User-Agent": "Mozilla/5.0 (frame-bond-maturity-sync)"}
PAUSE = 1.0          # секунды между запросами: бан IP сервера в ISS кладёт все фетчеры МосБиржи
MAX_FAILS = 5        # подряд сбоев → стоп (похоже на бан/блэкхол ISS)
RECHECK_DAYS = 7     # облигацию без даты переспрашиваем не чаще
_ISIN_RE = re.compile(r"^[A-Z]{2}[A-Z0-9]{9}[0-9]$")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("bond_maturity")

DDL = """
CREATE TABLE IF NOT EXISTS bond_maturity (
    isin        VARCHAR(20) PRIMARY KEY,
    secid       VARCHAR(40),
    shortname   VARCHAR(64),
    matdate     DATE,
    is_bond     BOOLEAN,
    checked_at  TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""

_last = [0.0]
_fails = [0]


class IssDown(RuntimeError):
    pass


def get(url: str):
    """GET к ISS с паузой PAUSE от предыдущего. MAX_FAILS сбоев подряд → IssDown."""
    wait = PAUSE - (time.monotonic() - _last[0])
    if wait > 0:
        time.sleep(wait)
    _last[0] = time.monotonic()
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers=_UA), timeout=20) as r:
            data = json.load(r)
        _fails[0] = 0
        return data
    except Exception as e:  # noqa: BLE001
        _fails[0] += 1
        log.warning("сбой ISS (%d подряд): %s: %s", _fails[0], type(e).__name__, str(e)[:120])
        if _fails[0] >= MAX_FAILS:
            raise IssDown(url)
        return None


def is_bond_row(sec_type: str | None, group: str | None) -> bool:
    return group == "stock_bonds" or bool(sec_type and sec_type.endswith("_bond"))


def lookup(isin: str) -> dict | None:
    """{secid, shortname, is_bond, matdate} по ISIN; None — сбой запроса (повторим в другой раз)."""
    d = get(f"{ISS}/securities.json?iss.meta=off&q={urllib.parse.quote(isin)}"
            "&securities.columns=secid,shortname,isin,type,group,is_traded")
    if d is None:
        return None
    rows = [r for r in d["securities"]["data"] if r[2] == isin]
    if not rows:
        return {"secid": None, "shortname": None, "is_bond": None, "matdate": None}
    rows.sort(key=lambda r: (0 if r[5] == 1 else 1, len(r[0] or "")))
    bond = next((r for r in rows if is_bond_row(r[3], r[4])), None)
    if bond is None:
        return {"secid": rows[0][0], "shortname": rows[0][1], "is_bond": False, "matdate": None}
    desc = get(f"{ISS}/securities/{urllib.parse.quote(bond[0])}.json?iss.meta=off&iss.only=description")
    if desc is None:
        return None
    fields = {r[0]: r[2] for r in desc["description"]["data"]}
    mat = fields.get("MATDATE") or ""
    return {"secid": bond[0], "shortname": bond[1], "is_bond": True,
            "matdate": mat if re.match(r"^\d{4}-\d{2}-\d{2}$", mat) else None}


def sync(engine, max_n: int) -> dict:
    with engine.begin() as conn:
        conn.execute(text(DDL))
        seeded = 0
        if conn.execute(text("SELECT to_regclass('bond_ref')")).scalar():
            seeded = conn.execute(text("""
                INSERT INTO bond_maturity (isin, secid, shortname, matdate, is_bond)
                SELECT isin, secid, shortname, matdate, TRUE FROM bond_ref WHERE matdate IS NOT NULL
                ON CONFLICT (isin) DO NOTHING
            """)).rowcount
        # Новые ISIN первыми, свежие (по последней справке) раньше старых.
        cands = conn.execute(text("""
            WITH h AS (
                SELECT isin, MAX(snapshot_date) AS last_seen
                FROM fund_holdings_history
                WHERE COALESCE(isin, '') <> ''
                GROUP BY isin
            )
            SELECT h.isin
            FROM h LEFT JOIN bond_maturity b ON b.isin = h.isin
            WHERE b.isin IS NULL
               OR (b.is_bond IS NOT FALSE AND b.matdate IS NULL
                   AND b.checked_at < now() - make_interval(days => :recheck))
            ORDER BY (b.isin IS NULL) DESC, h.last_seen DESC
        """), {"recheck": RECHECK_DAYS}).scalars().all()
    cands = [i for i in cands if _ISIN_RE.match(i)]
    todo = cands[:max_n]

    stats = {"засеяно_из_bond_ref": seeded, "к_проверке": len(cands), "проверено": 0,
             "облигаций": 0, "с_датой": 0, "не_облигаций": 0, "неизвестно_iss": 0, "сбоев": 0}
    try:
        for isin in todo:
            res = lookup(isin)
            if res is None:
                stats["сбоев"] += 1
                continue
            with engine.begin() as conn:
                conn.execute(text("""
                    INSERT INTO bond_maturity (isin, secid, shortname, matdate, is_bond, checked_at)
                    VALUES (:isin, :secid, :shortname, :matdate, :is_bond, now())
                    ON CONFLICT (isin) DO UPDATE SET
                        secid = COALESCE(EXCLUDED.secid, bond_maturity.secid),
                        shortname = COALESCE(EXCLUDED.shortname, bond_maturity.shortname),
                        matdate = COALESCE(EXCLUDED.matdate, bond_maturity.matdate),
                        is_bond = COALESCE(EXCLUDED.is_bond, bond_maturity.is_bond),
                        checked_at = now()
                """), {"isin": isin, **res})
            stats["проверено"] += 1
            if res["is_bond"] is True:
                stats["облигаций"] += 1
                stats["с_датой"] += 1 if res["matdate"] else 0
            elif res["is_bond"] is False:
                stats["не_облигаций"] += 1
            else:
                stats["неизвестно_iss"] += 1
    except IssDown:
        log.error("ISS не отвечает %d раз подряд, останавливаюсь; остаток доберёт следующий прогон", MAX_FAILS)
        stats["остановлен_по_сбоям"] = True
    stats["осталось"] = len(cands) - stats["проверено"]
    return stats


def main():
    parser = argparse.ArgumentParser(description="Синк bond_maturity с ISS")
    parser.add_argument("--once", action="store_true", help="Один прогон и выход")
    parser.add_argument("--max", type=int, default=300, help="Максимум бумаг за прогон")
    args = parser.parse_args()
    if not args.once:
        parser.print_help()
        return
    if not DB_URL:
        log.error("DB_URL не задан")
        sys.exit(1)
    engine = create_engine(DB_URL, connect_args={"ssl_context": False})
    try:
        res = sync(engine, args.max)
    finally:
        engine.dispose()
    print(json.dumps(res, ensure_ascii=False))
    if res.get("остановлен_по_сбоям"):
        sys.exit(1)


if __name__ == "__main__":
    main()
