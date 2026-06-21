#!/usr/bin/env python3
"""
Бэкфилл исторических ДНЕВНЫХ свечей фьючерсных контрактов с публичного ISS.

Зачем: старый объёмный фетчер качал только ликвидный фронт (по свежести свечи),
пропуская неликвидные промежуточные месяцы — напр. летние Brent BRQ/BRU/BRV.
Они РЕАЛЬНО торгуются на MOEX (есть на ISS с датами экспирации), но в нашей
candles их 0 → календарный ролл за лето откатывался на единственный ликвидный
летом BRX (он торгуется круглый год) → «BRX держится с июля по ноябрь».

Источник: публичный ISS (iss.moex.com, без авторизации). Проверено: дневные
свечи фьючерсов на ISS БАЙТ-В-БАЙТ совпадают с Algopack (прод-источник) →
расхождений на стыках не будет.

Что делает: для заданных sectype перебирает месяц×год, тянет дневные свечи и
описание контракта, сохраняет недостающие свечи (ON CONFLICT DO NOTHING) и
вносит контракт в futures_contracts (календарь, is_traded=false для истекших).

Usage:
  python Candles/backfill_futures_history.py --sectypes BR --dry-run
  python Candles/backfill_futures_history.py --sectypes BR
  python Candles/backfill_futures_history.py --sectypes BR NG --from-year 2019 --to-year 2027
"""
import os
import sys
import json
import time
import argparse
import logging
import urllib.request
from datetime import date, datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

MONTHS = "FGHJKMNQUVXZ"
ISS_CANDLES = ("https://iss.moex.com/iss/engines/futures/markets/forts/securities/{secid}/candles.json"
               "?iss.meta=off&interval=24&from={f}&till={t}"
               "&candles.columns=begin,open,high,low,close,volume,value")
ISS_DESC = "https://iss.moex.com/iss/securities/{secid}.json?iss.meta=off&iss.only=description"
_UA = {"User-Agent": "frame-futures-backfill/1.0"}


def _get(url, timeout=20):
    req = urllib.request.Request(url, headers=_UA)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


def _pdate(v):
    if not v:
        return None
    try:
        return date.fromisoformat(str(v)[:10])
    except (ValueError, TypeError):
        return None


def fetch_candles(secid, year):
    """Дневные свечи контракта за его жизнь (FRSTTRADE ~ за год до экспирации →
    тянем с year-1). Фильтруем по году, чтобы не зацепить контракт другой декады
    (один год-код раз в 10 лет)."""
    url = ISS_CANDLES.format(secid=secid, f=f"{year - 1}-01-01", t=f"{year}-12-31")
    try:
        rows = _get(url).get("candles", {}).get("data", [])
    except Exception as e:
        log.debug(f"{secid}: candles err {e}")
        return []
    return [r for r in rows if r[0][:4] in (str(year - 1), str(year))]


def fetch_desc(secid):
    try:
        rows = _get(ISS_DESC.format(secid=secid)).get("description", {}).get("data", [])
        kv = {x[0]: x[2] for x in rows if len(x) >= 3}
    except Exception:
        return None, None, None, None
    return (_pdate(kv.get("LSTTRADE")), _pdate(kv.get("FRSTTRADE")),
            _pdate(kv.get("LSTDELDATE")), kv.get("ASSETCODE"))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sectypes", nargs="+", required=True)
    ap.add_argument("--from-year", type=int, default=2019)
    ap.add_argument("--to-year", type=int, default=date.today().year + 1)
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()

    # NB: диапазон лет должен укладываться в 10 лет (год-код = 1 цифра, иначе
    # коллизия secid между декадами). 2019..2027 = 9 лет — ок.
    if a.to_year - a.from_year > 9:
        log.error("Диапазон лет > 10 — коллизия год-кода secid. Сузь.")
        sys.exit(1)

    engine = None
    candle_sql = upsert_cal_sql = None
    if not a.dry_run:
        from sqlalchemy import create_engine, text
        db_url = os.getenv("DB_URL")
        if not db_url:
            log.error("DB_URL не задан")
            sys.exit(1)
        engine = create_engine(db_url, pool_pre_ping=True)
        candle_sql = text("""
            INSERT INTO candles (secid, begin_time, interval, type, end_time,
                                 open, high, low, close, volume, value, sec_id)
            VALUES (:secid,:begin_time,24,'futures',:begin_time,
                    :open,:high,:low,:close,:volume,:value,:sec_id)
            ON CONFLICT (secid, begin_time, interval, type) DO NOTHING
        """)
        upsert_cal_sql = text("""
            INSERT INTO futures_contracts
                (secid,sectype,sec_id,assetcode,frsttrade,lsttrade,lstdeldate,
                 is_perpetual,is_traded,last_seen,updated_at)
            VALUES (:secid,:sectype,:sec_id,:assetcode,:frsttrade,:lsttrade,:lstdeldate,
                    false,false,now(),now())
            ON CONFLICT (secid) DO UPDATE SET
                lsttrade   = COALESCE(EXCLUDED.lsttrade, futures_contracts.lsttrade),
                frsttrade  = COALESCE(EXCLUDED.frsttrade, futures_contracts.frsttrade),
                lstdeldate = COALESCE(EXCLUDED.lstdeldate, futures_contracts.lstdeldate),
                assetcode  = COALESCE(EXCLUDED.assetcode, futures_contracts.assetcode),
                updated_at = now()
        """)

    total_candles = total_contracts = 0
    for st in a.sectypes:
        found = []
        for year in range(a.from_year, a.to_year + 1):
            yd = str(year)[-1]
            for m in MONTHS:
                secid = f"{st}{m}{yd}"
                rows = fetch_candles(secid, year)
                time.sleep(0.03)
                if not rows:
                    continue
                found.append((secid, year, rows))
        log.info(f"[{st}] контрактов со свечами: {len(found)}")
        for secid, year, rows in found:
            sec_id = secid[:3]
            if a.dry_run:
                log.info(f"  [dry] {secid}: {len(rows)} свечей {rows[0][0][:10]}..{rows[-1][0][:10]}")
                total_candles += len(rows)
                total_contracts += 1
                continue
            lst, frst, lstdel, ac = fetch_desc(secid)
            time.sleep(0.03)
            with engine.begin() as conn:
                for r in rows:
                    bt = datetime.fromisoformat(r[0])
                    conn.execute(candle_sql, {
                        "secid": secid, "begin_time": bt,
                        "open": r[1], "high": r[2], "low": r[3], "close": r[4],
                        "volume": r[5], "value": r[6], "sec_id": sec_id,
                    })
                conn.execute(upsert_cal_sql, {
                    "secid": secid, "sectype": st, "sec_id": sec_id, "assetcode": ac,
                    "frsttrade": frst, "lsttrade": lst, "lstdeldate": lstdel,
                })
            total_candles += len(rows)
            total_contracts += 1
            log.info(f"  {secid}: +{len(rows)} свечей (lsttrade={lst})")

    log.info(f"ИТОГО: контрактов {total_contracts}, свечей {total_candles}"
             + (" (dry-run, ничего не записано)" if a.dry_run else ""))


if __name__ == "__main__":
    main()
