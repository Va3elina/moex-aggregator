#!/usr/bin/env python3
"""
Бэкфилл ВНУТРИДНЕВНЫХ свечей (60м и 5м) для фьючерсных контрактов, у которых их
нет (напр. летние Brent BRQ/BRU/BRV — дневные уже залиты backfill_futures_history).

Зачем: живой фетчер качает intraday только для ТЕКУЩЕГО фронта. Истёкшие
неликвидные месяцы остались без 5м/60м → на этих ТФ календарный ролл за лето
откатывался на BRX. Дневной бэкфилл это не покрыл.

Источник: ПУБЛИЧНЫЙ ISS (interval=60 напрямую; interval=1 → агрегация в 5м;
проверено: дневные/intraday ISS = Algopack байт-в-байт, наш прод-источник).
Тянет ТОЛЬКО окно фронта каждого контракта (между экспирацией предыдущего и
своей) — контракт виден на графике только там → размер ограничен.

Скоупится на контракты, у которых интервала ещё НЕТ (ликвидные фронты пропускаются).

Usage (на сервере: нужен DB_URL + egress к ISS + pandas):
  python backfill_futures_intraday.py --sectypes BR --intervals 60 5
  python backfill_futures_intraday.py --sectypes BR --intervals 60 --dry-run
"""
import os
import sys
import json
import time
import argparse
import logging
import urllib.request
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

ISS = ("https://iss.moex.com/iss/engines/futures/markets/forts/securities/{secid}/candles.json"
       "?iss.meta=off&interval={iv}&from={f}&till={t}&start={s}"
       "&candles.columns=begin,open,high,low,close,volume,value")
_UA = {"User-Agent": "frame-futures-intraday-backfill/1.0"}
# окно фронта для самого старого контракта (нет предыдущего) — ~6 недель назад
FIRST_WINDOW_DAYS = 45


def _get(url, timeout=30):
    req = urllib.request.Request(url, headers=_UA)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


def fetch_paged(secid, iv, frm, till):
    """ISS candles с пагинацией (cap 500/страница)."""
    out, start = [], 0
    while True:
        try:
            page = _get(ISS.format(secid=secid, iv=iv, f=frm, t=till, s=start)).get(
                "candles", {}).get("data", [])
        except Exception as e:
            log.debug(f"{secid} iv={iv} start={start}: {e}")
            break
        if not page:
            break
        out += page
        if len(page) < 500:
            break
        start += len(page)
        time.sleep(0.04)
    return out


def agg_5min(rows):
    """1-минутки → 5-минутки (open=first, high=max, low=min, close=last, vol/val=sum)."""
    import pandas as pd
    if not rows:
        return []
    df = pd.DataFrame(rows, columns=["begin", "open", "high", "low", "close", "volume", "value"])
    df["begin"] = pd.to_datetime(df["begin"])
    df = df.set_index("begin").sort_index()
    g = df.resample("5min").agg(
        open=("open", "first"), high=("high", "max"), low=("low", "min"),
        close=("close", "last"), volume=("volume", "sum"), value=("value", "sum"),
    ).dropna(subset=["close"])
    return [(idx.to_pydatetime(), r.open, r.high, r.low, r.close, r.volume, r.value)
            for idx, r in g.iterrows()]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sectypes", nargs="+", required=True)
    ap.add_argument("--intervals", nargs="+", type=int, default=[60, 5])
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()

    from sqlalchemy import create_engine, text
    eng = create_engine(os.getenv("DB_URL"), pool_pre_ping=True)
    candle_sql = text("""
        INSERT INTO candles (secid,begin_time,interval,type,end_time,open,high,low,close,volume,value,sec_id)
        VALUES (:secid,:begin_time,:iv,'futures',:begin_time,:open,:high,:low,:close,:volume,:value,:sec_id)
        ON CONFLICT (secid,begin_time,interval,type) DO NOTHING
    """)

    total = 0
    for st in a.sectypes:
        with eng.connect() as c:
            rows = c.execute(text("""
                SELECT secid, sec_id, lsttrade FROM futures_contracts
                WHERE sectype=:s AND NOT is_perpetual AND lsttrade IS NOT NULL
                ORDER BY lsttrade
            """), {"s": st}).fetchall()
        # окна фронта: (lsttrade предыдущего +1д .. свой lsttrade]
        prev = None
        windows = []
        for secid, sec_id, lst in rows:
            start = (prev + timedelta(days=1)) if prev else (lst - timedelta(days=FIRST_WINDOW_DAYS))
            windows.append((secid, sec_id, start, lst))
            prev = lst
        log.info(f"[{st}] контрактов в календаре: {len(windows)}")

        for secid, sec_id, wstart, wend in windows:
            for iv in a.intervals:
                with eng.connect() as c:
                    have = c.execute(text("SELECT count(*) FROM candles WHERE secid=:s AND interval=:i"),
                                     {"s": secid, "i": iv}).scalar()
                if have and have > 0:
                    continue  # ликвидный фронт уже скачан — пропускаем
                src_iv = 1 if iv == 5 else iv
                raw = fetch_paged(secid, src_iv, wstart.isoformat(), wend.isoformat())
                if not raw:
                    continue
                data = (agg_5min(raw) if iv == 5
                        else [(datetime.fromisoformat(r[0]), r[1], r[2], r[3], r[4], r[5], r[6]) for r in raw])
                if a.dry_run:
                    log.info(f"  [dry] {secid} iv={iv}: {len(data)} баров {wstart}..{wend}")
                    total += len(data)
                    continue
                with eng.begin() as c:
                    for bt, o, h, l, cl, v, val in data:
                        c.execute(candle_sql, {"secid": secid, "begin_time": bt, "iv": iv,
                                               "open": o, "high": h, "low": l, "close": cl,
                                               "volume": v, "value": val, "sec_id": sec_id})
                total += len(data)
                log.info(f"  {secid} iv={iv}: +{len(data)}")
                time.sleep(0.04)
    log.info(f"ИТОГО баров: {total}" + (" (dry-run)" if a.dry_run else ""))


if __name__ == "__main__":
    main()
