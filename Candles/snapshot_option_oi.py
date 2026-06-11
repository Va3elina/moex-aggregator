#!/usr/bin/env python3
"""Снапшот опционного открытого интереса ПО СТРАЙКАМ (FORTS) → option_oi.

ISS отдаёт OPENPOSITION по каждому опциону в marketdata (БЕСПЛАТНО), но только
ТЕКУЩИЙ срез — истории нет. Поэтому копим ежедневные снапшоты (host-cron/orchestrator),
чтобы накопить историю OI по страйкам для анализа экспираций/pinning (паттерн #6).

Хранит только страйки с OPENPOSITION>0. Запуск раз в день после закрытия.
"""
import os
import json
import urllib.request
from datetime import date
from sqlalchemy import create_engine, text

DB_URL = os.getenv("DB_URL")
UA = "Mozilla/5.0 (compatible; FrameBot/1.0)"
# securities (статика: страйк/тип/базовый/экспирация) + marketdata (OPENPOSITION)
URL = ("https://iss.moex.com/iss/engines/futures/markets/options/securities.json"
       "?iss.meta=off&iss.only=securities,marketdata&"
       "securities.columns=SECID,ASSETCODE,STRIKE,OPTIONTYPE,LASTTRADEDATE&"
       "marketdata.columns=SECID,OPENPOSITION")


def main():
    req = urllib.request.Request(URL, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=60) as r:
        d = json.load(r)
    sc = d["securities"]["columns"]; sd = d["securities"]["data"]
    mc = d["marketdata"]["columns"]; md = d["marketdata"]["data"]
    oi = {row[mc.index("SECID")]: row[mc.index("OPENPOSITION")] for row in md}

    si = {k: sc.index(k) for k in ("SECID", "ASSETCODE", "STRIKE", "OPTIONTYPE", "LASTTRADEDATE")}
    rows = []
    for row in sd:
        sid = row[si["SECID"]]
        op = oi.get(sid)
        if not op or op <= 0:
            continue
        rows.append((sid, row[si["ASSETCODE"]], row[si["STRIKE"]],
                     row[si["OPTIONTYPE"]], row[si["LASTTRADEDATE"]], int(op)))
    print(f"опционов с OI>0: {len(rows)} (всего в выдаче {len(sd)})")

    eng = create_engine(DB_URL)
    with eng.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS option_oi (
                snapshot_date DATE NOT NULL,
                secid         VARCHAR(40) NOT NULL,
                asset_code    VARCHAR(20),
                strike        NUMERIC(20,4),
                opt_type      VARCHAR(2),     -- C/P
                expiry        DATE,
                open_position INTEGER,
                PRIMARY KEY (snapshot_date, secid)
            )
        """))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_option_oi_asset "
                          "ON option_oi (asset_code, snapshot_date, expiry)"))
        snap = date.today()
        n = 0
        for sid, ac, strike, ot, ltd, op in rows:
            conn.execute(text("""
                INSERT INTO option_oi (snapshot_date, secid, asset_code, strike, opt_type, expiry, open_position)
                VALUES (:sn, :sid, :ac, :st, :ot, :ex, :op)
                ON CONFLICT (snapshot_date, secid) DO UPDATE SET open_position = EXCLUDED.open_position
            """), {"sn": snap, "sid": sid, "ac": ac, "st": strike,
                   "ot": (ot[:1] if ot else None), "ex": (ltd or None), "op": op})
            n += 1
    print(f"снапшот {snap}: {n} строк в option_oi")


if __name__ == "__main__":
    main()
