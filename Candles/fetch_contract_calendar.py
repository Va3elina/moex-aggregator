#!/usr/bin/env python3
"""
Календарь экспираций фьючерсных контрактов MOEX → таблица futures_contracts.

Источник истины для КАЛЕНДАРНОГО ролловера (вместо объёмного). Фронт-контракт
выбирается по реальной дате экспирации lsttrade — см. api/services/contract_calendar.py.

Две фазы:
  Phase A (всегда, дёшево — 1 запрос):
      список активных контрактов FORTS
      https://iss.moex.com/iss/engines/futures/markets/forts/securities.json
      → SECID, ASSETCODE, LASTTRADEDATE(=lsttrade), LASTDELDATE(=lstdeldate)
      UPSERT. Контракты, пропавшие из листинга → is_traded=false (строки НЕ удаляем).
  Phase B (только --backfill, разово на деплое):
      засев ИСТОРИЧЕСКИХ контрактов, которые уже есть в candles, но ещё не в
      futures_contracts: для каждого тянем description
      https://iss.moex.com/iss/securities/{secid}.json?iss.only=description
      → FRSTTRADE, LSTTRADE, LSTDELDATE, ASSETCODE. Это закрывает недавнюю историю
      графиков календарём (иначе старые дни откатываются на объёмный fallback).

Связь кодов:
  secid 'BRN6' (PK) | sec_id 'BRN' (=candles.sec_id) | sectype 'BR' (=open_interest.sectype)

HTTP через stdlib urllib (как fetch_futures_turnover.py) — без внешних зависимостей.

Запуск:
    python fetch_contract_calendar.py --once --force      # Phase A (оркестратор, daily)
    python fetch_contract_calendar.py --backfill          # Phase A + Phase B (разово)
"""

import sys
import os
import json
import time
import logging
import argparse
import urllib.request
from datetime import date, datetime
from pathlib import Path

from sqlalchemy import create_engine, text
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent.parent))
load_dotenv(Path(__file__).parent.parent / ".env")

DB_URL = os.getenv("DB_URL")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

ISS_LIST_URL = (
    "https://iss.moex.com/iss/engines/futures/markets/forts/securities.json"
    "?iss.meta=off&iss.only=securities"
    "&securities.columns=SECID,ASSETCODE,LASTTRADEDATE,LASTDELDATE"
)
ISS_DESC_URL = "https://iss.moex.com/iss/securities/{secid}.json?iss.meta=off&iss.only=description"
_UA = {"User-Agent": "frame-contract-calendar/1.0"}

MONTH_LETTERS = set("FGHJKMNQUVXZ")
MAX_BACKFILL_PER_RUN = 800  # бережём ISS: за прогон догружаем не больше N историч. контрактов


def _get_json(url: str, timeout: int = 30):
    req = urllib.request.Request(url, headers=_UA)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _create_table(engine):
    """Идемпотентно создаёт таблицу + индекс (зеркало db/migrations/013_*.sql)."""
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS futures_contracts (
                secid           VARCHAR(32) PRIMARY KEY,
                sectype         VARCHAR(20) NOT NULL,
                sec_id          VARCHAR(20) NOT NULL,
                assetcode       VARCHAR(20),
                frsttrade       DATE,
                lsttrade        DATE,
                lstdeldate      DATE,
                expiration_time TIME,
                is_perpetual    BOOLEAN NOT NULL DEFAULT FALSE,
                is_traded       BOOLEAN NOT NULL DEFAULT TRUE,
                last_seen       TIMESTAMP,
                updated_at      TIMESTAMP DEFAULT now()
            )
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_futures_contracts_roll
                ON futures_contracts (sectype, lsttrade)
        """))


def _derive(secid: str):
    """secid → (sectype, sec_id, is_perpetual).

    Датированный контракт 'BRN6' (2 буквы + месяц + год) → ('BR','BRN',False).
    Иначе именной/вечный 'USDRUBF','GAZPF' → (secid, secid, True).
    """
    if len(secid) == 4 and secid[2] in MONTH_LETTERS and secid[3].isdigit():
        return secid[:2], secid[:3], False
    return secid, secid, True


def _parse_date(val):
    if not val:
        return None
    try:
        return date.fromisoformat(str(val)[:10])
    except (ValueError, TypeError):
        return None


def _fetch_active():
    """Phase A: список активных контрактов из FORTS. → list[dict]."""
    data = _get_json(ISS_LIST_URL)["securities"]
    cols = data["columns"]
    idx = {c: i for i, c in enumerate(cols)}
    out = []
    for row in data["data"]:
        secid = row[idx["SECID"]]
        if not secid:
            continue
        sectype, sec_id, is_perp = _derive(secid)
        out.append({
            "secid": secid,
            "sectype": sectype,
            "sec_id": sec_id,
            "assetcode": row[idx["ASSETCODE"]],
            "lsttrade": _parse_date(row[idx["LASTTRADEDATE"]]),
            "lstdeldate": _parse_date(row[idx["LASTDELDATE"]]),
            "frsttrade": None,
            "expiration_time": None,
            "is_perpetual": is_perp,
            "is_traded": True,
        })
    return out


def _fetch_description(secid: str):
    """Phase B: метаданные одного контракта из description. → dict | None."""
    try:
        rows = _get_json(ISS_DESC_URL.format(secid=secid), timeout=15).get(
            "description", {}).get("data", [])
        kv = {row[0]: row[2] for row in rows if len(row) >= 3}
    except Exception as e:
        log.debug(f"[{secid}] description error: {e}")
        return None

    sectype, sec_id, is_perp = _derive(secid)
    exp_time = kv.get("EXPIRATION_TIME") or kv.get("EXPIRATIONTIME")
    return {
        "secid": secid,
        "sectype": sectype,
        "sec_id": sec_id,
        "assetcode": kv.get("ASSETCODE"),
        "lsttrade": _parse_date(kv.get("LSTTRADE")),
        "lstdeldate": _parse_date(kv.get("LSTDELDATE")),
        "frsttrade": _parse_date(kv.get("FRSTTRADE")),
        "expiration_time": exp_time if exp_time else None,
        "is_perpetual": is_perp,
        "is_traded": False,   # бэкфилл — историч. контракты (не в активном листинге)
    }


_UPSERT_SQL = text("""
    INSERT INTO futures_contracts
        (secid, sectype, sec_id, assetcode, frsttrade, lsttrade, lstdeldate,
         expiration_time, is_perpetual, is_traded, last_seen, updated_at)
    VALUES
        (:secid, :sectype, :sec_id, :assetcode, :frsttrade, :lsttrade, :lstdeldate,
         :expiration_time, :is_perpetual, :is_traded, :run_ts, now())
    ON CONFLICT (secid) DO UPDATE SET
        sectype         = EXCLUDED.sectype,
        sec_id          = EXCLUDED.sec_id,
        assetcode       = COALESCE(EXCLUDED.assetcode, futures_contracts.assetcode),
        frsttrade       = COALESCE(EXCLUDED.frsttrade, futures_contracts.frsttrade),
        lsttrade        = COALESCE(EXCLUDED.lsttrade, futures_contracts.lsttrade),
        lstdeldate      = COALESCE(EXCLUDED.lstdeldate, futures_contracts.lstdeldate),
        expiration_time = COALESCE(EXCLUDED.expiration_time, futures_contracts.expiration_time),
        is_perpetual    = EXCLUDED.is_perpetual,
        is_traded       = EXCLUDED.is_traded,
        last_seen       = EXCLUDED.last_seen,
        updated_at      = now()
""")


def _upsert(engine, rows, run_ts):
    if not rows:
        return 0
    with engine.begin() as conn:
        for r in rows:
            conn.execute(_UPSERT_SQL, {**r, "run_ts": run_ts})
    return len(rows)


def run_phase_a(engine, run_ts) -> int:
    rows = _fetch_active()
    n = _upsert(engine, rows, run_ts)
    # Контракты, не виденные в этом прогоне (истёкшие / делистнутые) → is_traded=false.
    # Строки сохраняем (нужны для исторических фронт-окон).
    with engine.begin() as conn:
        res = conn.execute(text("""
            UPDATE futures_contracts
            SET is_traded = false, updated_at = now()
            WHERE is_traded = true AND (last_seen IS NULL OR last_seen < :run_ts)
        """), {"run_ts": run_ts})
        retired = res.rowcount or 0
    perp = sum(1 for r in rows if r["is_perpetual"])
    log.info(f"Phase A: активных {n} (вечных {perp}), снято с торгов {retired}")
    return n


def run_phase_b(engine, run_ts) -> int:
    """Засев историч. контрактов из candles, которых нет в futures_contracts."""
    with engine.connect() as conn:
        missing = [r[0] for r in conn.execute(text("""
            SELECT DISTINCT c.secid
            FROM candles c
            WHERE c.type = 'futures'
              AND char_length(c.secid) = 4
              AND NOT EXISTS (SELECT 1 FROM futures_contracts f WHERE f.secid = c.secid)
            ORDER BY c.secid
        """)).fetchall()]

    if not missing:
        log.info("Phase B: историч. контрактов для засева нет")
        return 0

    capped = missing[:MAX_BACKFILL_PER_RUN]
    log.info(f"Phase B: засев {len(capped)}/{len(missing)} историч. контрактов из candles...")
    rows = []
    for i, secid in enumerate(capped):
        d = _fetch_description(secid)
        if d and d["lsttrade"]:
            rows.append(d)
        time.sleep(0.05)  # вежливость к ISS
        if (i + 1) % 100 == 0:
            log.info(f"  ...{i + 1}/{len(capped)}")
    n = _upsert(engine, rows, run_ts)
    log.info(f"Phase B: засеяно {n} (из {len(capped)} запрошенных)")
    if len(missing) > len(capped):
        log.info(f"Phase B: осталось {len(missing) - len(capped)} — добьются на след. прогонах --backfill")
    return n


def main():
    ap = argparse.ArgumentParser(description="Календарь экспираций фьючерсов MOEX")
    ap.add_argument("--once", action="store_true", help="один прогон (для оркестратора)")
    ap.add_argument("--force", action="store_true", help="игнорировать gate (совместимость с оркестратором)")
    ap.add_argument("--backfill", action="store_true", help="+ засев историч. контрактов из candles")
    args = ap.parse_args()

    if not DB_URL:
        log.error("DB_URL не задан")
        sys.exit(1)

    engine = create_engine(DB_URL, pool_pre_ping=True)
    _create_table(engine)
    run_ts = datetime.now()

    try:
        run_phase_a(engine, run_ts)
    except Exception as e:
        log.error(f"Phase A провалилась: {e}")
        sys.exit(1)

    if args.backfill:
        try:
            run_phase_b(engine, run_ts)
        except Exception as e:
            log.error(f"Phase B провалилась: {e}")

    log.info("✓ Готово")


if __name__ == "__main__":
    main()
