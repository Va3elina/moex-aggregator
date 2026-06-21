#!/usr/bin/env python3
"""
Бэкфилл fund_holdings_history.isin для строк БЕЗ ISIN — по гос-рег-номеру,
зашитому в asset_name. Закрывает видимые дубли (одна бумага строкой с ISIN и
строкой без него) — после бэкфилла «сирота» встаёт на свой ISIN и сливается
в ISIN-группу в существующих диффах.

Извлекаем код из имени и резолвим через ISS /securities.json?q=код → isin:
  - ОФЗ:     SU26250RMFS9   («Россия, 26250 (ОФЗ-ПД, SU26250RMFS9)»)
  - гос-рег: 1-01-16812-A / 2-03-00161-A («…, ао, 1-01-…»)  → акции/корп.облиг
  - банк:    10301481B      («Сбербанк, ао, 10301481B»)

Безопасность: UPDATE только если рег-номер ISS-резолвится ОДНОЗНАЧНО в isin И
новый ключ (fund,isin,name,date,source) ещё не занят (guard от unique-конфликта).
--dry-run по умолчанию; --commit применяет. Идемпотентно.

Запуск на сервере: docker exec -i frame-orchestrator-1 python3 - --dry-run < файл
"""
import os
import re
import sys
import json
import time
import urllib.request
import urllib.parse
from sqlalchemy import create_engine, text

ISS_URL = ("https://iss.moex.com/iss/securities.json?iss.meta=off&q={q}"
           "&securities.columns=secid,shortname,isin,regnumber,type,is_traded")
_UA = {"User-Agent": "frame-holdings-isin-backfill/1.0"}

# Паттерны рег-кодов в порядке приоритета.
RE_OFZ = re.compile(r"\bSU\d{5}RMFS\d\b")
RE_GOSREG = re.compile(r"\b\d-\d{2}-\d{4,6}-[A-Z]{1,2}\b")
RE_BANK = re.compile(r"\b\d{7,9}[A-Z]\b")


def extract_code(name: str):
    for rx in (RE_OFZ, RE_GOSREG, RE_BANK):
        m = rx.search(name or "")
        if m:
            return m.group(0)
    return None


def iss(q):
    try:
        req = urllib.request.Request(ISS_URL.format(q=urllib.parse.quote(q)), headers=_UA)
        with urllib.request.urlopen(req, timeout=15) as r:
            return json.load(r)["securities"]["data"]
    except Exception:
        return None


def resolve_isin(code: str):
    """Рег-код → ISIN, только при ОДНОЗНАЧНОМ совпадении regnumber==code."""
    rows = iss(code)
    if not rows:
        return None
    cn = code.replace(" ", "")
    isins = {r[2] for r in rows if r[2] and r[3] and r[3].replace(" ", "") == cn}
    if len(isins) == 1:
        return next(iter(isins))
    return None  # 0 или неоднозначно → не трогаем


def main():
    commit = "--commit" in sys.argv
    engine = create_engine(os.getenv("DB_URL"), pool_pre_ping=True)

    with engine.connect() as conn:
        names = [r[0] for r in conn.execute(text("""
            SELECT DISTINCT asset_name FROM fund_holdings_history
            WHERE (isin IS NULL OR isin='') AND asset_name IS NOT NULL
        """)).fetchall()]
    print(f"distinct no-ISIN имён: {len(names)}")

    # name → isin (только однозначно резолвленные)
    name_isin = {}
    no_code = unresolved = 0
    for i, nm in enumerate(names):
        code = extract_code(nm)
        if not code:
            no_code += 1
            continue
        isin = resolve_isin(code)
        if isin:
            name_isin[nm] = isin
        else:
            unresolved += 1
        time.sleep(0.03)
        if (i + 1) % 150 == 0:
            print(f"  ...{i+1}/{len(names)} (резолвлено {len(name_isin)})")
    print(f"резолвлено имён: {len(name_isin)}; без кода в имени: {no_code}; код есть, но ISS не дал: {unresolved}")

    # сколько строк затронем + guard от конфликтов
    applied = skipped = 0
    upd = text("""
        UPDATE fund_holdings_history h SET isin = :isin
        WHERE (h.isin IS NULL OR h.isin='') AND h.asset_name = :name
          AND NOT EXISTS (
            SELECT 1 FROM fund_holdings_history h2
            WHERE h2.fund_id=h.fund_id AND h2.isin=:isin AND h2.asset_name=h.asset_name
              AND h2.snapshot_date=h.snapshot_date AND h2.source=h.source)
    """)
    cnt = text("SELECT count(*) FROM fund_holdings_history WHERE (isin IS NULL OR isin='') AND asset_name=:name")
    for nm, isin in name_isin.items():
        with engine.connect() as conn:
            rows = conn.execute(cnt, {"name": nm}).scalar()
        if commit:
            with engine.begin() as conn:
                res = conn.execute(upd, {"isin": isin, "name": nm})
                applied += res.rowcount or 0
        else:
            applied += rows
    print(f"\n{'ПРИМЕНЕНО' if commit else 'БУДЕТ ЗАТРОНУТО (dry-run)'}: ~{applied} строк по {len(name_isin)} именам")

    # примеры резолва (для глаз)
    print("\n=== примеры name → isin ===")
    for nm, isin in list(name_isin.items())[:15]:
        print(f"  {isin}  ←  {nm[:60]}")


if __name__ == "__main__":
    main()
