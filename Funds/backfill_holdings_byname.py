#!/usr/bin/env python3
"""
Бэкфилл isin для no-ISIN строк формата «Эмитент, акция об.» (одна УК пишет так,
БЕЗ рег-кода) → не ловятся code-бэкфиллом. Резолвим по ИМЕНИ эмитента через ISS
со СТРОГОЙ проверкой (обыкновенная акция, торгуется, нормализованное имя эмитента
совпадает). Неоднозначные — НЕ трогаем (репорт).

--dry-run (по умолч.) печатает предлагаемую карту name→isin для глазной проверки.
--commit применяет (guard от unique-конфликта).
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
           "&securities.columns=secid,shortname,isin,regnumber,type,is_traded,emitent_title")
_UA = {"User-Agent": "frame-byname-backfill/1.0"}

_STOP = re.compile(r"(публичн\w*|акционерн\w*|обществ\w*|пао|оао|мкпао|ооо|"
                   r"группа|гк|компани\w*|корпоративн\w*|центр|инвестментс|"
                   r"им\.?|россии|\bао\b|\bап\b|n\.?v\.?|plc)", re.I)


# Курируемые (авто-матч не справился: аббревиатура/скобки/имя ≠ эмитент).
# ISIN'ы — те же, что уже у ISIN'ованных строк этих бумаг в наших данных.
CURATED = {
    "Банк ВТБ (ПАО)": "RU000A0JP5V6",          # VTBR ао
    "ГМК Норильский никель": "RU0007288411",   # GMKN ао
    "АФК Система": "RU000A0DQZE3",              # AFKS ао
}


def norm(s: str) -> str:
    s = (s or "").lower().replace("ё", "е")
    s = _STOP.sub(" ", s)
    s = re.sub(r"[^a-zа-я0-9 ]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def iss(q):
    try:
        req = urllib.request.Request(ISS_URL.format(q=urllib.parse.quote(q)), headers=_UA)
        with urllib.request.urlopen(req, timeout=15) as r:
            return json.load(r)["securities"]["data"]
    except Exception:
        return []


def resolve(issuer: str):
    """Эмитент → (isin, shortname, emitent) для обыкновенной торгуемой акции.
    Приоритет: ТОЧНОЕ совпадение нормализованного имени (эмитент ИЛИ тикер-shortname),
    затем — подстрочное. Берём только если в выбранном пуле РОВНО один isin."""
    want = norm(issuer)
    rows = iss(issuer)
    exact, sub = [], []
    for secid, short, isin, reg, typ, traded, emit in rows:
        if typ != "common_share" or traded != 1 or not isin:
            continue
        ne, ns = norm(emit or ""), norm(short or "")
        if want and (want == ne or want == ns):
            exact.append((isin, short, emit))
        elif want and (want in ne or ne in want or want in ns):
            sub.append((isin, short, emit))
    for pool in (exact, sub):
        if len({c[0] for c in pool}) == 1:
            return pool[0]
    return None


def main():
    commit = "--commit" in sys.argv
    engine = create_engine(os.getenv("DB_URL"), pool_pre_ping=True)
    with engine.connect() as conn:
        names = [r[0] for r in conn.execute(text("""
            SELECT DISTINCT asset_name FROM fund_holdings_history
            WHERE (isin IS NULL OR isin='') AND asset_name ILIKE '%, акция об.%'
        """)).fetchall()]
    print(f"equity «акция об.» без ISIN: {len(names)} имён")

    resolved, ambiguous = {}, []
    for nm in names:
        issuer = nm.split(", акция об.")[0].strip()
        if issuer in CURATED:
            resolved[nm] = (CURATED[issuer], "(curated)", "(curated)")
        else:
            r = resolve(issuer)
            if r:
                resolved[nm] = r
            else:
                ambiguous.append(nm)
        time.sleep(0.03)

    print(f"\n=== РЕЗОЛВЛЕНО {len(resolved)} (для глазной проверки) ===")
    for nm, (isin, short, emit) in sorted(resolved.items()):
        print(f"  {isin}  {short:14}  ←  {nm.split(',')[0]:30}  (ISS: {emit})")
    if ambiguous:
        print(f"\n=== НЕ резолвлено / неоднозначно ({len(ambiguous)}) — пропускаем ===")
        for nm in ambiguous:
            print(f"  ?  {nm}")

    if commit:
        upd = text("""
            UPDATE fund_holdings_history h SET isin=:isin
            WHERE (h.isin IS NULL OR h.isin='') AND h.asset_name=:name
              AND NOT EXISTS (SELECT 1 FROM fund_holdings_history h2
                WHERE h2.fund_id=h.fund_id AND h2.isin=:isin AND h2.asset_name=h.asset_name
                  AND h2.snapshot_date=h.snapshot_date AND h2.source=h.source)
        """)
        total = 0
        for nm, (isin, _s, _e) in resolved.items():
            with engine.begin() as conn:
                total += conn.execute(upd, {"isin": isin, "name": nm}).rowcount or 0
        print(f"\nПРИМЕНЕНО: {total} строк по {len(resolved)} именам")
    else:
        print("\n(dry-run — ничего не записано; --commit чтобы применить)")


if __name__ == "__main__":
    main()
