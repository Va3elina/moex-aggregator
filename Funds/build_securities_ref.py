#!/usr/bin/env python3
"""
Канонический справочник бумаг `securities_ref` из MOEX ISS — для нормализации
отображаемых имён активов в /fund-trades (сейчас имя = свободный текст УК, по
одному ISIN до 20+ написаний; строки без ISIN не группируются → видимые дубли).

Что делает (идемпотентно, НЕ трогает fund_holdings_history):
- Для каждого distinct ISIN из fund_holdings_history тянет ISS /securities.json?q=ISIN,
  берёт лучшую строку (is_traded=1 → буквенный secid → короче) → short_name, type, secid.
- canonical_isin = isin, КРОМЕ курируемых алиасов редомициляции/конвертации
  (ISIN_ALIASES): старый ISIN → ISIN-преемник (X5 ГДР US98387E2054 → RU000A108X38),
  чтобы две строки ОДНОЙ бумаги сливались в одну. Матчинг остаётся по ISIN —
  просто по каноническому. ао/ап/разные серии (разные ISIN, НЕ в алиасах) НЕ сливаются.

Источник — ISS (публичный, наш штатный; НЕ УК-раскрытие). Запуск на сервере
(нужен DB_URL + egress к ISS): docker exec -i frame-orchestrator-1 python3 - < этот файл
"""
import os
import sys
import json
import time
import urllib.request
import urllib.parse
from sqlalchemy import create_engine, text

# Курируемые алиасы: старый_isin → ISIN-преемник (редомициляция / конверсия ГДР).
# Только ЯВНЫЕ случаи «тот же бизнес, новый ISIN». Имя для слияния НЕ используем.
ISIN_ALIASES = {
    "US98387E2054": "RU000A108X38",  # X5: FIVE-гдр (X5 Retail Group N.V.) → КЦ ИКС 5
}

ISS_URL = ("https://iss.moex.com/iss/securities.json?iss.meta=off&q={q}"
           "&securities.columns=secid,shortname,isin,regnumber,type,is_traded")
_UA = {"User-Agent": "frame-securities-ref/1.0"}


def iss(q):
    url = ISS_URL.format(q=urllib.parse.quote(q))
    try:
        req = urllib.request.Request(url, headers=_UA)
        with urllib.request.urlopen(req, timeout=15) as r:
            return json.load(r)["securities"]["data"]
    except Exception:
        return []


def best_row(rows, isin):
    """Лучшая строка ISS для ISIN: торгуемая → буквенный тикер → короче secid."""
    cand = [r for r in rows if r[2] == isin]
    if not cand:
        return None
    def keyf(r):
        secid = r[0] or ""
        traded = 0 if r[5] == 1 else 1
        # буквенный тикер (SBER) предпочтительнее технических (RU14…, SU…, цифры)
        lettery = 0 if (secid[:2].isalpha() and not secid.startswith("RU0") and not secid.startswith("RU1")) else 1
        return (traded, lettery, len(secid))
    cand.sort(key=keyf)
    return cand[0]


def main():
    commit = "--commit" in sys.argv  # для совместимости; скрипт и так пишет (additive)
    engine = create_engine(os.getenv("DB_URL"), pool_pre_ping=True)

    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS securities_ref (
                isin           VARCHAR(20) PRIMARY KEY,
                secid          VARCHAR(40),
                short_name     VARCHAR(120),
                sec_type       VARCHAR(40),
                is_traded      BOOLEAN,
                canonical_isin VARCHAR(20),
                updated_at     TIMESTAMP DEFAULT now()
            )
        """))

    with engine.connect() as conn:
        isins = [r[0] for r in conn.execute(text("""
            SELECT DISTINCT isin FROM fund_holdings_history
            WHERE isin IS NOT NULL AND isin <> '' ORDER BY isin
        """)).fetchall()]
    print(f"distinct ISIN в fund_holdings_history: {len(isins)}")

    upsert = text("""
        INSERT INTO securities_ref (isin, secid, short_name, sec_type, is_traded, canonical_isin, updated_at)
        VALUES (:isin, :secid, :short_name, :sec_type, :is_traded, :canonical_isin, now())
        ON CONFLICT (isin) DO UPDATE SET
            secid=EXCLUDED.secid, short_name=EXCLUDED.short_name, sec_type=EXCLUDED.sec_type,
            is_traded=EXCLUDED.is_traded, canonical_isin=EXCLUDED.canonical_isin, updated_at=now()
    """)

    found = miss = 0
    for i, isin in enumerate(isins):
        row = best_row(iss(isin), isin)
        canonical = ISIN_ALIASES.get(isin, isin)
        if row:
            secid, short, _isin, reg, typ, traded = row
            rec = {"isin": isin, "secid": secid, "short_name": short, "sec_type": typ,
                   "is_traded": bool(traded == 1), "canonical_isin": canonical}
            found += 1
        else:
            # ISS не нашёл (делистнут/иностранный без записи) — строка-заглушка с алиасом
            rec = {"isin": isin, "secid": None, "short_name": None, "sec_type": None,
                   "is_traded": None, "canonical_isin": canonical}
            miss += 1
        with engine.begin() as conn:
            conn.execute(upsert, rec)
        time.sleep(0.03)
        if (i + 1) % 200 == 0:
            print(f"  ...{i + 1}/{len(isins)} (найдено {found}, нет в ISS {miss})")

    print(f"securities_ref готов: {found} с именем из ISS, {miss} без (заглушка).")

    # === Проверка: X5 ===
    print("\n=== ПРОВЕРКА X5 (должны сойтись к одному canonical_isin) ===")
    with engine.connect() as conn:
        for r in conn.execute(text("""
            SELECT isin, secid, short_name, sec_type, is_traded, canonical_isin
            FROM securities_ref WHERE isin IN ('US98387E2054','RU000A108X38')
            ORDER BY isin
        """)).fetchall():
            print("  ", dict(zip(["isin","secid","short","type","traded","canon"], r)))


if __name__ == "__main__":
    main()
