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
import re
import sys
import json
import time
import urllib.request
import urllib.parse
from sqlalchemy import create_engine, text

# Курируемые алиасы: старый_isin → ISIN-преемник (редомициляция / конверсия ГДР).
# Только ЯВНЫЕ случаи «тот же бизнес, новый ISIN». Имя для слияния НЕ используем.
ISIN_ALIASES = {
    # Редомициляция: старый иностранный ISIN (ГДР/АДР) → новый РФ-ISIN того же бизнеса.
    "US98387E2054": "RU000A108X38",  # X5: FIVE-гдр → КЦ ИКС 5
    "US69269L1044": "RU000A10CW95",  # Озон ADR → МКПАО Озон
    "US7496552057": "RU000A0JQUZ6",  # Русагро AGRO-гдр → RAGR
    "US83418T1088": "RU000A10ANA1",  # Циан CIAN-адр → CNRU
    "US42207L1061": "RU000A107662",  # Хэдхантер HHRU-адр → HEAD
    "NL0009805522": "RU000A107T19",  # Яндекс (Yandex N.V.) → МКПАО Яндекс (YDEX)
    "US87238U2033": "RU000A107UL4",  # TCS-гдр → Т-Технологии (T)
    "US5603172082": "RU000A106YF0",  # VK-гдр (Mail.ru) → ВК (VKCO)
    "US29760G1031": "RU000A10C1L6",  # Эталон ETLN-гдр → МКПАО Эталон Груп
    "US33835G2057": "RU000A10B5G8",  # Фикс Прайс FIXP-гдр → ПАО «Фикс Прайс» (FIXR)
}

ISS_URL = ("https://iss.moex.com/iss/securities.json?iss.meta=off&q={q}"
           "&securities.columns=secid,shortname,isin,regnumber,type,is_traded")
_UA = {"User-Agent": "frame-securities-ref/1.0"}

# ISS-типы акций/расписок — на такие может нарваться облигация, если УК проставила
# на облигационной строке ISIN базовой акции эмитента (в ISS этот ISIN — акция).
SHARE_TYPES = {"common_share", "preferred_share", "depositary_receipt"}

# Курируемые ISIN'ы облигаций, которые ISS securities.json отдаёт как common_share
# (УК записали ISIN базовой акции эмитента на облигационную строку). Без override
# попадают в таб «Акции» пикера «Потоки по компании». Дополняется эвристикой
# _looks_like_bond по имени бумаги (см. ниже) для новых облигаций.
BOND_ISIN_OVERRIDES = {
    "RU0008959580",  # КАМАЗ, БО-11
    "RU0009100895",  # Славнефть, 001P-01
    "RU0009028674",  # Акрон, БО-001P-05
    "RU0009100507",  # НКНХ, 001P-03
    "RU000A0H1ES3",  # ТГК-14, 001Р-01
    "RU000A0ETZF2",  # Томск, 34009
    "RU0007796819",  # ЯТЭК, 001Р-06
    "RU000A0HL7A2",  # Авто Финанс Банк, БO-001P-11
    "RU000A0JR6A6",  # ГК Азот, 001Р-01
}

# Имя облигации у УК почти всегда несёт серию: «БО-11», «001P-01» / «001Р-06»
# (лат. и кир. P/Р), «БO-001P-11» (лат. O), региональные «34009». Строгие маркеры,
# чтобы НЕ задеть настоящие акции (напр. «Сбербанк», «Сургнфгз-п»).
_BOND_NAME_RE = re.compile(
    r"(?:^|[\s,])(?:БО|BO)[\s-]?\d"   # БО-11, БО-001P-05, BO-...
    r"|\d{3}[PРp]-\d"                 # 001P-01, 001Р-06 (лат./кир. P/Р)
    r"|(?:^|[\s,])3\d{4}(?:$|\D)"     # 34009 (муниципальные/старые серии)
    r"|обл(?:игаци)?\b|\bбонд\b|\bbond\b",
    re.IGNORECASE,
)


def _looks_like_bond(names):
    """True, если хоть одно имя бумаги у УК несёт облигационную серию."""
    return any(n and _BOND_NAME_RE.search(n) for n in names)


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
        rows = conn.execute(text("""
            SELECT isin, array_agg(DISTINCT asset_name) AS names
            FROM fund_holdings_history
            WHERE isin IS NOT NULL AND isin <> ''
            GROUP BY isin ORDER BY isin
        """)).fetchall()
    isins = [r[0] for r in rows]
    names_by_isin = {r[0]: (r[1] or []) for r in rows}
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
            # Облигация под ISIN акции эмитента: ISS вернул share-тип, но имя у УК —
            # облигационная серия (или ISIN в курируемом списке). Метим как облигацию,
            # иначе бумага попадёт в таб «Акции» пикера «Потоки по компании».
            if typ in SHARE_TYPES and (
                    isin in BOND_ISIN_OVERRIDES
                    or _looks_like_bond(names_by_isin.get(isin, []))):
                typ = "exchange_bond"
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
