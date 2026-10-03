#!/usr/bin/env python3
"""
Мандатный контекст: кто обязан будет купить или продать и на сколько.

ВТОРОЕ НАПРАВЛЕНИЕ ЗАВОДА ПОСТОВ, рядом с существующим, а не вместо него.

⚠️ ЧЕМ ОНО ОТЛИЧАЕТСЯ. Нынешний конвейер отвечает на вопрос «что сделала толпа» —
ему нужна аномалия открытого интереса, а значит нужен фьючерс. Замер на проде
04.09.2026: из 1 143 кандидатов аномалию имеют 28, и ВСЕ 5 опубликованных постов —
из этих 28. Остальные 1 115 отсеяны конструкцией, а не смыслом.

Мандатный поток отвечает на другой вопрос — «кто ОБЯЗАН будет купить или продать».
Ему аномалия не нужна и даже мешала бы: вынужденный поток случается ДО того, как
толпа отреагирует, в этом весь смысл. Фьючерс тоже не нужен.

Живой случай, с которого это писалось. 03.08.2026 Мосбиржа понизила Ленте уровень
листинга с первого на третий «за несоблюдение требований к корпоративному
управлению», 28.08 объявила об исключении из IMOEX и RTSI с 18 сентября. История с
причиной, реакцией компании и последствием лежала в архиве целиком — и не стала
постом, потому что у Ленты НЕТ ФЬЮЧЕРСА, а значит нет и аномалии.

⚠️ ДВА ИСТОЧНИКА, А НЕ ОДИН, и это требование Вадима. У затронутых мандатом бумаг
открытый интерес есть не у всех: по пресс-релизу 31.08 у ЛУКОЙЛа, Транснефти и
Татнефти он есть, у Ленты, Мосэнерго и ЕвроТранса — ноль. Зато Ленту держит EQMX с
весом 0,38%. Там, где слеп один источник, видит другой; брать надо оба.

Использование:
    python -m signals.mandate_context --mandate 16      # предпросмотр блока
    python -m signals.mandate_context --list            # что нашёл скаут
"""

import argparse
import json
import os
import sys
from datetime import date, timedelta

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# .env + подмена DB_URL, как в content_match и content_ai: скрипт живёт на ХОСТЕ,
# а в .env адрес базы записан для сети докера («@db:»).
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(_ROOT, ".env"))
_db = os.environ.get("DB_URL", "")
if "@db:" in _db:
    os.environ["DB_URL"] = _db.replace("@db:", "@127.0.0.1:")

# Тикеры мандата скаут пишет прозой: «LKOH, TRNFP, TATN/TATNP, PIKK, LENT, MSNG,
# EUTR» или «акции/облигации (широкий список)». Разбираем по разделителям и
# резолвим через справочник — вот ради чего он и заводился.
_SPLIT = ",;/ \n\t"

_RESOLVE = text("""
    SELECT s.secid, i.name_short
    FROM issuer_aliases a
    JOIN issuers i USING (issuer_id)
    JOIN issuer_securities s ON s.issuer_id = i.issuer_id AND s.share_class = 'common'
    WHERE a.alias_value = :v AND a.instrument_kind <> 'bond'
    LIMIT 1
""")

_FUNDS = text("""
    SELECT f.ticker, f.name, h.weight, h.snapshot_date
    FROM fund_holdings_history h
    JOIN funds f ON f.fund_id = h.fund_id
    WHERE h.isin = (SELECT isin FROM issuer_securities WHERE secid = :secid)
      AND h.snapshot_date = (
          SELECT MAX(snapshot_date) FROM fund_holdings_history
          WHERE isin = (SELECT isin FROM issuer_securities WHERE secid = :secid))
    ORDER BY h.weight DESC NULLS LAST
    LIMIT 5
""")

_INDEX_WEIGHT = text("""
    SELECT index_id, weight, trade_date FROM index_composition
    WHERE ticker = :secid ORDER BY trade_date DESC LIMIT 1
""")

_OI = text("""
    SELECT s.futures_sectype_quarterly, s.futures_sectype_perpetual,
           (SELECT COUNT(*) FROM open_interest o
             WHERE o.sectype IN (s.futures_sectype_quarterly, s.futures_sectype_perpetual)
               AND o.tradedate > CURRENT_DATE - 30) AS точек
    FROM issuer_securities s WHERE s.secid = :secid
""")


def _тикеры(asset: str):
    """Из прозы скаута — список ключей, которые стоит попробовать резолвить."""
    if not asset:
        return []
    куски, буфер = [], ""
    for ch in asset:
        if ch in _SPLIT or ch in "()":
            if буфер:
                куски.append(буфер); буфер = ""
        else:
            буфер += ch
    if буфер:
        куски.append(буфер)
    # Только то, что похоже на тикер: латиница, 2-6 знаков. «акции», «широкий»,
    # «Альфа-Капитал» сюда не попадут — резолвить их бессмысленно.
    return [k.upper() for k in куски if 2 <= len(k) <= 6 and k.isascii() and k.isalpha()]


def контекст(db, mandate_id: int) -> dict:
    м = db.execute(text("""
        SELECT id, source_ref, source_url, mandate_type, participant, sector, asset,
               status_now, mechanical_trigger, trigger_description, hypothesis, found_at
        FROM mandate_candidates WHERE id = :i"""), {"i": mandate_id}).mappings().first()
    if not м:
        return {}

    затронуты, не_опознаны, видели = [], [], set()
    for ключ in _тикеры(м["asset"]):
        r = db.execute(_RESOLVE, {"v": ключ}).first()
        if not r:
            не_опознаны.append(ключ)
            continue
        secid, имя = r
        # ⚠️ Скаут пишет «TATN/TATNP» — обе бумаги одного эмитента, и справочник
        # честно резолвит их в одну обыкновенную. Без этой проверки Татнефть
        # приезжает в бриф дважды, и модель считает, что речь о двух компаниях.
        if secid in видели:
            continue
        видели.add(secid)
        # Порядок колонок в _FUNDS: ticker, name, weight, snapshot_date. Индексы
        # руками — источник ошибок; берём по имени через .mappings().
        фонды = [{"фонд": f["ticker"], "название": f["name"],
                  "вес_в_фонде": float(f["weight"]) if f["weight"] is not None else None,
                  "снимок": str(f["snapshot_date"])}
                 for f in db.execute(_FUNDS, {"secid": secid}).mappings()]
        iw = db.execute(_INDEX_WEIGHT, {"secid": secid}).first()
        oi = db.execute(_OI, {"secid": secid}).first()
        затронуты.append({
            "бумага": secid, "компания": имя,
            "вес_в_индексе": ({"индекс": iw[0], "вес": float(iw[1]), "на": str(iw[2])}
                              if iw and iw[1] is not None else None),
            "держат_фонды": фонды,
            # ⚠️ Открытый интерес показываем ТОЛЬКО когда он есть. Пустой блок в брифе
            # модель всё равно попробует израсходовать — напишет «данных по фьючерсу
            # нет», и это займёт место абзаца ни на чём.
            "открытый_интерес": ("есть, %d точек за 30 дней" % oi[2]
                                 if oi and oi[2] else None),
        })

    return {
        "мандат": {
            "акт": м["source_ref"], "ссылка": м["source_url"],
            "тип": м["mandate_type"], "кого_обязывает": м["participant"],
            "статус": м["status_now"],
            "механический_триггер": bool(м["mechanical_trigger"]),
            "в_чём_триггер": м["trigger_description"],
            "найден": str(м["found_at"])[:10],
        },
        "затронутые_бумаги": затронуты,
        "не_опознаны": не_опознаны,
        "ПОЯСНЕНИЕ": (
            "ВЫНУЖДЕННЫЙ ПОТОК, А НЕ ПРОГНОЗ. Здесь написано, кто ОБЯЗАН совершить "
            "сделку и по какому правилу, а не что кто-то предполагает. Утверждать "
            "можно только обязанность и её срок; размер потока называть лишь там, где "
            "он посчитан из веса и состава фондов, а не на глаз. Если у бумаги нет ни "
            "фондов, ни открытого интереса — сказать, что оценить объём не из чего."
        ),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mandate", type=int)
    ap.add_argument("--list", action="store_true")
    a = ap.parse_args()
    engine = create_engine(os.environ["DB_URL"])
    with engine.connect() as db:
        if a.list or not a.mandate:
            for r in db.execute(text("""
                SELECT id, mandate_type, mechanical_trigger, left(source_ref, 46) AS акт,
                       left(asset, 52) AS активы, found_at::date
                FROM mandate_candidates ORDER BY found_at DESC""")).fetchall():
                print("%-4s %-20s триггер=%-5s %-46s %-52s %s" % tuple(str(x) for x in r))
            return
        print(json.dumps(контекст(db, a.mandate), ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
