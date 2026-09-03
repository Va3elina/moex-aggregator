#!/usr/bin/env python3
"""
Сид справочника эмитентов: issuers → issuer_securities → issuer_aliases.

Источники — только то, что уже проверено на проде, ничего не выдумывается:
  • issuer_draft.json      — состав вселенной: эмитент, бумаги, класс, ISIN, тикер smart-lab;
  • securities_ref         — ISIN → secid, тип бумаги, canonical_isin (склейка после
                             редомициляции: US87238U2033 → RU000A107UL4);
  • futures_contracts      — sectype, assetcode и признак вечного контракта;
  • instruments            — человеческое имя и сектор;
  • fund_holdings_history  — ВСЕ варианты имени бумаги в справках УК. Это главная
                             ценность: у обыкновенной акции Сбербанка их восемь.

⚠️ ФЬЮЧЕРС ПРИВЯЗАН К КЛАССУ БУМАГИ, НЕ К ЭМИТЕНТУ. У префов свои контракты: Татнефть
TT и TP, Сбербанк SR и SP, Сургутнефтегаз SN и SG. И хранятся ОБА поколения: вечный
SBERF даёт открытый интерес только с окт-2024, квартальный SR — с 2012.

⚠️ ИМЕНА ИЗ СПРАВОК УК БЕРУТСЯ ТОЛЬКО ПО ISIN. Матчить по подстроке нельзя: «Сбер»
находит облигации Сбербанка, «Газпром» — Газпром нефть.

Использование:
    python seed_issuers.py issuer_draft.json --dry-run
    python seed_issuers.py issuer_draft.json
"""

import argparse
import json
import logging
import os
import re
import sys
from datetime import date
from pathlib import Path

from sqlalchemy import create_engine, text

BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR / "logs"
log = logging.getLogger("seed_issuers")

CLASS_TITLE = {"common": "", "pref": " (прив)", "dr": " (расписка)"}

# ⚠️ ОБЛИГАЦИЯ ПОД ISIN АКЦИИ — не теория. У Сбербанка выпуски «001Р-SBER51» и
# «002СУБ-02R» приходят в справках УК под ISIN обыкновенной акции RU0009029540.
# То есть даже строгий матч по ISIN затаскивает в алиасы акции облигационные имена.
# Ловим их по признакам выпуска: серия, регистрационный номер, суборд.
BOND_PATTERNS = re.compile(
    r"(\bSb\d|СУБ|\bБО-|\bПБО|\b\d{3}[РP]-|4B02|"
    r"\d-\d{2}-\d{5}|\b\d{5}-[A-ZА-Я]|облигац|бонд)",
    re.IGNORECASE)


def looks_like_bond(name: str) -> bool:
    return bool(BOND_PATTERNS.search(name))


# ISIN, которых нет в securities_ref (он строится по составам фондов, а туда бумага
# попадает только если её кто-то держит). Значения взяты из наших же данных —
# fund_holdings_history, проверено 03.09.2026, — а не из головы.
ISIN_PATCH = {"BANE": "RU0007976957"}

# Квартальные серии, которых нет в ticker_futures_map: она хранит ОДИН sectype на тикер,
# и для Газпрома там стоит вечный GAZPF. Квартальный GZ (assetcode GAZR) при этом живой
# и даёт открытый интерес с 2012 года — проверено в futures_contracts 03.09.2026.
FUTURES_PATCH = {"GAZP": "GZ"}

# ⚠️ НЕ ЭМИТЕНТЫ. RGBI — индекс государственных облигаций, он попал во вселенную из
# ticker_futures_map (миграция 034 завела его туда ради фьючерса на индекс). Компании
# за ним нет: ни отчётности, ни акционеров, ни дивидендов. В справочнике эмитентов
# ему делать нечего.
NOT_A_COMPANY = {"RGBI"}


def из_инструментов(conn, уже_есть: set):
    """
    Кандидаты во вселенную из НАШЕЙ ЖЕ базы: акции, по которым мы храним свечи.

    ⚠️ ПОЧЕМУ ЭТО ПРАВИЛЬНАЯ ГРАНИЦА. Первая вселенная строилась по модалкам
    открытого интереса и сделок фондов — 86 эмитентов. Но замер 03.09.2026 показал,
    что справочник ОКАЗАЛСЯ УЖЕ СОБСТВЕННЫХ ДАННЫХ: 129 акций в instruments, 167
    бумаг со свечами, 117 в составе индекса — и 91 в справочнике. По ОГК-2 у нас
    4 926 дней котировок и ни строчки карточки.

    Разрыв нашёлся не из теории, а из детектора сигналов: треть очереди оказалась
    про компании, которых справочник не знает, хотя цены по ним мы качаем годами.

    Проверено пробой: у всех 40 таких бумаг карточка на smart-lab ЕСТЬ.

    Побочный эффект важнее самого расширения: вселенная перестаёт быть замороженным
    файлом. Новая бумага появляется в instruments — следующий сид её подхватит.
    """
    rows = conn.execute(text("""
        SELECT i.sec_id, i.name, i.sector,
               (SELECT s.isin FROM securities_ref s WHERE s.secid = i.sec_id LIMIT 1),
               (SELECT s.sec_type FROM securities_ref s WHERE s.secid = i.sec_id LIMIT 1),
               (SELECT COUNT(*) FROM candles c WHERE c.secid = i.sec_id AND c.interval = 24)
        FROM instruments i
        WHERE i.type = 'stock'
        ORDER BY i.sec_id
    """)).fetchall()
    известные = {r[0] for r in rows}
    новые = []
    for secid, name, sector, isin, sec_type, дней in rows:
        if secid in уже_есть or secid in NOT_A_COMPANY:
            continue
        if not дней:
            # Без котировок бумага для нас не существует: карточку показать не с чем,
            # а справочник заполнится тикерами-призраками.
            log.info("пропуск %s: нет свечей", secid)
            continue
        # Преф опознаём по типу бумаги, а если его нет — по тому, что существует
        # обычка с тем же тикером без хвостовой P (LSNGP → LSNG). Ставить класс по
        # одной букве нельзя: у GLTR и MGKL P в конце нет вовсе, а SNGSP есть.
        преф = (sec_type == "preferred_share"
                or (secid.endswith("P") and secid[:-1] in известные))
        key = secid[:-1] if преф and secid[:-1] in известные else secid
        новые.append({
            "issuer_key": key, "secid": secid,
            "class": "pref" if преф else "common",
            "isin": isin, "smartlab_ticker": key,
            "oi_display_name": name, "futures_sectype": None,
            "источник": "instruments",
        })
    return новые


def setup_logging():
    LOG_DIR.mkdir(exist_ok=True)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")
    log.setLevel(logging.DEBUG)
    log.handlers.clear()
    day = date.today().strftime("%Y%m")
    for path, level in ((LOG_DIR / f"seed_issuers_{day}.log", logging.DEBUG),
                        (LOG_DIR / f"seed_issuers_errors_{day}.log", logging.WARNING)):
        h = logging.FileHandler(path, encoding="utf-8")
        h.setLevel(level); h.setFormatter(fmt); log.addHandler(h)
    c = logging.StreamHandler(sys.stderr)
    c.setLevel(logging.INFO); c.setFormatter(fmt); log.addHandler(c)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("draft", type=Path)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--from-instruments", action="store_true",
                    help="дополнить вселенную акциями из instruments, по которым есть свечи")
    a = ap.parse_args()
    setup_logging()

    rows = json.loads(a.draft.read_text(encoding="utf-8"))["кандидаты"]
    engine = create_engine(os.environ["DB_URL"])
    stats = {"эмитентов": 0, "бумаг": 0, "алиасов": 0, "имён_УК": 0,
             "облигаций_в_именах": 0, "без_фьючерса": 0, "без_smartlab": 0,
             "пропущено_не_компаний": 0, "из_instruments": 0}

    with engine.begin() as conn:
        if a.from_instruments:
            добавка = из_инструментов(conn, {r["secid"] for r in rows})
            log.info("из instruments добавлено кандидатов: %d", len(добавка))
            stats["из_instruments"] = len(добавка)
            rows = rows + добавка

        # --- справочные срезы одним заходом, чтобы не долбить БД в цикле
        instruments = {r[0]: (r[1], r[2]) for r in conn.execute(text(
            "SELECT sec_id, name, sector FROM instruments WHERE type='stock'")).fetchall()}
        # ⚠️ У одной бумаги бывает НЕСКОЛЬКО ISIN: после редомициляции старый остаётся
        # в справках УК. TCS-гдр US87238U2033 и Т-Техно ао RU000A107UL4 — одна компания,
        # и агент должен резолвить оба. Поэтому не «secid → isin», а «canonical_isin →
        # все его ISIN» плюс список старых secid (TCS-ME, YNDX, FIVE, MAIL, AGRO, HHRU).
        secref, isins_by_canon, old_secids = {}, {}, {}
        for sid, isin, canon in conn.execute(text(
                "SELECT secid, isin, canonical_isin FROM securities_ref "
                "WHERE secid IS NOT NULL")).fetchall():
            canon = canon or isin
            isins_by_canon.setdefault(canon, set()).add(isin)
            if isin == canon:
                secref[sid] = (isin, canon)
            else:
                old_secids.setdefault(canon, set()).add(sid)
            secref.setdefault(sid, (isin, canon))
        # sectype → (assetcode, вечный ли)
        futures = {}
        for st, ac, perp in conn.execute(text(
                "SELECT DISTINCT sectype, assetcode, is_perpetual FROM futures_contracts")).fetchall():
            futures.setdefault(st, (ac, perp))
        tfm = {r[0]: r[1] for r in conn.execute(text(
            "SELECT stock_ticker, futures_sectype FROM ticker_futures_map")).fetchall()}
        # ISIN → все варианты имени в справках УК
        fund_names = {}
        for isin, names in conn.execute(text("""
                SELECT isin, array_agg(DISTINCT asset_name) FROM fund_holdings_history
                WHERE isin IS NOT NULL AND isin <> '' GROUP BY isin""")).fetchall():
            fund_names[isin] = [n for n in (names or []) if n]

        # --- эмитенты
        by_issuer = {}
        for r in rows:
            by_issuer.setdefault(r["issuer_key"], []).append(r)

        for key, secs in sorted(by_issuer.items()):
            if key in NOT_A_COMPANY:
                log.info("пропуск %s: это не компания, а индекс", key)
                stats["пропущено_не_компаний"] += 1
                continue
            common = next((s for s in secs if s.get("class") == "common"), secs[0])
            name = (instruments.get(common["secid"], (None, None))[0]
                    or common.get("oi_display_name") or key)
            sector = instruments.get(common["secid"], (None, None))[1]
            smartlab = common.get("smartlab_ticker")
            if not smartlab:
                stats["без_smartlab"] += 1

            issuer_id = conn.execute(text("""
                INSERT INTO issuers (issuer_key, name_short, name_full, sector, smartlab_ticker)
                VALUES (:k,:n,NULL,:s,:sl)
                ON CONFLICT (issuer_key) DO UPDATE SET name_short=EXCLUDED.name_short,
                  sector=COALESCE(EXCLUDED.sector, issuers.sector),
                  smartlab_ticker=COALESCE(EXCLUDED.smartlab_ticker, issuers.smartlab_ticker),
                  updated_at=now()
                RETURNING issuer_id
            """), {"k": key, "n": name[:120], "s": sector, "sl": smartlab}).scalar()
            stats["эмитентов"] += 1

            def alias(atype, value, secid, kind="share"):
                if not value:
                    return
                conn.execute(text("""
                    INSERT INTO issuer_aliases (alias_type, alias_value, issuer_id, secid,
                                                instrument_kind, source)
                    VALUES (:t,:v,:i,:s,:k,'seed_universe')
                    ON CONFLICT (alias_type, alias_value) DO UPDATE
                      SET issuer_id=EXCLUDED.issuer_id, secid=EXCLUDED.secid,
                          instrument_kind=EXCLUDED.instrument_kind
                """), {"t": atype, "v": str(value)[:300], "i": issuer_id,
                       "s": secid, "k": kind})
                stats["алиасов"] += 1

            for s in secs:
                secid, cls = s["secid"], s.get("class", "common")
                isin = (s.get("isin") or secref.get(secid, (None, None))[0]
                        or ISIN_PATCH.get(secid))
                if not isin:
                    log.warning("%s: ISIN не найден нигде — алиас по ISIN не заведён", secid)
                canon = secref.get(secid, (None, None))[1] or isin

                # Фьючерсы: sectype из черновика + из ticker_futures_map, разложенные
                # на квартальный и вечный по признаку is_perpetual.
                sectypes = {x for x in (s.get("futures_sectype"), tfm.get(secid),
                                        FUTURES_PATCH.get(secid)) if x}
                quarterly = perpetual = assetcode = None
                for st in sectypes:
                    ac, perp = futures.get(st, (None, None))
                    assetcode = assetcode or ac
                    if perp:
                        perpetual = st
                    else:
                        quarterly = st
                if not sectypes:
                    stats["без_фьючерса"] += 1

                conn.execute(text("""
                    INSERT INTO issuer_securities (secid, issuer_id, share_class, isin,
                        canonical_isin, futures_sectype_quarterly, futures_sectype_perpetual,
                        futures_assetcode)
                    VALUES (:sid,:iid,:cls,:isin,:canon,:q,:p,:ac)
                    ON CONFLICT (secid) DO UPDATE SET issuer_id=EXCLUDED.issuer_id,
                      share_class=EXCLUDED.share_class, isin=COALESCE(EXCLUDED.isin, issuer_securities.isin),
                      canonical_isin=COALESCE(EXCLUDED.canonical_isin, issuer_securities.canonical_isin),
                      futures_sectype_quarterly=COALESCE(EXCLUDED.futures_sectype_quarterly, issuer_securities.futures_sectype_quarterly),
                      futures_sectype_perpetual=COALESCE(EXCLUDED.futures_sectype_perpetual, issuer_securities.futures_sectype_perpetual),
                      futures_assetcode=COALESCE(EXCLUDED.futures_assetcode, issuer_securities.futures_assetcode),
                      updated_at=now()
                """), {"sid": secid, "iid": issuer_id, "cls": cls, "isin": isin,
                       "canon": canon, "q": quarterly, "p": perpetual, "ac": assetcode})
                stats["бумаг"] += 1

                alias("secid", secid, secid)
                alias("isin", isin, secid)
                # Все ISIN этой бумаги, включая доредомициляционные, плюс её старые
                # тикеры: и «US87238U2033», и «TCS-гдр» должны вести к Т-Технологиям.
                for other in sorted(isins_by_canon.get(canon or isin, set())):
                    if other != isin:
                        alias("isin_old", other, secid)
                for old_sid in sorted(old_secids.get(canon or isin, set())):
                    if old_sid != secid:
                        alias("secid_old", old_sid, secid)
                        for nm in fund_names.get(
                                next(iter([i for i in isins_by_canon.get(canon or isin, set())
                                           if i != isin]), ""), []):
                            if not looks_like_bond(nm):
                                alias("fund_asset_name", nm, secid)
                for st in sectypes:
                    alias("sectype", st, secid, "future")
                if assetcode:
                    alias("assetcode", assetcode, secid, "future")
                if s.get("oi_display_name"):
                    alias("display_name", s["oi_display_name"], secid)
                # ⚠️ только по ISIN — подстрочный матч дал бы облигации
                for nm in set(fund_names.get(isin, []) + fund_names.get(canon, [])):
                    if looks_like_bond(nm):
                        # Ребро к эмитенту сохраняем, но к БУМАГЕ не привязываем:
                        # это долг компании, а не её акция.
                        alias("fund_asset_name", nm, None, "bond")
                        stats["облигаций_в_именах"] += 1
                    else:
                        alias("fund_asset_name", nm, secid)
                        stats["имён_УК"] += 1

            if smartlab:
                alias("smartlab", smartlab, common["secid"])

        if a.dry_run:
            conn.rollback()
            log.info("DRY-RUN, откат")

    log.info("итог: %s", json.dumps(stats, ensure_ascii=False))
    print(json.dumps(stats, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
