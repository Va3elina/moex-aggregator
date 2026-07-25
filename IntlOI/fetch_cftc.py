#!/usr/bin/env python3
"""
Международный OI — CFTC Commitments of Traders (Legacy report), США.

Источник: Socrata Public Reporting API, без ключа.
  https://publicreporting.cftc.gov/resource/6dca-aqww.json
Публикуется по вторникам (данные), релиз по пятницам — фетчим раз в день,
идемпотентно (ON CONFLICT DO UPDATE), лишние прогоны безвредны.

Категории CFTC (все — доля от Open_Interest_All, аддитивны только приблизительно,
поэтому total берём НАПРЯМУЮ из open_interest_all, а не суммой категорий):
  TOTAL        ← open_interest_all
  COMMERCIAL   ← comm_positions_long_all / comm_positions_short_all
  NONCOMMERCIAL← noncomm_positions_long_all / noncomm_positions_short_all
  NONREPORTABLE← nonrept_positions_long_all / nonrept_positions_short_all

Рынки матчатся по cftc_contract_market_code, а НЕ по market_and_exchange_names.
Имя рынка у CFTC несколько раз менялось за 40 лет (BRITISH POUND STERLING →
BRITISH POUND, 10-YEAR U.S. TREASURY NOTES → UST 10Y NOTE, COPPER-GRADE #1 →
COPPER- #1, и т.д.), а иногда и регистр "плавает" (S&P 500 Consolidated —
с маленькой "c", не "CONSOLIDATED") — но cftc_contract_market_code при этом
остаётся неизменным. Проверено вживую 2026-07-24: матчинг по имени терял
20-30 лет истории у половины рынков.

Некоторые рынки (кукуруза, соя) в 1998-м сменили САМ contract_market_code
(старый контракт пересчитан на новый) — у них ASSETS.codes — список из
НЕСКОЛЬКИХ кодов, все схлопываются в один логический asset_code для
непрерывной истории с 1986.

Запуск:
    python3 fetch_cftc.py --once             # разовый прогон, последние 4 отчёта (для оркестратора/cron)
    python3 fetch_cftc.py --once --weeks 12  # догнать историю за N последних отчётов
    python3 fetch_cftc.py --once --full-history  # ВСЯ доступная история по ASSETS (Socrata $where по коду — не тянет чужие рынки)
    python3 fetch_cftc.py --list-markets     # напечатать все market_and_exchange_names+код с сервера (для добавления нового актива)
    python3 fetch_cftc.py --debug-columns    # напечатать сырые ключи одной записи (для сверки полей)
"""
import argparse
import logging
import sys
from datetime import datetime

import httpx

from common import DB_URL, get_engine, upsert_oi_intl_rows

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

CFTC_URL = "https://publicreporting.cftc.gov/resource/6dca-aqww.json"

# asset_code (наш, стабильный) → {name, codes: [cftc_contract_market_code, ...]}.
# Коды сверены вживую 2026-07-24 (см. докстринг выше — почему код, а не имя).
# При добавлении нового актива: найти код через
#   python3 -c "import httpx; print(httpx.get('https://publicreporting.cftc.gov/resource/6dca-aqww.json', params={'\$select':'distinct market_and_exchange_names, cftc_contract_market_code','\$where':\"market_and_exchange_names like '%ИМЯ%'\"}).json())"
# Отбор — давно существующие ликвидные рынки (голубые фишки CFTC): основные
# индексы, валюты G10, металлы, ключевые сырьевые/сельхоз-рынки,
# treasury-рынки разных сроков + VIX. CFTC не публикует OI по отдельным
# компаниям — только по индексам/товарам/валютам/ставкам (single-stock
# futures в США по сути не торгуются с 2020, OneChicago закрыт).
ASSETS: dict[str, dict] = {
    # Индексы акций — берём futures-only E-mini код (не "Consolidated"
    # futures+options, тот появился только в 2010): у E-mini код НЕ менялся
    # с конца 90-х, менялось только имя рынка (IMM → CME, "STOCK INDEX" →
    # без него) — проверено вживую 2026-07-24.
    "CFTC_SP500": {"name": "S&P 500", "codes": ["13874A"]},              # E-mini, с 1997-09
    "CFTC_NASDAQ100": {"name": "NASDAQ-100", "codes": ["209742"]},       # E-mini, с 1999-06
    "CFTC_DJIA": {"name": "Dow Jones", "codes": ["124603"]},             # mini $5, с 2002-05
    "CFTC_RUSSELL2000": {"name": "Russell 2000", "codes": ["239742"]},   # E-MINI (не Micro!), с 2002-08
    "CFTC_NIKKEI225": {"name": "Nikkei 225", "codes": ["240743"]},       # с 2004-11 (продукт запущен тогда же)
    "CFTC_VIX": {"name": "VIX", "codes": ["1170E1"]},                    # с 2004-07 (продукт запущен тогда же)
    # Валюты (G10)
    "CFTC_EURUSD": {"name": "EUR/USD", "codes": ["099741"]},
    "CFTC_USDJPY": {"name": "USD/JPY", "codes": ["097741"]},
    "CFTC_GBPUSD": {"name": "GBP/USD", "codes": ["096742"]},
    "CFTC_USDCAD": {"name": "USD/CAD", "codes": ["090741"]},
    "CFTC_USDCHF": {"name": "USD/CHF", "codes": ["092741"]},
    "CFTC_AUDUSD": {"name": "AUD/USD", "codes": ["232741"]},
    "CFTC_USDMXN": {"name": "USD/MXN", "codes": ["095741"]},
    # Металлы
    "CFTC_GOLD": {"name": "Золото", "codes": ["088691"]},
    "CFTC_SILVER": {"name": "Серебро", "codes": ["084691"]},
    "CFTC_PLATINUM": {"name": "Платина", "codes": ["076651"]},
    "CFTC_PALLADIUM": {"name": "Палладий", "codes": ["075651"]},
    "CFTC_COPPER": {"name": "Медь", "codes": ["085692"]},
    # Энергия
    "CFTC_WTI": {"name": "Нефть WTI", "codes": ["067651"]},
    "CFTC_RBOB": {"name": "Бензин RBOB", "codes": ["111659"]},           # с 2006 (продукт запущен тогда же)
    "CFTC_NATGAS": {"name": "Природный газ", "codes": ["03565B"]},       # с 2002
    # Сельхоз — контракт пересчитан в 1998, объединяем старый+новый код
    "CFTC_CORN": {"name": "Кукуруза", "codes": ["002601", "002602"]},
    "CFTC_SOYBEANS": {"name": "Соя", "codes": ["005601", "005602"]},
    "CFTC_WHEAT": {"name": "Пшеница", "codes": ["001602"]},              # только новый код с 1998
    "CFTC_COTTON": {"name": "Хлопок", "codes": ["033661"]},
    "CFTC_COFFEE": {"name": "Кофе", "codes": ["083731"]},
    "CFTC_COCOA": {"name": "Какао", "codes": ["073732"]},
    "CFTC_SUGAR": {"name": "Сахар", "codes": ["080732"]},
    # Ставки/Treasury
    "CFTC_UST2Y": {"name": "US 2Y Treasury", "codes": ["042601"]},
    "CFTC_UST5Y": {"name": "US 5Y Treasury", "codes": ["044601"]},
    "CFTC_UST10Y": {"name": "US 10Y Treasury", "codes": ["043602"]},
    "CFTC_UST30Y": {"name": "US 30Y Treasury", "codes": ["020601"]},
    "CFTC_FEDFUNDS": {"name": "Fed Funds", "codes": ["045601"]},
}

# Обратный индекс: cftc_contract_market_code → наш asset_code (для build_rows)
CODE_TO_ASSET: dict[str, str] = {
    code: asset_code for asset_code, info in ASSETS.items() for code in info["codes"]
}


def _to_int(v) -> int | None:
    if v is None or v == "":
        return None
    return int(float(v))


def fetch_reports(weeks: int) -> list[dict]:
    """Тянет последние N отчётов (все рынки), фильтрует client-side по CODE_TO_ASSET."""
    limit = weeks * 400  # с запасом — 358 рынков в отчёте на 2026-07-14, округляем вверх
    params = {"$order": "report_date_as_yyyy_mm_dd DESC", "$limit": str(limit)}
    with httpx.Client(timeout=30) as client:
        resp = client.get(CFTC_URL, params=params)
        resp.raise_for_status()
        return resp.json()


def fetch_full_history() -> list[dict]:
    """
    Тянет ВСЮ доступную историю только по кодам из ASSETS — фильтрация на
    стороне Socrata ($where cftc_contract_market_code IN (...)), а не
    client-side по всем 358 рынкам. Пагинация $offset — Socrata отдаёт
    максимум 50000 строк за запрос.
    """
    codes = list(CODE_TO_ASSET.keys())
    quoted = ",".join(f"'{c}'" for c in codes)
    where = f"cftc_contract_market_code in ({quoted})"
    page_size = 50000
    all_rows: list[dict] = []
    offset = 0
    with httpx.Client(timeout=60) as client:
        while True:
            params = {
                "$where": where,
                "$order": "report_date_as_yyyy_mm_dd ASC",
                "$limit": str(page_size),
                "$offset": str(offset),
            }
            resp = client.get(CFTC_URL, params=params)
            resp.raise_for_status()
            page = resp.json()
            all_rows.extend(page)
            log.info(f"  ...страница offset={offset}: +{len(page)} строк (всего {len(all_rows)})")
            if len(page) < page_size:
                break
            offset += page_size
    return all_rows


def build_rows(reports: list[dict]) -> list[dict]:
    rows = []
    for r in reports:
        raw_code = r.get("cftc_contract_market_code", "")
        asset_code = CODE_TO_ASSET.get(raw_code)
        if not asset_code:
            continue
        asset_name = ASSETS[asset_code]["name"]

        trade_date_raw = r.get("report_date_as_yyyy_mm_dd", "")
        try:
            trade_date = datetime.fromisoformat(trade_date_raw.replace("T00:00:00.000", "")).date()
        except ValueError:
            log.warning(f"Пропуск: не распознана дата '{trade_date_raw}'")
            continue

        total_oi = _to_int(r.get("open_interest_all"))
        if total_oi is None:
            continue

        base = dict(exchange="CFTC", country="US", asset_code=asset_code, asset_name=asset_name, trade_date=trade_date, granularity="weekly")
        rows.append({**base, "category": "TOTAL", "oi_long": None, "oi_short": None, "oi_total": total_oi})

        for cat, long_field, short_field in (
            ("COMMERCIAL", "comm_positions_long_all", "comm_positions_short_all"),
            ("NONCOMMERCIAL", "noncomm_positions_long_all", "noncomm_positions_short_all"),
            ("NONREPORTABLE", "nonrept_positions_long_all", "nonrept_positions_short_all"),
        ):
            long_v, short_v = _to_int(r.get(long_field)), _to_int(r.get(short_field))
            if long_v is None and short_v is None:
                continue
            cat_total = (long_v or 0) + (short_v or 0)
            rows.append({**base, "category": cat, "oi_long": long_v, "oi_short": short_v, "oi_total": cat_total})
    return rows


def main():
    parser = argparse.ArgumentParser(description="CFTC COT (Legacy) → open_interest_intl")
    parser.add_argument("--once", action="store_true", help="Разовый прогон")
    parser.add_argument("--weeks", type=int, default=4, help="Сколько последних отчётов подтянуть")
    parser.add_argument("--list-markets", action="store_true", help="Напечатать все market_and_exchange_names+код и выйти")
    parser.add_argument("--debug-columns", action="store_true", help="Напечатать сырые ключи одной записи и выйти")
    parser.add_argument("--full-history", action="store_true", help="Вся доступная история по ASSETS (Socrata $where, без лишнего трафика)")
    args = parser.parse_args()

    if args.list_markets:
        reports = fetch_reports(weeks=1)
        names = sorted({f"{r.get('cftc_contract_market_code','')}\t{r.get('market_and_exchange_names','')}" for r in reports})
        print("\n".join(names))
        return

    if args.debug_columns:
        reports = fetch_reports(weeks=1)
        if reports:
            print(sorted(reports[0].keys()))
        return

    if not DB_URL:
        log.critical("DB_URL не задан (.env)")
        sys.exit(1)

    if args.full_history:
        log.info(f"Полная история по {len(ASSETS)} активам ({len(CODE_TO_ASSET)} кодов, Socrata $where)...")
        reports = fetch_full_history()
    else:
        log.info(f"Запрос CFTC COT: последние {args.weeks} отчётов...")
        reports = fetch_reports(weeks=args.weeks)

    rows = build_rows(reports)
    log.info(f"Отобрано {len(rows)} строк по {len(ASSETS)} активам")

    engine = get_engine()
    n = upsert_oi_intl_rows(engine, rows)
    log.info(f"✓ Записано/обновлено {n} строк в open_interest_intl")


if __name__ == "__main__":
    main()
