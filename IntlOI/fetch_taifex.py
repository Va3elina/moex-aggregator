#!/usr/bin/env python3
"""
Международный OI — отдельные акции TAIFEX (Тайвань), голубые фишки.

Третий рынок (после NSE) с ГЕНУИННО ликвидными фьючерсами на отдельные
компании — не зомби-листинг вроде "Futures on S&P 500 Components" у Eurex
(там 3.2% покрытия). Проверено вживую 2026-07-24 на данных 2024 года:
TSMC 20%, UMC 56.7%, Foxconn 37.4%, MediaTek 65.8%, Fubon 67.2%, Delta
Electronics и др. — доля торговых дней с ненулевым OI (посчитано по ВСЕМ
контрактным месяцам сразу, включая дальние с нулевым OI — на переднем
месяце доля будет выше).

Источник: официальная статистика TAIFEX, бесплатно, без ключа/логина.
ДВА механизма в зависимости от возраста данных:

  Прошлые годы (архив, до текущего года включительно на момент когда год
  закрылся) — один POST отдаёт ZIP со ВСЕМИ контрактами биржи за год:
    POST https://www.taifex.com.tw/enl/eng3/futDataDown
    down_type=2, his_year=<YYYY>
    → ZIP → <YYYY>_fut.csv, история подтверждена с 1998 года (проверено
    вживую: 626 строк, только индексный TX — отдельных акций тогда ещё не
    было, появились позже).

  Текущий (незавершённый) год — годового архива ещё нет, нужен ПОДневный
  запрос ПО ОДНОМУ тикеру за раз (макс. 31 день на запрос):
    POST https://www.taifex.com.tw/enl/eng3/futDataDown
    down_type=1, commodity_id=specialid, commodity_id2=<TICKER>,
    queryStartDate=YYYY/MM/DD, queryEndDate=YYYY/MM/DD
    → обычный CSV (не zip), тот же набор колонок.

В обоих случаях один тикер может иметь НЕСКОЛЬКО одновременно торгуемых
контрактных месяцев — суммируем open_interest по всем месяцам тикера на
дату (как и с NSE bhavcopy: общий OI по инструменту, не по экспирации).
Схема колонок слегка плавает по годам (1998: нет trading_halt/Trading
Session, volume с маленькой буквы) — не важно, используем только
date/contract/open_interest, эти три стабильны всегда.

НЕТ разбивки по типу держателя (это отдельный отчёт по крупным трейдерам,
не совпадает с методологией NSE participant-wise) — только category='TOTAL'.

Запуск:
    python3 fetch_taifex.py --once                    # текущий год (подневный запрос)
    python3 fetch_taifex.py --once --year 2020        # конкретный завершённый год (архив)
    python3 fetch_taifex.py --full-history            # 1998 → сегодня
    python3 fetch_taifex.py --full-history --from-year 2015  # ограничить глубину
"""
import argparse
import csv
import io
import logging
import re
import sys
import zipfile
from collections import defaultdict
from datetime import date, datetime, timedelta

import httpx

from common import DB_URL, get_engine, upsert_oi_intl_rows, upsert_candle_intl_rows

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
DOWNLOAD_URL = "https://www.taifex.com.tw/enl/eng3/futDataDown"
REFERER = "https://www.taifex.com.tw/enl/eng3/futDailyMarketView"

FIRST_YEAR = 1998

# Основной (самый ликвидный/долгоживущий) тикер на компанию — у некоторых
# акций несколько параллельных кодов (напр. TSMC: CDF и QFF), сверено вживую
# 2026-07-24 через выпадающий список "Single Stock Futures-by Ticker Symbol"
# на странице /enl/eng3/futDailyMarketReport.
BLUE_CHIPS: dict[str, str] = {
    "CDF": "TSMC",
    "CCF": "UMC",
    "DHF": "Hon Hai Precision (Foxconn)",
    "DVF": "MediaTek",
    "CEF": "Fubon Financial",
    "CKF": "Cathay Financial",
    "CNF": "CTBC Financial",
    "CFF": "Formosa Plastics",
    "CAF": "Nan Ya Plastics",
    "IJF": "Largan Precision",
    "CBF": "China Steel",
    "FRF": "Delta Electronics",
    "LCF": "Taiwan Mobile",
    "JSF": "Pegatron",
    "CZF": "Evergreen Marine",
    "DAF": "Yang Ming Marine Transport",
    "IRF": "Unimicron Technology",
    "DLF": "Chunghwa Telecom",
    "DKF": "Quanta Computer",
    "DXF": "Wistron",
}


def _parse_csv_rows(text: str) -> list[dict]:
    return list(csv.DictReader(text.splitlines()))


def fetch_year_archive(client: httpx.Client, year: int) -> list[dict] | None:
    """Годовой ZIP-архив — все контракты биржи за год (down_type=2)."""
    resp = client.post(DOWNLOAD_URL, data={"down_type": "2", "his_year": str(year)}, headers={"Referer": REFERER})
    if resp.status_code != 200 or not resp.content:
        return None
    try:
        z = zipfile.ZipFile(io.BytesIO(resp.content))
    except zipfile.BadZipFile:
        return None
    name = z.namelist()[0]
    with z.open(name) as f:
        text = f.read().decode("utf-8", errors="replace")
    return _parse_csv_rows(text)


def fetch_ticker_range(client: httpx.Client, ticker: str, start: date, end: date) -> list[dict] | None:
    """Подневный запрос по одному тикеру (down_type=1), макс ~31 день за раз."""
    resp = client.post(DOWNLOAD_URL, data={
        "down_type": "1",
        "commodity_id": "specialid",
        "commodity_id2": ticker,
        "queryStartDate": start.strftime("%Y/%m/%d"),
        "queryEndDate": end.strftime("%Y/%m/%d"),
    }, headers={"Referer": REFERER})
    if resp.status_code != 200 or not resp.content:
        return None
    text = resp.content.decode("utf-8", errors="replace")
    if not text.strip().startswith("date,"):
        return None
    return _parse_csv_rows(text)


def _month_chunks(start: date, end: date):
    """Разбивает диапазон на куски по <=27 дней — с запасом от лимита сайта
    в 1 календарный месяц на запрос, без риска зацепить edge-case на границах
    месяцев с разным числом дней."""
    cur = start
    while cur <= end:
        chunk_end = min(end, cur + timedelta(days=27))
        yield cur, chunk_end
        cur = chunk_end + timedelta(days=1)


def aggregate_rows(raw_rows: list[dict]) -> list[dict]:
    """Суммирует open_interest по всем контрактным месяцам на (тикер, дата)."""
    totals: dict[tuple[str, date], int] = {}
    for row in raw_rows:
        code = (row.get("contract") or "").strip()
        if code not in BLUE_CHIPS:
            continue
        date_raw = (row.get("date") or "").strip()
        if not date_raw or "/" not in date_raw:
            continue
        try:
            d = datetime.strptime(date_raw, "%Y/%m/%d").date()
        except ValueError:
            continue
        oi_raw = (row.get("open_interest") or "").strip()
        try:
            oi = int(oi_raw)
        except ValueError:
            continue
        key = (code, d)
        totals[key] = totals.get(key, 0) + oi

    return [
        {
            "exchange": "TAIFEX", "country": "TW", "asset_code": f"TAIFEX_{code}", "asset_name": BLUE_CHIPS[code],
            "category": "TOTAL", "trade_date": d, "granularity": "daily",
            "oi_long": None, "oi_short": None, "oi_total": oi,
        }
        for (code, d), oi in totals.items()
    ]


def _to_float(v) -> float | None:
    if v is None:
        return None
    v = v.strip()
    if not v or v == "-":
        return None
    try:
        return float(v)
    except ValueError:
        return None


def _third_wednesday(year: int, month: int) -> date:
    """Экспирация TAIFEX single-stock futures — 3-я среда месяца контракта.
    Подтверждено вживую на данных: CDF (TSMC) 202403 — settlement_price
    обнуляется и OI обрушивается ровно 2024-03-20, это 3-я среда марта 2024."""
    d = date(year, month, 1)
    count = 0
    while True:
        if d.weekday() == 2:  # среда
            count += 1
            if count == 3:
                return d
        d += timedelta(days=1)


def _taifex_front_month(trade_date: date, months: list[str]) -> str | None:
    """Из списка 'YYYYMM' — ближайший месяц, чья экспирация СТРОГО позже
    trade_date (в день экспирации контракт уже считается истёкшим —
    без этого settlement_price=0 в последний день попадал бы в свечи)."""
    candidates = []
    for m in months:
        try:
            y, mo = int(m[:4]), int(m[4:6])
            expiry = _third_wednesday(y, mo)
        except ValueError:
            continue
        if expiry > trade_date:
            candidates.append((expiry, m))
    return min(candidates)[1] if candidates else None


def build_price_rows(raw_rows: list[dict]) -> list[dict]:
    """
    Цена ПЕРЕДНЕГО месяца — определяется КАЛЕНДАРНО (экспирация контракта),
    а не по тому, у кого сегодня больше OI. OI постепенно перетекает на
    следующий месяц ЗАДОЛГО до экспирации (проверено вживую: у CDF/TSMC OI
    следующего месяца обгонял текущий уже за 5 торговых дней до экспирации) —
    выбор "у кого OI больше" в этот переходный период даёт скачок цены между
    двумя разными контрактами день-в-день, которого не было на самом рынке.
    """
    by_key: dict[tuple[str, date], dict[str, dict]] = defaultdict(dict)
    for row in raw_rows:
        code = (row.get("contract") or "").strip()
        if code not in BLUE_CHIPS:
            continue
        date_raw = (row.get("date") or "").strip()
        if not date_raw or "/" not in date_raw:
            continue
        try:
            d = datetime.strptime(date_raw, "%Y/%m/%d").date()
        except ValueError:
            continue
        month = (row.get("contract month(Week)") or "").strip()
        if not re.fullmatch(r"\d{6}", month):
            continue  # пропускаем спред-строки вида "202403/202404" и мусор
        session = (row.get("Trading Session") or "").strip()
        if session and session != "Regular":
            continue  # After-Hours не несёт своего settlement, не годится для выбора цены
        by_key[(code, d)][month] = row

    rows = []
    for (code, d), months_map in by_key.items():
        front = _taifex_front_month(d, list(months_map.keys()))
        if front is None:
            continue
        row = months_map[front]
        o, h, l, c, s = (
            _to_float(row.get("open")), _to_float(row.get("high")), _to_float(row.get("low")),
            _to_float(row.get("last")), _to_float(row.get("settlement_price")),
        )
        if all(v is None for v in (o, h, l, c, s)):
            continue
        rows.append({
            "exchange": "TAIFEX", "country": "TW", "asset_code": f"TAIFEX_{code}", "asset_name": BLUE_CHIPS[code],
            "trade_date": d, "granularity": "daily",
            "open": o, "high": h, "low": l, "close": c, "settlement_price": s,
        })
    return rows


def fetch_current_year_partial(client: httpx.Client, from_date: date) -> list[dict]:
    """Текущий год ещё не закрыт архивом — идём по каждому тикеру отдельно, месяц за месяцем."""
    today = date.today()
    all_raw: list[dict] = []
    for ticker in BLUE_CHIPS:
        for chunk_start, chunk_end in _month_chunks(from_date, today):
            rows = fetch_ticker_range(client, ticker, chunk_start, chunk_end)
            if rows:
                all_raw.extend(rows)
        log.info(f"  {ticker} ({BLUE_CHIPS[ticker]}): готово")
    return all_raw


def main():
    parser = argparse.ArgumentParser(description="TAIFEX single-stock futures OI → open_interest_intl")
    parser.add_argument("--once", action="store_true", help="Разовый прогон — текущий год (подневный запрос)")
    parser.add_argument("--year", type=int, help="Конкретный завершённый год (годовой архив)")
    parser.add_argument("--full-history", action="store_true", help=f"Все года с {FIRST_YEAR} по сегодня")
    parser.add_argument("--from-year", type=int, default=FIRST_YEAR, help="Ограничить глубину --full-history")
    args = parser.parse_args()

    if not DB_URL:
        log.critical("DB_URL не задан (.env)")
        sys.exit(1)

    engine = get_engine()
    total = 0
    current_year = date.today().year

    with httpx.Client(timeout=60, headers=HEADERS) as client:
        if args.year:
            years = [args.year]
        elif args.full_history:
            years = list(range(args.from_year, current_year))  # текущий год — отдельной веткой ниже
        else:
            years = []

        for year in years:
            raw_rows = fetch_year_archive(client, year)
            if raw_rows is None:
                log.warning(f"{year}: не удалось скачать/распаковать архив")
                continue
            oi_rows = aggregate_rows(raw_rows)
            n_oi = upsert_oi_intl_rows(engine, oi_rows)
            price_rows = build_price_rows(raw_rows)
            n_price = upsert_candle_intl_rows(engine, price_rows)
            total += n_oi
            log.info(f"{year}: OI +{n_oi}, свечи +{n_price} ({len(raw_rows)} сырых записей за год)")

        # Текущий год — годового архива ещё нет, подневный запрос по тикерам.
        # --full-history: с 1 января (полный год-к-дате). --once: только
        # последние ~35 дней (лёгкий инкрементальный догон для cron).
        if args.once or args.full_history:
            from_date = date(current_year, 1, 1) if args.full_history else date.today() - timedelta(days=35)
            log.info(f"Текущий год {current_year} с {from_date}: подневный запрос по {len(BLUE_CHIPS)} тикерам...")
            raw_rows = fetch_current_year_partial(client, from_date)
            n_oi = upsert_oi_intl_rows(engine, aggregate_rows(raw_rows))
            n_price = upsert_candle_intl_rows(engine, build_price_rows(raw_rows))
            total += n_oi
            log.info(f"{current_year} (частично): OI +{n_oi}, свечи +{n_price}")

    log.info(f"✓ Итого OI: {total} строк (+ свечи в candles_intl)")


if __name__ == "__main__":
    main()
