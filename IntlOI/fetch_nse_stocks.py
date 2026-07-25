#!/usr/bin/env python3
"""
Международный OI + свечи — отдельные акции NSE (Индия), голубые фишки.

Источник: официальный F&O Bhavcopy (ежедневный дамп ВСЕХ контрактов —
фьючерсы+опционы, все страйки/экспирации), без ключа/регистрации.

OI:     сумма Open Interest по ВСЕМ контрактам данного тикера (FUTSTK +
        OPTSTK, все страйки) за день — как и раньше, пишется в open_interest_intl.
Свечи:  ТОЛЬКО фьючерсы (FUTSTK/STF, без опционов — премия опциона не
        сравнима с ценой акции), и только ПЕРЕДНИЙ/самый ликвидный
        контрактный месяц (макс. OI среди фьючерсных месяцев тикера на
        дату) — цены разных месяцев складывать нельзя, в отличие от OI.
        Пишется в candles_intl (те же колонки OPEN/HIGH/LOW/CLOSE/SETTLE_PR,
        уже лежат в тех же строках bhavcopy, что и OI).

ДВА формата, NSE сменил его в середине 2024:
  СТАРЫЙ (до ~2024-07, архив тут же остаётся): archives.nseindia.com,
    zip с CSV, колонки SYMBOL/INSTRUMENT/OPEN_INT/OPEN/HIGH/LOW/CLOSE/SETTLE_PR.
  НОВЫЙ (с 2024-07): nsearchives.nseindia.com, UDiFF-формат, zip с CSV,
    колонки TckrSymb/FinInstrmTp/OpnIntrst/OpnPric/HghPric/LwPric/ClsPric/SttlmPric.
Пробуем новый формат, при 404 — старый. Проверено вживую 2026-07-24:
архив archives.nseindia.com отдаёт 403 (не 404!) для дат ДО 2016 —
это отдаёт Akamai edge, похоже на реальное отсутствие файла в этом
конкретном пути (не геоблок — тот же 403 для РФ и не-РФ IP выглядит
идентично, но 2016+ дни с не-РФ IP отдаются нормально). Итог: глубина
истории по факту ограничена 2016 годом, а не 2008 — задокументировано,
при появлении альтернативного источника до 2016 можно дополнить.

Голубые фишки — тикеры NSE, входящие в Nifty50 непрерывно (или почти)
с 2008 по сей день.

Запуск:
    python3 fetch_nse_stocks.py --once                      # сегодня
    python3 fetch_nse_stocks.py --once --date 23072026      # конкретный день (ДДММГГГГ)
    python3 fetch_nse_stocks.py --once --backfill-days 30   # догнать N последних дней
    python3 fetch_nse_stocks.py --once --backfill-from 2016-01-01  # максимально глубокий бэкфилл
"""
import argparse
import csv
import io
import logging
import sys
import zipfile
from datetime import date, datetime, timedelta

import httpx

from common import DB_URL, get_engine, upsert_oi_intl_rows, upsert_candle_intl_rows

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

# Голубые фишки NSE — в Nifty50 практически непрерывно с 2008 по сей день.
BLUE_CHIPS: dict[str, str] = {
    "RELIANCE": "Reliance Industries",
    "TCS": "Tata Consultancy Services",
    "INFY": "Infosys",
    "HDFCBANK": "HDFC Bank",
    "ICICIBANK": "ICICI Bank",
    "SBIN": "State Bank of India",
    "ITC": "ITC",
    "HINDUNILVR": "Hindustan Unilever",
    "LT": "Larsen & Toubro",
    "BHARTIARTL": "Bharti Airtel",
    "KOTAKBANK": "Kotak Mahindra Bank",
    "AXISBANK": "Axis Bank",
    "MARUTI": "Maruti Suzuki",
    "SUNPHARMA": "Sun Pharma",
    "TATASTEEL": "Tata Steel",
    "TATAMOTORS": "Tata Motors",  # ⚠️ реальный демерж 2025 (CV/PV split) — тикер мог перестать торговаться в F&O, фетчер просто пропустит его, не упадёт
    "ONGC": "ONGC",
    "NTPC": "NTPC",
    "WIPRO": "Wipro",
    "HCLTECH": "HCL Technologies",
}

# Значения INSTRUMENT/FinInstrmTp, соответствующие ФЬЮЧЕРСАМ на акцию (не
# опционам и не индексным фьючерсам) — старый формат / новый UDiFF формат.
FUTURES_TYPES = {"FUTSTK", "STF"}

# Бенчмарк для «силы рынка» (не входит в BLUE_CHIPS — это индекс, не компания,
# исключается из breadth ровно как NSE_ALL). Индексные фьючерсы — отдельный
# INSTRUMENT/FinInstrmTp, проверено вживую 2026-07-25 (старый формат 2020:
# FUTIDX, новый UDiFF 2026: IDF), но тот же EXPIRY_DT/XpryDt и та же логика
# календарного ролловера, что и у голубых фишек.
BENCHMARKS: dict[str, str] = {"NIFTY": "Nifty 50 Index"}
INDEX_FUTURES_TYPES = {"FUTIDX", "IDF"}

NEW_FORMAT_URL_TMPL = "https://nsearchives.nseindia.com/content/fo/BhavCopy_NSE_FO_0_0_0_{yyyymmdd}_F_0000.csv.zip"
OLD_FORMAT_URL_TMPL = "https://archives.nseindia.com/content/historical/DERIVATIVES/{yyyy}/{mon}/fo{ddmonyyyy}bhav.csv.zip"


def _read_zip_csv(content: bytes) -> list[dict] | None:
    # Akamai-эдж иногда отдаёт 200 с HTML "Service Temporarily Unavailable"
    # вместо zip (проверено вживую на 2024-06-17) — не 404, поэтому статус-код
    # не спасает, нужна проверка самого содержимого.
    try:
        z = zipfile.ZipFile(io.BytesIO(content))
    except zipfile.BadZipFile:
        return None
    with z.open(z.namelist()[0]) as f:
        text = f.read().decode()
    return list(csv.DictReader(text.splitlines()))


def _to_int(v) -> int:
    # OPEN_INT/OpnIntrst иногда приходит как строка-float ('0.0000000') —
    # int() на такой строке падает напрямую, нужен промежуточный float().
    if v is None or v == "":
        return 0
    return int(float(v))


def _to_float(v) -> float | None:
    if v is None or v == "" or v == "-":
        return None
    try:
        return float(v)
    except ValueError:
        return None


def _parse_expiry(v: str | None, fmt: str) -> date | None:
    if not v:
        return None
    try:
        return datetime.strptime(v.strip(), fmt).date()
    except ValueError:
        return None


def fetch_new_format(client: httpx.Client, d: date) -> list[dict] | None:
    url = NEW_FORMAT_URL_TMPL.format(yyyymmdd=d.strftime("%Y%m%d"))
    resp = client.get(url, headers=HEADERS)
    if resp.status_code != 200:
        return None
    rows = _read_zip_csv(resp.content)
    if rows is None:
        return None
    return [
        {
            "symbol": r["TckrSymb"], "instrument": r["FinInstrmTp"], "oi": _to_int(r["OpnIntrst"]),
            "expiry": _parse_expiry(r.get("XpryDt"), "%Y-%m-%d"),
            "open": r.get("OpnPric"), "high": r.get("HghPric"), "low": r.get("LwPric"),
            "close": r.get("ClsPric"), "settle": r.get("SttlmPric"),
        }
        for r in rows
    ]


def fetch_old_format(client: httpx.Client, d: date) -> list[dict] | None:
    mon = d.strftime("%b").upper()
    url = OLD_FORMAT_URL_TMPL.format(yyyy=d.year, mon=mon, ddmonyyyy=f"{d.day:02d}{mon}{d.year}")
    resp = client.get(url, headers=HEADERS)
    if resp.status_code != 200:
        return None
    rows = _read_zip_csv(resp.content)
    if rows is None:
        return None
    return [
        {
            "symbol": r["SYMBOL"], "instrument": r["INSTRUMENT"], "oi": _to_int(r["OPEN_INT"]),
            "expiry": _parse_expiry(r.get("EXPIRY_DT"), "%d-%b-%Y"),
            "open": r.get("OPEN"), "high": r.get("HIGH"), "low": r.get("LOW"),
            "close": r.get("CLOSE"), "settle": r.get("SETTLE_PR"),
        }
        for r in rows
    ]


def fetch_day(client: httpx.Client, d: date) -> list[dict] | None:
    rows = fetch_new_format(client, d)
    if rows is None:
        rows = fetch_old_format(client, d)
    return rows


def build_oi_rows(contract_rows: list[dict], d: date) -> list[dict]:
    """Сумма OI по всем контрактам (фьючерсы+опционы, все страйки) на тикер."""
    totals: dict[str, int] = {}
    for r in contract_rows:
        sym = r["symbol"]
        if sym not in BLUE_CHIPS:
            continue
        totals[sym] = totals.get(sym, 0) + r["oi"]

    return [
        {
            "exchange": "NSE", "country": "IN", "asset_code": f"NSE_{sym}", "asset_name": BLUE_CHIPS[sym],
            "category": "TOTAL", "trade_date": d, "granularity": "daily", "oi_long": None, "oi_short": None, "oi_total": oi,
        }
        for sym, oi in totals.items()
    ]


def build_price_rows(contract_rows: list[dict], d: date) -> list[dict]:
    """
    Цена ПЕРЕДНЕГО месяца — определяется по РЕАЛЬНОЙ дате экспирации из
    файла (EXPIRY_DT/XpryDt), не по тому, у кого сегодня больше OI. OI
    перетекает на следующий месяц ЗАДОЛГО до экспирации (тот же эффект
    подтверждён на TAIFEX/TSMC — см. fetch_taifex.py) — выбор "у кого OI
    больше" даёт скачок цены между двумя разными контрактами день-в-день,
    которого не было на самом рынке. Берём фьючерс с БЛИЖАЙШЕЙ ещё не
    истёкшей (строго > d) экспирацией.

    Кроме голубых фишек, тем же способом тянем NIFTY (индексный фьючерс,
    не акция) — бенчмарк для «силы рынка», см. BENCHMARKS.
    """
    names = {**BLUE_CHIPS, **BENCHMARKS}
    candidates: dict[str, list[dict]] = {}
    for r in contract_rows:
        sym = r["symbol"]
        if sym in BLUE_CHIPS:
            if r["instrument"] not in FUTURES_TYPES:
                continue
        elif sym in BENCHMARKS:
            if r["instrument"] not in INDEX_FUTURES_TYPES:
                continue
        else:
            continue
        if r["expiry"] is None or r["expiry"] <= d:
            continue
        candidates.setdefault(sym, []).append(r)

    rows = []
    for sym, rs in candidates.items():
        r = min(rs, key=lambda x: x["expiry"])
        o, h, l, c, s = _to_float(r["open"]), _to_float(r["high"]), _to_float(r["low"]), _to_float(r["close"]), _to_float(r["settle"])
        if all(v is None for v in (o, h, l, c, s)):
            continue
        rows.append({
            "exchange": "NSE", "country": "IN", "asset_code": f"NSE_{sym}", "asset_name": names[sym],
            "trade_date": d, "granularity": "daily",
            "open": o, "high": h, "low": l, "close": c, "settlement_price": s,
        })
    return rows


def main():
    parser = argparse.ArgumentParser(description="NSE per-stock OI+свечи (blue chips) → open_interest_intl / candles_intl")
    parser.add_argument("--once", action="store_true", help="Разовый прогон")
    parser.add_argument("--date", type=str, help="Дата ДДММГГГГ (по умолчанию — сегодня)")
    parser.add_argument("--backfill-days", type=int, default=0, help="Догнать N последних дней")
    parser.add_argument("--backfill-from", type=str, help="Максимально глубокий бэкфилл с даты YYYY-MM-DD")
    args = parser.parse_args()

    if not DB_URL:
        logging.critical("DB_URL не задан (.env)")
        sys.exit(1)

    if args.date:
        dates = [datetime.strptime(args.date, "%d%m%Y").date()]
    elif args.backfill_from:
        start = datetime.strptime(args.backfill_from, "%Y-%m-%d").date()
        end = date.today()
        dates = [start + timedelta(days=i) for i in range((end - start).days + 1) if (start + timedelta(days=i)).weekday() < 5]
    elif args.backfill_days:
        today = date.today()
        dates = [today - timedelta(days=i) for i in range(args.backfill_days) if (today - timedelta(days=i)).weekday() < 5]
    else:
        dates = [date.today()]

    engine = get_engine()
    total_oi_rows = 0
    total_price_rows = 0
    with httpx.Client(timeout=30, follow_redirects=True) as client:
        for d in dates:
            try:
                contract_rows = fetch_day(client, d)
            except Exception as e:
                log.warning(f"{d}: пропущен из-за ошибки запроса: {e!r}")
                continue
            if not contract_rows:
                log.debug(f"{d}: нет данных (выходной/праздник/вне архива)")
                continue

            oi_rows = build_oi_rows(contract_rows, d)
            n_oi = upsert_oi_intl_rows(engine, oi_rows)
            total_oi_rows += n_oi

            price_rows = build_price_rows(contract_rows, d)
            n_price = upsert_candle_intl_rows(engine, price_rows)
            total_price_rows += n_price

            log.info(f"{d}: OI +{n_oi} ({len(oi_rows)}/{len(BLUE_CHIPS)}), свечи +{n_price} ({len(price_rows)}/{len(BLUE_CHIPS)})")

    log.info(f"✓ Итого OI: {total_oi_rows} строк, свечи: {total_price_rows} строк")


if __name__ == "__main__":
    main()
