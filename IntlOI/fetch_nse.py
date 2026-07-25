#!/usr/bin/env python3
"""
Международный OI — NSE Participant wise Open Interest (Индия).

Источник: прямой CSV, без ключа/регистрации.
  https://archives.nseindia.com/content/nsccl/fao_participant_oi_ДДММГГГГ.csv
Публикуется ежедневно после закрытия торгов (~19:00 IST). Формат — сводная
таблица по категориям участников с колонками Future Index Long/Short,
Future Stock Long/Short, Option Index Call/Put Long/Short, Option Stock
Long/Short (в контрактах). Категория участника — колонка "Client Type":
Client / DII / FII / Pro.

Мы не разбиваем по конкретным индексам/акциям (NSE не даёт этого в этом
отчёте — только агрегат по всему рынку деривативов на дату), поэтому
asset_code = 'NSE_ALL' — единый "инструмент" для всего рынка. oi_total на
строку категории = сумма long+short по всем инструментам этой категории.

Запуск:
    python3 fetch_nse.py --once                # сегодняшний отчёт
    python3 fetch_nse.py --once --date 15072026 # конкретная дата (ДДММГГГГ)
    python3 fetch_nse.py --once --backfill-days 30  # догнать N последних торговых дней
"""
import argparse
import io
import logging
import sys
from datetime import date, timedelta

import httpx
import pandas as pd

from common import DB_URL, get_engine, upsert_oi_intl_rows

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

NSE_URL_TMPL = "https://archives.nseindia.com/content/nsccl/fao_participant_oi_{ddmmyyyy}.csv"

# NSE банит запросы без нормального User-Agent
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

CATEGORY_MAP = {"Client": "CLIENT", "DII": "DII", "FII": "FII", "Pro": "PRO"}

# Реальный формат (сверено вживую 2026-07-24): первая строка CSV — заголовок с
# датой в кавычках ("Participant wise Open Interest ... as on Jul 23, 2026"),
# настоящие колонки — со второй строки. NSE сам считает "Total Long/Short
# Contracts" по каждой категории — берём их напрямую вместо ручного суммирования
# отдельных Future/Option колонок (меньше риска разойтись с источником).
LONG_TOTAL_COL = "Total Long Contracts"
SHORT_TOTAL_COL = "Total Short Contracts"


def fetch_csv(trade_date: date) -> pd.DataFrame | None:
    url = NSE_URL_TMPL.format(ddmmyyyy=trade_date.strftime("%d%m%Y"))
    with httpx.Client(timeout=20, headers=HEADERS, follow_redirects=True) as client:
        resp = client.get(url)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
    df = pd.read_csv(io.StringIO(resp.text), skiprows=1)
    df.columns = [c.strip() for c in df.columns]
    return df


def build_rows(df: pd.DataFrame, trade_date: date) -> list[dict]:
    rows = []
    total_long = total_short = 0
    for _, row in df.iterrows():
        client_type = str(row.get("Client Type", "")).strip()
        category = CATEGORY_MAP.get(client_type)
        if not category:
            continue
        long_v = int(row[LONG_TOTAL_COL]) if pd.notna(row.get(LONG_TOTAL_COL)) else 0
        short_v = int(row[SHORT_TOTAL_COL]) if pd.notna(row.get(SHORT_TOTAL_COL)) else 0
        total_long += long_v
        total_short += short_v
        rows.append({
            "exchange": "NSE", "country": "IN", "asset_code": "NSE_ALL", "asset_name": "NSE Деривативы (весь рынок)",
            "category": category, "trade_date": trade_date, "granularity": "daily",
            "oi_long": long_v, "oi_short": short_v, "oi_total": long_v + short_v,
        })

    if rows:
        rows.append({
            "exchange": "NSE", "country": "IN", "asset_code": "NSE_ALL", "asset_name": "NSE Деривативы (весь рынок)",
            "category": "TOTAL", "trade_date": trade_date, "granularity": "daily",
            "oi_long": total_long, "oi_short": total_short, "oi_total": total_long + total_short,
        })
    return rows


def main():
    parser = argparse.ArgumentParser(description="NSE Participant OI → open_interest_intl")
    parser.add_argument("--once", action="store_true", help="Разовый прогон")
    parser.add_argument("--date", type=str, help="Дата ДДММГГГГ (по умолчанию — сегодня)")
    parser.add_argument("--backfill-days", type=int, default=0, help="Догнать N последних дней")
    args = parser.parse_args()

    if not DB_URL:
        logging.critical("DB_URL не задан (.env)")
        sys.exit(1)

    engine = get_engine()
    if args.date:
        dates = [pd.to_datetime(args.date, format="%d%m%Y").date()]
    elif args.backfill_days:
        today = date.today()
        dates = [today - timedelta(days=i) for i in range(args.backfill_days)]
    else:
        dates = [date.today()]

    total_rows = 0
    for d in dates:
        try:
            df = fetch_csv(d)
        except httpx.HTTPStatusError as e:
            log.warning(f"{d}: HTTP {e.response.status_code}, пропуск")
            continue
        if df is None:
            log.debug(f"{d}: нет отчёта (выходной/праздник)")
            continue
        rows = build_rows(df, d)
        n = upsert_oi_intl_rows(engine, rows)
        total_rows += n
        log.info(f"{d}: +{n} строк")

    log.info(f"✓ Итого записано/обновлено {total_rows} строк в open_interest_intl")


if __name__ == "__main__":
    main()
