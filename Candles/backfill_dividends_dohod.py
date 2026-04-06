"""
Одноразовый скрипт: загрузка исторических дивидендов с dohod.ru.
Парсит HTML-страницы для каждого тикера.

Использование:
  python Candles/backfill_dividends_dohod.py          # все тикеры
  python Candles/backfill_dividends_dohod.py --ticker SBER  # один тикер
  python Candles/backfill_dividends_dohod.py --dry-run      # только показать
"""

import os
import sys
import re
import time
import argparse
import logging
from datetime import date, datetime
from urllib.parse import quote
from pathlib import Path

import httpx
import pg8000
from urllib.parse import urlparse
from dotenv import load_dotenv

BASE_DIR = Path(__file__).parent.parent
load_dotenv(BASE_DIR / ".env")
DB_URL = os.getenv("DB_URL")

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)-8s | %(message)s', datefmt='%H:%M:%S')
log = logging.getLogger(__name__)

DOHOD_URL = "https://www.dohod.ru/ik/analytics/dividend/{ticker}"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}


def get_connection():
    p = urlparse(DB_URL)
    return pg8000.connect(
        host=p.hostname, port=p.port or 5432,
        user=p.username, password=p.password,
        database=p.path.lstrip("/")
    )


def parse_dohod_dividends(html: str, ticker: str) -> list[dict]:
    """Парсит HTML страницы dohod.ru и извлекает дивиденды."""
    results = []

    # Извлекаем все <td> содержимое
    cells = re.findall(r'<td[^>]*>(.*?)</td>', html, re.DOTALL)
    cells_clean = [re.sub(r'<[^>]+>', '', c).strip() for c in cells]

    # Паттерн: две даты подряд (дата объявления + дата реестра), потом число (дивиденд)
    for i in range(len(cells_clean) - 4):
        c0 = cells_clean[i]
        c1 = cells_clean[i + 1]
        if re.match(r'\d{2}\.\d{2}\.\d{4}', c0) and re.match(r'\d{2}\.\d{2}\.\d{4}', c1):
            # Ищем число в следующих ячейках
            for j in range(i + 2, min(i + 6, len(cells_clean))):
                try:
                    val = float(cells_clean[j].replace(',', '.'))
                    if 0.001 < val < 100:  # макс дивиденд ~50₽, отсекаем годы (2001-2026)
                        d = datetime.strptime(c1, '%d.%m.%Y').date()
                        results.append({"registry_close_date": d, "value": val})
                        break
                except:
                    continue

    # Дедупликация
    seen = set()
    unique = []
    for r in results:
        key = (r["registry_close_date"], r["value"])
        if key not in seen:
            seen.add(key)
            unique.append(r)

    return sorted(unique, key=lambda x: x["registry_close_date"])


def fetch_dohod_page(ticker: str) -> str:
    """Загружает HTML страницу dohod.ru для тикера."""
    url = DOHOD_URL.format(ticker=ticker.lower())
    try:
        resp = httpx.get(url, headers=HEADERS, timeout=15, follow_redirects=True)
        if resp.status_code == 200:
            return resp.text
        else:
            log.debug(f"  {ticker}: HTTP {resp.status_code}")
            return ""
    except Exception as e:
        log.debug(f"  {ticker}: error {e}")
        return ""


def determine_ex_date(conn, ticker: str, registry_date: date, div_value: float) -> date:
    """
    Определяет ex-date по гэпу в ценах.
    Ищет день в окне [registry-5, registry+1] где open << prev_close.
    """
    cur = conn.cursor()
    from datetime import timedelta
    date_from = registry_date - timedelta(days=7)
    date_to = registry_date + timedelta(days=2)
    cur.execute("""
        SELECT begin_time::date, open, close,
               LAG(close) OVER (ORDER BY begin_time) as prev_close
        FROM candles
        WHERE secid = %s AND interval = 24 AND type = 'stock' AND close > 0
          AND begin_time::date BETWEEN %s AND %s
          AND EXTRACT(ISODOW FROM begin_time) BETWEEN 1 AND 5
        ORDER BY begin_time
    """, (ticker, date_from, date_to))

    rows = cur.fetchall()
    best_date = registry_date
    best_gap = 0

    for row in rows:
        d, open_p, close_p, prev_close = row
        if prev_close and open_p and prev_close > 0:
            gap = prev_close - open_p
            # Гэп должен быть примерно равен дивиденду (±50%)
            if gap > div_value * 0.4 and gap < div_value * 2.0:
                if gap > best_gap:
                    best_gap = gap
                    best_date = d

    return best_date


def main():
    parser = argparse.ArgumentParser(description="Загрузка дивидендов с dohod.ru")
    parser.add_argument("--ticker", type=str, help="Конкретный тикер")
    parser.add_argument("--dry-run", action="store_true", help="Только показать, не записывать")
    args = parser.parse_args()

    conn = get_connection()
    cur = conn.cursor()

    # Получаем все тикеры акций
    if args.ticker:
        tickers = [args.ticker.upper()]
    else:
        cur.execute("""
            SELECT DISTINCT secid FROM candles
            WHERE type = 'stock' AND interval = 24 AND close > 0
            ORDER BY secid
        """)
        tickers = [r[0] for r in cur.fetchall()]

    log.info(f"Тикеров: {len(tickers)}")

    total_new = 0
    total_existing = 0

    for i, ticker in enumerate(tickers):
        html = fetch_dohod_page(ticker)
        if not html:
            continue

        dividends = parse_dohod_dividends(html, ticker)
        if not dividends:
            continue

        new = 0
        for div in dividends:
            # Проверяем есть ли уже в БД
            cur.execute("""
                SELECT 1 FROM dividends
                WHERE secid = %s AND registry_close_date = %s AND value = %s
            """, (ticker, div["registry_close_date"], div["value"]))

            if cur.fetchone():
                total_existing += 1
                continue

            # Определяем ex_date
            ex_date = determine_ex_date(conn, ticker, div["registry_close_date"], div["value"])

            if args.dry_run:
                log.info(f"  [DRY] {ticker}: {div['registry_close_date']} val={div['value']} ex={ex_date}")
                new += 1
                continue

            try:
                cur.execute("""
                    INSERT INTO dividends (secid, registry_close_date, ex_date, value)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (secid, registry_close_date, value) DO UPDATE SET ex_date = EXCLUDED.ex_date
                """, (ticker, div["registry_close_date"], ex_date, div["value"]))
                new += 1
            except Exception as e:
                log.error(f"  {ticker}: insert error {e}")
                conn.rollback()
                continue

        if new > 0:
            conn.commit()
            total_new += new
            log.info(f"  [{i+1}/{len(tickers)}] {ticker}: +{new} новых дивидендов (из {len(dividends)} найденных)")

        # Rate limiting
        time.sleep(0.3)

    cur.close()
    conn.close()
    log.info(f"\nИТОГО: +{total_new} новых, {total_existing} уже существовали")


if __name__ == "__main__":
    main()
