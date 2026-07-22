#!/usr/bin/env python3
"""
Разовый бэкфилл ГЛУБОКОЙ истории спот-свечей с Coinbase Exchange (публичный API,
без ключей). Coinbase — одна из старейших непрерывно работающих регулируемых
бирж (на нём базируется CME BRR fixing), поэтому его спот-история самая длинная
и чистая из доступных без ключа/подписки.

⚠️ Не путать с Crypto/fetch_bybit_realtime.py — тот реалтайм (5m/1h/1d, деривативы
Bybit), этот — разовый (или редко перезапускаемый) бэкфилл глубокой ДНЕВНОЙ
спот-истории. Оба пишут в одну таблицу crypto_candles, но разные exchange
('bybit' vs 'coinbase') — колонка exchange для этого и заложена с первого дня.

Проверено вручную: BTC-USD candles на Coinbase начинаются ~2016-01-01
(до этого — пусто, хотя биржа торгует с 2015; видимо старее данные не отдаёт).

Usage:
  python Crypto/backfill_coinbase_history.py --dry-run
  python Crypto/backfill_coinbase_history.py --products BTC-USD ETH-USD
  python Crypto/backfill_coinbase_history.py --products BTC-USD --from-date 2016-01-01
"""

import argparse
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List

import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

sys.path.insert(0, str(Path(__file__).parent.parent))
load_dotenv(Path(__file__).parent.parent / ".env")

DB_URL = os.getenv("DB_URL")

EXCHANGE = "coinbase"
BASE_URL = "https://api.exchange.coinbase.com"
HEADERS = {"User-Agent": "frame-crypto-backfill/1.0"}

# granularity в секундах → наш label
GRANULARITY_SEC = {"1d": 86400, "1h": 3600, "5m": 300}
CANDLES_PER_REQUEST = 300  # жёсткий лимит Coinbase на один запрос

DEFAULT_FROM_DATE = "2014-01-01"  # заведомо раньше реального старта — пустые окна просто пропускаются
REQUEST_SLEEP = 0.25

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s", datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)


def fetch_window(product_id: str, granularity_sec: int, start: datetime, end: datetime) -> List[list]:
    params = {
        "granularity": granularity_sec,
        "start": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "end": end.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    try:
        resp = requests.get(
            f"{BASE_URL}/products/{product_id}/candles", params=params, headers=HEADERS, timeout=20
        )
        if resp.status_code != 200:
            log.warning(f"[{product_id}] HTTP {resp.status_code} {start:%Y-%m-%d}..{end:%Y-%m-%d}")
            return []
        return resp.json() or []
    except requests.RequestException as e:
        log.error(f"[{product_id}] Ошибка запроса {start:%Y-%m-%d}: {e}")
        return []


def backfill_product(engine, product_id: str, label: str, from_date: datetime, dry_run: bool) -> int:
    granularity_sec = GRANULARITY_SEC[label]
    window_span = timedelta(seconds=granularity_sec * (CANDLES_PER_REQUEST - 1))

    now = datetime.now(timezone.utc)
    cursor = from_date
    total_rows = 0
    total_inserted = 0

    log.info(f"[{product_id} {label}] бэкфилл с {from_date:%Y-%m-%d} до сегодня")

    while cursor < now:
        window_end = min(cursor + window_span, now)
        rows = fetch_window(product_id, granularity_sec, cursor, window_end)

        if rows:
            total_rows += len(rows)
            if not dry_run:
                total_inserted += save_candles(engine, product_id, label, rows)
            log.info(f"[{product_id} {label}] {cursor:%Y-%m-%d}..{window_end:%Y-%m-%d}: +{len(rows)} свечей")

        cursor = window_end + timedelta(seconds=granularity_sec)
        time.sleep(REQUEST_SLEEP)

    log.info(f"[{product_id} {label}] ГОТОВО: получено {total_rows}, upsert {total_inserted}")
    return total_inserted


def save_candles(engine, product_id: str, label: str, rows: List[list]) -> int:
    """rows: [time, low, high, open, close, volume] (порядок колонок Coinbase)."""
    inserted = 0
    with engine.begin() as conn:
        for r in rows:
            ts, low, high, open_, close, volume = r
            result = conn.execute(
                text(
                    """
                    INSERT INTO crypto_candles
                        (exchange, symbol, interval, open_time, open, high, low, close, volume, quote_volume)
                    VALUES (:ex, :sym, :iv, to_timestamp(:ts), :o, :h, :l, :c, :v, NULL)
                    ON CONFLICT (exchange, symbol, interval, open_time) DO UPDATE SET
                        open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
                        close = EXCLUDED.close, volume = EXCLUDED.volume
                    """
                ),
                {"ex": EXCHANGE, "sym": product_id, "iv": label, "ts": ts, "o": open_, "h": high, "l": low, "c": close, "v": volume},
            )
            inserted += result.rowcount
    return inserted


def main():
    parser = argparse.ArgumentParser(description="Бэкфилл глубокой истории Coinbase")
    parser.add_argument("--products", nargs="+", default=["BTC-USD"], help="Список продуктов, напр. BTC-USD ETH-USD")
    parser.add_argument("--granularity", default="1d", choices=list(GRANULARITY_SEC), help="Гранулярность")
    parser.add_argument("--from-date", default=DEFAULT_FROM_DATE, help="Начало окна (YYYY-MM-DD)")
    parser.add_argument("--dry-run", action="store_true", help="Только скачать и посчитать, не писать в БД")
    args = parser.parse_args()

    if not args.dry_run and not DB_URL:
        log.critical("❌ Не задан DB_URL. Задай его в .env")
        sys.exit(1)

    from_date = datetime.strptime(args.from_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)

    engine = None
    if not args.dry_run:
        engine = create_engine(DB_URL, connect_args={"ssl_context": False})
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        log.info("✓ БД подключена")

    log.info("=" * 60)
    log.info(f"БЭКФИЛЛ COINBASE — продукты: {args.products}, гранулярность: {args.granularity}")
    log.info(f"Режим: {'DRY-RUN (без записи в БД)' if args.dry_run else 'запись в БД'}")
    log.info("=" * 60)

    grand_total = 0
    for product_id in args.products:
        grand_total += backfill_product(engine, product_id, args.granularity, from_date, args.dry_run)

    log.info(f"✅ Всего upsert: {grand_total}")


if __name__ == "__main__":
    main()
