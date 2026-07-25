#!/usr/bin/env python3
"""
Разовый бэкфилл ГЛУБОКОЙ истории Bybit (candles + open interest) вглубь до
запуска контракта BTCUSDT-perp (~2020-01-01), а не только окна по умолчанию
из fetch_bybit_realtime.py (BACKFILL_DAYS — это дефолт для первого realtime-
запуска, не реальный потолок источника).

ГОТЧА, из-за которой этот скрипт вообще понадобился: реальная глубина истории
Bybit ПО ОБОИМ типам данных (candles И open-interest) — не 2 года, а с самого
запуска контракта (~2020). Прежнее предположение «OI капается на 2 годах» было
артефактом собственного BACKFILL_DAYS=730 в реалтайм-скрипте, а не ограничением
Bybit — проверено вручную зондированием API с разными датами.

Переиспользует fetch_klines/fetch_open_interest/CryptoUpdater из
fetch_bybit_realtime.py — та же логика паджинации и upsert, разница только
в стартовой дате (глубоко в прошлое вместо "последняя дата в БД").

Usage:
  python Crypto/backfill_bybit_history.py --dry-run
  python Crypto/backfill_bybit_history.py --from-date 2020-01-01
  python Crypto/backfill_bybit_history.py --candles-only
"""

import argparse
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from fetch_bybit_realtime import (  # noqa: E402
    DB_URL, EXCHANGE, SYMBOLS, CANDLE_INTERVALS, OI_INTERVALS, STEP_MS,
    fetch_klines, fetch_open_interest, CryptoUpdater, log as _base_log,
)

log = logging.getLogger(__name__)

DEFAULT_FROM_DATE = "2020-01-01"  # заведомо раньше реального запуска контракта


def backfill_candles(updater: CryptoUpdater, symbol: str, label: str, start_ms: int, dry_run: bool) -> int:
    code = CANDLE_INTERVALS[label]
    step_ms = STEP_MS[label]
    df = fetch_klines(symbol, code, step_ms, start_ms)
    if df.empty:
        log.info(f"[{symbol} {label}] candles: пусто")
        return 0
    inserted = 0 if dry_run else updater.save_candles(symbol, label, df)
    log.info(f"[{symbol} {label}] candles: получено {len(df)}, upsert {inserted}"
              f"{' (dry-run)' if dry_run else ''} | {df['open_time'].min()} .. {df['open_time'].max()}")
    return inserted


def backfill_oi(updater: CryptoUpdater, symbol: str, label: str, start_ms: int, dry_run: bool) -> int:
    interval_time = OI_INTERVALS[label]
    step_ms = STEP_MS[label]
    df = fetch_open_interest(symbol, interval_time, step_ms, start_ms)
    if df.empty:
        log.info(f"[{symbol} {label}] OI: пусто")
        return 0
    inserted = 0 if dry_run else updater.save_open_interest(symbol, label, df)
    log.info(f"[{symbol} {label}] OI: получено {len(df)}, upsert {inserted}"
              f"{' (dry-run)' if dry_run else ''} | {df['ts'].min()} .. {df['ts'].max()}")
    return inserted


def main():
    parser = argparse.ArgumentParser(description="Глубокий бэкфилл истории Bybit")
    parser.add_argument("--symbols", nargs="+", default=SYMBOLS)
    parser.add_argument("--from-date", default=DEFAULT_FROM_DATE)
    parser.add_argument("--candles-only", action="store_true")
    parser.add_argument("--oi-only", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if not args.dry_run and not DB_URL:
        log.critical("❌ Не задан DB_URL")
        sys.exit(1)

    start_dt = datetime.strptime(args.from_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    start_ms = int(start_dt.timestamp() * 1000)

    updater = None if args.dry_run else CryptoUpdater(DB_URL)

    log.info("=" * 60)
    log.info(f"ГЛУБОКИЙ БЭКФИЛЛ BYBIT — символы: {args.symbols}, с {args.from_date}")
    log.info(f"Режим: {'DRY-RUN' if args.dry_run else 'запись в БД'}")
    log.info("=" * 60)

    grand_total = 0
    for symbol in args.symbols:
        for label in CANDLE_INTERVALS:
            if not args.oi_only:
                grand_total += backfill_candles(updater, symbol, label, start_ms, args.dry_run)
                time.sleep(0.2)
            if not args.candles_only:
                grand_total += backfill_oi(updater, symbol, label, start_ms, args.dry_run)
                time.sleep(0.2)

    log.info(f"✅ Всего upsert: {grand_total}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s", datefmt="%H:%M:%S")
    main()
