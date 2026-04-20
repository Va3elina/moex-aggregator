#!/usr/bin/env python3
"""
Одноразовый бэкфилл истории CNYRUB_TOM с 15.04.2013.

Причина: ранее start_date в fetch_indices_realtime.py был 2022-03-01,
из-за чего в index_data нет данных за 2013-2022, и dropdown «Сравнение»
на странице Сезонности показывает только 2023-2025.

На ISS непрерывная история CNYRUB_TOM есть с 15.04.2013 (~250 торговых
дней в году). После бэкфилла годовая сезонность CNYRUB заработает
корректно (13+ лет истории).

Запуск на сервере:
    docker cp Funds/backfill_cnyrub_history.py frame-orchestrator-1:/app/Funds/
    docker exec frame-orchestrator-1 python /app/Funds/backfill_cnyrub_history.py

Идемпотентен: используется ON CONFLICT DO UPDATE, повторный запуск
просто перезатрёт те же строки.
"""
import asyncio
import aiohttp
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from fetch_indices_realtime import (
    INDICES,
    fetch_index_full,
    save_to_db,
    get_engine,
    create_tables,
    log,
)

BACKFILL_SECID = "CNYRUB_TOM"
BACKFILL_START = date(2013, 4, 15)


async def main():
    info = INDICES[BACKFILL_SECID]
    engine = get_engine()
    create_tables(engine)

    today = date.today()
    log.info("=" * 60)
    log.info(f"🔄 БЭКФИЛЛ {BACKFILL_SECID}: {BACKFILL_START} → {today}")
    log.info("=" * 60)

    async with aiohttp.ClientSession() as session:
        records = await fetch_index_full(session, BACKFILL_SECID, info, BACKFILL_START, today)
        log.info(f"Получено с ISS: {len(records)} записей")

        saved = save_to_db(engine, BACKFILL_SECID, records)
        log.info(f"Сохранено/обновлено: {saved} строк в index_data")

    # Проверка результата
    from sqlalchemy import text
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT
              MIN(trade_date) AS min_d,
              MAX(trade_date) AS max_d,
              COUNT(*)        AS cnt,
              COUNT(DISTINCT EXTRACT(YEAR FROM trade_date)) AS years
            FROM index_data
            WHERE secid = :s
        """), {"s": BACKFILL_SECID}).fetchone()

    log.info("─" * 60)
    log.info(f"Итого в БД по {BACKFILL_SECID}:")
    log.info(f"  min_date: {result[0]}")
    log.info(f"  max_date: {result[1]}")
    log.info(f"  rows:     {result[2]}")
    log.info(f"  years:    {result[3]}")
    log.info("=" * 60)

    engine.dispose()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Остановлено")
    except Exception as e:
        log.critical(f"Ошибка: {e}")
        sys.exit(1)
