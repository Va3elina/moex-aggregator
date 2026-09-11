#!/usr/bin/env python3
"""
Значение индексов MOEX (IMOEX, RTSI, IMOEX2) на закрытие дня.

С 2026-09-11 индексы на сайте обновляются раз в день, в конце дня (решение
владельца вместе с правилом «цена только на закрытие 19:00», см.
api/services/session_close). Скрипт по-прежнему крутится в 5-минутном цикле,
но до PUBLISH_AFTER (19:10 МСК) ничего не пишет, а после пишет значение уже
закрытой основной сессии — сегодняшняя точка «Силы рынка» появляется сразу
после закрытия, не дожидаясь ночного прогона. Дневной fetch_indices_realtime.py
перезаписывает эту же строку официальным закрытием (ON CONFLICT по
(secid, trade_date)).

Никакой внутридневной истории НЕ накапливаем: ровно одна строка на сегодня,
которая всегда держит последнее значение (close = CURRENTVALUE).

Источник: ISS marketdata (публичный, без авторизации) —
  /iss/engines/stock/markets/index/securities/{secid}.json?iss.only=marketdata
Берём CURRENTVALUE (фоллбэк LASTVALUE). Если значения нет (до открытия /
выходной) — тихо пропускаем (это норма, не ошибка).

Запуск:
    python fetch_index_intraday.py            # один проход (для оркестратора)
    python fetch_index_intraday.py --force    # игнорировать проверку торгового дня
"""

import argparse
import json
import asyncio
import logging
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

import aiohttp
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

sys.path.insert(0, str(Path(__file__).parent.parent))
load_dotenv(Path(__file__).parent.parent / ".env")

DB_URL = os.getenv("DB_URL")

try:
    from moex_calendar import get_moscow_time, is_trading_day
except ImportError:
    def get_moscow_time():
        return datetime.utcnow() + timedelta(hours=3)

    def is_trading_day(check_date=None):
        check = check_date or get_moscow_time().date()
        if check.weekday() in (5, 6):
            return False, "Выходной"
        return True, "Торговый день"


# secid → board: marketdata возвращает строки по всем бордам инструмента,
# выбираем нужный (IMOEX торгуется на SNDX, RTSI — на одноимённом RTSI).
INDICES = {
    "IMOEX": "SNDX",
    "RTSI": "RTSI",
    # IMOEX2 — индекс МосБиржи доп./выходных сессий (тот же борд SNDX). Нужен,
    # чтобы рублёвая «Сила рынка» рисовала верхний график и по субботам/
    # воскресеньям: IMOEX и RTSI в выходные не торгуются, IMOEX2 живой.
    "IMOEX2": "SNDX",
}

# С какого момента (МСК) значение дня публикуем: основная сессия закончилась в
# 18:50, к 19:10 значение IMOEX/RTSI окончательное. Совпадает с
# api/services/session_close.PUBLISH_AT.
PUBLISH_AFTER = (19, 10)

ISS_URL = "https://iss.moex.com/iss/engines/stock/markets/index/securities/{secid}.json"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("index_intraday")


def get_engine():
    if not DB_URL:
        raise ValueError("DB_URL не установлен в .env")
    return create_engine(DB_URL, connect_args={"ssl_context": False})


async def fetch_current(session: aiohttp.ClientSession, secid: str, board: str) -> Optional[dict]:
    """Текущее значение индекса из ISS marketdata. None если сессии нет."""
    url = ISS_URL.format(secid=secid)
    params = {"iss.only": "marketdata", "iss.meta": "off"}
    async with session.get(url, params=params, headers=HEADERS, timeout=20) as resp:
        if resp.status != 200:
            log.warning(f"[{secid}] HTTP {resp.status}")
            return None
        data = await resp.json()

    md = data.get("marketdata", {})
    cols = md.get("columns", [])
    rows = md.get("data", [])
    if not rows:
        return None

    idx = {c: i for i, c in enumerate(cols)}
    chosen = next((r for r in rows if r[idx.get("BOARDID", -1)] == board), rows[0])

    def g(col):
        i = idx.get(col)
        return chosen[i] if i is not None else None

    # CURRENTVALUE — живое значение сессии; до открытия/после null → LASTVALUE.
    current = g("CURRENTVALUE") or g("LASTVALUE")
    if current is None or float(current) <= 0:
        return None

    trade_date_str = g("TRADEDATE")
    try:
        trade_date = datetime.strptime(str(trade_date_str), "%Y-%m-%d").date()
    except (ValueError, TypeError):
        trade_date = get_moscow_time().date()

    return {
        "secid": secid,
        "trade_date": trade_date,
        "open": g("OPENVALUE"),
        "high": g("HIGH"),
        "low": g("LOW"),
        "close": float(current),
        "value": g("VALTODAY"),
        "capitalization": g("CAPITALIZATION"),
    }


def upsert(engine, rec: dict) -> None:
    """Апсерт одной сегодняшней строки index_data (close = текущее значение)."""
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO index_data
                (secid, trade_date, open, high, low, close, value, capitalization)
            VALUES
                (:secid, :trade_date, :open, :high, :low, :close, :value, :capitalization)
            ON CONFLICT (secid, trade_date) DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                value = EXCLUDED.value,
                capitalization = EXCLUDED.capitalization,
                created_at = CURRENT_TIMESTAMP
        """), rec)
        conn.commit()


async def run_once(force: bool = False) -> int:
    if not force:
        is_trade, reason = is_trading_day()
        if not is_trade:
            log.info(f"⏭️ Пропуск: {reason} (--force чтобы форсировать)")
            print(json.dumps({"пропуск": reason}, ensure_ascii=False))
            return 0

    # Внутри дня индекс не публикуем — только значение после закрытия.
    now = get_moscow_time()
    if (now.hour, now.minute) < PUBLISH_AFTER:
        log.info("⏭️ Пропуск: значение индекса публикуем только после закрытия (19:10 МСК)")
        print(json.dumps({"пропуск": "до закрытия"}, ensure_ascii=False))
        return 0
    weekend = now.weekday() >= 5

    engine = get_engine()
    written = 0
    try:
        async with aiohttp.ClientSession() as session:
            for secid, board in INDICES.items():
                # IMOEX2 в будни продолжает считаться в вечерней сессии: после
                # 19:05 его текущее значение — уже вечерняя цена. Нужен он только
                # для выходных точек «Силы рынка» (выходная сессия заканчивается
                # в 19:00), в будни верхний график рисует IMOEX.
                if secid == "IMOEX2" and not weekend:
                    continue
                try:
                    rec = await fetch_current(session, secid, board)
                except Exception as e:
                    log.warning(f"[{secid}] {type(e).__name__}: {e}")
                    continue
                if not rec:
                    log.info(f"[{secid}] нет текущего значения (вне сессии) — пропуск")
                    continue
                upsert(engine, rec)
                written += 1
                log.info(f"✓ {secid} {rec['trade_date']} close={rec['close']}")
    finally:
        engine.dispose()

    log.info(f"Готово: обновлено {written}/{len(INDICES)} индексов")
    return written


async def main() -> None:
    parser = argparse.ArgumentParser(description="Внутридневное значение индексов MOEX (ISS)")
    parser.add_argument("--force", action="store_true", help="Игнорировать проверку торгового дня")
    args = parser.parse_args()
    _n = await run_once(force=args.force)
    print(json.dumps({"обновлено": _n if isinstance(_n, int) else None}, ensure_ascii=False))


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Остановлено")
    except Exception as e:
        log.critical(f"Ошибка: {e}", exc_info=True)
        sys.exit(1)
