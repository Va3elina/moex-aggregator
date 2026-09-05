#!/usr/bin/env python3
"""
Рублёвый оборот фьючерсов (VALTODAY) → candles.value.

Зачем: у фьючерсов в свечах (Algopack/ISS candles) поле value = 0 — есть только
volume в КОНТРАКТАХ. Из-за этого в поиске активов «Объём» по фьючерсам показывался
числом контрактов, а не рублями (на Investing по тому же контракту ~десятки млрд ₽).
MOEX отдаёт рублёвый оборот в marketdata.VALTODAY (engine=futures, market=forts) —
одним bulk-запросом по всем контрактам. Кладём его в candles.value дневной свечи
ТЕКУЩЕГО торгового дня.

Выходные/праздники отдельно обрабатывать НЕ нужно: запрос в api/routers/instruments.py
берёт последнюю БУДНЮЮ дневную свечу и использует CASE WHEN value>0 THEN value ELSE
volume — поэтому в выходной показывается рублёвый оборот последнего торгового дня
(значение, записанное в пятницу, остаётся в свече).

Источник — публичный ISS (бесплатный, без ключа), задержка/EOD для нас не важна:
оборот обновляется в течение дня (VALTODAY накапливается) и финализируется к закрытию.

История не нужна: пишем только текущий день, прошлое не трогаем.

Запуск:
  python fetch_futures_turnover.py [--force]
    --force — не пропускать в выходной (по умолчанию в выходной выходим сразу,
              VALTODAY там всё равно 0).
"""

import json
import logging
import sys
import urllib.request
from pathlib import Path

from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

# === Импорт из корневой папки (как в соседних Candles-скриптах) ===
sys.path.insert(0, str(Path(__file__).parent.parent))
load_dotenv(Path(__file__).parent.parent / ".env")

DB_URL = os.getenv("DB_URL")

try:
    from moex_calendar import get_moscow_time, is_trading_day
except ImportError:
    from datetime import datetime, timedelta

    def get_moscow_time():
        return datetime.utcnow() + timedelta(hours=3)

    def is_trading_day(check_date=None):
        d = check_date or get_moscow_time().date()
        return (d.weekday() < 5, "выходной" if d.weekday() >= 5 else "будни")


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("futures_turnover")

# iss.only=marketdata — только рыночные данные (без статичной спеки),
# marketdata.columns — сужаем до пары полей, чтобы ответ был маленьким.
ISS_URL = (
    "https://iss.moex.com/iss/engines/futures/markets/forts/securities.json"
    "?iss.only=marketdata&iss.meta=off&marketdata.columns=SECID,VALTODAY"
)


def fetch_valtoday():
    """Возвращает [(secid, value_rub), ...] для контрактов с VALTODAY > 0."""
    req = urllib.request.Request(ISS_URL, headers={"User-Agent": "frame-oi/1.0"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    md = data["marketdata"]
    cols = md["columns"]
    i_sec, i_val = cols.index("SECID"), cols.index("VALTODAY")
    out = []
    for row in md["data"]:
        val = row[i_val]
        if val is None:
            continue
        try:
            v = float(val)
        except (TypeError, ValueError):
            continue
        if v > 0:
            out.append((row[i_sec], v))
    return out


def main():
    force = "--force" in sys.argv

    is_trade, reason = is_trading_day()
    if not is_trade and not force:
        log.info("Выходной (%s) — VALTODAY = 0, пропускаем", reason)
        return 0

    if not DB_URL:
        log.error("DB_URL не задан")
        return 1

    try:
        pairs = fetch_valtoday()
    except Exception as e:
        log.error("ISS marketdata fetch failed: %s", e)
        return 1

    if not pairs:
        log.info("VALTODAY пуст (рынок закрыт / нет торгов сегодня) — нечего обновлять")
        return 0

    today = get_moscow_time().date()
    engine = create_engine(DB_URL)
    updated = 0
    try:
        with engine.begin() as conn:
            for secid, val in pairs:
                # Матчим по dated-контракту (candles.secid = 'SiM6' и т.п.) —
                # это однозначно фьючерс, акции под такой secid не лежат, поэтому
                # фильтр по type не нужен. interval=24 — дневная свеча.
                res = conn.execute(text(
                    "UPDATE candles SET value = :val "
                    "WHERE secid = :secid AND interval = 24 "
                    "AND begin_time::date = :d"
                ), {"val": val, "secid": secid, "d": today})
                updated += res.rowcount
    finally:
        engine.dispose()

    log.info(
        "Рублёвый оборот записан: %d дневных свечей фьючерсов (из %d контрактов с VALTODAY>0) за %s",
        updated, len(pairs), today,
    )
    print(json.dumps({"свечей": updated, "контрактов": len(pairs)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
