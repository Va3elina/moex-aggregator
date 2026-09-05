#!/usr/bin/env python3
"""
Загрузка дивидендов акций и определение экс-дивидендных дат.

⚠️ ИСТОЧНИК СМЕНИЛСЯ (03.09.2026). Раньше данные брались с ISS MOEX
(https://iss.moex.com/iss/securities/{SECID}/dividends.json). MOEX убрал сегмент
/dividends из роутинга ISS: запрос по-прежнему отдаёт HTTP 200, но вместо блока
"dividends" возвращается общая карточка бумаги (description + boards). Ни один
вариант (.xml, iss.only=dividends, iss.meta=off) блок не возвращает — эндпоинт
мёртв, а не переименован. Из-за этого фетчер тихо перестал писать строки
9 июня 2026 и пропустил весь дивидендный сезон, оставаясь при этом «зелёным».

Поэтому источник — dohod.ru (тот же, что у разового бэкфилла
Candles/backfill_dividends_dohod.py). Это НЕ биржевые данные, а агрегатор,
поэтому: (а) парсим таблицу структурно по заголовку, а не эвристикой по величине
числа — иначе теряются крупные плательщики (LKOH 541 ₽, TRNFP 204 ₽, GMKN 1523 ₽);
(б) держим порог покрытия — если источник перестал отдавать данные, скрипт падает
с ненулевым кодом, а не рапортует успех в пустоту.

Таблица: dividends(secid, registry_close_date, ex_date, value)

Запуск:
    python fetch_dividends.py                 # обновить все акции
    python fetch_dividends.py --once --force  # для оркестратора
    python fetch_dividends.py --ticker SBER --dry-run
"""

import sys
import json
import re
import logging
import argparse
import httpx
import time
from datetime import date, datetime, timedelta
from pathlib import Path

from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

sys.path.insert(0, str(Path(__file__).parent.parent))
load_dotenv(Path(__file__).parent.parent / ".env")

DB_URL = os.getenv("DB_URL")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger(__name__)

DOHOD_URL = "https://www.dohod.ru/ik/analytics/dividend/{ticker}"
HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
}

# Доля тикеров, по которым источник обязан вернуть хоть что-то. Ниже порога
# считаем, что источник сменил формат или закрылся, и валим прогон — иначе
# повторится история с ISS: пульс зелёный, данные стоят.
MIN_COVERAGE = 0.30

# Тикеры для исключения (индексы и фьючерсы, дивидендов не имеют)
EXCLUDED = {
    'IMOEX', 'IMOEXF', 'RGBI', 'USDRUBF', 'CNYRUBF', 'EURRUBF',
    'GLDRUBF', 'GAZPF', 'SBERF',
}


def create_schema(engine) -> None:
    """Создаёт таблицу dividends."""
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS dividends (
                secid             VARCHAR(20) NOT NULL,
                registry_close_date DATE NOT NULL,
                ex_date           DATE,
                value             REAL NOT NULL,
                PRIMARY KEY (secid, registry_close_date, value)
            )
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_dividends_secid
            ON dividends(secid)
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_dividends_ex_date
            ON dividends(ex_date)
        """))
        conn.commit()
    log.info("Схема dividends готова")


def get_stock_tickers(engine) -> list[str]:
    """
    Список акций: instruments (type=stock) ∪ дневные свечи акций.
    Объединение, а не только instruments: в свечах бумаг заметно больше
    (167 против 129 на 03.09.2026), и разница — это живые бумаги,
    которые иначе навсегда останутся без дивидендов.
    """
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT sec_id FROM instruments WHERE type = 'stock'
            UNION
            SELECT DISTINCT secid FROM candles
            WHERE type = 'stock' AND interval = 24 AND close > 0
        """)).fetchall()
    tickers = sorted({r[0] for r in rows if r[0] and r[0] not in EXCLUDED})
    log.info(f"Тикеры: {len(tickers)} акций")
    return tickers


def parse_dohod_dividends(html: str) -> list[dict]:
    """
    Парсит таблицу дивидендов dohod.ru структурно.

    Ищем таблицу с заголовком «Дата закрытия реестра» и берём колонки по их
    позиции в <th>. Прошлая версия скользила по плоскому списку <td> и брала
    первое число в диапазоне 0.001..100, отсекая «годы» — но заодно отсекала
    и все дивиденды крупнее 100 ₽ (LKOH, GMKN, TRNFP, PHOR, MGNT…).
    """
    results: list[tuple[date, float]] = []

    for tab in re.findall(r'<table.*?</table>', html, re.DOTALL):
        hdr = [re.sub(r'<[^>]+>', '', c).strip().lower()
               for c in re.findall(r'<th[^>]*>(.*?)</th>', tab, re.DOTALL)]
        i_reg = next((i for i, h in enumerate(hdr) if 'закрытия реестра' in h), None)
        i_val = next((i for i, h in enumerate(hdr) if h.startswith('дивиденд')), None)
        if i_reg is None or i_val is None:
            continue

        for tr in re.findall(r'<tr[^>]*>(.*?)</tr>', tab, re.DOTALL):
            tds = [re.sub(r'<[^>]+>', '', c).replace('\xa0', ' ').strip()
                   for c in re.findall(r'<td[^>]*>(.*?)</td>', tr, re.DOTALL)]
            if len(tds) <= max(i_reg, i_val):
                continue

            m = re.match(r'^(\d{2}\.\d{2}\.\d{4})$', tds[i_reg])
            if not m:
                continue

            raw = re.sub(r'[^0-9.]', '', tds[i_val].replace(' ', '').replace(',', '.'))
            try:
                value = float(raw)
            except ValueError:
                continue
            if value <= 0:
                continue

            results.append((datetime.strptime(m.group(1), '%d.%m.%Y').date(), value))

    seen = set()
    unique = []
    for d, v in sorted(results):
        if (d, v) in seen:
            continue
        seen.add((d, v))
        unique.append({"registry_close_date": d, "value": v})
    return unique


def fetch_dividends(secid: str) -> list[dict]:
    """Загружает и парсит дивиденды одного тикера."""
    url = DOHOD_URL.format(ticker=secid.lower())
    try:
        resp = httpx.get(url, headers=HTTP_HEADERS, timeout=20, follow_redirects=True)
        if resp.status_code != 200:
            log.debug(f"  {secid}: HTTP {resp.status_code}")
            return []
        return parse_dohod_dividends(resp.text)
    except Exception as e:
        log.debug(f"  {secid}: ошибка загрузки — {e}")
        return []


def find_ex_date(engine, secid: str, reg_date: date, dividend_value: float) -> date | None:
    """
    Эмпирически определяет экс-дивидендную дату.
    Ищет день с гэпом prev_close - open ≈ dividend_value
    в окне [registry_date - 5, registry_date + 1].

    Для ОБЪЯВЛЕННЫХ, но ещё не прошедших выплат (дата реестра в будущем) гэпа
    в свечах ещё нет и быть не может — возвращаем None. Строка ляжет с
    ex_date IS NULL и будет дозаполнена на одном из следующих прогонов
    (см. проверку existing[0] is not None в main).
    """
    if reg_date > date.today():
        return None

    window_start = reg_date - timedelta(days=5)
    window_end = reg_date + timedelta(days=1)

    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT
                begin_time::date as trade_date,
                open,
                LAG(close) OVER (ORDER BY begin_time) as prev_close
            FROM candles
            WHERE secid = :secid
              AND interval = 24
              AND type = 'stock'
              AND open > 0
              AND begin_time::date BETWEEN :start AND :end_ext
            ORDER BY begin_time
        """), {
            "secid": secid,
            "start": (window_start - timedelta(days=3)).isoformat(),
            "end_ext": window_end.isoformat(),
        }).fetchall()

    best_date = None
    best_diff = float("inf")

    for row in rows:
        td = row[0]
        open_price = float(row[1])
        prev_close = float(row[2]) if row[2] is not None else None

        if prev_close is None or td < window_start or td > window_end:
            continue

        gap = prev_close - open_price
        if gap <= 0 or gap < dividend_value * 0.3:
            continue

        diff = abs(gap - dividend_value)
        if diff < best_diff:
            best_diff = diff
            best_date = td

    if best_date:
        return best_date

    # Fallback: T-2 от даты реестра
    with engine.connect() as conn:
        trade_days = conn.execute(text("""
            SELECT DISTINCT begin_time::date as td
            FROM candles
            WHERE secid = :secid AND interval = 24 AND type = 'stock'
              AND begin_time::date <= :reg AND begin_time::date >= :start
            ORDER BY td DESC
            LIMIT 3
        """), {
            "secid": secid,
            "reg": reg_date.isoformat(),
            "start": (reg_date - timedelta(days=10)).isoformat(),
        }).fetchall()

    if len(trade_days) >= 3:
        return trade_days[2][0]

    return None


def main() -> None:
    parser = argparse.ArgumentParser(description="Загрузка дивидендов акций")
    parser.add_argument("--once", action="store_true", help="Однократный запуск")
    parser.add_argument("--force", action="store_true", help="Принудительный запуск")
    parser.add_argument("--ticker", type=str, help="Только один тикер")
    parser.add_argument("--dry-run", action="store_true", help="Ничего не писать в БД")
    args = parser.parse_args()

    t0 = time.time()

    engine = create_engine(DB_URL, connect_args={"ssl_context": False})
    create_schema(engine)

    if args.ticker:
        tickers = [args.ticker.upper()]
    else:
        tickers = get_stock_tickers(engine)
    if not tickers:
        log.error("Нет тикеров — выход")
        sys.exit(1)

    total_new = 0
    total_updated = 0
    with_data = 0

    for i, secid in enumerate(tickers):
        divs = fetch_dividends(secid)
        if not divs:
            continue
        with_data += 1

        new_here = 0
        for div in divs:
            reg_date = div["registry_close_date"]
            value = div["value"]

            with engine.connect() as conn:
                existing = conn.execute(text("""
                    SELECT ex_date FROM dividends
                    WHERE secid = :secid AND registry_close_date = :reg AND value = :val
                """), {"secid": secid, "reg": reg_date, "val": value}).fetchone()

            if existing and existing[0] is not None:
                continue  # уже есть с экс-датой

            ex = find_ex_date(engine, secid, reg_date, value)

            if args.dry_run:
                log.info(f"  [DRY] {secid}: reg={reg_date} val={value} ex={ex}")
                new_here += 1
                if not existing:
                    total_new += 1
                else:
                    total_updated += 1
                continue

            with engine.connect() as conn:
                conn.execute(text("""
                    INSERT INTO dividends (secid, registry_close_date, ex_date, value)
                    VALUES (:secid, :reg, :ex, :val)
                    ON CONFLICT (secid, registry_close_date, value) DO UPDATE SET
                        ex_date = COALESCE(EXCLUDED.ex_date, dividends.ex_date)
                """), {"secid": secid, "reg": reg_date, "ex": ex, "val": value})
                conn.commit()

            new_here += 1
            if existing:
                total_updated += 1
            else:
                total_new += 1

        if new_here:
            log.info(f"  [{i+1}/{len(tickers)}] {secid}: {new_here} строк "
                     f"(из {len(divs)} на странице)")

        # Пауза между тикерами, чтобы не долбить источник
        time.sleep(0.3)

    engine.dispose()

    coverage = with_data / len(tickers) if tickers else 0
    log.info(f"Готово за {time.time() - t0:.1f}с. Новых: {total_new}, "
             f"обновлённых: {total_updated}, покрытие: {with_data}/{len(tickers)} "
             f"({coverage:.0%})")
    print(json.dumps({"новых": total_new, "обновлено": total_updated,
                      "покрытие": round(coverage, 3), "тикеров": len(tickers)}, ensure_ascii=False))

    # Тихий отказ источника — это отказ, а не успех.
    if not args.ticker and coverage < MIN_COVERAGE:
        log.error(f"Покрытие {coverage:.0%} ниже порога {MIN_COVERAGE:.0%} — "
                  f"похоже, источник сменил формат или закрылся. Прогон провален.")
        sys.exit(1)


if __name__ == "__main__":
    main()
