#!/usr/bin/env python3
"""
Загрузка цен сырьевых товаров (commodity) для индикатора Сезонность.

Источник:    Yahoo Finance, публичный chart-API /v8/finance/chart/<ticker>
             (голый GET, crumb/cookie не нужны). Из дата-центра в РФ прямой Yahoo
             недоступен (IPv4 мёртв, РКН), поэтому ходим через релей Cloudflare
             Worker — тот же паттерн, что для Telegram (signals/relay/cf-worker.js).
             COMMODITY_API_ROOT=https://<worker>.workers.dev/<RELAY_SECRET>/yahoo
             (дефолт — прямой Yahoo; локально/где доступен — работает без env).
Назначение:  index_data таблица (тот же tabular layout что у IMOEX, GLDRUB_TOM)
Тикеры:      внутренние secid'ы (BRENT, GOLD) маппятся на Yahoo (BZ=F, GC=F)

Расписание:  раз в день, 08:00 МСК (после закрытия US market в 07:00 МСК).

Режимы:
- по умолчанию (incremental):  fetch последних ~30 дней, upsert
- --backfill:                  fetch full history с COMMODITIES[*].start_date
- --secid TICKER:              только один тикер (для отладки)

Зависимости: только pandas + stdlib (urllib/json). yfinance/curl_cffi больше НЕ нужны.
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import date, datetime, timedelta, timezone

# Контейнер живёт в UTC: с 00:00 до 03:00 МСК date.today() (UTC) отстаёт на
# день. Для границ загрузки используем московскую дату.
_MSK = timezone(timedelta(hours=3))


def _msk_today() -> date:
    return datetime.now(_MSK).date()
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

import json
import urllib.request
import urllib.error

# Yahoo тянем голым GET к публичному chart-API (crumb/cookie не нужны), поэтому
# yfinance/curl_cffi БОЛЬШЕ НЕ НУЖНЫ. Из дата-центра в РФ прямой Yahoo недоступен
# (IPv4 мёртв, РКН), поэтому ходим через релей Cloudflare Worker — тот же паттерн,
# что для Telegram (signals/relay/cf-worker.js):
#   COMMODITY_API_ROOT = https://<worker>.workers.dev/<RELAY_SECRET>/yahoo
# Дефолт — прямой Yahoo (локально / где Yahoo доступен — работает без env).
COMMODITY_API_ROOT = os.environ.get(
    "COMMODITY_API_ROOT", "https://query1.finance.yahoo.com"
).rstrip("/")
_YAHOO_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
             "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

sys.path.insert(0, str(Path(__file__).parent.parent))
load_dotenv(Path(__file__).parent.parent / ".env")

DB_URL = os.getenv("DB_URL")

LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)


# ════════════════════════════════════════════════════════════════════════════
# КАТАЛОГ COMMODITIES
# ════════════════════════════════════════════════════════════════════════════
# stooq — Stooq тикер (lowercase, .f суффикс = futures, .us = US stock).
# start_date — нижняя граница: либо первый day torгов фьючерса на бирже,
#              либо округлённое значение для seasonality (1Y нужно ≥10 лет).

COMMODITIES = {
    "BRENT": {
        "yahoo": "BZ=F",       # Brent crude oil front-month (ICE)
        "name": "Нефть Brent (USD/баррель)",
        "sector": "Энергоносители",
        "start_date": date(2007, 7, 30),  # Yahoo BZ=F inception
    },
    "NATGAS_HH": {
        "yahoo": "NG=F",       # Natural Gas Henry Hub futures (NYMEX)
        "name": "Природный газ Henry Hub (USD/MMBtu)",
        "sector": "Энергоносители",
        "start_date": date(2000, 1, 1),
    },
    "GOLD": {
        "yahoo": "GC=F",       # Gold futures (Comex)
        "name": "Золото (USD/унция)",
        "sector": "Драгметаллы",
        "start_date": date(2000, 1, 1),
    },
    "SILVER": {
        "yahoo": "SI=F",       # Silver futures (Comex)
        "name": "Серебро (USD/унция)",
        "sector": "Драгметаллы",
        "start_date": date(2000, 1, 1),
    },
    "PLATINUM": {
        "yahoo": "PL=F",       # Platinum futures (NYMEX)
        "name": "Платина (USD/унция)",
        "sector": "Драгметаллы",
        "start_date": date(2000, 1, 1),
    },
    "PALLADIUM": {
        "yahoo": "PA=F",       # Palladium futures (NYMEX)
        "name": "Палладий (USD/унция)",
        "sector": "Драгметаллы",
        "start_date": date(2000, 1, 1),
    },
    "COPPER": {
        "yahoo": "HG=F",       # Copper futures (Comex)
        "name": "Медь (USD/фунт)",
        "sector": "Промышленные металлы",
        "start_date": date(2000, 1, 1),
    },
    "ALUMINUM": {
        "yahoo": "ALI=F",      # Comex Aluminum futures (с 2014)
        "name": "Алюминий (USD/тонна)",
        "sector": "Промышленные металлы",
        "start_date": date(2014, 5, 12),
    },
    "WHEAT": {
        "yahoo": "ZW=F",       # CBOT Wheat futures
        "name": "Пшеница (USD/бушель)",
        "sector": "Сельхозкультуры",
        "start_date": date(2000, 1, 1),
    },
    "LIT": {
        "yahoo": "LIT",        # Global X Lithium ETF (proxy для lithium-spot)
        "name": "Литий (LIT ETF, USD)",
        "sector": "Промышленные металлы",
        "start_date": date(2010, 7, 22),  # LIT ETF inception
    },
}


# Синтетические continuous series, собираемые из MOEX expiry contracts.
# Для TTF и Nickel на Yahoo нет данных — строим front-month rolled series
# из MOEX FORTS contracts (FFV5, FFX5, ... / NCH5, NCZ5, ...).
# Глубина истории ограничена тем когда MOEX запустил соответствующий
# контракт (TTF — Aug 2025, Nickel — Oct 2024) — для seasonality будет
# мало точек, но базовый ряд всё равно нужен.
MOEX_CONTINUOUS = {
    "TTF_GAS": {
        "moex_prefix": "FF",   # FFV5, FFX5, FFZ5, FFF6 etc.
        "name": "Газ TTF (MOEX continuous, USD/MWh)",
        "sector": "Энергоносители",
    },
    "NICKEL": {
        "moex_prefix": "NC",   # NCH5, NCZ5, NCM6 etc. (commodity nickel, NOT Норникель)
        "name": "Никель (MOEX continuous, USD/тонна)",
        "sector": "Промышленные металлы",
    },
}


# ════════════════════════════════════════════════════════════════════════════
# ЛОГИРОВАНИЕ
# ════════════════════════════════════════════════════════════════════════════

def setup_logging() -> logging.Logger:
    fmt = logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s",
                            datefmt="%Y-%m-%d %H:%M:%S")
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.handlers.clear()

    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    root.addHandler(ch)

    fh = logging.FileHandler(
        LOG_DIR / f"commodity_{datetime.now():%Y%m%d}.log",
        encoding="utf-8",
    )
    fh.setFormatter(fmt)
    root.addHandler(fh)

    return logging.getLogger(__name__)


log = setup_logging()


# ════════════════════════════════════════════════════════════════════════════
# DB UTILITIES
# ════════════════════════════════════════════════════════════════════════════

def get_last_trade_date(engine, secid: str) -> date | None:
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT MAX(trade_date) FROM index_data WHERE secid = :secid"),
            {"secid": secid},
        ).fetchone()
    return row[0] if row and row[0] else None


def upsert_instrument(engine, secid: str, meta: dict) -> None:
    """Регистрирует commodity в instruments если ещё нет.

    group='Сырьё' (Cyrillic) — frontend InstrumentSearchModal фильтрует по
    `inst.group === 'Сырьё'`, должно совпадать с user-facing label.
    type='commodity' — для backend logic / discriminator.
    sectype = secid (используется в URL и pill labels).
    """
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO instruments (sec_id, name, sectype, type, "group", sector)
                VALUES (:sec_id, :name, :sectype, 'commodity', 'Сырьё', :sector)
                ON CONFLICT (sec_id) DO UPDATE SET
                    name = EXCLUDED.name,
                    sectype = EXCLUDED.sectype,
                    "group" = EXCLUDED."group",
                    sector = EXCLUDED.sector
            """),
            {
                "sec_id": secid,
                "name": meta["name"],
                "sectype": secid,  # short label = secid (BRENT, GOLD)
                "sector": meta.get("sector", "Сырьё"),
            },
        )


def upsert_prices(engine, secid: str, df: pd.DataFrame) -> int:
    """Вставка/обновление цен из DataFrame (columns: Date, Open, High, Low, Close, Volume)."""
    if df is None or df.empty:
        return 0

    inserted = 0
    with engine.begin() as conn:
        for _, row in df.iterrows():
            trade_date = row["Date"]
            if isinstance(trade_date, str):
                trade_date = datetime.strptime(trade_date, "%Y-%m-%d").date()
            elif hasattr(trade_date, "date"):
                trade_date = trade_date.date()

            close = row.get("Close")
            if pd.isna(close):
                continue
            close = float(close)

            # value (в monetary units) у Stooq нет — derive Volume × Close.
            volume = row.get("Volume")
            value = float(volume) * close if pd.notna(volume) and volume else None

            try:
                conn.execute(
                    text("""
                        INSERT INTO index_data
                            (secid, trade_date, open, high, low, close, value, capitalization)
                        VALUES (:secid, :trade_date, :open, :high, :low, :close, :value, NULL)
                        ON CONFLICT (secid, trade_date) DO UPDATE SET
                            open = EXCLUDED.open,
                            high = EXCLUDED.high,
                            low = EXCLUDED.low,
                            close = EXCLUDED.close,
                            value = EXCLUDED.value,
                            created_at = CURRENT_TIMESTAMP
                    """),
                    {
                        "secid": secid,
                        "trade_date": trade_date,
                        "open": float(row["Open"]) if pd.notna(row.get("Open")) else None,
                        "high": float(row["High"]) if pd.notna(row.get("High")) else None,
                        "low": float(row["Low"]) if pd.notna(row.get("Low")) else None,
                        "close": close,
                        "value": value,
                    },
                )
                inserted += 1
            except Exception as e:
                log.warning(f"  {secid} {trade_date}: insert failed: {e}")

    return inserted


# ════════════════════════════════════════════════════════════════════════════
# YAHOO FETCH (chart-API голым GET через релей COMMODITY_API_ROOT)
# ════════════════════════════════════════════════════════════════════════════

def fetch_yahoo(ticker: str, start: date, end: date | None = None) -> pd.DataFrame:
    """Дневные OHLC из Yahoo chart-API. Columns: Date, Open, High, Low, Close, Volume.

    Публичный эндпоинт /v8/finance/chart/<ticker>?period1&period2&interval=1d —
    crumb/cookie НЕ нужны. URL строится от COMMODITY_API_ROOT (на проде — релей CF,
    локально — прямой Yahoo). UA выставляет релей-воркер; ставим и здесь на случай
    прямого доступа (без UA Yahoo отдаёт 429).
    """
    end = end or (_msk_today() + timedelta(days=1))  # +1 чтобы включить today
    p1 = int(datetime(start.year, start.month, start.day, tzinfo=timezone.utc).timestamp())
    p2 = int(datetime(end.year, end.month, end.day, tzinfo=timezone.utc).timestamp())
    url = (f"{COMMODITY_API_ROOT}/v8/finance/chart/{ticker}"
           f"?period1={p1}&period2={p2}&interval=1d")
    log.info(f"  Yahoo fetch {ticker} [{start} → {end}]...")

    try:
        req = urllib.request.Request(
            url, headers={"User-Agent": _YAHOO_UA, "Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=30) as r:
            payload = json.loads(r.read().decode("utf-8"))
    except Exception as e:
        log.error(f"  {ticker}: chart-API error: {e}")
        return pd.DataFrame()

    chart = payload.get("chart") or {}
    if chart.get("error"):
        log.warning(f"  {ticker}: Yahoo error {chart['error']}")
        return pd.DataFrame()
    results = chart.get("result") or []
    if not results:
        log.warning(f"  {ticker}: пустой ответ от Yahoo")
        return pd.DataFrame()

    res = results[0]
    ts = res.get("timestamp") or []
    gmtoffset = int((res.get("meta") or {}).get("gmtoffset") or 0)  # сек, биржевой tz
    quote = ((res.get("indicators") or {}).get("quote") or [{}])[0]
    opens = quote.get("open") or []
    highs = quote.get("high") or []
    lows = quote.get("low") or []
    closes = quote.get("close") or []
    vols = quote.get("volume") or []

    rows = []
    for i, t in enumerate(ts):
        c = closes[i] if i < len(closes) else None
        if c is None:
            continue  # день без закрытия (праздник/halt) — пропускаем
        # Дата бара в биржевом tz (gmtoffset), не UTC — иначе US-сессия у границы
        # суток съезжала бы на день.
        bar_date = datetime.fromtimestamp(t + gmtoffset, tz=timezone.utc).date()
        rows.append({
            "Date": bar_date.isoformat(),
            "Open": opens[i] if i < len(opens) else None,
            "High": highs[i] if i < len(highs) else None,
            "Low": lows[i] if i < len(lows) else None,
            "Close": c,
            "Volume": vols[i] if i < len(vols) else 0,
        })
    return pd.DataFrame(rows)


# ════════════════════════════════════════════════════════════════════════════
# MOEX CONTINUOUS (front-month rolled из expiry contracts)
# ════════════════════════════════════════════════════════════════════════════

def build_moex_continuous(engine, target_secid: str, meta: dict) -> int:
    """Строит front-month rolled continuous series из MOEX expiry contracts.

    Алгоритм:
    1. Для каждого trade_date находим contracts активные на эту дату
       (first_trade_date <= date <= last_trade_date) с указанным prefix.
    2. Front-month = contract с минимальным last_trade_date среди активных
       (он первым истекает → он и есть front-month).
    3. Берём close из этого contract → пишем в index_data под `target_secid`.

    Идемпотентно: ON CONFLICT DO UPDATE.
    """
    prefix = meta["moex_prefix"]

    # Регистрируем target в instruments (idempotent)
    upsert_instrument(engine, target_secid, {
        "name": meta["name"],
        "sector": meta.get("sector", "Сырьё"),
    })

    log.info(f"--- MOEX continuous {target_secid} (prefix {prefix}) ---")

    # Pattern: prefix + 1 letter (F/G/H/J/K/M/N/Q/U/V/X/Z = month) + 1 digit (year)
    # e.g. FFV5 = TTF Oct 2025, NCH6 = Nickel Mar 2026
    pattern = f"^{prefix}[FGHJKMNQUVXZ][0-9]$"

    with engine.connect() as conn:
        # Сначала получаем все relevant contracts с их жизненными интервалами
        contracts = conn.execute(text("""
            SELECT secid,
                   MIN(begin_time)::date AS first_dt,
                   MAX(begin_time)::date AS last_dt
            FROM candles
            WHERE secid ~ :pattern AND interval = 24
            GROUP BY secid
            ORDER BY MIN(begin_time)
        """), {"pattern": pattern}).fetchall()

    if not contracts:
        log.warning(f"  {target_secid}: нет contracts с prefix {prefix} в БД")
        return 0

    log.info(f"  {target_secid}: {len(contracts)} expiry contracts найдено")

    # Один SQL: для каждого торгового дня → выбрать close front-month contract.
    # `front-month` определяется по самой ранней `last_trade_date` среди active.
    # Это standard rolling logic: переключение на следующий контракт когда
    # текущий expired.
    with engine.connect() as conn:
        rows = conn.execute(text("""
            WITH ranges AS (
                SELECT secid,
                       MIN(begin_time)::date AS first_dt,
                       MAX(begin_time)::date AS last_dt
                FROM candles
                WHERE secid ~ :pattern AND interval = 24
                GROUP BY secid
            ),
            day_candidates AS (
                SELECT c.begin_time::date AS trade_date,
                       c.secid,
                       c.open, c.high, c.low, c.close, c.value,
                       r.last_dt,
                       ROW_NUMBER() OVER (
                           PARTITION BY c.begin_time::date
                           ORDER BY r.last_dt ASC, c.secid ASC
                       ) AS rn
                FROM candles c
                JOIN ranges r ON r.secid = c.secid
                WHERE c.secid ~ :pattern AND c.interval = 24
            )
            SELECT trade_date, open, high, low, close, value
            FROM day_candidates
            WHERE rn = 1
            ORDER BY trade_date
        """), {"pattern": pattern}).fetchall()

    inserted = 0
    with engine.begin() as conn:
        for row in rows:
            trade_date, op, hi, lo, cl, val = row
            if cl is None or cl <= 0:
                continue
            try:
                conn.execute(text("""
                    INSERT INTO index_data
                        (secid, trade_date, open, high, low, close, value, capitalization)
                    VALUES (:secid, :trade_date, :open, :high, :low, :close, :value, NULL)
                    ON CONFLICT (secid, trade_date) DO UPDATE SET
                        open = EXCLUDED.open,
                        high = EXCLUDED.high,
                        low = EXCLUDED.low,
                        close = EXCLUDED.close,
                        value = EXCLUDED.value,
                        created_at = CURRENT_TIMESTAMP
                """), {
                    "secid": target_secid,
                    "trade_date": trade_date,
                    "open": float(op) if op is not None else None,
                    "high": float(hi) if hi is not None else None,
                    "low": float(lo) if lo is not None else None,
                    "close": float(cl),
                    "value": float(val) if val is not None else None,
                })
                inserted += 1
            except Exception as e:
                log.warning(f"  {target_secid} {trade_date}: insert failed: {e}")

    if rows:
        log.info(f"  {target_secid}: вставлено {inserted} строк ({rows[0][0]} → {rows[-1][0]})")
    return inserted


# ════════════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════════════

def process_commodity(engine, secid: str, meta: dict, mode: str) -> int:
    log.info(f"--- {secid} ({meta['yahoo']}) ---")

    upsert_instrument(engine, secid, meta)

    if mode == "backfill":
        start = meta["start_date"]
    else:
        last = get_last_trade_date(engine, secid)
        if last is None:
            log.info(f"  В БД пусто → backfill с {meta['start_date']}")
            start = meta["start_date"]
        else:
            # Шаг назад на 14 дней для overlap с late updates.
            start = last - timedelta(days=14)

    df = fetch_yahoo(meta["yahoo"], start)
    if df.empty:
        return 0

    inserted = upsert_prices(engine, secid, df)
    if not df.empty and "Date" in df.columns:
        first = df["Date"].iloc[0]
        last_d = df["Date"].iloc[-1]
        # Convert pandas Timestamp to plain date string для лога
        first_str = first.date().isoformat() if hasattr(first, "date") else str(first)
        last_str = last_d.date().isoformat() if hasattr(last_d, "date") else str(last_d)
        log.info(f"  {secid}: вставлено/обновлено {inserted} строк (range {first_str} → {last_str})")

    # Pause 0.3s между тикерами — anti-burst
    time.sleep(0.3)
    return inserted


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--backfill", action="store_true",
                        help="Полная история с start_date")
    parser.add_argument("--secid", help="Только один тикер (для отладки)")
    args = parser.parse_args()

    mode = "backfill" if args.backfill else "incremental"
    log.info(f"=== Commodity fetch [{mode}] ===")

    engine = create_engine(DB_URL)

    secids_to_process = [args.secid] if args.secid else list(COMMODITIES.keys())

    total_rows = 0
    failed = []
    yahoo_attempted = 0
    yahoo_success = 0
    consecutive_fail = 0
    CIRCUIT_BREAK = 3  # подряд пустых Yahoo-тикеров → считаем источник недоступным
    start_time = time.time()

    for secid in secids_to_process:
        if secid not in COMMODITIES:
            log.error(f"Неизвестный secid: {secid}. Известные: {list(COMMODITIES.keys())}")
            continue
        # Circuit-breaker: каждый недоступный тикер висит до таймаута (~30с).
        # Если Yahoo заблокирован целиком (Москва/ДЦ-бан), без брейка прогон зря
        # ждёт 10×30с = 5 минут. После CIRCUIT_BREAK подряд пустых — прекращаем
        # дёргать Yahoo и идём к MOEX-continuous; guard ниже всё равно сработает.
        if consecutive_fail >= CIRCUIT_BREAK:
            log.error(
                f"Circuit-breaker: {consecutive_fail} Yahoo-тикеров подряд пустые "
                f"→ источник недоступен, пропускаю остальные ({secid}…)."
            )
            break
        yahoo_attempted += 1
        try:
            rows = process_commodity(engine, secid, COMMODITIES[secid], mode)
            total_rows += rows
            if rows == 0:
                failed.append(secid)
                consecutive_fail += 1
            else:
                yahoo_success += 1
                consecutive_fail = 0
        except Exception as e:
            log.error(f"  {secid}: FAILED — {e}", exc_info=True)
            failed.append(secid)
            consecutive_fail += 1

    # === MOEX continuous series (TTF, Nickel) ===
    # Строятся из existing candles в БД, не требуют HTTP запросов наружу.
    # Запускаются всегда, не только в backfill режиме.
    if not args.secid or args.secid in MOEX_CONTINUOUS:
        log.info("=== MOEX continuous series ===")
        targets = [args.secid] if args.secid in MOEX_CONTINUOUS else list(MOEX_CONTINUOUS.keys())
        for target in targets:
            try:
                rows = build_moex_continuous(engine, target, MOEX_CONTINUOUS[target])
                total_rows += rows
            except Exception as e:
                log.error(f"  {target}: continuous build FAILED — {e}", exc_info=True)
                failed.append(target)

    duration = time.time() - start_time
    log.info(f"=== Готово: {total_rows} строк, {len(failed)} ошибок/пустых, {duration:.1f}s ===")
    print(json.dumps({"строк": total_rows, "пустых": len(failed), "не_отдали": failed[:10]}, ensure_ascii=False))
    if failed:
        log.warning(f"Empty/failed tickers: {failed}")

    # Молли-гард видимости: одиночный пустой тикер (биржевой выходной по
    # конкретному инструменту) — не повод падать. Но если ВСЕ Yahoo-тикеры
    # пустые при непустом запуске без --secid — это системный сбой (Yahoo
    # недоступен / заблокирован), и раньше скрипт всё равно выходил с кодом 0,
    # рапортуя оркестратору «✓ Commodity Daily». Теперь возвращаем ненулевой
    # код → оркестратор логирует ✗ и инкрементит errors (он запускает commodity
    # раз в день в 08:00, а не каждые 5 минут, так что ретрай-шторма не будет).
    if not args.secid and yahoo_attempted > 0 and yahoo_success == 0:
        log.error(
            f"СИСТЕМНЫЙ СБОЙ: ни один Yahoo-тикер не отдал данные "
            f"({yahoo_attempted} попыток) — источник недоступен (сеть/блокировка). "
            f"Exit 1 для видимости оркестратору."
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
