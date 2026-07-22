#!/usr/bin/env python3
"""
Реалтайм обновление крипто-данных с Bybit (linear perpetuals).

Источник: Bybit v5 public API (candles + open interest), без ключей/KYC.
Крипта торгуется 24/7 — в отличие от MOEX-фетчеров тут нет торговых часов
и оркестратора: скрипт самодостаточен, запускается host-cron раз в 5 минут
в режиме --once (см. Bybit-эквивалент update_5min/60min/daily в
Candles/fetch_candles_futures_realtime.py, откуда взят паттерн).

Таймфреймы: 5м, 1ч, 1д (candles + open interest на тех же трёх гранулярностях).
Плюс раз в скрипт — сеть BTC (высота блока, сложность, хешрейт) через mempool.space.

Режимы запуска:
- --once (по умолчанию): однократное обновление всех таймфреймов + сети
"""

import argparse
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

sys.path.insert(0, str(Path(__file__).parent.parent))
load_dotenv(Path(__file__).parent.parent / ".env")

DB_URL = os.getenv("DB_URL")

EXCHANGE = "bybit"
SYMBOLS = ["BTCUSDT"]

# Наши гранулярности → коды Bybit
CANDLE_INTERVALS = {"5m": "5", "1h": "60", "1d": "D"}
OI_INTERVALS = {"5m": "5min", "1h": "1h", "1d": "1d"}
STEP_MS = {"5m": 5 * 60_000, "1h": 60 * 60_000, "1d": 24 * 60 * 60_000}

# Backfill-окно при первом запуске (когда в БД ещё нет данных по символу)
BACKFILL_DAYS = {"5m": 7, "1h": 30, "1d": 730}

BYBIT_BASE = "https://api.bybit.com/v5/market"
MEMPOOL_BASE = "https://mempool.space/api"

REQUEST_TIMEOUT = 15
KLINE_LIMIT = 1000  # максимум за запрос у Bybit
OI_LIMIT = 200

LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)


def setup_logging() -> logging.Logger:
    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.handlers.clear()

    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    root.addHandler(ch)

    fh = logging.FileHandler(
        LOG_DIR / f"crypto_bybit_{datetime.now():%Y%m%d}.log", encoding="utf-8"
    )
    fh.setFormatter(fmt)
    root.addHandler(fh)

    return logging.getLogger(__name__)


log = setup_logging()


# ======================================================================
#                    BYBIT FETCHER
# ======================================================================

def _get(url: str, params: dict) -> Optional[dict]:
    try:
        resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        if resp.status_code != 200:
            log.warning(f"HTTP {resp.status_code} {url} {params}")
            return None
        data = resp.json()
        if data.get("retCode") != 0:
            log.warning(f"Bybit retCode={data.get('retCode')} retMsg={data.get('retMsg')} {params}")
            return None
        return data["result"]
    except requests.RequestException as e:
        log.error(f"Ошибка запроса {url} {params}: {e}")
        return None


def fetch_klines(symbol: str, interval_code: str, step_ms: int, start_ms: int) -> pd.DataFrame:
    """Тянет свечи, паджинируя ОКНАМИ [cursor, cursor+limit*step] вперёд до текущего момента.

    ГОТЧА (проверено вручную): Bybit kline игнорирует чистый `start` без `end` —
    без `end` отдаёт последние `limit` баров от текущего момента, а не окно от
    `start`. С обоими параметрами окно соблюдается точно.
    """
    all_rows = []
    cursor = start_ms
    now_ms = int(time.time() * 1000)

    while cursor < now_ms:
        window_end = min(cursor + (KLINE_LIMIT - 1) * step_ms, now_ms)
        params = {
            "category": "linear",
            "symbol": symbol,
            "interval": interval_code,
            "limit": KLINE_LIMIT,
            "start": cursor,
            "end": window_end,
        }

        result = _get(f"{BYBIT_BASE}/kline", params)
        if not result or not result.get("list"):
            break

        rows = result["list"]
        all_rows.extend(rows)

        newest_ts = int(rows[0][0])
        cursor = newest_ts + step_ms
        time.sleep(0.1)

    if not all_rows:
        return pd.DataFrame()

    df = pd.DataFrame(
        all_rows, columns=["open_time_ms", "open", "high", "low", "close", "volume", "turnover"]
    )
    df["open_time_ms"] = df["open_time_ms"].astype("int64")
    if start_ms:
        df = df[df["open_time_ms"] >= start_ms]
    df["open_time"] = pd.to_datetime(df["open_time_ms"], unit="ms", utc=True)
    for col in ["open", "high", "low", "close", "volume", "turnover"]:
        df[col] = df[col].astype(float)
    df = df.rename(columns={"turnover": "quote_volume"})
    df = df.drop_duplicates(subset=["open_time"]).sort_values("open_time")
    return df[["open_time", "open", "high", "low", "close", "volume", "quote_volume"]]


def fetch_open_interest(symbol: str, interval_time: str, step_ms: int, start_ms: int) -> pd.DataFrame:
    """Тянет OI, паджинируя ОКНАМИ [cursor, cursor+limit*step] вперёд до текущего момента.

    ГОТЧА: как и kline, без `endTime` Bybit отдаёт последние `limit` записей от
    текущего момента, игнорируя `startTime`. С обоими параметрами окно точное.
    """
    all_rows = []
    cursor = start_ms
    now_ms = int(time.time() * 1000)

    while cursor < now_ms:
        window_end = min(cursor + (OI_LIMIT - 1) * step_ms, now_ms)
        params = {
            "category": "linear",
            "symbol": symbol,
            "intervalTime": interval_time,
            "limit": OI_LIMIT,
            "startTime": cursor,
            "endTime": window_end,
        }

        result = _get(f"{BYBIT_BASE}/open-interest", params)
        if not result or not result.get("list"):
            break

        rows = result["list"]
        all_rows.extend(rows)

        newest_ts = int(rows[0]["timestamp"])
        cursor = newest_ts + step_ms
        time.sleep(0.1)

    if not all_rows:
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)
    df["timestamp"] = df["timestamp"].astype("int64")
    if start_ms:
        df = df[df["timestamp"] >= start_ms]
    df["ts"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    df["oi_contracts"] = df["openInterest"].astype(float)
    df = df.drop_duplicates(subset=["ts"]).sort_values("ts")
    return df[["ts", "oi_contracts"]]


def fetch_btc_network_stats() -> Optional[dict]:
    try:
        blocks_resp = requests.get(f"{MEMPOOL_BASE}/blocks", timeout=REQUEST_TIMEOUT)
        blocks_resp.raise_for_status()
        latest = blocks_resp.json()[0]

        hr_resp = requests.get(f"{MEMPOOL_BASE}/v1/mining/hashrate/1m", timeout=REQUEST_TIMEOUT)
        hr_resp.raise_for_status()
        hashrates = hr_resp.json().get("hashrates", [])
        hashrate_eh = hashrates[-1]["avgHashrate"] / 1e18 if hashrates else None

        return {
            "block_height": latest["height"],
            "difficulty": latest["difficulty"],
            "hashrate_eh": hashrate_eh,
        }
    except (requests.RequestException, KeyError, IndexError) as e:
        log.error(f"Ошибка получения сети BTC (mempool.space): {e}")
        return None


# ======================================================================
#                    DB UPDATER
# ======================================================================

class CryptoUpdater:
    def __init__(self, db_url: str):
        self.engine = create_engine(db_url, connect_args={"ssl_context": False})
        with self.engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        log.info("✓ БД подключена")

    def get_last_candle_time(self, symbol: str, interval: str) -> Optional[datetime]:
        with self.engine.connect() as conn:
            row = conn.execute(
                text(
                    """SELECT MAX(open_time) FROM crypto_candles
                       WHERE exchange = :ex AND symbol = :sym AND interval = :iv"""
                ),
                {"ex": EXCHANGE, "sym": symbol, "iv": interval},
            ).fetchone()
            return row[0] if row and row[0] else None

    def get_last_oi_time(self, symbol: str, interval: str) -> Optional[datetime]:
        with self.engine.connect() as conn:
            row = conn.execute(
                text(
                    """SELECT MAX(ts) FROM crypto_open_interest
                       WHERE exchange = :ex AND symbol = :sym AND interval = :iv"""
                ),
                {"ex": EXCHANGE, "sym": symbol, "iv": interval},
            ).fetchone()
            return row[0] if row and row[0] else None

    def save_candles(self, symbol: str, interval: str, df: pd.DataFrame) -> int:
        if df.empty:
            return 0
        with self.engine.begin() as conn:
            inserted = 0
            for row in df.itertuples(index=False):
                result = conn.execute(
                    text(
                        """
                        INSERT INTO crypto_candles
                            (exchange, symbol, interval, open_time, open, high, low, close, volume, quote_volume)
                        VALUES (:ex, :sym, :iv, :ot, :o, :h, :l, :c, :v, :qv)
                        ON CONFLICT (exchange, symbol, interval, open_time) DO UPDATE SET
                            open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
                            close = EXCLUDED.close, volume = EXCLUDED.volume,
                            quote_volume = EXCLUDED.quote_volume
                        """
                    ),
                    {
                        "ex": EXCHANGE, "sym": symbol, "iv": interval,
                        "ot": row.open_time, "o": row.open, "h": row.high,
                        "l": row.low, "c": row.close, "v": row.volume, "qv": row.quote_volume,
                    },
                )
                inserted += result.rowcount
        return inserted

    def save_open_interest(self, symbol: str, interval: str, df: pd.DataFrame) -> int:
        if df.empty:
            return 0
        with self.engine.begin() as conn:
            inserted = 0
            for row in df.itertuples(index=False):
                result = conn.execute(
                    text(
                        """
                        INSERT INTO crypto_open_interest
                            (exchange, symbol, interval, ts, oi_contracts, oi_usd)
                        VALUES (:ex, :sym, :iv, :ts, :oi, NULL)
                        ON CONFLICT (exchange, symbol, interval, ts) DO UPDATE SET
                            oi_contracts = EXCLUDED.oi_contracts
                        """
                    ),
                    {
                        "ex": EXCHANGE, "sym": symbol, "iv": interval,
                        "ts": row.ts, "oi": row.oi_contracts,
                    },
                )
                inserted += result.rowcount
        return inserted

    def save_network_stats(self, stats: dict) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO btc_network_stats (ts, block_height, difficulty, hashrate_eh)
                    VALUES (NOW(), :height, :diff, :hr)
                    ON CONFLICT (ts) DO NOTHING
                    """
                ),
                {"height": stats["block_height"], "diff": stats["difficulty"], "hr": stats["hashrate_eh"]},
            )

    def update_candles(self, symbol: str, label: str) -> int:
        code = CANDLE_INTERVALS[label]
        step_ms = STEP_MS[label]
        last_time = self.get_last_candle_time(symbol, label)
        if last_time:
            start_ms = int(last_time.timestamp() * 1000)
        else:
            start_ms = int(time.time() * 1000) - BACKFILL_DAYS[label] * 86400 * 1000
            log.info(f"[{symbol} {label}] нет данных — backfill {BACKFILL_DAYS[label]}д")

        df = fetch_klines(symbol, code, step_ms, start_ms)
        inserted = self.save_candles(symbol, label, df)
        log.info(f"[{symbol} {label}] candles: получено {len(df)}, upsert {inserted}")
        return inserted

    def update_open_interest(self, symbol: str, label: str) -> int:
        interval_time = OI_INTERVALS[label]
        step_ms = STEP_MS[label]
        last_time = self.get_last_oi_time(symbol, label)
        if last_time:
            start_ms = int(last_time.timestamp() * 1000)
        else:
            start_ms = int(time.time() * 1000) - BACKFILL_DAYS[label] * 86400 * 1000
            log.info(f"[{symbol} {label}] нет OI-данных — backfill {BACKFILL_DAYS[label]}д")

        df = fetch_open_interest(symbol, interval_time, step_ms, start_ms)
        inserted = self.save_open_interest(symbol, label, df)
        log.info(f"[{symbol} {label}] OI: получено {len(df)}, upsert {inserted}")
        return inserted

    def run_once(self) -> None:
        for symbol in SYMBOLS:
            for label in CANDLE_INTERVALS:
                try:
                    self.update_candles(symbol, label)
                except Exception as e:
                    log.error(f"[{symbol} {label}] Ошибка candles: {e}", exc_info=True)
                try:
                    self.update_open_interest(symbol, label)
                except Exception as e:
                    log.error(f"[{symbol} {label}] Ошибка OI: {e}", exc_info=True)

        stats = fetch_btc_network_stats()
        if stats:
            self.save_network_stats(stats)
            log.info(
                f"[BTC network] height={stats['block_height']} "
                f"difficulty={stats['difficulty']:.3e} hashrate={stats['hashrate_eh']:.1f} EH/s"
            )


def main():
    parser = argparse.ArgumentParser(description="Загрузка крипто-данных с Bybit")
    parser.add_argument("--once", action="store_true", default=True, help="Однократное обновление (по умолчанию)")
    parser.parse_args()

    if not DB_URL:
        log.critical("❌ Не задан DB_URL. Задай его в .env")
        sys.exit(1)

    log.info("=" * 60)
    log.info(f"ЗАПУСК КРИПТО-ФЕТЧЕРА (Bybit) — {datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S} UTC")
    log.info(f"Символы: {SYMBOLS}, гранулярности: {list(CANDLE_INTERVALS)}")
    log.info("=" * 60)

    try:
        updater = CryptoUpdater(DB_URL)
        updater.run_once()
        log.info("✅ Готово")
    except Exception as e:
        log.critical(f"❌ Фатальная ошибка: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
