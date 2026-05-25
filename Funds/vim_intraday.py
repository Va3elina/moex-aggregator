"""
ВИМ Intraday Parser — каждые 30 минут в торговое время.

Парсит wealthim.ru/products/bpif/{slug}/structure/ для 3 ВИМ-БПИФ
(LQDT, EQMX, GOLD), сравнивает positions с последним записанным snapshot'ом,
INSERT новой строки только если что-то изменилось.

Время работы:
  10:00-19:00 МСК, пн-пт (часы работы МосБиржи).
  Вне этого окна — скрипт сразу выходит (для cron safety).

Cron:
  */30 * * * * docker exec frame-api-1 python3 /app/Funds/vim_intraday.py

Storage: api.models.fund_holdings_intraday.FundHoldingsIntraday.
"""
import logging
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

# Добавим parent в path
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import text

from api.database import get_engine
from Funds.parsers.vim_parser import VIM_FUND_SLUGS, fetch_vim_holdings


# Тикеры которые отслеживаем intraday.
INTRADAY_TICKERS = ["LQDT", "EQMX", "GOLD"]

# МСК timezone (UTC+3).
MSK = timezone(timedelta(hours=3))


def setup_logging():
    fmt = logging.Formatter("%(asctime)s | %(levelname)-7s | %(message)s",
                             datefmt="%H:%M:%S")
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.handlers.clear()
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    root.addHandler(ch)


log = logging.getLogger(__name__)


def is_trading_hours(now: Optional[datetime] = None) -> bool:
    """
    True если сейчас в торговое время МосБиржи: 10:00-19:00 МСК, пн-пт.

    Не учитывает праздничные дни (это нормально — парсер просто прочитает
    тот же snapshot и skip'нет если данные не изменились).
    """
    if now is None:
        now = datetime.now(MSK)
    # Пн=0, Вс=6
    if now.weekday() >= 5:
        return False
    hour = now.hour
    return 10 <= hour < 19


def get_fund_id(engine, ticker: str) -> Optional[int]:
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT fund_id FROM funds WHERE ticker = :t"),
            {"t": ticker},
        ).fetchone()
    return row[0] if row else None


def get_last_intraday_positions(engine, fund_id: int) -> dict[str, int]:
    """
    Возвращает {asset_name → positions} последнего intraday snapshot'а для фонда.

    Используется для skip-detection: если все positions совпадают с last
    snapshot'ом, новый INSERT не нужен.
    """
    with engine.connect() as conn:
        # Берём timestamp самого свежего snapshot'а
        latest_ts = conn.execute(text("""
            SELECT MAX(snapshot_timestamp) FROM fund_holdings_intraday
            WHERE fund_id = :fid
        """), {"fid": fund_id}).scalar()

        if not latest_ts:
            return {}

        rows = conn.execute(text("""
            SELECT asset_name, positions FROM fund_holdings_intraday
            WHERE fund_id = :fid AND snapshot_timestamp = :ts
        """), {"fid": fund_id, "ts": latest_ts}).fetchall()
        return {r[0]: (r[1] or 0) for r in rows}


def positions_changed(prev: dict[str, int], current: list[dict]) -> bool:
    """
    Сравнивает positions: True если что-то изменилось (новый актив, исчезший
    актив, или другое число штук).
    """
    if not prev:
        # Первый snapshot — всегда INSERT
        return True

    current_map = {h["name"]: (h.get("positions") or 0) for h in current}

    # Разные наборы asset_names → изменилось
    if set(prev.keys()) != set(current_map.keys()):
        return True

    # Разные positions у каких-то активов
    for asset_name, pos in current_map.items():
        if prev.get(asset_name, 0) != pos:
            return True

    return False


def save_intraday_snapshot(engine, fund_id: int, holdings: list[dict],
                          page_date, snapshot_ts: datetime) -> int:
    """INSERT всех positions фонда в fund_holdings_intraday за один timestamp."""
    if not holdings:
        return 0
    inserted = 0
    with engine.connect() as conn:
        for h in holdings:
            result = conn.execute(text("""
                INSERT INTO fund_holdings_intraday
                    (fund_id, asset_name, weight, positions,
                     snapshot_timestamp, page_date,
                     delta_weight_to_yesterday, source, created_at)
                VALUES
                    (:fid, :name, :weight, :positions,
                     :ts, :page_date,
                     :delta, 'vim_intraday', NOW())
                ON CONFLICT (fund_id, asset_name, snapshot_timestamp) DO NOTHING
                RETURNING id
            """), {
                "fid": fund_id,
                "name": h["name"][:255],
                "weight": h.get("weight"),
                "positions": h.get("positions"),
                "ts": snapshot_ts,
                "page_date": page_date,
                "delta": h.get("delta_weight"),
            })
            if result.rowcount > 0:
                inserted += 1
        conn.commit()
    return inserted


def process_ticker(engine, ticker: str, snapshot_ts: datetime) -> dict:
    """Парсит /structure/ для одного тикера и сохраняет если изменилось."""
    log.info(f"  [{ticker}] парсим...")

    fund_id = get_fund_id(engine, ticker)
    if not fund_id:
        log.error(f"  [{ticker}] fund_id не найден, skip")
        return {"ticker": ticker, "error": "no_fund_id"}

    try:
        page_date, holdings = fetch_vim_holdings(ticker)
    except Exception as e:
        log.warning(f"  [{ticker}] parser error: {e}")
        return {"ticker": ticker, "error": "parser_error"}

    if not holdings:
        log.warning(f"  [{ticker}] empty response from VIM")
        return {"ticker": ticker, "error": "empty"}

    # Skip detection
    prev_positions = get_last_intraday_positions(engine, fund_id)
    if not positions_changed(prev_positions, holdings):
        log.info(f"  [{ticker}] positions unchanged (prev={len(prev_positions)} assets), skip")
        return {"ticker": ticker, "skipped": True, "assets": len(holdings)}

    inserted = save_intraday_snapshot(engine, fund_id, holdings, page_date, snapshot_ts)
    log.info(
        f"  [{ticker}] ✅ saved snapshot ({len(holdings)} assets, "
        f"page_date={page_date}, +{inserted} rows)"
    )
    return {
        "ticker": ticker,
        "inserted": inserted,
        "assets": len(holdings),
        "page_date": str(page_date),
    }


def main():
    setup_logging()

    now_msk = datetime.now(MSK)

    if not is_trading_hours(now_msk):
        log.info(f"Не торговое время ({now_msk.strftime('%a %H:%M МСК')}). Skip.")
        return

    log.info(f"ВИМ Intraday запуск ({now_msk.strftime('%a %d %b %H:%M МСК')})")
    log.info(f"Тикеры: {INTRADAY_TICKERS}")
    log.info("")

    engine = get_engine()
    snapshot_ts = datetime.now()  # UTC server time

    for ticker in INTRADAY_TICKERS:
        process_ticker(engine, ticker, snapshot_ts)
        time.sleep(1.0)  # дружелюбно к ВИМ

    engine.dispose()
    log.info("\nIntraday done.")


if __name__ == "__main__":
    main()
