"""Database helpers for signal-engine.

Использует те же SessionLocal/ORM-модели что и FastAPI, но без Depends —
функции работают вне FastAPI request context (cron job).
"""
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timedelta, date
from typing import Optional, List

from sqlalchemy import text
from api.database import SessionLocal
from api.models import OpenInterest, Instrument, Candle


@dataclass(frozen=True)
class OIPoint:
    tradedate: date
    pos_long: int
    pos_short: int   # хранится отрицательным числом в БД
    net: int         # pos_long + pos_short (с учётом знака)


@dataclass(frozen=True)
class CandlePoint:
    begin_time: datetime
    close: float


def get_oi_daily(
    sectype: str,
    clgroup: str,
    days: int,
    as_of_date: Optional[date] = None,
) -> List[OIPoint]:
    """Daily OI snapshots отсортированные по возрастанию даты (старые → новые).

    `as_of_date` — для backtest: вернуть данные «как если бы сегодня была эта дата».
    Если не задан — today().
    """
    end = as_of_date or date.today()
    cutoff = end - timedelta(days=days)
    with SessionLocal() as session:
        rows = (
            session.query(OpenInterest)
            .filter(
                OpenInterest.sectype == sectype,
                OpenInterest.clgroup == clgroup,
                OpenInterest.interval == 24,
                OpenInterest.tradedate >= cutoff,
                OpenInterest.tradedate <= end,
            )
            .order_by(OpenInterest.tradedate.asc())
            .all()
        )
    return [
        OIPoint(
            tradedate=r.tradedate,
            pos_long=r.pos_long,
            pos_short=r.pos_short,
            net=r.pos_long + r.pos_short,
        )
        for r in rows
    ]


def get_candles_continuous(sectype: str, days: int) -> List[CandlePoint]:
    """Continuous daily price series, склеенная из всех контрактов sectype.

    Простой rolling-front подход: на каждый день берём свечу того контракта,
    у которого на эту дату наибольший объём (= front-month). Этого достаточно
    для отображения «цены актива X» на графике.
    """
    cutoff = date.today() - timedelta(days=days)
    with SessionLocal() as session:
        # Front contract picker через window function: на каждый день берём
        # свечу с максимальным volume среди всех контрактов sectype.
        rows = session.execute(
            text("""
                SELECT begin_time, close
                FROM (
                    SELECT
                        begin_time, close,
                        ROW_NUMBER() OVER (
                            PARTITION BY begin_time::date
                            ORDER BY volume DESC NULLS LAST
                        ) AS rn
                    FROM candles
                    WHERE secid LIKE :prefix
                      AND interval = 24
                      AND type = 'futures'
                      AND begin_time::date >= :cutoff
                ) t
                WHERE rn = 1
                ORDER BY begin_time ASC
            """),
            {"prefix": f"{sectype}%", "cutoff": cutoff},
        ).fetchall()
    return [CandlePoint(begin_time=r[0], close=float(r[1])) for r in rows]


def get_latest_price(sectype: str) -> Optional[tuple]:
    """Самая свежая цена sectype — intraday, когда актив ликвиден.

    Берём свечу с НАИБОЛЕЕ свежим begin_time среди интервалов 5/60/24 (внутри одного
    begin_time — с макс. объёмом = фронт-контракт). Во время сессии это 5-мин свеча
    (цена «сейчас»); у неликвидных активов 5м/60м может не быть → откатывается на
    дневную (вчерашнее закрытие). Возвращает (close, begin_time, interval) или None.

    interval в ответе нужен вызывающему: 5/60 → intraday, 24 → дневная (EOD) —
    чтобы предупредить, что у актива нет внутридневных данных.
    """
    with SessionLocal() as session:
        row = session.execute(
            text("""
                SELECT close, begin_time, interval
                FROM candles
                WHERE secid LIKE :prefix AND type = 'futures'
                  AND interval IN (5, 60, 24) AND close > 0
                ORDER BY begin_time DESC, volume DESC NULLS LAST
                LIMIT 1
            """),
            {"prefix": f"{sectype}%"},
        ).fetchone()
    if not row:
        return None
    return float(row[0]), row[1], int(row[2])


def get_asset_name(sectype: str) -> Optional[str]:
    """Human-readable название инструмента (None если нет в instruments)."""
    with SessionLocal() as session:
        row = (
            session.query(Instrument)
            .filter(Instrument.sectype == sectype)
            .first()
        )
    return row.name if row else None


def last_signal_ts(asset: str, indicator: str, signal_type: str) -> Optional[datetime]:
    """Timestamp последнего сигнала для (asset, indicator, signal_type). None если не было."""
    with SessionLocal() as session:
        ts = session.execute(
            text("""
                SELECT MAX(ts) FROM signal_log
                WHERE asset = :asset
                  AND indicator = :indicator
                  AND signal_type = :signal_type
            """),
            {"asset": asset, "indicator": indicator, "signal_type": signal_type},
        ).scalar()
    return ts


def insert_signal_log(
    *,
    asset: str,
    indicator: str,
    signal_type: str,
    direction: str,
    z_score: float,
    raw_value: float,
    channel_id: int,
    message_id: Optional[int] = None,
    message_text: Optional[str] = None,
) -> int:
    """Записать факт публикации сигнала. Возвращает id вставленной строки."""
    with SessionLocal() as session:
        result = session.execute(
            text("""
                INSERT INTO signal_log
                    (asset, indicator, signal_type, direction,
                     z_score, raw_value, channel_id, message_id, message_text)
                VALUES
                    (:asset, :indicator, :signal_type, :direction,
                     :z_score, :raw_value, :channel_id, :message_id, :message_text)
                RETURNING id
            """),
            {
                "asset": asset,
                "indicator": indicator,
                "signal_type": signal_type,
                "direction": direction,
                "z_score": z_score,
                "raw_value": raw_value,
                "channel_id": channel_id,
                "message_id": message_id,
                "message_text": message_text,
            },
        )
        row_id = result.scalar()
        session.commit()
    return row_id
