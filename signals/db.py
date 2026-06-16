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


def get_position_series(sectype: str, clgroup: str, days: int,
                        as_of_date: Optional[date] = None,
                        interval: int = 24) -> List[tuple]:
    """Дневной ряд (date, net, npart, pos_long, pos_short) для ATR-детектора
    «резкого движения позиции».
    net = pos_long + pos_short (pos_short отрицательный); npart = число участников
    (pos_long_num + pos_short_num) — для guard'а ликвидности. pos_long/pos_short —
    сами ноги (длинная положительная, короткая отрицательная) — чтобы текст алерта
    мог сказать, какая нога двинулась. Порядок элементов 0-2 неизменен — на них
    индексируются compute_position_atr (net) и compute_participants_atr (npart).

    `interval` — таймфрейм источника бара:
      24 → дневная публикация (одна точка/день — текущее поведение);
      5/60 → внутридневные бары. Берём ПОСЛЕДНИЙ бар каждого дня (закрытие дня)
      через DISTINCT ON (tradedate) … ORDER BY tradetime DESC. Для прошлых дней это
      их EOD-значение; для СЕГОДНЯ (незакрытый день) последний элемент = бегущий
      внутридневной бар → даёт «net сейчас» для раннего срабатывания дневного сигнала.
      Итог отсортирован по tradedate ASC (как и при interval=24)."""
    end = as_of_date or date.today()
    cutoff = end - timedelta(days=days)
    with SessionLocal() as session:
        rows = session.execute(
            text("""
                SELECT tradedate,
                       (pos_long + pos_short) AS net,
                       (pos_long_num + pos_short_num) AS npart,
                       pos_long, pos_short
                FROM (
                    SELECT DISTINCT ON (tradedate)
                        tradedate, pos_long, pos_short, pos_long_num, pos_short_num
                    FROM open_interest
                    WHERE sectype = :sectype
                      AND clgroup = :clgroup
                      AND interval = :interval
                      AND tradedate >= :cutoff
                      AND tradedate <= :end
                    ORDER BY tradedate, tradetime DESC
                ) t
                ORDER BY tradedate ASC
            """),
            {"sectype": sectype, "clgroup": clgroup, "interval": interval,
             "cutoff": cutoff, "end": end},
        ).fetchall()
    return [
        (r[0], (r[1] or 0), (r[2] or 0), (r[3] or 0), (r[4] or 0))
        for r in rows
    ]


# Категории фондов → бенчмарк-индекс (зеркало CATEGORY_INDEX_MAP в
# api/routers/funds.py). Юань НЕ включён — на сайте «Скоро». money_market/gold с
# min_date=2022-01-09 (ранняя история шумная), но для ATR-серии берём окно ~30-45д
# назад — все категории к этой дате давно «чистые», поэтому min_date тут не нужен.
FUND_CATEGORIES = {
    "money_market": "RUSFAR3M",
    "stocks": "IMOEX",
    "bonds": "RGBITR",
    "gold": "GLDRUB_TOM",
}


def get_fund_flow_series(category: str, days: int = 60) -> List[tuple]:
    """Дневной ряд (date, net_flow) суммарного «аномального потока» категории —
    для ATR-детектора fund-сигналов. Зеркало net_flow-логики из
    api/routers/funds.py get_funds_flows (timeframe='1d').

    net_flow дня = Σ по фондам категории [ΔNAV − market_change], где
    market_change = prev_nav · (curr_pay − prev_pay) / prev_pay — «бумажная»
    переоценка пая (рост СЧА за счёт цены, а не притока денег). То, что остаётся
    после вычитания переоценки = реальный приток/отток средств в фонд.

    Логика повторяет ветку `timeframe == "1d"` в API:
      - forward-fill nav/pay по последней доступной точке ДО даты (фонд мог не
        обновиться в конкретный день);
      - фонд учитывается в дне только если есть И prev_d, И curr_d (иначе появление/
        исчезновение фонда даёт ложный «поток» — структурное изменение, не деньги);
      - market_change вычитается лишь когда обе pay > 0 (иначе net_flow = ΔNAV).

    Возвращает список (date, net_flow_rub) по возрастанию даты. net_flow в рублях
    (НЕ делим на 1e9 как API — ATR-кратность безразмерна, масштаб не важен).
    Категории/фонды — как в API (funds.category + фильтр «>=2 точки NAV»).
    """
    if category not in FUND_CATEGORIES:
        return []
    cutoff = date.today() - timedelta(days=days)
    with SessionLocal() as session:
        # fund_id'ы категории с историей (>=2 точки NAV) — тот же фильтр, что
        # load_fund_categories в API (фонд с одной точкой не даёт «изменения»).
        fund_ids = [
            r[0] for r in session.execute(
                text("""
                    SELECT f.fund_id FROM funds f
                    WHERE f.category = :cat
                      AND (SELECT count(*) FROM fund_data fd
                           WHERE fd.fund_id = f.fund_id AND fd.nav IS NOT NULL) >= 2
                """),
                {"cat": category},
            ).fetchall()
        ]
        if not fund_ids:
            return []
        # Per-fund nav/pay с forward-fill (зеркало all_dates/fund_filled из API).
        rows = session.execute(
            text("""
                WITH all_dates AS (
                    SELECT DISTINCT trade_date FROM fund_data
                    WHERE fund_id = ANY(:fund_ids) AND trade_date >= :cutoff
                )
                SELECT f.fund_id, d.trade_date,
                    COALESCE(fd.nav,
                        (SELECT fd2.nav FROM fund_data fd2
                         WHERE fd2.fund_id = f.fund_id AND fd2.trade_date < d.trade_date
                           AND fd2.nav IS NOT NULL
                         ORDER BY fd2.trade_date DESC LIMIT 1)) AS nav,
                    COALESCE(fd.pay,
                        (SELECT fd2.pay FROM fund_data fd2
                         WHERE fd2.fund_id = f.fund_id AND fd2.trade_date < d.trade_date
                           AND fd2.pay IS NOT NULL
                         ORDER BY fd2.trade_date DESC LIMIT 1)) AS pay
                FROM all_dates d
                CROSS JOIN (SELECT DISTINCT fund_id FROM fund_data
                            WHERE fund_id = ANY(:fund_ids)) f
                LEFT JOIN fund_data fd ON fd.fund_id = f.fund_id AND fd.trade_date = d.trade_date
                ORDER BY f.fund_id, d.trade_date
            """),
            {"fund_ids": fund_ids, "cutoff": cutoff},
        ).fetchall()
    # date -> {fund_id: (nav, pay)} (только дни с nav, как API: WHERE nav IS NOT NULL)
    by_fund: dict = {}
    for fid, d, nav, pay in rows:
        if nav is None:
            continue
        by_fund.setdefault(fid, {})[d] = (float(nav), float(pay or 0))
    all_dates = sorted({d for fmap in by_fund.values() for d in fmap})
    series: List[tuple] = []
    for i in range(1, len(all_dates)):
        prev_d, curr_d = all_dates[i - 1], all_dates[i]
        total_net = 0.0
        for fid, date_map in by_fund.items():
            if prev_d not in date_map or curr_d not in date_map:
                continue  # фонд отсутствует в одном из дней — не поток
            prev_nav, prev_pay = date_map[prev_d]
            curr_nav, curr_pay = date_map[curr_d]
            total_flow = curr_nav - prev_nav
            if prev_pay > 0 and curr_pay > 0:
                market_change = prev_nav * (curr_pay - prev_pay) / prev_pay
                net_flow = total_flow - market_change
            else:
                net_flow = total_flow
            total_net += net_flow
        series.append((curr_d, total_net))
    return series


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
    дневную. Возвращает (close, begin_time, interval) или None (нет свежих данных).

    interval в ответе: 5/60 → intraday, 24 → дневная (EOD) — чтобы предупредить,
    что у актива нет внутридневных данных.

    ⚡ Производительность: резолвим sec_id через крошечную instruments + окно
    `begin_time >= now()-14д`. Тогда запрос ложится на индекс
    idx_candles_sec_interval_time (sec_id, interval, begin_time) → ~2 мс вместо
    12-130 с (наивный `secid LIKE` делал Seq Scan по 23М строк / 8 ГБ). Окно 14 дней
    переживает новогодние каникулы; если последняя свеча старше — актив «мёртв» → None.
    """
    with SessionLocal() as session:
        row = session.execute(
            text("""
                SELECT close, begin_time, interval
                FROM candles
                WHERE sec_id IN (SELECT sec_id FROM instruments
                                 WHERE sectype = :sectype AND type = 'futures')
                  AND interval IN (5, 60, 24) AND close > 0
                  AND begin_time >= now() - interval '14 days'
                ORDER BY begin_time DESC, volume DESC NULLS LAST
                LIMIT 1
            """),
            {"sectype": sectype},
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
