"""
FundHoldingsIntraday — intraday snapshots состава ВИМ-фондов.

Отдельная таблица от `fund_holdings_history` потому что:
  - другая гранулярность: timestamp, не date
  - другой источник: только ВИМ-сайт /structure/ (real-time T+15min)
  - другой UI: вкладка "Intraday сделки" vs "Месячные движения"
  - другая логика skip'а: если positions не изменились с прошлого парса — INSERT не делаем

Ингестия запускается cron'ом каждые 30 минут в торговое время МосБиржи
(10:00-19:00 МСК, пн-пт). Парсер дёргает /structure/ для LQDT/EQMX/GOLD,
сравнивает positions с последним записанным snapshot'ом, INSERT только
если что-то изменилось.

Использование в API:
  /api/fund-trades/intraday/{ticker} — список событий "что и когда изменилось"
"""
from sqlalchemy import (
    BigInteger,
    Column,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    UniqueConstraint,
)
from sqlalchemy.sql import func

from api.database import Base


class FundHoldingsIntraday(Base):
    __tablename__ = "fund_holdings_intraday"
    __table_args__ = (
        # Не дублируем одинаковые позиции на одинаковый timestamp
        UniqueConstraint(
            "fund_id", "asset_name", "snapshot_timestamp",
            name="uq_fund_intraday_pos",
        ),
        # Главный access: «возьми все snapshots фонда за последние N часов»
        Index("idx_fhi_fund_ts", "fund_id", "snapshot_timestamp"),
        # Для cross-fund analysis (опционально): все события за период
        Index("idx_fhi_ts", "snapshot_timestamp"),
    )

    id = Column(Integer, primary_key=True, index=True)
    fund_id = Column(
        Integer,
        ForeignKey("funds.fund_id", ondelete="CASCADE"),
        nullable=False,
    )
    asset_name = Column(String(255), nullable=False)

    # weight % (0-100) на момент парса
    weight = Column(Numeric(10, 6), nullable=True)

    # positions = штуки. Главное поле для intraday — реальные сделки УК.
    positions = Column(BigInteger, nullable=True)

    # snapshot_timestamp = время нашего парсинга (= ~T+15min от реальной сделки)
    snapshot_timestamp = Column(
        DateTime,
        nullable=False,
        server_default=func.now(),
    )

    # page_date = дата ВИМ-страницы ("Портфель фонда (25.05.2026)").
    # Это другое чем snapshot_timestamp! page_date может быть вчерашней
    # если мы парсим утром до открытия торгов.
    page_date = Column(Date, nullable=True)

    # delta_weight_to_yesterday — то что ВИМ показывает в колонке "Изменение доли"
    # на странице. Это дельта к предыдущему ТОРГОВОМУ дню, не к 5 минутам назад.
    delta_weight_to_yesterday = Column(Numeric(10, 6), nullable=True)

    source = Column(
        String(50),
        nullable=False,
        default="vim_intraday",
        server_default="vim_intraday",
    )

    created_at = Column(DateTime, server_default=func.now(), nullable=False)
