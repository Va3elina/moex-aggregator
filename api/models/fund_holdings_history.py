"""
FundHoldingsHistory — snapshots состава фондов по датам.

Зачем отдельная таблица от `fund_holdings`:
  - `fund_holdings` хранит CURRENT snapshot (DELETE+INSERT при обновлении).
    Это нужно для быстрого «что в фонде сейчас» — без поиска latest даты.
  - `fund_holdings_history` хранит ВСЕ snapshots с датами для аналитики
    типа «что фонд купил/продал за последний месяц».

Источники данных:
  - 'cbonds' — мобильный API Cbonds. Snapshot месячный, лаг ~10-30 дней,
    weight only (без `positions` штук). Покрывает большинство УК.
  - 'vim' — прямой парсинг сайта ВИМ для LQDT/EQMX/GOLD. Daily, T+1,
    есть `positions`.
  - 'pervaya', 'tcap', 'alfa' — будущие парсеры основных УК (monthly).

UNIQUE (fund_id, asset_name, snapshot_date) гарантирует идемпотентность —
повторный INSERT с тем же snapshot'ом игнорируется (ON CONFLICT DO NOTHING).
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


class FundHoldingsHistory(Base):
    __tablename__ = "fund_holdings_history"
    __table_args__ = (
        UniqueConstraint(
            "fund_id", "asset_name", "snapshot_date",
            name="uq_fund_holdings_history",
        ),
        # Главный access pattern: «возьми последние N snapshots для фонда».
        Index("idx_fhh_fund_date", "fund_id", "snapshot_date"),
        # Для top-movers за период: «все строки за дату X».
        Index("idx_fhh_date", "snapshot_date"),
    )

    id = Column(Integer, primary_key=True, index=True)
    fund_id = Column(
        Integer,
        ForeignKey("funds.fund_id", ondelete="CASCADE"),
        nullable=False,
    )
    asset_name = Column(String(255), nullable=False)

    # Доля актива в фонде в % (0-100). Может быть NULL если источник
    # не даёт weight (редко).
    weight = Column(Numeric(10, 6), nullable=True)

    # Число штук актива. Заполняется только парсерами УК которые дают
    # positions (ВИМ HTML, иногда Первая PDF). Cbonds НЕ даёт — там NULL.
    positions = Column(BigInteger, nullable=True)

    # Объём в рублях (NAV × weight или фактическая сумма). Если есть.
    amount_rub = Column(Numeric(20, 2), nullable=True)

    # Дата к которой относится snapshot (от УК). НЕ дата записи в БД.
    # Это критично: cron может запускаться ежедневно но snapshot date
    # обновится только когда УК опубликует новый отчёт.
    snapshot_date = Column(Date, nullable=False)

    # Источник: 'cbonds' / 'vim' / 'pervaya' / etc.
    source = Column(String(50), nullable=False, default="cbonds", server_default="cbonds")

    created_at = Column(
        DateTime,
        server_default=func.now(),
        nullable=False,
    )
