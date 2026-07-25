"""
Модель таблиц open_interest_intl / oi_intl_strength_history

Международный OI (CFTC/NSE/Eurex/B3) — admin-only, см. db/migrations/041_open_interest_intl.sql
"""
from sqlalchemy import Column, String, BigInteger, SmallInteger, Date, DateTime, Float
from api.database import Base


class OpenInterestIntl(Base):
    """Международный открытый интерес по странам/биржам (admin-only)."""

    __tablename__ = "open_interest_intl"

    exchange = Column(String, primary_key=True)    # 'CFTC' | 'NSE' | 'EUREX' | 'B3'
    asset_code = Column(String, primary_key=True)   # нативный код источника
    category = Column(String, primary_key=True)     # 'TOTAL' + source-specific holder-type
    trade_date = Column(Date, primary_key=True)
    granularity = Column(String, primary_key=True)  # 'daily' | 'weekly' | 'monthly' — источники с разной частотой не должны делить одну ячейку на совпадающих датах (см. db/migrations/042)

    country = Column(String, nullable=False)         # 'US' | 'IN' | 'DE' | 'BR'
    asset_name = Column(String, nullable=False)
    oi_long = Column(BigInteger)
    oi_short = Column(BigInteger)
    oi_total = Column(BigInteger, nullable=False)
    systime = Column(DateTime)

    def __repr__(self):
        return f"<OpenInterestIntl(exchange='{self.exchange}', asset='{self.asset_code}', category='{self.category}', date='{self.trade_date}')>"


class OiIntlStrengthHistory(Base):
    """Предвычисленная 'сила рынка по ОИ' (% активов выше EMA) по бирже/ALL."""

    __tablename__ = "oi_intl_strength_history"

    exchange = Column(String, primary_key=True)      # конкретная биржа, либо 'ALL'
    ema_period = Column(SmallInteger, primary_key=True)
    trade_date = Column(Date, primary_key=True)

    percent_above = Column(Float, nullable=False)
    count_above = Column(SmallInteger, nullable=False)
    count_total = Column(SmallInteger, nullable=False)
