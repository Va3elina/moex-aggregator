"""
Модель таблицы candles_intl — свечи (OHLC) для международных активов.

Отдельно от open_interest_intl (см. db/migrations/043_candles_intl.sql) —
как и в остальном проекте candles/open_interest разнесены по разным
таблицам. Один тикер может иметь несколько одновременно торгуемых
контрактных месяцев — здесь хранится только передний/самый ликвидный
(макс. OI среди фьючерсных контрактов на дату), не сумма/среднее.
"""
from sqlalchemy import Column, String, Date, DateTime, Numeric
from api.database import Base


class CandleIntl(Base):
    __tablename__ = "candles_intl"

    exchange = Column(String, primary_key=True)      # 'NSE' | 'TAIFEX'
    asset_code = Column(String, primary_key=True)
    trade_date = Column(Date, primary_key=True)
    granularity = Column(String, primary_key=True)   # 'daily'

    country = Column(String, nullable=False)
    asset_name = Column(String, nullable=False)
    open = Column(Numeric)
    high = Column(Numeric)
    low = Column(Numeric)
    close = Column(Numeric)
    settlement_price = Column(Numeric)
    systime = Column(DateTime)

    def __repr__(self):
        return f"<CandleIntl(exchange='{self.exchange}', asset='{self.asset_code}', date='{self.trade_date}')>"
