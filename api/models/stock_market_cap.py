"""
Модель таблицы stock_market_cap — капитализация по компаниям (SmartLab)
"""
from sqlalchemy import Column, Integer, String, Date, Numeric, DateTime, func
from api.database import Base


class StockMarketCap(Base):
    """
    Рыночная капитализация отдельных акций (млрд руб).
    Источник: SmartLab, обновляется ежедневно.
    """
    __tablename__ = "stock_market_cap"

    id = Column(Integer, primary_key=True)
    sec_id = Column(String(20), nullable=False)
    period_date = Column(Date, nullable=False)
    market_cap = Column(Numeric(24, 2), nullable=False)
    created_at = Column(DateTime, server_default=func.now())

    def __repr__(self):
        return f"<StockMarketCap(sec_id='{self.sec_id}', date={self.period_date}, cap={self.market_cap})>"
