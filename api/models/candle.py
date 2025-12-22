"""
Модель таблицы candles
"""
from sqlalchemy import Column, String, Integer, Numeric, DateTime
from api.database import Base


class Candle(Base):
    """
    Модель свечи (OHLCV данные)

    Связь с instruments через sec_id
    secid — полный код контракта (SiH5, SiM5)
    sec_id — обрезанный код для связи (SiH, SiM)
    """
    __tablename__ = "candles"

    # Составной первичный ключ
    secid = Column(String(32), primary_key=True)  # "SiH5" — полный код контракта
    begin_time = Column(DateTime, primary_key=True)  # Время начала свечи
    interval = Column(Integer, primary_key=True)  # 5, 60, 24 (минуты)
    type = Column(String(10), primary_key=True)  # "futures" или "stock"

    # Остальные поля
    end_time = Column(DateTime)  # Время окончания свечи
    open = Column(Numeric(20, 6))  # Цена открытия
    high = Column(Numeric(20, 6))  # Максимум
    low = Column(Numeric(20, 6))  # Минимум
    close = Column(Numeric(20, 6))  # Цена закрытия
    volume = Column(Numeric(20, 6))  # Объём
    value = Column(Numeric)  # Оборот
    sec_id = Column(String(20))  # "SiH" — для связи с instruments

    def __repr__(self):
        return f"<Candle(secid='{self.secid}', time='{self.begin_time}', close={self.close})>"