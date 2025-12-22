"""
Модель таблицы open_interest
"""
from sqlalchemy import Column, String, Integer, BigInteger, Date, Time, DateTime
from api.database import Base


class OpenInterest(Base):
    """
    Модель открытого интереса

    Связь с instruments через sectype
    Только для фьючерсов (type="futures")
    """
    __tablename__ = "open_interest"

    # Составной первичный ключ
    sectype = Column(String, primary_key=True)  # "Si", "RI" — базовый тикер
    tradedate = Column(Date, primary_key=True)  # Дата торгов
    tradetime = Column(Time, primary_key=True)  # Время торгов
    clgroup = Column(String, primary_key=True)  # "FIZ" или "YUR"
    interval = Column(Integer, primary_key=True, default=5)  # 5, 60, 24 (минуты)

    # Данные по позициям
    pos = Column(BigInteger)  # Общий открытый интерес
    pos_long = Column(BigInteger)  # Длинные позиции (покупки)
    pos_short = Column(BigInteger)  # Короткие позиции (продажи)
    pos_long_num = Column(BigInteger)  # Количество покупателей
    pos_short_num = Column(BigInteger)  # Количество продавцов

    # Системное время
    systime = Column(DateTime)  # Время записи

    def __repr__(self):
        return f"<OpenInterest(sectype='{self.sectype}', date='{self.tradedate}', clgroup='{self.clgroup}')>"