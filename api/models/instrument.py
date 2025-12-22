"""
Модель таблицы instruments
"""
from sqlalchemy import Column, String
from api.database import Base


class Instrument(Base):
    """
    Модель инструмента (фьючерс, акция)

    Примеры:
    - stock:   sec_id="SBER", sectype="SBER", name="Сбербанк"
    - futures: sec_id="SiH", sectype="Si", name="Фьючерс на курс доллар-рубль"
    - вечный:  sec_id="USDRUBF", sectype="USDRUBF", name="USD/RUB (вечн)"
    """
    __tablename__ = "instruments"

    sec_id = Column(String, primary_key=True)       # "SiH", "SBER" — для связи с candles
    sectype = Column(String, nullable=False)        # "Si", "SBER" — базовый тикер, для связи с open_interest
    name = Column(String, nullable=False)           # "Фьючерс на курс доллар-рубль"
    type = Column(String(32))                       # "futures" или "stock"
    group = Column("group", String(50))             # "Валюта", "Акции", "Индексы"
    iss_code = Column(String(20))                   # Код ISS API: "USDRUBTOM"

    def __repr__(self):
        return f"<Instrument(sec_id='{self.sec_id}', name='{self.name}', type='{self.type}')>"