from sqlalchemy import (
    create_engine, Column, Integer, String, Numeric, ForeignKey,
    DateTime, Date, Time, BigInteger, CHAR, text, UniqueConstraint
)
from sqlalchemy.orm import declarative_base, relationship
from config import DATABASE_URL

Base = declarative_base()

# 1️⃣ Таблица instruments
class Instrument(Base):
    __tablename__ = "instruments"

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    ticker = Column(String, unique=True, nullable=False)


# 2️⃣ Таблица candles
class Candle(Base):
    __tablename__ = "candles"

    id = Column(Integer, primary_key=True)
    instrument_id = Column(Integer, ForeignKey("instruments.id", ondelete="CASCADE"))
    begin_time = Column(DateTime, nullable=False)
    end_time = Column(DateTime, nullable=False)
    open = Column(Numeric(20, 6))
    close = Column(Numeric(20, 6))
    high = Column(Numeric(20, 6))
    low = Column(Numeric(20, 6))
    value = Column(BigInteger)
    volume = Column(Numeric(20, 6))
    interval = Column(Integer)

    __table_args__ = (
        UniqueConstraint("instrument_id", "begin_time", "interval", name="uix_candles_instrument_time_interval"),
    )


# 3️⃣ Таблица orderbook
class OrderBook(Base):
    __tablename__ = "orderbook"

    id = Column(Integer, primary_key=True)
    instrument_id = Column(Integer, ForeignKey("instruments.id", ondelete="CASCADE"))
    boardid = Column(String)
    secid = Column(String)
    buysell = Column(CHAR(1))
    price = Column(Numeric(20, 6))
    quantity = Column(BigInteger)
    seqnum = Column(BigInteger)
    updatetime = Column(Time)
    decimals = Column(Integer)
    snapshot_time = Column(DateTime, server_default=text("NOW()"))


# 4️⃣ Таблица trades
class Trade(Base):
    __tablename__ = "trades"

    id = Column(Integer, primary_key=True)
    instrument_id = Column(Integer, ForeignKey("instruments.id", ondelete="CASCADE"))
    tradeno = Column(BigInteger, unique=True)
    boardname = Column(String)
    secid = Column(String)
    tradedate = Column(Date)
    tradetime = Column(Time)
    price = Column(Numeric(20, 6))
    quantity = Column(Integer)
    systime = Column(DateTime)
    recno = Column(BigInteger)
    openposition = Column(BigInteger)
    offmarketdeal = Column(Integer)
    buysell = Column(CHAR(1))


# 5️⃣ Таблица open_interest
class OpenInterest(Base):
    __tablename__ = "open_interest"

    id = Column(Integer, primary_key=True)
    instrument_id = Column(Integer, ForeignKey("instruments.id", ondelete="CASCADE"))
    sess_id = Column(Integer)
    seqnum = Column(Integer)
    tradedate = Column(Date)
    tradetime = Column(Time)
    ticker = Column(String)
    clgroup = Column(String)
    pos = Column(BigInteger)
    pos_long = Column(BigInteger)
    pos_short = Column(BigInteger)
    pos_long_num = Column(BigInteger)
    pos_short_num = Column(BigInteger)
    systime = Column(DateTime)


# Подключение к БД
engine = create_engine(DATABASE_URL)


# 6️⃣ Индексы для ускорения запросов
def create_indexes(conn):
    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_candles_instrument_time ON candles (instrument_id, begin_time);"))
    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_trades_instrument_time ON trades (instrument_id, tradedate, tradetime);"))
    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_orderbook_instrument_time ON orderbook (instrument_id, snapshot_time);"))
    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_oi_instrument_time ON open_interest (instrument_id, tradedate, tradetime);"))
