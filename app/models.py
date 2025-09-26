from sqlalchemy import (
    create_engine,
    Column,
    String,
    Integer,
    Float,
    DateTime,
    UniqueConstraint,
    Date,
)
from sqlalchemy.orm import declarative_base, sessionmaker
from datetime import datetime

Base = declarative_base()

class Trade(Base):
    __tablename__ = "trades"
    id = Column(String, primary_key=True)   # unique: exchange_id
    exchange = Column(String, index=True)
    symbol = Column(String, index=True)
    side = Column(String)                   # 'buy' / 'sell'
    amount = Column(Float)
    price = Column(Float)
    fee = Column(Float)
    fee_currency = Column(String)
    ts = Column(Integer, index=True)        # ms since epoch
    iso = Column(DateTime)                  # UTC datetime

    __table_args__ = (UniqueConstraint('id', name='uq_trade_id'),)


class AssetPrice(Base):
    __tablename__ = "asset_prices"

    id = Column(Integer, primary_key=True, autoincrement=True)
    asset = Column(String, index=True, nullable=False)
    day = Column(Date, index=True, nullable=False)
    price_usd = Column(Float, nullable=False)
    symbol = Column(String, nullable=True)
    source = Column(String, nullable=True)

    __table_args__ = (UniqueConstraint('asset', 'day', name='uq_asset_day'),)


class Transfer(Base):
    __tablename__ = "transfers"

    id = Column(String, primary_key=True)
    exchange = Column(String, index=True, nullable=False)
    direction = Column(String, index=True, nullable=False)  # deposit / withdraw
    asset = Column(String, index=True, nullable=True)
    amount = Column(Float)
    fee = Column(Float)
    fee_currency = Column(String, nullable=True)
    status = Column(String, nullable=True)
    address = Column(String, nullable=True)
    txid = Column(String, nullable=True)
    ts = Column(Integer, index=True)
    iso = Column(DateTime)

    __table_args__ = (UniqueConstraint('id', name='uq_transfer_id'),)


def make_session(db_url: str):
    eng = create_engine(db_url, future=True)
    Base.metadata.create_all(eng)
    return sessionmaker(bind=eng, autoflush=False, autocommit=False)
