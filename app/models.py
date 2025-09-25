# app/models.py
from sqlalchemy import (create_engine, Column, String, Integer, Float,
                        DateTime, UniqueConstraint)
from sqlalchemy.orm import declarative_base, sessionmaker

Base = declarative_base()

class Trade(Base):
    __tablename__ = "trades"
    id = Column(String, primary_key=True)    # exchange_id
    exchange = Column(String, index=True)
    symbol = Column(String, index=True)
    side = Column(String)
    amount = Column(Float)
    price = Column(Float)
    fee = Column(Float)
    fee_currency = Column(String)
    ts = Column(Integer, index=True)
    iso = Column(DateTime)

    __table_args__ = (UniqueConstraint('id', name='uq_trade_id'),)

def init_db(url="sqlite:///pnl.db"):
    eng = create_engine(url)
    Base.metadata.create_all(eng)
    return sessionmaker(bind=eng)
