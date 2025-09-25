from sqlalchemy import create_engine, Column, String, Integer, Float, DateTime, UniqueConstraint
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

def make_session(db_url: str):
    eng = create_engine(db_url, future=True)
    Base.metadata.create_all(eng)
    return sessionmaker(bind=eng, autoflush=False, autocommit=False)
