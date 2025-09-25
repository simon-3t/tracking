"""Database models for the tracking application."""

from sqlalchemy import Column, DateTime, Float, Integer, Numeric, String
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class Trade(Base):
    """Represents an executed trade."""

    __tablename__ = "trades"

    id = Column(Integer, primary_key=True, index=True)
    exchange = Column(String, nullable=False)
    symbol = Column(String, nullable=False)
    side = Column(String, nullable=False)
    quantity = Column(Numeric(38, 18), nullable=False)
    price = Column(Numeric(38, 18), nullable=False)
    fee = Column(Numeric(38, 18), nullable=True)
    executed_at = Column(DateTime, nullable=False)


class Price(Base):
    """Represents a historical price point."""

    __tablename__ = "prices"

    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String, nullable=False)
    price = Column(Float, nullable=False)
    source = Column(String, nullable=False)
    collected_at = Column(DateTime, nullable=False)
