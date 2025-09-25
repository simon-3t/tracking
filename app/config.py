"""Configuration management for the tracking application."""

from functools import lru_cache
from pathlib import Path
from typing import Optional

from pydantic import BaseSettings


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    database_url: str
    binance_api_key: Optional[str] = None
    binance_api_secret: Optional[str] = None
    kraken_api_key: Optional[str] = None
    kraken_api_secret: Optional[str] = None
    etherscan_api_key: Optional[str] = None

    class Config:
        env_file = Path(__file__).resolve().parent.parent / ".env"
        env_file_encoding = "utf-8"


@lru_cache()
def get_settings() -> Settings:
    """Return cached application settings."""

    return Settings()


# A module-level singleton for convenience
settings = get_settings()
