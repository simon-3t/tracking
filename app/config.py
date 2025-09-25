# app/config.py
import os
from dotenv import load_dotenv

load_dotenv()  # charge le fichier .env

class Settings:
    DB_URL = os.getenv("DB_URL", "sqlite:///pnl.db")
    BINANCE_KEY = os.getenv("BINANCE_KEY")
    BINANCE_SECRET = os.getenv("BINANCE_SECRET")
    KRAKEN_KEY = os.getenv("KRAKEN_KEY")
    KRAKEN_SECRET = os.getenv("KRAKEN_SECRET")

settings = Settings()
