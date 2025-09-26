# scripts/ingest_kraken.py
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import ccxt
from dotenv import load_dotenv
from sqlalchemy.exc import SQLAlchemyError


# Ensure the repository root (which contains the ``app`` package) is on PYTHONPATH
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.models import Trade, make_session

load_dotenv()
DB_URL = os.getenv("DB_URL", "sqlite:///pnl.db")
KRAKEN_KEY = os.getenv("KRAKEN_KEY")
KRAKEN_SECRET = os.getenv("KRAKEN_SECRET")

if not KRAKEN_KEY or not KRAKEN_SECRET:
    raise SystemExit("⚠️  KRAKEN_KEY / KRAKEN_SECRET manquants dans .env")

# DB session
Session = make_session(DB_URL)
session = Session()

# Exchange (REST)
ex = ccxt.kraken({
    'apiKey': KRAKEN_KEY,
    'secret': KRAKEN_SECRET,
    'enableRateLimit': True,
})
ex.load_markets()  # utile pour normaliser les symboles

def upsert_trade(t):
    """
    Normalise et upsert un trade CCXT dans la table trades.
    """
    sym = t.get('symbol') or ''
    ts = int(t.get('timestamp') or 0)
    row = Trade(
        id=f"kraken_{t.get('id') or t.get('order') or ts}",
        exchange="kraken",
        symbol=sym,
        side=(t.get('side') or '').lower(),
        amount=float(t.get('amount') or 0.0),
        price=float(t.get('price') or 0.0),
        fee=(t.get('fee') or {}).get('cost') if t.get('fee') else 0.0,
        fee_currency=(t.get('fee') or {}).get('currency') if t.get('fee') else None,
        ts=ts,
        iso=datetime.fromtimestamp(ts/1000, tz=timezone.utc) if ts else None,
    )
    session.merge(row)

def ingest_all_trades():
    """
    Utilise l'endpoint 'TradesHistory' via ccxt.fetch_my_trades().

    Note: Kraken renvoie l'historique global (pas besoin de boucler par symbol).
    On pagine avec 'since' (millisecondes) jusqu'à épuisement.
    """
    total = 0
    since = None  # pour un vrai incrémental, persiste ce curseur dans une table
    while True:
        try:
            batch = ex.fetch_my_trades(symbol=None, since=since, limit=50)
        except ccxt.DDoSProtection as e:
            # backoff simple
            time.sleep(2)
            continue
        except ccxt.BaseError as e:
            print(f"⚠️  Kraken API error: {e}")
            break

        if not batch:
            break

        # upsert
        try:
            for t in batch:
                upsert_trade(t)
                total += 1
            session.commit()
        except SQLAlchemyError as e:
            session.rollback()
            print(f"⚠️  DB error, rollback: {e}")
            break

        # pagination: avance le curseur
        since = batch[-1]['timestamp'] + 1
        # respect du rate limit
        time.sleep(ex.rateLimit / 1000)

    return total

if __name__ == "__main__":
    n = ingest_all_trades()
    print(f"✅ Kraken ingestion terminée. {n} trades insérés/à jour.")
