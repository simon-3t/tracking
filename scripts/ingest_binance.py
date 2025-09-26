import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

import ccxt
from sqlalchemy.exc import SQLAlchemyError


# Ensure the repository root (which contains the ``app`` package) is on PYTHONPATH
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.models import Trade, make_session

load_dotenv()
DB_URL = os.getenv("DB_URL", "sqlite:///pnl.db")
BINANCE_KEY = os.getenv("BINANCE_KEY")
BINANCE_SECRET = os.getenv("BINANCE_SECRET")

if not BINANCE_KEY or not BINANCE_SECRET:
    raise SystemExit("⚠️  BINANCE_KEY / BINANCE_SECRET manquants (.env)")

Session = make_session(DB_URL)
session = Session()

# Exchange
ex = ccxt.binance({
    'apiKey': BINANCE_KEY,
    'secret': BINANCE_SECRET,
    'enableRateLimit': True,
})
# Évite l'appel SAPI currencies (peut être bloqué dans certaines régions)
ex.has['fetchCurrencies'] = False
ex.options['warnOnFetchCurrencies'] = False
ex.load_markets()

def upsert_trade(t):
    row = Trade(
        id=f"binance_{t.get('id') or t.get('order') or t['timestamp']}",
        exchange="binance",
        symbol=t.get('symbol') or '',
        side=t.get('side') or '',
        amount=float(t.get('amount') or 0),
        price=float(t.get('price') or 0),
        fee=(t.get('fee') or {}).get('cost') if t.get('fee') else 0.0,
        fee_currency=(t.get('fee') or {}).get('currency') if t.get('fee') else None,
        ts=int(t.get('timestamp') or 0),
        iso=datetime.fromtimestamp((t.get('timestamp') or 0)/1000, tz=timezone.utc),
    )
    session.merge(row)

def ingest():
    count = 0
    # Parcourt tous les symbols connus ; seuls ceux où tu as tradé renverront des lignes
    for sym in ex.symbols:
        try:
            # pour un vrai incrémental: stocker un "since" par symbol (table cursors)
            batch = ex.fetch_my_trades(symbol=sym, since=None, limit=100)
            if not batch:
                continue
            for t in batch:
                upsert_trade(t)
                count += 1
            session.commit()
            time.sleep(ex.rateLimit/1000)
        except ccxt.BaseError:
            session.rollback()
            continue
        except SQLAlchemyError:
            session.rollback()
            continue
    return count

if __name__ == "__main__":
    total = ingest()
    print(f"✅ Binance ingestion terminée. {total} trades insérés/à jour.")
