# scripts/ingest_binance.py
import ccxt, os, time
from datetime import datetime, timezone
from dotenv import load_dotenv
from app.models import Trade, init_db

load_dotenv()
DB_URL = os.getenv("DB_URL", "sqlite:///pnl.db")
BINANCE_KEY = os.getenv("BINANCE_KEY")
BINANCE_SECRET = os.getenv("BINANCE_SECRET")

Session = init_db(DB_URL)
session = Session()

ex = ccxt.binance({'apiKey': BINANCE_KEY, 'secret': BINANCE_SECRET, 'enableRateLimit': True})
ex.has['fetchCurrencies'] = False
ex.options['warnOnFetchCurrencies'] = False
ex.load_markets()

def upsert(trade):
    t = Trade(
        id=f"binance_{trade['id']}",
        exchange="binance",
        symbol=trade['symbol'],
        side=trade['side'],
        amount=float(trade['amount']),
        price=float(trade['price']),
        fee=(trade.get("fee") or {}).get("cost") if trade.get("fee") else 0,
        fee_currency=(trade.get("fee") or {}).get("currency") if trade.get("fee") else None,
        ts=trade['timestamp'],
        iso=datetime.fromtimestamp(trade['timestamp']/1000, tz=timezone.utc),
    )
    session.merge(t)
    session.commit()

for sym in ex.symbols:
    try:
        trades = ex.fetch_my_trades(symbol=sym, limit=100)
        for tr in trades:
            upsert(tr)
        time.sleep(ex.rateLimit/1000)
    except Exception:
        continue

print("✅ Binance ingestion terminée")
