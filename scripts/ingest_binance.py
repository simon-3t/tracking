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

from app.models import Trade, Transfer, make_session

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


def upsert_transfer(tx, direction: str):
    ts = int(tx.get('timestamp') or 0)
    fee_info = tx.get('fee') or {}
    fee_cost = fee_info.get('cost') if isinstance(fee_info, dict) else 0.0
    fee_currency = fee_info.get('currency') if isinstance(fee_info, dict) else None

    info = tx.get('info') or {}
    raw_identifier = (
        tx.get('id')
        or tx.get('txid')
        or tx.get('txId')
        or info.get('id')
        or info.get('tranId')
        or info.get('applyTime')
        or f"{ts}_{tx.get('currency') or tx.get('code')}_{tx.get('amount')}_{tx.get('address')}"
    )

    row = Transfer(
        id=f"binance_{direction}_{raw_identifier}",
        exchange="binance",
        direction=direction,
        asset=tx.get('currency') or tx.get('code'),
        amount=float(tx.get('amount') or 0.0),
        fee=float(fee_cost or 0.0),
        fee_currency=fee_currency,
        status=tx.get('status'),
        address=tx.get('address') or tx.get('toAddress') or tx.get('addressFrom'),
        txid=tx.get('txid') or tx.get('txId'),
        ts=ts,
        iso=datetime.fromtimestamp(ts / 1000, tz=timezone.utc) if ts else None,
    )
    session.merge(row)

def ingest_trades():
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
            time.sleep(ex.rateLimit / 1000)
        except ccxt.BaseError:
            session.rollback()
            continue
        except SQLAlchemyError:
            session.rollback()
            continue
    return count


def ingest_transfers(fetcher, direction: str) -> int:
    try:
        batch = fetcher(limit=500)
    except ccxt.BaseError as exc:
        print(f"⚠️  Binance API error ({direction}): {exc}")
        return 0

    if not batch:
        return 0

    total = 0
    try:
        for tx in batch:
            upsert_transfer(tx, direction)
            total += 1
        session.commit()
    except SQLAlchemyError as exc:
        session.rollback()
        print(f"⚠️  DB error while storing {direction}s: {exc}")
        return 0
    finally:
        time.sleep(ex.rateLimit / 1000)

    return total

if __name__ == "__main__":
    trades = deposits = withdrawals = 0
    try:
        trades = ingest_trades()
        deposits = ingest_transfers(ex.fetch_deposits, "deposit")
        withdrawals = ingest_transfers(ex.fetch_withdrawals, "withdraw")
    finally:
        session.close()

    print(
        "✅ Binance ingestion terminée. "
        f"{trades} trades, {deposits} dépôts et {withdrawals} retraits insérés/à jour."
    )
