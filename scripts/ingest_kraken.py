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

from app.models import Trade, Transfer, make_session

load_dotenv()
DB_URL = os.getenv("DB_URL", "sqlite:///pnl.db")
KRAKEN_KEY = os.getenv("KRAKEN_KEY")
KRAKEN_SECRET = os.getenv("KRAKEN_SECRET")


def parse_history_start(default_year: int = 2018) -> int:
    """Return the history anchor in milliseconds since epoch.

    Allows overriding via the TRANSFER_HISTORY_START env var. The value may be
    expressed either as a millisecond timestamp or an ISO date (``YYYY-MM-DD``)
    optionally including a time component.
    """

    raw = os.getenv("TRANSFER_HISTORY_START")
    if not raw:
        return int(datetime(default_year, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)

    raw = raw.strip()
    if raw.isdigit():
        return int(raw)

    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise SystemExit(
            "⚠️  TRANSFER_HISTORY_START doit être un timestamp en millisecondes ou "
            "une date ISO (YYYY-MM-DD)."
        ) from exc

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)

    return int(parsed.timestamp() * 1000)


TRANSFER_HISTORY_START = parse_history_start()

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

def upsert_transfer(tx, direction: str):
    ts = int(tx.get('timestamp') or 0)
    fee_info = tx.get('fee')

    if isinstance(fee_info, dict):
        fee_cost = fee_info.get('cost')
        fee_currency = fee_info.get('currency')
    else:
        fee_cost = fee_info or 0.0
        fee_currency = tx.get('feeCurrency')

    info = tx.get('info') or {}
    raw_identifier = (
        tx.get('id')
        or tx.get('txid')
        or tx.get('refid')
        or tx.get('referenceId')
        or info.get('id')
        or info.get('refid')
        or info.get('txid')
        or f"{ts}_{tx.get('currency') or tx.get('code')}_{tx.get('amount')}_{tx.get('address')}"
    )

    row = Transfer(
        id=f"kraken_{direction}_{raw_identifier}",
        exchange="kraken",
        direction=direction,
        asset=tx.get('currency') or tx.get('code'),
        amount=float(tx.get('amount') or 0.0),
        fee=float(fee_cost or 0.0),
        fee_currency=fee_currency,
        status=tx.get('status') or info.get('status'),
        address=tx.get('address'),
        txid=tx.get('txid') or info.get('txid'),
        ts=ts,
        iso=datetime.fromtimestamp(ts / 1000, tz=timezone.utc) if ts else None,
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
    requests_made = 0
    additional_page_expected = False
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

        requests_made += 1

        if not batch:
            break

        sorted_batch = sorted(batch, key=lambda t: t.get('timestamp') or 0)

        oldest_timestamp = sorted_batch[0].get('timestamp') if sorted_batch and isinstance(sorted_batch[0], dict) else None
        if oldest_timestamp is None:
            print("⚠️  Impossible de déterminer le plus ancien timestamp pour la pagination.")
            break

        if len(sorted_batch) == 50:
            additional_page_expected = True

        # upsert
        try:
            for t in sorted_batch:
                upsert_trade(t)
                total += 1
            session.commit()
        except SQLAlchemyError as e:
            session.rollback()
            print(f"⚠️  DB error, rollback: {e}")
            break

        # pagination: avance le curseur
        since = oldest_timestamp + 1
        print(f"ℹ️  Pagination vers l'historique plus ancien avec since={since} (timestamp initial {oldest_timestamp}).")
        # respect du rate limit
        time.sleep(ex.rateLimit / 1000)

    if additional_page_expected:
        assert requests_made > 1, "La pagination n'a pas demandé de page supplémentaire malgré un lot complet."
        print(f"ℹ️  Pagination confirmée: {requests_made} requêtes effectuées pour récupérer l'historique.")

    return total


PERMISSION_HINTS = {
    "deposit": (
        "Activez les autorisations Kraken “Funding → Consulter les dépôts” et "
        "“Ledger → Consulter les écritures” puis régénérez la clé si vous venez de "
        "modifier les droits."
    ),
    "withdraw": (
        "Activez les autorisations Kraken “Funding → Consulter les retraits” et "
        "“Ledger → Consulter les écritures” puis régénérez la clé si vous venez de "
        "modifier les droits."
    ),
}


def ingest_transfers(fetcher, direction: str) -> int:
    since = TRANSFER_HISTORY_START
    total = 0

    while True:
        try:
            batch = fetcher(since=since, limit=500)
        except ccxt.BaseError as e:
            message = str(e)
            lowered = message.lower()
            if "permission denied" in lowered:
                hint = PERMISSION_HINTS.get(
                    direction,
                    "Activez les autorisations Kraken Funding pour cette opération et "
                    "régénérez la clé si nécessaire.",
                )
                print(
                    "ℹ️  Kraken n'a pas les permissions nécessaires pour "
                    f"récupérer les {direction}s. {hint}"
                )
            else:
                print(f"⚠️  Kraken API error ({direction}): {message}")
            break

        if not batch:
            break

        try:
            for tx in batch:
                upsert_transfer(tx, direction)
                total += 1
            session.commit()
        except SQLAlchemyError as e:
            session.rollback()
            print(f"⚠️  DB error while storing Kraken {direction}s: {e}")
            break
        finally:
            time.sleep(ex.rateLimit / 1000)

        last_ts = max(int(tx.get('timestamp') or 0) for tx in batch)
        if not last_ts:
            break
        since = last_ts + 1

    return total

if __name__ == "__main__":
    trades = deposits = withdrawals = 0
    try:
        trades = ingest_all_trades()
        deposits = ingest_transfers(ex.fetch_deposits, "deposit")
        withdrawals = ingest_transfers(ex.fetch_withdrawals, "withdraw")
    finally:
        session.close()

    print(
        "✅ Kraken ingestion terminée. "
        f"{trades} trades, {deposits} dépôts et {withdrawals} retraits insérés/à jour."
    )
