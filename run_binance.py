import os, time, csv
from datetime import datetime, timezone
from dotenv import load_dotenv
import ccxt

load_dotenv()
API_KEY = os.getenv("BINANCE_KEY")
API_SECRET = os.getenv("BINANCE_SECRET")

if not API_KEY or not API_SECRET:
    raise SystemExit("⚠️  BINANCE_KEY / BINANCE_SECRET manquants dans .env")

ex = ccxt.binance({'apiKey': API_KEY, 'secret': API_SECRET, 'enableRateLimit': True})
ex.load_markets()

out_file = f"binance_trades_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.csv"

fields = ["id","datetime","symbol","side","amount","price","cost","fee_cost","fee_currency","order","takerOrMaker"]
count = 0

with open(out_file, "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=fields)
    w.writeheader()

    # On parcourt les marchés; les symboles sans trade lèveront souvent une erreur -> on ignore.
    for sym in ex.symbols:
        try:
            # Récupère un lot de trades récents (ajuste limit si besoin)
            trades = ex.fetch_my_trades(symbol=sym, since=None, limit=100)
            if not trades:
                continue
            for t in trades:
                row = {
                    "id": t.get("id") or "",
                    "datetime": t.get("datetime") or "",
                    "symbol": t.get("symbol") or sym,
                    "side": t.get("side") or "",
                    "amount": t.get("amount") or 0,
                    "price": t.get("price") or 0,
                    "cost": t.get("cost") or 0,
                    "fee_cost": (t.get("fee") or {}).get("cost") if t.get("fee") else 0,
                    "fee_currency": (t.get("fee") or {}).get("currency") if t.get("fee") else "",
                    "order": t.get("order") or "",
                    "takerOrMaker": t.get("takerOrMaker") or "",
                }
                w.writerow(row)
                count += 1
            # Respecte le rate limit de ccxt
            time.sleep(ex.rateLimit / 1000)
        except Exception:
            # Beaucoup de symboles n'auront aucun trade → on continue
            continue

print(f"✅ Terminé. {count} trades écrits dans {out_file}")
