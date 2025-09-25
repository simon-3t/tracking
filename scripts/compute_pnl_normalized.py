# scripts/compute_pnl_normalized.py
import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
import ccxt
from collections import deque, defaultdict

load_dotenv()
DB_URL = os.getenv("DB_URL", "sqlite:///pnl.db")
REPORT_CCY = os.getenv("REPORT_CCY", "USD")

# --- 1) Charger trades
eng = create_engine(DB_URL, future=True)
df = pd.read_sql_table("trades", eng).sort_values("ts")
if df.empty:
    raise SystemExit("No trades found.")

# --- 2) P&L FIFO par symbol (en quote d'origine)
lots = defaultdict(lambda: deque())     # symbol -> deque([amount_base, price_quote])
realized_quote = defaultdict(float)     # symbol -> pnl in quote

for _, r in df.iterrows():
    sym = r["symbol"]
    side = str(r["side"]).lower()
    amt  = float(r.get("amount") or 0.0)
    px   = float(r.get("price") or 0.0)
    if amt == 0: 
        continue
    if side == "buy":
        lots[sym].append([amt, px])
    elif side == "sell":
        remain = amt
        while remain > 1e-12 and lots[sym]:
            lot_amt, lot_px = lots[sym][0]
            used = min(remain, lot_amt)
            realized_quote[sym] += used * (px - lot_px)
            lot_amt -= used
            remain -= used
            if lot_amt <= 1e-12:
                lots[sym].popleft()
            else:
                lots[sym][0][0] = lot_amt

# --- 3) Construire conversions vers REPORT_CCY
# Règle simple: stables -> 1 USD; sinon on prend le prix spot via Binance
stable_map = {"USDT":"USD", "USDC":"USD", "BUSD":"USD", "TUSD":"USD", "FDUSD":"USD"}
def quote_of(symbol:str)->str:
    return symbol.split("/")[-1] if "/" in symbol else symbol

needed_quotes = sorted({quote_of(s) for s in realized_quote.keys()})
# Map quote->USD rate (approx spot)
quote_to_usd = {}
ex = ccxt.binance({'enableRateLimit': True})
for q in needed_quotes:
    if q in stable_map: 
        quote_to_usd[q] = 1.0
    elif q == "USD": 
        quote_to_usd[q] = 1.0
    else:
        # essayer paire Q/USDT ou Q/USDC
        pair = f"{q}/USDT"
        try:
            t = ex.fetch_ticker(pair)
            quote_to_usd[q] = float(t["last"]) if t and t.get("last") else None
        except Exception:
            # fallback USDC
            try:
                t = ex.fetch_ticker(f"{q}/USDC")
                quote_to_usd[q] = float(t["last"]) if t and t.get("last") else None
            except Exception:
                quote_to_usd[q] = None

# --- 4) Normaliser le P&L en USD
rows = []
for sym, pnl_q in realized_quote.items():
    q = quote_of(sym)
    rate = quote_to_usd.get(q)
    pnl_usd = pnl_q * rate if (rate is not None) else None
    rows.append({
        "symbol": sym,
        "pnl_in_quote": pnl_q,
        "quote": q,
        "quote_to_USD": rate,
        "pnl_USD_est": pnl_usd
    })

out = pd.DataFrame(rows).sort_values("pnl_USD_est", na_position="last", ascending=False)
print("📊 P&L réalisé normalisé (estimation en USD, spot courant):")
for _, r in out.iterrows():
    q = r["quote"]
    rate = r["quote_to_USD"]
    usd = r["pnl_USD_est"]
    note = "" if rate is not None else " (⚠️ pas de taux, laissé en quote)"
    print(f"{r['symbol']:>15}  P&L_quote={r['pnl_in_quote']:>12,.2f} {q:<6}  -> USD≈ {'' if usd is None else f'{usd:>12,.2f}'}{note}")

# Export CSV
out.to_csv("pnl_realized_normalized.csv", index=False)
print("\n💾 Exporté: pnl_realized_normalized.csv")
