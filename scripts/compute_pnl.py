import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from app.models import Trade

load_dotenv()
DB_URL = os.getenv("DB_URL", "sqlite:///pnl.db")

eng = create_engine(DB_URL, future=True)
df = pd.read_sql_table(Trade.__tablename__, eng).sort_values("ts")

# FIFO par symbol ; calcule P&L réalisé en "quote" (ex: USDT)
from collections import deque, defaultdict
lots = defaultdict(lambda: deque())   # symbol -> deque([amount_base, price_quote])
realized = defaultdict(float)

for _, r in df.iterrows():
    sym = r["symbol"]
    side = str(r["side"]).lower()
    amt  = float(r["amount"] or 0.0)
    px   = float(r["price"] or 0.0)

    if amt == 0:
        continue

    if side == "buy":
        # on empile un lot (amount à prix px)
        lots[sym].append([amt, px])
    elif side == "sell":
        remain = amt
        while remain > 1e-12 and lots[sym]:
            lot_amt, lot_px = lots[sym][0]
            used = min(remain, lot_amt)
            realized[sym] += used * (px - lot_px)
            lot_amt -= used
            remain -= used
            if lot_amt <= 1e-12:
                lots[sym].popleft()
            else:
                lots[sym][0][0] = lot_amt

print("📊 P&L réalisé (quote currency par symbol) :")
for sym, pnl in sorted(realized.items()):
    print(f"{sym:>15}  {pnl:,.2f}")
