# scripts/compute_pnl.py
import pandas as pd
from app.models import init_db, Trade

Session = init_db("sqlite:///pnl.db")
session = Session()

df = pd.read_sql(session.query(Trade).statement, session.bind)
df = df.sort_values("ts")

results = {}
positions = {}

for _, row in df.iterrows():
    sym = row.symbol
    amt, price = row.amount, row.price
    if row.side == "buy":
        positions.setdefault(sym, []).append([amt, price])
    elif row.side == "sell":
        remain = amt
        realized = 0
        while remain > 0 and positions[sym]:
            lot = positions[sym][0]
            used = min(remain, lot[0])
            realized += used * (price - lot[1])
            lot[0] -= used
            remain -= used
            if lot[0] <= 0: positions[sym].pop(0)
        results[sym] = results.get(sym, 0) + realized

print("📊 Realized P&L:")
for sym, pnl in results.items():
    print(f"{sym}: {pnl:.2f}")
