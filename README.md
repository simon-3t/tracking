# Tracking

Base project structure for crypto tracking application.

## Structure

```
tracking/
├─ app/
│  ├─ __init__.py
│  ├─ config.py
│  ├─ db.py
│  ├─ models.py
│  ├─ ingest/
│  │   ├─ binance.py
│  │   ├─ kraken.py
│  │   └─ onchain_eth.py
│  ├─ pnl.py
│  └─ utils.py
├─ scripts/
│  ├─ ingest_binance.py
│  ├─ ingest_kraken.py
│  ├─ ingest_onchain.py
│  └─ compute_pnl.py
├─ requirements.txt
├─ .env.example
└─ README.md
```

## Setup

1. Copy `.env.example` to `.env` and update values.
2. Install dependencies with `pip install -r requirements.txt`.
3. Run the scripts from the `scripts/` directory as needed.
