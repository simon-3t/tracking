"""CLI utility to ingest Binance data."""

from app.ingest import binance


def main() -> None:
    """Entrypoint for Binance ingestion."""

    binance.fetch_trades()


if __name__ == "__main__":
    main()
