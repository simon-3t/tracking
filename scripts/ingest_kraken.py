"""CLI utility to ingest Kraken data."""

from app.ingest import kraken


def main() -> None:
    """Entrypoint for Kraken ingestion."""

    kraken.fetch_trades()


if __name__ == "__main__":
    main()
