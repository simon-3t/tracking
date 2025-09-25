"""CLI utility to ingest on-chain Ethereum data."""

from app.ingest import onchain_eth


def main() -> None:
    """Entrypoint for on-chain ingestion."""

    onchain_eth.fetch_transfers()


if __name__ == "__main__":
    main()
