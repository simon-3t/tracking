"""CLI utility to ingest on-chain Ethereum data."""

from pathlib import Path
import sys


# Ensure the repository root (which contains the ``app`` package) is on PYTHONPATH
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.ingest import onchain_eth


def main() -> None:
    """Entrypoint for on-chain ingestion."""

    onchain_eth.fetch_transfers()


if __name__ == "__main__":
    main()
