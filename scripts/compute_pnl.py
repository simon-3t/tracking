"""CLI utility to compute PnL."""

from decimal import Decimal

from app import pnl


def main() -> None:
    """Entrypoint for PnL computation."""

    sample_trades = [
        (Decimal("1"), Decimal("100")),
        (Decimal("-1"), Decimal("110")),
    ]
    result = pnl.fifo_realized_pnl(sample_trades)
    print(f"Realized PnL: {result}")


if __name__ == "__main__":
    main()
