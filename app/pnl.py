"""Profit and loss computation helpers."""

from decimal import Decimal
from typing import Iterable, List, Tuple

TradeLot = Tuple[Decimal, Decimal]


def fifo_realized_pnl(trades: Iterable[TradeLot]) -> Decimal:
    """Compute realized PnL using FIFO method."""

    realized = Decimal("0")
    inventory: List[TradeLot] = []

    for quantity, price in trades:
        if quantity > 0:
            inventory.append((quantity, price))
            continue

        sell_qty = -quantity
        sell_price = price

        while sell_qty > 0 and inventory:
            buy_qty, buy_price = inventory[0]
            matched = min(buy_qty, sell_qty)
            realized += (sell_price - buy_price) * matched

            if matched == buy_qty:
                inventory.pop(0)
            else:
                inventory[0] = (buy_qty - matched, buy_price)

            sell_qty -= matched

    return realized


def unrealized_pnl(inventory: Iterable[TradeLot], market_price: Decimal) -> Decimal:
    """Compute unrealized PnL for the remaining inventory."""

    return sum((market_price - price) * quantity for quantity, price in inventory)
