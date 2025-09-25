#!/usr/bin/env python3
"""Télécharge les transactions Spot Binance et les exporte en CSV."""

from __future__ import annotations

import csv
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Sequence

from binance.client import Client
from binance.exceptions import BinanceAPIException

API_KEY = os.environ["BINANCE_API_KEY"]
API_SECRET = os.environ["BINANCE_API_SECRET"]

client = Client(API_KEY, API_SECRET)


@dataclass(frozen=True)
class ExportConfig:
    """Configuration pour l'export CSV."""

    symbols: Sequence[str]
    output_dir: Path
    pause_s: float = 0.2


def fetch_all_trades(symbol: str, pause_s: float = 0.2) -> list[dict]:
    """Récupère toutes les transactions pour un symbole donné."""

    trades: list[dict] = []
    last_trade_id: int | None = None

    while True:
        try:
            batch = client.get_my_trades(symbol=symbol, fromId=last_trade_id)
        except BinanceAPIException as exc:
            if exc.code == -1003:
                time.sleep(1)
                continue
            raise

        if not batch:
            break

        trades.extend(batch)
        last_trade_id = batch[-1]["id"] + 1
        time.sleep(pause_s)

    return trades


def export_to_csv(trades: Iterable[dict], output_path: Path) -> None:
    """Écrit les transactions dans un fichier CSV."""

    keys = [
        "symbol",
        "id",
        "orderId",
        "price",
        "qty",
        "commission",
        "commissionAsset",
        "time",
        "isBuyer",
        "isMaker",
    ]

    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=keys)
        writer.writeheader()
        for trade in trades:
            trade = trade.copy()
            trade["time"] = datetime.fromtimestamp(
                trade["time"] / 1000, tz=timezone.utc
            ).isoformat()
            writer.writerow({key: trade.get(key) for key in keys})


def run_export(config: ExportConfig) -> None:
    for symbol in config.symbols:
        trades = fetch_all_trades(symbol, pause_s=config.pause_s)
        output_file = config.output_dir / f"trades_{symbol}.csv"
        export_to_csv(trades, output_file)
        print(f"Export terminé pour {symbol} ({len(trades)} transactions)")


def main() -> None:
    raw_symbols = os.environ.get("BINANCE_SYMBOLS", "BTCUSDT,ETHUSDT")
    symbols = tuple(
        symbol.strip() for symbol in raw_symbols.split(",") if symbol.strip()
    )

    output_dir = Path(os.environ.get("BINANCE_OUTPUT_DIR", "exports"))

    pause_s = float(os.environ.get("BINANCE_API_PAUSE", "0.2"))

    config = ExportConfig(symbols=symbols, output_dir=output_dir, pause_s=pause_s)
    run_export(config)


if __name__ == "__main__":
    main()
