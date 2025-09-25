# Connexion à l'API de Binance et récupération des transactions

## 1. Préparer l'environnement Ubuntu 24.04 LTS
1. Mettre à jour le système :
   ```bash
   sudo apt update && sudo apt upgrade -y
   ```
2. Installer Python 3 et les outils nécessaires (déjà présents par défaut sur Ubuntu 24.04, mais à installer si besoin) :
   ```bash
   sudo apt install -y python3 python3-venv python3-pip git
   ```
3. Créer un utilisateur non-root (si vous travaillez en root) et lui donner les droits sudo pour plus de sécurité.

## 2. Créer une clé API Binance
1. Connectez-vous sur [https://www.binance.com](https://www.binance.com) et ouvrez le **Binance Dashboard**.
2. Dans le menu **API Management**, créez une nouvelle clé API.
3. Donnez un nom à la clé (ex. « VPS-Ubuntu ») puis sauvegardez la **clé API** et la **clé secrète** dans un endroit sûr. Vous ne pourrez plus relire la clé secrète plus tard.
4. Restreignez les permissions de la clé à ce dont vous avez besoin (Spot/Margin/Futures, lecture seule si vous ne tradez pas depuis le VPS).
5. **Autorisez l’adresse IP publique de votre VPS** dans les réglages de la clé pour que Binance accepte vos requêtes.

## 3. Stocker les identifiants de manière sécurisée
- Ne placez jamais les clés API en clair dans votre code.
- Déposez-les dans des variables d’environnement ou dans un gestionnaire de secrets (`.env` non versionné, `pass`, `aws secrets manager`, etc.).
- Un fichier d’exemple `.binance.env.example` est fourni dans ce dépôt. Copiez-le vers `~/.binance.env` et remplacez les valeurs :
  ```bash
  cp .binance.env.example ~/.binance.env
  chmod 600 ~/.binance.env
  ```
- Chargez ensuite les variables avant de lancer votre script :
  ```bash
  set -a
  source ~/.binance.env
  set +a
  ```
- Si vous versionnez le projet, ajoutez `~/.binance.env` à votre `.gitignore` pour éviter toute fuite de secrets.

## 4. Installer la bibliothèque Python officielle
```bash
source ~/venvs/binance/bin/activate
pip install --upgrade python-binance pandas
```

## 5. Exemple de script pour récupérer toutes les transactions Spot
Un script `download_trades.py` est inclus dans ce dépôt. Il lit les variables d’environnement définies précédemment et enregistre les transactions dans des CSV.
```python
#!/usr/bin/env python3
from __future__ import annotations

import csv
import os
import time
from datetime import datetime, timezone

from dataclasses import dataclass
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
    symbols = tuple(symbol.strip() for symbol in raw_symbols.split(",") if symbol.strip())

    output_dir = Path(os.environ.get("BINANCE_OUTPUT_DIR", "exports"))

    pause_s = float(os.environ.get("BINANCE_API_PAUSE", "0.2"))

    config = ExportConfig(symbols=symbols, output_dir=output_dir, pause_s=pause_s)
    run_export(config)


if __name__ == "__main__":
    main()
```

### Lancer le script
```bash
source ~/venvs/binance/bin/activate
set -a && source ~/.binance.env && set +a
python download_trades.py
```

## 6. Récupérer l’historique complet
- Pour de gros volumes, Binance limite la taille des réponses. Utilisez la pagination `fromId` (comme ci-dessus) ou `startTime`/`endTime` en ms.
- Si vous avez besoin d’archives anciennes, consultez **Binance Data Export** (requête manuelle via l’interface ou l’endpoint `GET /sapi/v1/accountSnapshot`).

## 7. Gérer les limites de taux (rate limits)
- Respectez les pauses entre les appels (Binance applique des pénalités si vous dépassez les limites).
- Gérez les erreurs `-1003 TOO MANY REQUESTS` avec des `time.sleep()` exponentiels.

## 8. Sécuriser et automatiser
- Activez `ufw` ou une autre solution pour limiter l’accès SSH au VPS.
- Exécutez vos scripts avec un utilisateur dédié non-root.
- Automatisez les exports avec `cron` ou `systemd` :
  ```bash
  crontab -e
  # Exécuter tous les jours à 2h00
  0 2 * * * . /home/votre_user/venvs/binance/bin/activate && . /home/votre_user/.binance.env && python /home/votre_user/download_trades.py >> /var/log/binance_trades.log 2>&1
  ```
- Surveillez les journaux et mettez à jour régulièrement la bibliothèque `python-binance`.

## 9. Aller plus loin
- Pour les transactions futures ou margin, utilisez les endpoints dédiés (`futures_coin`, `futures_usdt`, `margin`).
- Consultez la documentation officielle : [https://binance-docs.github.io/apidocs/spot/en/](https://binance-docs.github.io/apidocs/spot/en/)
- Envisagez de stocker les données dans une base PostgreSQL ou SQLite pour analyses ultérieures.

