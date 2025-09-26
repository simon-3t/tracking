import os
import sys
import subprocess
from datetime import datetime, timezone, time as dtime, timedelta
from collections import deque, defaultdict
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
import streamlit as st
import plotly.express as px
import ccxt
from dotenv import load_dotenv, find_dotenv

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from app.models import Base, AssetPrice

dotenv_path = find_dotenv(usecwd=True)
load_dotenv(dotenv_path=dotenv_path if dotenv_path else None, override=False)

DB_URL = os.getenv("DB_URL", "sqlite:///pnl.db")
eng = create_engine(DB_URL, future=True)
Base.metadata.create_all(eng)
SessionLocal = sessionmaker(bind=eng, autoflush=False, autocommit=False)

BINANCE_PUBLIC = ccxt.binance({'enableRateLimit': True})
BINANCE_PUBLIC.load_markets()
STABLE_USD_MAP = {"USDT": 1.0, "USDC": 1.0, "BUSD": 1.0, "TUSD": 1.0, "FDUSD": 1.0, "USD": 1.0}

@st.cache_data(ttl=120)
def load_trades():
    try:
        df = pd.read_sql_table("trades", eng).sort_values("ts")
    except Exception:
        df = pd.DataFrame()
    if not df.empty:
        df["datetime"] = pd.to_datetime(df["ts"], unit="ms", utc=True).dt.tz_convert("UTC")
    return df

def fifo_realized(df):
    """P&L réalisé par symbol, en devise de cotation d'origine (quote)."""
    lots = defaultdict(lambda: deque())
    realized_quote = defaultdict(float)
    for _, r in df.iterrows():
        sym = r["symbol"]
        side = str(r.get("side") or "").lower()
        amt  = float(r.get("amount") or 0.0)
        px   = float(r.get("price")  or 0.0)
        if amt == 0: 
            continue
        if side == "buy":
            lots[sym].append([amt, px])
        elif side == "sell":
            remain = amt
            while remain > 1e-12 and lots[sym]:
                lot_amt, lot_px = lots[sym][0]
                used = min(remain, lot_amt)
                realized_quote[sym] += used * (px - lot_px)
                lot_amt -= used
                remain -= used
                if lot_amt <= 1e-12:
                    lots[sym].popleft()
                else:
                    lots[sym][0][0] = lot_amt
    return realized_quote

def quote_of(symbol: str) -> str:
    return symbol.split("/")[-1] if "/" in symbol else symbol


def base_of(symbol: str) -> str:
    return symbol.split("/")[0] if "/" in symbol else symbol


@st.cache_data(ttl=120)
def spot_to_usd(quotes):
    res = {}
    for q in quotes:
        if q in STABLE_USD_MAP:
            res[q] = STABLE_USD_MAP[q]
            continue
        rate = None
        for base in ("USDT", "USDC", "BUSD", "TUSD", "FDUSD", "USD"):
            pair = f"{q}/{base}"
            if pair not in BINANCE_PUBLIC.symbols:
                continue
            try:
                t = BINANCE_PUBLIC.fetch_ticker(pair)
                if t and t.get("last"):
                    rate = float(t["last"]) * STABLE_USD_MAP.get(base, 1.0)
                    break
            except Exception:
                pass
        res[q] = rate
    return res


def _date_range(start_day, end_day):
    days = []
    cur = start_day
    while cur <= end_day:
        days.append(cur)
        cur += timedelta(days=1)
    return days


def _fetch_asset_prices(asset: str, start_day, end_day):
    days = _date_range(start_day, end_day)
    if asset in STABLE_USD_MAP:
        return [
            {
                "asset": asset,
                "day": day,
                "price_usd": STABLE_USD_MAP[asset],
                "symbol": f"{asset}/USD",
                "source": "static",
            }
            for day in days
        ]

    for quote in ("USDT", "BUSD", "USDC", "TUSD", "FDUSD", "USD"):
        pair = f"{asset}/{quote}"
        if pair not in BINANCE_PUBLIC.symbols:
            continue
        since_dt = datetime.combine(start_day, dtime.min).replace(tzinfo=timezone.utc)
        since = int(since_dt.timestamp() * 1000)
        limit = min(2000, len(days) + 5)
        try:
            ohlcv = BINANCE_PUBLIC.fetch_ohlcv(pair, timeframe="1d", since=since, limit=limit)
        except Exception:
            continue
        if not ohlcv:
            continue

        rows = []
        for ts, _open, _high, _low, close, _vol in ohlcv:
            if close is None:
                continue
            day = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).date()
            if start_day <= day <= end_day:
                rows.append({
                    "asset": asset,
                    "day": day,
                    "price_usd": float(close) * STABLE_USD_MAP.get(quote, 1.0),
                    "symbol": pair,
                    "source": "binance",
                })

        if not rows:
            continue

        df = pd.DataFrame(rows).sort_values("day")
        df["day"] = pd.to_datetime(df["day"])
        df.set_index("day", inplace=True)
        full_index = pd.date_range(start=start_day, end=end_day, freq="D")
        df = df.reindex(full_index)
        df["asset"] = asset
        df["symbol"] = pair
        df["source"] = "binance"
        df["price_usd"] = df["price_usd"].ffill()
        df.dropna(subset=["price_usd"], inplace=True)
        df.reset_index(inplace=True)
        df.rename(columns={"index": "day"}, inplace=True)
        df["day"] = df["day"].dt.date
        return df.to_dict("records")

    return []


def ensure_price_history(assets, start_day, end_day):
    assets = sorted(set(a for a in assets if a))
    if not assets or start_day > end_day:
        return set()

    session = SessionLocal()
    missing_assets = {}
    failed = set()
    try:
        stmt = (
            select(AssetPrice.asset, AssetPrice.day)
            .where(AssetPrice.asset.in_(assets))
            .where(AssetPrice.day >= start_day)
            .where(AssetPrice.day <= end_day)
        )
        existing = session.execute(stmt).all()
        existing_map = defaultdict(set)
        for asset, day in existing:
            existing_map[asset].add(day)

        all_days = set(_date_range(start_day, end_day))
        for asset in assets:
            missing = all_days - existing_map.get(asset, set())
            if missing:
                missing_assets[asset] = missing

        if not missing_assets:
            return set()

        for asset in missing_assets:
            rows = _fetch_asset_prices(asset, start_day, end_day)
            if not rows:
                failed.add(asset)
                continue
            for row in rows:
                session.merge(
                    AssetPrice(
                        asset=row["asset"],
                        day=row["day"],
                        price_usd=row["price_usd"],
                        symbol=row.get("symbol"),
                        source=row.get("source"),
                    )
                )
        session.commit()
    finally:
        session.close()

    return failed


@st.cache_data(ttl=900)
def load_price_history(assets, start_day, end_day):
    assets = sorted(set(a for a in assets if a))
    if not assets or start_day > end_day:
        return pd.DataFrame(columns=["asset", "day", "price_usd"]), []

    failed = ensure_price_history(assets, start_day, end_day)

    with eng.connect() as conn:
        stmt = (
            select(AssetPrice.asset, AssetPrice.day, AssetPrice.price_usd)
            .where(AssetPrice.asset.in_(assets))
            .where(AssetPrice.day >= start_day)
            .where(AssetPrice.day <= end_day)
        )
        df = pd.read_sql(stmt, conn)

    if df.empty:
        return df, sorted(failed)

    df["day"] = pd.to_datetime(df["day"]).dt.date
    full_days = _date_range(start_day, end_day)
    filled = []
    for asset, grp in df.groupby("asset"):
        g = grp.sort_values("day").set_index("day")
        g = g.reindex(full_days)
        g["price_usd"] = g["price_usd"].ffill()
        g.dropna(subset=["price_usd"], inplace=True)
        g["asset"] = asset
        filled.append(g.reset_index().rename(columns={"index": "day"}))

    if filled:
        df = pd.concat(filled, ignore_index=True)
    else:
        df = pd.DataFrame(columns=["asset", "day", "price_usd"])

    df["day"] = pd.to_datetime(df["day"]).dt.date
    return df, sorted(failed)

# --- UI ---
st.set_page_config(page_title="Crypto P&L Tracker", layout="wide")

APP_USERNAME = os.getenv("APP_USERNAME")
APP_PASSWORD = os.getenv("APP_PASSWORD")

if not APP_PASSWORD:
    st.error(
        "Aucun mot de passe n'est configuré pour l'application. "
        "Définis la variable d'environnement `APP_PASSWORD` avant de lancer Streamlit."
    )
    st.stop()

if "authenticated" not in st.session_state:
    st.session_state.authenticated = False

if not st.session_state.authenticated:
    st.title("🔐 Accès protégé")
    with st.form("login"):
        if APP_USERNAME:
            username = st.text_input("Utilisateur")
        password = st.text_input("Mot de passe", type="password")
        submit = st.form_submit_button("Se connecter")

    if submit:
        user_ok = True if not APP_USERNAME else username.strip() == APP_USERNAME
        if user_ok and password == APP_PASSWORD:
            st.session_state.authenticated = True
            if hasattr(st, "rerun"):
                st.rerun()
            else:
                st.experimental_rerun()
        else:
            st.error("Identifiants invalides.")

    st.stop()

st.title("📈 Crypto P&L Tracker")

def run_ingestion(script_path: str, exchange_label: str):
    """Launch an ingestion script and return a feedback dict."""
    with st.spinner(f"Mise à jour {exchange_label} en cours…"):
        try:
            result = subprocess.run(
                ["python", script_path],
                capture_output=True,
                text=True,
                check=True,
            )
        except FileNotFoundError:
            return {
                "exchange": exchange_label,
                "status": "error",
                "message": f"Script introuvable: {script_path}",
                "details": None,
            }
        except subprocess.CalledProcessError as exc:
            details = "\n".join(filter(None, [exc.stdout or "", exc.stderr or ""]))
            return {
                "exchange": exchange_label,
                "status": "error",
                "message": f"Erreur lors de la mise à jour {exchange_label}.",
                "details": details.strip() or None,
            }
        else:
            load_trades.clear()
            load_price_history.clear()
            stdout = (result.stdout or "").strip()
            stderr = (result.stderr or "").strip()
            details = "\n".join(filter(None, [stdout, stderr])) or None
            return {
                "exchange": exchange_label,
                "status": "success",
                "message": stdout or f"{exchange_label} mis à jour.",
                "details": details,
            }

if "last_update" not in st.session_state:
    st.session_state["last_update"] = None

st.subheader("🔄 Mise à jour des données")
update_feedback = None
controls = st.columns(2)
with controls[0]:
    if st.button("Mettre à jour Binance", use_container_width=True):
        update_feedback = run_ingestion("scripts/ingest_binance.py", "Binance")
with controls[1]:
    if st.button("Mettre à jour Kraken", use_container_width=True):
        update_feedback = run_ingestion("scripts/ingest_kraken.py", "Kraken")

if update_feedback:
    st.session_state["last_update"] = update_feedback

feedback = st.session_state.get("last_update")
if feedback:
    message = feedback.get("message")
    details = feedback.get("details")
    if feedback.get("status") == "success":
        st.success(message or f"{feedback['exchange']} mis à jour avec succès.")
    else:
        st.error(message or f"{feedback['exchange']} n'a pas pu être mis à jour.")
    if details:
        with st.expander("Afficher les détails"):
            st.code(details)

df = load_trades()
if df.empty:
    st.warning("Aucune donnée trouvée dans la table `trades`. Lance d'abord l'ingestion.")
    st.stop()

# Filtres
cols = st.columns(4)
with cols[0]:
    ex_filter = st.multiselect("Exchange", sorted(df["exchange"].dropna().unique().tolist()))
with cols[1]:
    sym_filter = st.multiselect("Symboles", sorted(df["symbol"].dropna().unique().tolist()))
with cols[2]:
    start = st.date_input("Date début", value=df["datetime"].min().date())
with cols[3]:
    end = st.date_input("Date fin", value=df["datetime"].max().date())

mask = (df["datetime"].dt.date >= start) & (df["datetime"].dt.date <= end)
if ex_filter:
    mask &= df["exchange"].isin(ex_filter)
if sym_filter:
    mask &= df["symbol"].isin(sym_filter)
dff = df.loc[mask].copy()

if not dff.empty:
    dff.sort_values("datetime", inplace=True)
    dff["quote"] = dff["symbol"].map(quote_of)
    fee_currencies = dff["fee_currency"].dropna().unique().tolist()
    quotes_for_rates = sorted(set(dff["quote"].dropna().unique().tolist()) | set(fee_currencies))
    try:
        quote_rates = spot_to_usd(quotes_for_rates) if quotes_for_rates else {}
    except Exception:
        quote_rates = {q: None for q in quotes_for_rates}
else:
    quote_rates = {}

# Résumé
st.subheader("Résumé")
real_q = fifo_realized(dff)
rows = []
quotes = set()
for sym, val in real_q.items():
    q = quote_of(sym)
    quotes.add(q)
    rows.append({"symbol": sym, "pnl_quote": val, "quote": q})
summary = pd.DataFrame(rows).sort_values("pnl_quote", ascending=False)

if not summary.empty:
    rates = {q: quote_rates.get(q) for q in quotes}
    summary["quote_to_USD"] = summary["quote"].map(rates)
    summary["pnl_USD_est"] = summary.apply(
        lambda r: r["pnl_quote"] * r["quote_to_USD"] if pd.notnull(r["quote_to_USD"]) else None,
        axis=1,
    )

    c1, c2, c3 = st.columns([2,2,1])
    with c1:
        st.metric("Total P&L (USD estimé)", f"{summary['pnl_USD_est'].dropna().sum():,.2f}")
    with c2:
        top = summary.nlargest(10, "pnl_USD_est") if "pnl_USD_est" in summary else summary.nlargest(10, "pnl_quote")
        fig = px.bar(top, x="symbol", y="pnl_USD_est", title="Top P&L (USD estimé)")
        st.plotly_chart(fig, use_container_width=True)
    with c3:
        st.dataframe(summary, use_container_width=True, height=400)
else:
    st.info("Pas encore de P&L réalisé dans la période/filtres.")

st.subheader("Valeur du portefeuille (USD)")
if dff.empty:
    st.info("Aucune donnée pour calculer la valeur du portefeuille.")
else:
    sides = dff["side"].fillna("").str.lower()
    dff["amount_signed"] = dff["amount"]
    sell_mask = sides == "sell"
    dff.loc[sell_mask, "amount_signed"] = -dff.loc[sell_mask, "amount"].abs()
    invalid_mask = ~sides.isin(["buy", "sell"])
    dff.loc[invalid_mask, "amount_signed"] = 0.0
    dff["net_base"] = dff.groupby("symbol")["amount_signed"].cumsum()

    quote_cash_events = []
    for row in dff.itertuples():
        side = (row.side or "").lower()
        amount = float(row.amount or 0.0)
        price = float(row.price or 0.0)
        dt = row.datetime
        quote = getattr(row, "quote", None)
        if side == "buy" and quote:
            quote_cash_events.append({"asset": quote, "datetime": dt, "delta": -amount * price})
        elif side == "sell" and quote:
            quote_cash_events.append({"asset": quote, "datetime": dt, "delta": amount * price})

        fee_currency = getattr(row, "fee_currency", None)
        fee_amount = float(getattr(row, "fee", 0.0) or 0.0)
        if fee_currency and fee_amount:
            quote_cash_events.append({"asset": fee_currency, "datetime": dt, "delta": -fee_amount})

    symbols_in_scope = sorted(dff["symbol"].dropna().unique().tolist())
    start_dt = pd.Timestamp(start).tz_localize("UTC")
    end_dt = pd.Timestamp(end).tz_localize("UTC")
    all_days_utc = pd.date_range(start=start_dt.normalize(), end=end_dt.normalize(), freq="D", tz="UTC")
    all_days_dates = [day.date() for day in all_days_utc]

    daily_positions = []
    for sym, grp in dff.groupby("symbol"):
        series = grp.set_index("datetime")["net_base"].resample("D").last()
        series = series.reindex(all_days_utc).ffill().fillna(0.0)
        daily_positions.append(
            pd.DataFrame(
                {
                    "symbol": sym,
                    "day": series.index,
                    "net_base": series.values,
                }
            )
        )

    cash_positions = []
    cash_assets_needed = set()
    if quote_cash_events:
        cash_df = pd.DataFrame(quote_cash_events)
        cash_df.sort_values("datetime", inplace=True)
        cash_df["net_amount"] = cash_df.groupby("asset")["delta"].cumsum()
        for asset, grp in cash_df.groupby("asset"):
            cash_assets_needed.add(asset)
            series = grp.set_index("datetime")["net_amount"].resample("D").last()
            series = series.reindex(all_days_utc).ffill().fillna(0.0)
            cash_positions.append(
                pd.DataFrame(
                    {
                        "asset": asset,
                        "day": series.index,
                        "net_amount": series.values,
                    }
                )
            )

    if not daily_positions and not cash_positions:
        st.info("Impossible de calculer la valeur nette (positions indisponibles).")
    else:
        base_assets = {base_of(sym) for sym in symbols_in_scope if sym}
        assets_for_prices = sorted(base_assets | cash_assets_needed)
        price_df, failed_assets = load_price_history(assets_for_prices, start, end)

        unresolved_assets = set(failed_assets)
        if not price_df.empty:
            unresolved_assets |= set(a for a in assets_for_prices if a not in set(price_df["asset"].unique()))
        elif assets_for_prices:
            unresolved_assets |= set(assets_for_prices)

        if unresolved_assets:
            st.warning(
                "Prix USD indisponibles pour : " + ", ".join(sorted(unresolved_assets))
            )

        base_value = pd.DataFrame(columns=["day", "base_value_usd"])
        if daily_positions:
            if price_df.empty:
                st.warning("Impossible de valoriser les positions en base (prix manquants).")
            else:
                positions_df = pd.concat(daily_positions, ignore_index=True)
                positions_df["day"] = pd.to_datetime(positions_df["day"]).dt.date
                positions_df["asset"] = positions_df["symbol"].map(base_of)

                valuations = positions_df.merge(price_df, on=["asset", "day"], how="left")
                valuations = valuations.dropna(subset=["price_usd"])
                valuations["value_usd"] = valuations["net_base"] * valuations["price_usd"]

                if not valuations.empty:
                    base_value = (
                        valuations.groupby("day", as_index=False)["value_usd"].sum()
                    )
                    base_value.rename(columns={"value_usd": "base_value_usd"}, inplace=True)

        cash_value = pd.DataFrame(columns=["day", "cash_value_usd"])
        if cash_positions:
            if price_df.empty:
                st.warning("Impossible de valoriser les soldes en quote/frais (prix manquants).")
            else:
                cash_df_daily = pd.concat(cash_positions, ignore_index=True)
                cash_df_daily["day"] = pd.to_datetime(cash_df_daily["day"]).dt.date
                cash_df_daily = cash_df_daily.merge(price_df, on=["asset", "day"], how="left")
                cash_df_daily = cash_df_daily.dropna(subset=["price_usd"])
                cash_df_daily["value_usd"] = cash_df_daily["net_amount"] * cash_df_daily["price_usd"]

                if not cash_df_daily.empty:
                    cash_value = (
                        cash_df_daily.groupby("day", as_index=False)["value_usd"].sum()
                    )
                    cash_value.rename(columns={"value_usd": "cash_value_usd"}, inplace=True)

        if base_value.empty and cash_value.empty:
            st.info("Impossible de calculer la valeur nette (prix USD manquants ?).")
        else:
            total = pd.DataFrame({"day": all_days_dates})
            if not base_value.empty:
                total = total.merge(base_value, on="day", how="left")
            else:
                total["base_value_usd"] = 0.0

            if not cash_value.empty:
                total = total.merge(cash_value, on="day", how="left")
            else:
                total["cash_value_usd"] = 0.0

            total["base_value_usd"] = total["base_value_usd"].fillna(0.0)
            total["cash_value_usd"] = total["cash_value_usd"].fillna(0.0)
            total["value_usd"] = total["base_value_usd"] + total["cash_value_usd"]

            plot_df = total.copy()
            plot_df["day"] = pd.to_datetime(plot_df["day"])

            fig_value = px.line(
                plot_df,
                x="day",
                y="value_usd",
                markers=True,
                title="Valeur nette du portefeuille (USD)",
            )
            st.plotly_chart(fig_value, use_container_width=True)

st.subheader("Trades")
st.dataframe(
    dff[["datetime","exchange","symbol","side","amount","price","fee","fee_currency"]]
      .sort_values("datetime", ascending=False),
    use_container_width=True, height=420
)

st.subheader("Activité")
# Nb de trades / jour
dff["day"] = dff["datetime"].dt.date
by_day = dff.groupby(["day"]).size().reset_index(name="trades")
fig2 = px.line(by_day, x="day", y="trades", title="Nombre de trades par jour")
st.plotly_chart(fig2, use_container_width=True)

# Notional échangé par jour (approx amount*price, somme absolue)
dff["notional"] = (dff["amount"].abs() * dff["price"].abs())
notional = dff.groupby(["day"]).agg({"notional":"sum"}).reset_index()
fig3 = px.bar(notional, x="day", y="notional", title="Notional échangé par jour (approx)")
st.plotly_chart(fig3, use_container_width=True)
