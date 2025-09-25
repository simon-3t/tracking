import os
from datetime import datetime
from collections import deque, defaultdict

import pandas as pd
from sqlalchemy import create_engine
import streamlit as st
import plotly.express as px
import ccxt
from dotenv import load_dotenv, find_dotenv

dotenv_path = find_dotenv(usecwd=True)
load_dotenv(dotenv_path=dotenv_path if dotenv_path else None, override=False)

DB_URL = os.getenv("DB_URL", "sqlite:///pnl.db")
eng = create_engine(DB_URL, future=True)

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

@st.cache_data(ttl=120)
def spot_to_usd(quotes):
    ex = ccxt.binance({'enableRateLimit': True})
    stable_map = {"USDT":1.0, "USDC":1.0, "BUSD":1.0, "TUSD":1.0, "FDUSD":1.0, "USD":1.0}
    res = {}
    for q in quotes:
        if q in stable_map:
            res[q] = stable_map[q]
            continue
        rate = None
        for base in ("USDT","USDC"):
            pair = f"{q}/{base}"
            try:
                t = ex.fetch_ticker(pair)
                if t and t.get("last"):
                    rate = float(t["last"])
                    break
            except Exception:
                pass
        res[q] = rate
    return res

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
    login_placeholder = st.empty()
    with login_placeholder.container():
        st.title("🔐 Accès protégé")
        with st.form("login"):
            username = ""
            if APP_USERNAME:
                username = st.text_input("Utilisateur")
            password = st.text_input("Mot de passe", type="password")
            submit = st.form_submit_button("Se connecter")

    if submit:
        user_ok = True if not APP_USERNAME else username.strip() == APP_USERNAME
        if user_ok and password == APP_PASSWORD:
            st.session_state.authenticated = True
            login_placeholder.empty()
            rerun = getattr(st, "experimental_rerun", None) or getattr(st, "rerun", None)
            if callable(rerun):
                rerun()
        else:
            login_placeholder.error("Identifiants invalides.")

    if not st.session_state.authenticated:
        st.stop()

st.title("📈 Crypto P&L Tracker")

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
    rates = spot_to_usd(sorted(quotes))
    summary["quote_to_USD"] = summary["quote"].map(rates)
    summary["pnl_USD_est"] = summary.apply(lambda r: r["pnl_quote"] * r["quote_to_USD"] if pd.notnull(r["quote_to_USD"]) else None, axis=1)

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
