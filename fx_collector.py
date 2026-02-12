import os, sys
import datetime as dt
import yfinance as yf
import pymysql

def die(msg):
    print(f"::error::{msg}")
    sys.exit(1)

DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")
DB_PORT = int(os.getenv("DB_PORT") or "3306")

def connect():
    return pymysql.connect(
        host=DB_HOST, user=DB_USER, password=DB_PASS,
        database=DB_NAME, port=DB_PORT,
        autocommit=True, connect_timeout=10,
    )

def get_usd_per_eur():
    # EURUSD=X is typically USD per 1 EUR
    tk = yf.Ticker("EURUSD=X")
    try:
        hist = tk.history(period="2d", interval="1d")
        if not hist.empty:
            return float(hist["Close"].dropna().iloc[-1])
    except Exception:
        pass
    fi = getattr(tk, "fast_info", None)
    if isinstance(fi, dict):
        p = fi.get("lastPrice") or fi.get("last_price")
        if p: return float(p)
    if fi is not None:
        p = getattr(fi, "last_price", None)
        if p: return float(p)
    return None

def upsert_rate(cur, base, quote, as_of_date, rate):
    cur.execute(
        "INSERT INTO fx_rates (base_currency, quote_currency, as_of_date, rate) "
        "VALUES (%s,%s,%s,%s) "
        "ON DUPLICATE KEY UPDATE rate=VALUES(rate)",
        (base, quote, as_of_date, rate),
    )

def main():
    usd_per_eur = get_usd_per_eur()
    if usd_per_eur is None or usd_per_eur <= 0:
        die("could not fetch EURUSD=X rate from yfinance")

    eur_per_usd = 1.0 / usd_per_eur
    today = dt.date.today()

    cn = connect()
    cur = cn.cursor()

    # Convention:
    # rate means: 1 unit of base_currency = rate units of quote_currency
    # Store both directions for convenience:
    upsert_rate(cur, "USD", "EUR", today, eur_per_usd)  # 1 USD = X EUR
    upsert_rate(cur, "EUR", "USD", today, usd_per_eur)  # 1 EUR = X USD

    print(f"[ok] FX {today}: 1 USD = {eur_per_usd:.6f} EUR ; 1 EUR = {usd_per_eur:.6f} USD")

if __name__ == "__main__":
    main()
