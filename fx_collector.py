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

def get_eurusd():
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
        if p:
            return float(p)
    if fi is not None:
        p = getattr(fi, "last_price", None)
        if p:
            return float(p)
    return None

def upsert_rate(cur, base, quote, as_of_date, rate):
    cur.execute(
        "INSERT INTO fx_rates (base_currency, quote_currency, as_of_date, rate) "
        "VALUES (%s,%s,%s,%s) "
        "ON DUPLICATE KEY UPDATE rate=VALUES(rate)",
        (base, quote, as_of_date, rate),
    )

def main():
    eurusd = get_eurusd()
    if eurusd is None or eurusd <= 0:
        die("could not fetch EURUSD=X rate from yfinance")

    today = dt.date.today()

    cn = connect()
    cur = cn.cursor()

    # OPTION A: store ONLY one canonical direction in fx_rates.
    # Convention: rate = quote per 1 base.
    # Canonical pair for now: EUR -> USD (EURUSD=X)
    upsert_rate(cur, "EUR", "USD", today, eurusd)

    print(f"[ok] FX {today}: 1 EUR = {eurusd:.6f} USD (stored only EUR->USD)")

if __name__ == "__main__":
    main()
