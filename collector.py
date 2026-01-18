import os, sys, datetime as dt
import yfinance as yf
import pymysql


def env_int(name, default):
    v = os.getenv(name)
    if v is None or v.strip() == "":
        return default
    try:
        return int(v.strip())
    except ValueError:
        raise ValueError(f"{name} must be an integer, got {v!r}")

DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")
DB_PORT = os.getenv("DB_PORT")
DB_PORT = env_int("DB_PORT", 27533)

AS_OF = dt.date.today()

SQL_HOLDINGS = (
    "SELECT t.id, t.symbol, h.quantity, h.avg_cost "
    "FROM holdings h JOIN tickers t ON t.id = h.ticker_id"
)

def connect():
    return pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME, port=DB_PORT, autocommit=True)


def insert_snapshot(cur, ticker_id, price, source="yfinance"):
    cur.execute("""
        INSERT INTO price_snapshots (ticker_id, as_of_date, price, price_source)
        VALUES (%s, UTC_TIMESTAMP(), %s, %s)
        ON DUPLICATE KEY UPDATE
          price = VALUES(price),
          price_source = VALUES(price_source)
    """, (ticker_id, price, source))

def main():
    cn = connect()
    cur = cn.cursor()

    cur.execute(SQL_HOLDINGS)
    rows = cur.fetchall()  # (ticker_id, symbol, qty, avg_cost)
    if not rows:
        print("No holdings found. Seed tickers and holdings first.", file=sys.stderr)
        sys.exit(1)

    ticker_ids = [r[0] for r in rows]
    symbols = [r[1] for r in rows]

    data = yf.download(symbols, period="2d", interval="1d", group_by="ticker", auto_adjust=False, threads=True)

    for idx, sym in enumerate(symbols):
        try:
            close = float(data[sym]["Close"].dropna().iloc[-1]) if len(symbols) > 1 else float(data["Close"].dropna().iloc[-1])
        except Exception:
            print(f"No close for {sym}", file=sys.stderr)
            continue
        insert_snapshot(cur, ticker_ids[idx], AS_OF, close)

    print(f"Inserted/updated {len(symbols)} snapshots for {AS_OF}")

if __name__ == "__main__":
    main()
