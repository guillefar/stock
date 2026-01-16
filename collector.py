import os, sys, datetime as dt
import yfinance as yf
import pymysql

DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")

AS_OF = dt.date.today()

def connect():
    return pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME, autocommit=True)

def upsert_ticker(cur, symbol, currency="USD"):
    cur.execute("""
        INSERT INTO tickers(symbol, currency) VALUES(%s,%s)
        ON DUPLICATE KEY UPDATE currency=VALUES(currency)
    """, (symbol, currency))
    cur.execute("SELECT id FROM tickers WHERE symbol=%s", (symbol,))
    return cur.fetchone()[0]

def insert_snapshot(cur, ticker_id, as_of, price, source="yfinance"):
    cur.execute("""
        INSERT INTO price_snapshots(ticker_id, as_of_date, price, price_source)
        VALUES(%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE price=VALUES(price), price_source=VALUES(price_source)
    """, (ticker_id, as_of, price, source))

def main():
    symbols = os.getenv("SYMBOLS", "AAPL,MSFT").split(",")
    quantities = os.getenv("QUANTITIES", "10,5").split(",")
    avg_costs = os.getenv("AVG_COSTS", "100,200").split(",")
    assert len(symbols)==len(quantities)==len(avg_costs), "Mismatched env lists"

    cn = connect()
    cur = cn.cursor()

    for sym in symbols:
        upsert_ticker(cur, sym)

    data = yf.download(symbols, period="2d", interval="1d", group_by="ticker", auto_adjust=False, threads=True)

    for i, sym in enumerate(symbols):
        # last available close (yfinance returns timezone-aware timestamps)
        try:
            close = float(data[sym]["Close"].dropna().iloc[-1]) if len(symbols)>1 else float(data["Close"].dropna().iloc[-1])
        except Exception:
            print(f"No close for {sym}", file=sys.stderr)
            continue
        cur.execute("SELECT id FROM tickers WHERE symbol=%s", (sym,))
        tid = cur.fetchone()[0]
        insert_snapshot(cur, tid, AS_OF, close)

    print("Done")

if __name__ == "__main__":
    main()
