import os, sys, datetime as dt
import yfinance as yf
import pymysql

def env_int(name, default):
    v = os.getenv(name)
    return default if not v else int(v)

DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")
DB_PORT = env_int("DB_PORT", 3306)

def connect():
    cn = pymysql.connect(
        host=DB_HOST, user=DB_USER, password=DB_PASS,
        database=DB_NAME, port=DB_PORT, autocommit=False,  # <- off; we control commit
        connect_timeout=10,
    )
    return cn

INSERT_SQL = """
INSERT INTO price_snapshots (ticker_id, as_of_date, price, price_source)
VALUES (%s, UTC_TIMESTAMP(), %s, %s)
"""

def insert_snapshot(cur, ticker_id, price, source="yfinance"):
    cur.execute(INSERT_SQL, (ticker_id, float(price), source))
    # rowcount should be 1 if a row was inserted
    return cur.rowcount

def main():
    cn = connect()
    cur = cn.cursor()
    # sanity: print where we're connected
    cur.execute("SELECT DATABASE(), @@hostname, @@version")
    db, host, ver = cur.fetchone()
    print(f"[info] Connected to db={db}, host={host}, version={ver}")

    # read holdings from DB
    cur.execute("""
      SELECT t.id, t.symbol
      FROM holdings h JOIN tickers t ON t.id = h.ticker_id
    """)
    rows = cur.fetchall()
    if not rows:
        print("[warn] No holdings found; nothing to do")
        return

    symbols = [r[1] for r in rows]
    id_by_symbol = {r[1]: r[0] for r in rows}

    data = yf.download(symbols, period="2d", interval="1d", group_by="ticker", auto_adjust=False, threads=True)
    inserted = 0
    errors = 0

    for sym in symbols:
        try:
            close = float(data[sym]["Close"].dropna().iloc[-1]) if len(symbols) > 1 else float(data["Close"].dropna().iloc[-1])
            inserted += insert_snapshot(cur, id_by_symbol[sym], close, "yfinance")
        except Exception as e:
            errors += 1
            print(f"[error] {sym}: {e}", file=sys.stderr)

    cn.commit()  # <- commit writes
    print(f"[summary] inserted={inserted}, errors={errors}, symbols={len(symbols)}")
    # show today’s row count (UTC date)
    cur.execute("SELECT COUNT(*) FROM price_snapshots WHERE DATE(as_of_date) = UTC_DATE()")
    today_cnt = cur.fetchone()[0]
    print(f"[summary] rows today (UTC): {today_cnt}")

if __name__ == "__main__":
    main()
