import os, sys, time, random
import datetime as dt
import yfinance as yf
import pymysql
import pandas as pd

def env_int(name, default):
    v = os.getenv(name)
    return default if not v else int(v)

DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")
DB_PORT = env_int("DB_PORT", 3306)

# Timestamp alignment:
# - Scheduled runs: align to "current hour at SNAP_MINUTE_OFFSET" (default :07 UTC)
# - Manual runs (workflow_dispatch): use the current minute (nice-to-have)
SNAP_MINUTE_OFFSET = env_int("SNAP_MINUTE_OFFSET", 7)  # minutes past the hour
USE_CURRENT_MINUTE_ON_MANUAL = (os.getenv("USE_CURRENT_MINUTE_ON_MANUAL", "1") == "1")

def connect():
    cn = pymysql.connect(
        host=DB_HOST, user=DB_USER, password=DB_PASS,
        database=DB_NAME, port=DB_PORT, autocommit=False,
        connect_timeout=10,
    )
    return cn

INSERT_SQL = """
INSERT INTO price_snapshots (ticker_id, as_of_date, price, price_source)
VALUES (%s, %s, %s, %s)
"""

def compute_run_ts(now_utc: dt.datetime) -> dt.datetime:
    """Return a run timestamp (naive UTC datetime) used for *all* inserts in this run."""
    event = os.getenv("GITHUB_EVENT_NAME", "").strip().lower()  # schedule | workflow_dispatch | ...
    if USE_CURRENT_MINUTE_ON_MANUAL and event == "workflow_dispatch":
        # Manual run: keep user's current minute to feel responsive
        aligned = now_utc.replace(second=0, microsecond=0)
        return aligned

    # Scheduled/default: pin to the hour at SNAP_MINUTE_OFFSET (e.g., HH:07:00)
    # If we are before the offset minute (e.g., HH:03) we pin to the *previous* hour's offset
    aligned = now_utc.replace(minute=SNAP_MINUTE_OFFSET, second=0, microsecond=0)
    if now_utc.minute < SNAP_MINUTE_OFFSET:
        aligned = aligned - dt.timedelta(hours=1)
        aligned = aligned.replace(minute=SNAP_MINUTE_OFFSET, second=0, microsecond=0)
    return aligned

def insert_snapshot(cur, ticker_id, as_of_date, price, source="yfinance"):
    cur.execute(INSERT_SQL, (ticker_id, as_of_date, float(price), source))
    return cur.rowcount

def is_rate_limit_error(e: Exception) -> bool:
    msg = str(e).lower()
    return ("ratelimit" in msg) or ("rate limit" in msg) or ("too many requests" in msg) or ("http 429" in msg) or ("status code 429" in msg)

def download_with_retry(symbols, *, period="2d", interval="1d", group_by="ticker", max_attempts=6):
    last_err = None
    for attempt in range(1, max_attempts + 1):
        try:
            return yf.download(
                symbols,
                period=period,
                interval=interval,
                group_by=group_by,
                auto_adjust=False,
                threads=True,
                progress=False,
            )
        except Exception as e:
            last_err = e
            if not is_rate_limit_error(e) and attempt >= 2:
                raise
            sleep_s = min(300, (2 ** (attempt - 1)) * 5)
            sleep_s = sleep_s + random.uniform(0, 1.5)
            print(f"[warn] yfinance download attempt {attempt}/{max_attempts} failed: {e}", file=sys.stderr)
            if attempt < max_attempts:
                print(f"[info] sleeping {sleep_s:.1f}s before retry", file=sys.stderr)
                time.sleep(sleep_s)
    raise RuntimeError(f"yfinance download failed after {max_attempts} attempts: {last_err}")

def extract_close(data: pd.DataFrame, sym: str, multi: bool) -> float:
    if multi:
        s = data[sym]
        return float(s["Close"].dropna().iloc[-1])
    return float(data["Close"].dropna().iloc[-1])

def main():
    # Compute run timestamp ONCE (so retries don't change it)
    now_utc = dt.datetime.utcnow()
    run_ts = compute_run_ts(now_utc)
    print(f"[info] now_utc={now_utc} ; run_ts={run_ts} ; event={os.getenv('GITHUB_EVENT_NAME','')}")
    if run_ts > now_utc:
        # Safety: should never happen, but guard against clock/logic issues
        run_ts = now_utc.replace(second=0, microsecond=0)

    cn = connect()
    cur = cn.cursor()

    cur.execute("SELECT DATABASE(), @@hostname, @@version")
    db, host, ver = cur.fetchone()
    print(f"[info] Connected to db={db}, host={host}, version={ver}")

    cur.execute("""
        SELECT t.id, t.symbol
        FROM tickers t
        JOIN holdings h ON h.ticker_id = t.id
        UNION
        SELECT t.id, t.symbol
        FROM tickers t
        JOIN watchlist w ON w.ticker_id = t.id AND w.active = 1;
    """)
    rows = cur.fetchall()
    if not rows:
        print("[warn] No symbols found; nothing to do")
        return

    symbols = [r[1] for r in rows]
    id_by_symbol = {r[1]: r[0] for r in rows}

    data = download_with_retry(symbols, period="2d", interval="1d", group_by="ticker")

    inserted = 0
    errors = 0
    multi = isinstance(getattr(data, "columns", None), pd.MultiIndex) and len(symbols) > 1

    for sym in symbols:
        try:
            close = extract_close(data, sym, multi)
            inserted += insert_snapshot(cur, id_by_symbol[sym], run_ts, close, "yfinance")
        except Exception as e:
            errors += 1
            print(f"[error] {sym}: {e}", file=sys.stderr)

    cn.commit()
    print(f"[summary] inserted={inserted}, errors={errors}, symbols={len(symbols)}")

if __name__ == "__main__":
    main()
