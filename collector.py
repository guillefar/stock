import os
import sys
import time
import random
import datetime as dt
from collections import defaultdict

import pandas as pd
import pymysql
import yfinance as yf


def env_int(name, default):
    v = os.getenv(name)
    return default if not v else int(v)


DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")
DB_PORT = env_int("DB_PORT", 3306)

SNAP_MINUTE_OFFSET = env_int("SNAP_MINUTE_OFFSET", 7)


def connect():
    return pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        port=DB_PORT,
        autocommit=False,
        connect_timeout=10,
    )


INSERT_SQL = """
INSERT INTO price_snapshots (ticker_id, as_of_date, price, price_source, phase)
VALUES (%s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE
  price = VALUES(price),
  price_source = VALUES(price_source)
"""


def compute_run_ts(now_utc: dt.datetime) -> dt.datetime:
    """
    Timestamp canónico:
      - HH:07:00 de la hora UTC actual
      - si la ejecución empieza antes de :07, usar la hora anterior a :07
    """
    aligned = now_utc.replace(minute=SNAP_MINUTE_OFFSET, second=0, microsecond=0)
    if now_utc.minute < SNAP_MINUTE_OFFSET:
        aligned = (aligned - dt.timedelta(hours=1)).replace(
            minute=SNAP_MINUTE_OFFSET,
            second=0,
            microsecond=0,
        )
    return aligned


def insert_snapshot(cur, ticker_id, as_of_date, price, phase, source="yfinance"):
    cur.execute(INSERT_SQL, (ticker_id, as_of_date, float(price), source, phase))
    return cur.rowcount


def is_rate_limit_error(e: Exception) -> bool:
    msg = str(e).lower()
    return (
        ("ratelimit" in msg)
        or ("rate limit" in msg)
        or ("too many requests" in msg)
        or ("http 429" in msg)
        or ("status code 429" in msg)
    )


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
            sleep_s = min(300, (2 ** (attempt - 1)) * 5) + random.uniform(0, 1.5)
            print(
                f"[warn] yfinance download attempt {attempt}/{max_attempts} failed: {e}",
                file=sys.stderr,
            )
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
    now_utc = dt.datetime.utcnow()
    run_ts = compute_run_ts(now_utc)
    print(f"[info] now_utc={now_utc} ; run_ts={run_ts} ; event={os.getenv('GITHUB_EVENT_NAME', '')}")

    cn = connect()
    cur = cn.cursor()

    cur.execute("SELECT DATABASE(), @@hostname, @@version")
    db, host, ver = cur.fetchone()
    print(f"[info] Connected to db={db}, host={host}, version={ver}")

    cur.execute(
        """
        SELECT t.id, t.symbol, 'HOLDING' AS phase
        FROM tickers t
        JOIN holdings h ON h.ticker_id = t.id
        WHERE h.quantity > 0

        UNION ALL

        SELECT t.id, t.symbol, 'WATCHLIST' AS phase
        FROM tickers t
        JOIN watchlist w ON w.ticker_id = t.id
        WHERE w.active = 1
        """
    )
    rows = cur.fetchall()
    if not rows:
        print("[warn] No symbols found; nothing to do")
        cur.close()
        cn.close()
        return

    targets_by_symbol = defaultdict(list)
    symbols = []
    seen_symbols = set()

    for ticker_id, sym, phase in rows:
        targets_by_symbol[sym].append((ticker_id, phase))
        if sym not in seen_symbols:
            seen_symbols.add(sym)
            symbols.append(sym)

    data = download_with_retry(symbols, period="2d", interval="1d", group_by="ticker")
    multi = isinstance(data.columns, pd.MultiIndex)

    inserted = 0
    errors = 0

    for sym in symbols:
        try:
            close = extract_close(data, sym, multi)
            for ticker_id, phase in targets_by_symbol[sym]:
                inserted += insert_snapshot(cur, ticker_id, run_ts, close, phase, "yfinance")
        except Exception as e:
            errors += 1
            print(f"[error] {sym}: {e}", file=sys.stderr)

    cn.commit()
    cur.close()
    cn.close()
    print(f"[summary] inserted={inserted}, errors={errors}, symbols={len(symbols)}, targets={len(rows)}")


if __name__ == "__main__":
    main()
