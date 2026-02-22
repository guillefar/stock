import os, sys, decimal, time, random
import datetime as dt
import pymysql
import yfinance as yf

def die(msg: str) -> None:
    print(f"::error::{msg}")
    sys.exit(1)

def dec_or_none(s: str):
    try:
        return decimal.Decimal(s)
    except Exception:
        return None

def is_blank(s):
    return s is None or str(s).strip() == ""

def is_rate_limit_error(e: Exception) -> bool:
    msg = str(e).lower()
    return ("ratelimit" in msg) or ("rate limit" in msg) or ("too many requests" in msg) or ("http 429" in msg) or ("status code 429" in msg)

def yf_history_with_retry(tk: yf.Ticker, *, start: dt.date, end: dt.date, interval: str, max_attempts: int = 6):
    last_err = None
    for attempt in range(1, max_attempts + 1):
        try:
            return tk.history(
                start=start.strftime("%Y-%m-%d"),
                end=end.strftime("%Y-%m-%d"),
                interval=interval,
                auto_adjust=False,
            )
        except Exception as e:
            last_err = e
            if not is_rate_limit_error(e) and attempt >= 2:
                break
            sleep_s = min(120, (2 ** (attempt - 1)) * 2) + random.uniform(0, 1.0)
            print(f"::warning::yfinance history interval={interval} attempt {attempt}/{max_attempts} failed: {e}")
            time.sleep(sleep_s)
    print(f"::warning::yfinance history interval={interval} failed after retries: {last_err}")
    return None

def to_naive_datetime(x) -> dt.datetime:
    try:
        x = x.to_pydatetime()
    except Exception:
        pass
    if isinstance(x, dt.datetime):
        if x.tzinfo is not None:
            return x.astimezone(dt.timezone.utc).replace(tzinfo=None)
        return x.replace(tzinfo=None)
    raise TypeError(f"Unsupported datetime type: {type(x)}")

def bucket_to_07(ts: dt.datetime, interval: str) -> dt.datetime:
    """
    Canonical bucket that matches your collector convention (:07).
    - 1h: HH:07:00
    - 1d: 00:07:00
    - 1wk: Monday 00:07:00
    """
    if interval == "1h":
        return ts.replace(minute=7, second=0, microsecond=0)
    if interval == "1d":
        d = ts.date()
        return dt.datetime.combine(d, dt.time(hour=0, minute=7))
    if interval == "1wk":
        d = ts.date()
        monday = d - dt.timedelta(days=d.weekday())
        return dt.datetime.combine(monday, dt.time(hour=0, minute=7))
    return ts

def backfill_watch_history(cur, ticker_id: int, symbol: str):
    """
    Backfill WATCHLIST history on add_watch with tiered strategy:
      - 1h for 7 days
      - 1d for 60 days
      - 1wk for 365 days
    Buckets timestamps to :07 to avoid Grafana gaps/dots.
    """
    ENABLE = (os.getenv("BACKFILL_WATCH_HISTORY", "1") == "1")
    if not ENABLE:
        print("::notice::BACKFILL_WATCH_HISTORY=0, skipping history backfill")
        return

    tk = yf.Ticker(symbol)

    # Use UTC "today" as end anchor for history pulls
    end_inclusive = dt.datetime.now(dt.timezone.utc).date()
    end_excl = end_inclusive + dt.timedelta(days=1)

    windows = [
        ("1h", 7),
        ("1d", 60),
        ("1wk", 365),
    ]

    # Dedup by bucketed timestamp; keep last seen price
    dedup = {}

    for interval, days in windows:
        start = end_excl - dt.timedelta(days=days)
        df = yf_history_with_retry(tk, start=start, end=end_excl, interval=interval)
        if df is None or getattr(df, "empty", True):
            print(f"::notice::no history for {symbol} interval={interval}")
            continue

        try:
            closes = df["Close"].dropna()
        except Exception:
            print(f"::warning::unexpected history format for {symbol} interval={interval}")
            continue

        count = 0
        for idx, px in closes.items():
            ts = bucket_to_07(to_naive_datetime(idx), interval)
            try:
                dedup[ts] = float(px)
                count += 1
            except Exception:
                continue

        print(f"::notice::{symbol} history interval={interval} rows={count}")

    if not dedup:
        print(f"::warning::no history rows collected for {symbol}")
        return

    rows = sorted(
        [(ticker_id, ts, price, "yfinance_history:tiered", "WATCHLIST") for ts, price in dedup.items()],
        key=lambda x: x[1]
    )

    cur.executemany(
        """
        INSERT IGNORE INTO price_snapshots (ticker_id, as_of_date, price, price_source, phase)
        VALUES (%s,%s,%s,%s,%s)
        """,
        rows
    )
    print(f"::notice::watch history inserted rows={len(rows)} for {symbol}")

def main():
    action    = (os.getenv("ACTION") or "").strip().lower()
    symbol    = (os.getenv("SYMBOL") or "").strip()
    qty_in    = (os.getenv("QTY_IN") or "").strip()
    price_in  = (os.getenv("PRICE_IN") or "").strip()
    curr_in   = (os.getenv("CURR_IN") or "").strip()
    note_in   = (os.getenv("NOTE_IN") or "").strip()
    broker_in = (os.getenv("BROKER_IN") or "").strip()

    if not symbol:
        die("symbol is required")

    tk = yf.Ticker(symbol)

    # currency (only if blank)
    currency = curr_in or None
    if not currency:
        fi = getattr(tk, "fast_info", None)
        currency = None
        if isinstance(fi, dict):
            currency = fi.get("currency")
        elif fi is not None:
            currency = getattr(fi, "currency", None)
        if not currency:
            try:
                info = tk.get_info()
                currency = info.get("currency")
            except Exception:
                currency = None
        if not currency:
            currency = "USD"
            print(f"::warning::currency not found for {symbol}, defaulting to USD")

    def resolve_last_price():
        fi = getattr(tk, "fast_info", None)
        if isinstance(fi, dict):
            p = fi.get("lastPrice") or fi.get("last_price") or fi.get("last_traded_price")
            if p:
                return float(p)
        elif fi is not None:
            p = getattr(fi, "last_price", None)
            if p:
                return float(p)
        try:
            hist = tk.history(period="1d", interval="1d")
            if not hist.empty:
                return float(hist["Close"].dropna().iloc[-1])
        except Exception:
            pass
        return None

    if not price_in:
        last_price = resolve_last_price()
        if last_price is None and action in ("buy", "sell"):
            die(f"could not resolve current price for {symbol}")
        if last_price is None and action == "add_watch":
            print(f"::warning::could not resolve initial price for watchlist for {symbol}, storing NULL")
    else:
        try:
            last_price = float(price_in)
        except Exception:
            die("price must be a number if provided")

    long_name = None
    try:
        info = tk.get_info()
        long_name = info.get("longName") or info.get("shortName")
    except Exception:
        pass

    qty = dec_or_none(qty_in) if qty_in else None
    price_dec = decimal.Decimal(str(last_price)) if last_price is not None else None

    cn = pymysql.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        database=os.getenv("DB_NAME"),
        port=int(os.getenv("DB_PORT") or "3306"),
        autocommit=True,
        connect_timeout=10,
    )
    cur = cn.cursor()

    def resolve_broker_id(broker_name: str):
        if is_blank(broker_name):
            return None
        cur.execute("SELECT id FROM brokers WHERE name=%s", (broker_name,))
        r = cur.fetchone()
        if not r:
            die(f"unknown broker: {broker_name!r} (expected an existing row in brokers)")
        return r[0]

    broker_id = None
    if action in ("buy", "sell"):
        broker_id = resolve_broker_id(broker_in)

    def insert_txn(txn_type, qty, price_dec, currency, note, broker_id):
        cur.execute("SELECT id FROM tickers WHERE symbol=%s", (symbol,))
        r = cur.fetchone()
        if not r:
            die(f"could not resolve ticker_id for {symbol}")
        ticker_id = r[0]
        cur.execute(
            "INSERT INTO transactions (ticker_id, txn_type, quantity, price, fees, currency, note, source, broker_id) "
            "VALUES (%s,%s,%s,%s,NULL,%s,%s,'admin',%s)",
            (ticker_id, txn_type, str(qty), str(price_dec), currency, (note or None), broker_id),
        )

    if action == "buy":
        if qty is None or qty <= 0:
            die("quantity is required and must be > 0 for buy")
        if price_dec is None:
            die("price could not be resolved for buy action")

        cur.execute(
            "CALL sp_buy(%s,%s,%s,%s,%s,%s)",
            (symbol, currency, str(qty), str(price_dec), long_name, (note_in or None)),
        )
        insert_txn("BUY", qty, price_dec, currency, note_in, broker_id)
        print(f"Buy OK -> {symbol}")

    elif action == "sell":
        if qty is None or qty <= 0:
            die("quantity is required and must be > 0 for sell")

        cur.execute("CALL sp_sell(%s,%s,%s)", (symbol, str(qty), (note_in or None)))

        if price_dec is None:
            die("price could not be resolved for sell action")

        insert_txn("SELL", qty, price_dec, currency, note_in, broker_id)
        print(f"Sell OK -> {symbol}")

    elif action == "add_watch":
        cur.execute(
            "CALL sp_add_watch(%s,%s,%s,%s,%s)",
            (symbol, currency, (note_in or None), long_name, (str(price_dec) if price_dec is not None else None)),
        )

        cur.execute("SELECT id FROM tickers WHERE symbol=%s", (symbol,))
        r = cur.fetchone()
        if not r:
            die(f"after sp_add_watch, ticker not found in tickers for {symbol}")
        ticker_id = r[0]

        backfill_watch_history(cur, ticker_id, symbol)

        print(f"Watch OK -> {symbol}")

    elif action == "deactivate_watch":
        cur.execute("CALL sp_deactivate_watch(%s)", (symbol,))
        print(f"Watch deactivated -> {symbol}")

    else:
        die(f"unsupported action: {action}")

if __name__ == "__main__":
    main()
