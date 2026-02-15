import os, sys, decimal, time, random
import datetime as dt
import pymysql
import yfinance as yf

# Canonical minute offset to align snapshots (matches collector)
SNAP_MINUTE_OFFSET = 7

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
            # Retry mostly on rate limits/transient issues
            if not is_rate_limit_error(e) and attempt >= 2:
                break
            sleep_s = min(120, (2 ** (attempt - 1)) * 2) + random.uniform(0, 1.0)
            print(f"::warning::yfinance history interval={interval} attempt {attempt}/{max_attempts} failed: {e}")
            time.sleep(sleep_s)
    print(f"::warning::yfinance history interval={interval} failed after retries: {last_err}")
    return None

def to_naive_datetime(x) -> dt.datetime:
    # yfinance returns pandas timestamps; avoid importing pandas here by using duck-typing
    try:
        py = x.to_pydatetime()
        x = py
    except Exception:
        pass
    if isinstance(x, dt.datetime):
        if x.tzinfo is not None:
            return x.astimezone(dt.timezone.utc).replace(tzinfo=None)
        return x.replace(tzinfo=None)
    raise TypeError(f"Unsupported datetime type: {type(x)}")


def bucket_ts(ts: dt.datetime, tier: str, minute_offset: int = SNAP_MINUTE_OFFSET) -> dt.datetime:
    """
    Bucket timestamps onto a canonical grid to align across symbols for Grafana.

    We intentionally map intraday watchlist history onto the production collector cadence:
      - tier '1h' (yfinance fetch) -> 3-hour buckets at HH where HH % 3 == 0, minute=:07
      - tier '1d' -> 00:07
      - tier '1wk' -> Monday 00:07
    """
    if tier == "1h":
        h = ts.hour - (ts.hour % 3)
        return ts.replace(hour=h, minute=minute_offset, second=0, microsecond=0)
    if tier == "1d":
        return dt.datetime.combine(ts.date(), dt.time(hour=0, minute=minute_offset))
    if tier == "1wk":
        d = ts.date()
        monday = d - dt.timedelta(days=d.weekday())
        return dt.datetime.combine(monday, dt.time(hour=0, minute=minute_offset))
    return ts

def fetch_last_snapshot_before(cur, ticker_id: int, phase: str, before_ts: dt.datetime):
    """Return latest snapshot price strictly before before_ts for (ticker_id, phase)."""
    cur.execute(
        """
        SELECT as_of_date, price
        FROM price_snapshots
        WHERE ticker_id=%s AND phase=%s AND as_of_date < %s
        ORDER BY as_of_date DESC
        LIMIT 1
        """,
        (ticker_id, phase, before_ts),
    )
    row = cur.fetchone()
    if not row:
        return None
    # cursor is tuple-based
    return (row[0], float(row[1]))


def build_grid(start_dt: dt.datetime, end_dt_exclusive: dt.datetime, tier: str, minute_offset: int = SNAP_MINUTE_OFFSET):
    """Build canonical grid timestamps >= start_dt and < end_dt_exclusive."""
    grid = []

    if tier == "1d":
        cur = dt.datetime.combine(start_dt.date(), dt.time(hour=0, minute=minute_offset))
        step = dt.timedelta(days=1)
    elif tier == "1wk":
        d = start_dt.date()
        monday = d - dt.timedelta(days=d.weekday())
        cur = dt.datetime.combine(monday, dt.time(hour=0, minute=minute_offset))
        step = dt.timedelta(days=7)
    else:  # '1h' fetched -> 3h grid
        base = start_dt.replace(minute=minute_offset, second=0, microsecond=0)
        h = base.hour - (base.hour % 3)
        cur = base.replace(hour=h)
        if cur < start_dt:
            cur += dt.timedelta(hours=3)
        step = dt.timedelta(hours=3)

    while cur < end_dt_exclusive:
        if cur >= start_dt:
            grid.append(cur)
        cur += step
    return grid


def ffill_on_grid(grid, known, prior_price):
    """Return (real_rows, ffill_rows) as lists of (ts, price)."""
    real_rows = []
    ffill_rows = []
    last = prior_price
    for g in grid:
        if g in known:
            last = known[g]
            real_rows.append((g, last))
        elif last is not None:
            ffill_rows.append((g, last))
    return real_rows, ffill_rows


def backfill_watch_history(cur, ticker_id: int, symbol: str):
    """
    Backfill history for WATCHLIST phase using a shared timestamp grid + forward-fill
    (prevents Grafana dots/gaps when multiple series are plotted together).

    Tier windows (fixed):
      - 1wk: last 1 year (bucket to Monday 00:07)
      - 1d:  last 60 days (bucket to 00:07)
      - 1h:  last 7 days fetched from yfinance, but mapped to a 3-hour grid at :07

    Notes:
      - INSERT IGNORE respects UNIQUE(ticker_id, as_of_date)
      - We process tiers oldest->newest so intraday can seed from daily if needed.
      - Forward-fill happens *within WATCHLIST phase only* (never crosses to HOLDING).
    """
    ENABLE = (os.getenv("BACKFILL_WATCH_HISTORY", "1") == "1")
    if not ENABLE:
        print("::notice::BACKFILL_WATCH_HISTORY=0, skipping history backfill")
        return

    tk = yf.Ticker(symbol)
    today = dt.datetime.utcnow().date()
    end_excl = today + dt.timedelta(days=1)

    # Oldest -> newest (important for seeding intraday ffill)
    windows = [
        (end_excl - dt.timedelta(days=365), end_excl, "1wk"),
        (end_excl - dt.timedelta(days=60),  end_excl, "1d"),
        (end_excl - dt.timedelta(days=7),   end_excl, "1h"),
    ]

    total_rows = 0

    for start, end, interval in windows:
        df = yf_history_with_retry(tk, start=start, end=end, interval=interval)
        if df is None or getattr(df, "empty", True):
            print(f"::notice::no history for {symbol} interval={interval}")
            continue

        try:
            closes = df["Close"].dropna()
        except Exception:
            print(f"::warning::unexpected history format for {symbol} interval={interval}")
            continue

        # Collect known points bucketed to the canonical grid
        known = {}
        count = 0
        for idx, px in closes.items():
            ts = bucket_ts(to_naive_datetime(idx), interval)
            try:
                known[ts] = float(px)
                count += 1
            except Exception:
                continue

        print(f"::notice::{symbol} history interval={interval} rows={count}")

        if not known:
            continue

        # Build grid and forward-fill
        start_dt = dt.datetime.combine(start, dt.time.min)
        end_dt = dt.datetime.combine(end, dt.time.min)

        prior = fetch_last_snapshot_before(cur, ticker_id, "WATCHLIST", start_dt)
        prior_price = prior[1] if prior else None

        grid = build_grid(start_dt, end_dt, interval)
        real_rows, ffill_rows = ffill_on_grid(grid, known, prior_price)

        # Insert (real + ffill)
        def _insert(rows, source):
            nonlocal total_rows
            if not rows:
                return
            cur.executemany(
                """
                INSERT IGNORE INTO price_snapshots (ticker_id, as_of_date, price, price_source, phase)
                VALUES (%s,%s,%s,%s,%s)
                """,
                [(ticker_id, ts, price, source, "WATCHLIST") for (ts, price) in rows],
            )
            total_rows += len(rows)

        _insert(real_rows, f"yfinance_history:{interval}")
        tier_label = "3h" if interval == "1h" else interval
        _insert(ffill_rows, f"ffill_grid:{tier_label}")

        print(f"::notice::{symbol} interval={interval} grid={len(grid)} real={len(real_rows)} ffill={len(ffill_rows)}")

    print(f"::notice::backfill inserted (ignore-collisions) rows={total_rows} for {symbol}")

def main():
    action    = (os.getenv("ACTION") or "").strip().lower()
    symbol    = (os.getenv("SYMBOL") or "").strip()
    qty_in    = (os.getenv("QTY_IN") or "").strip()
    price_in  = (os.getenv("PRICE_IN") or "").strip()
    curr_in   = (os.getenv("CURR_IN") or "").strip()
    note_in   = (os.getenv("NOTE_IN") or "").strip()
    broker_in = (os.getenv("BROKER_IN") or "").strip()  # optional

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

    # price resolver (buy/sell if price blank; watchlist also tries)
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

    # longName -> tickers.name
    long_name = None
    try:
        info = tk.get_info()
        long_name = info.get("longName") or info.get("shortName")
    except Exception:
        pass

    qty = dec_or_none(qty_in) if qty_in else None
    price_dec = decimal.Decimal(str(last_price)) if last_price is not None else None

    # DB connect
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

        print(
            f"Buy OK -> {symbol} qty={qty} price={price_dec} cc={currency} "
            f"broker={(broker_in or None)!r} name={long_name!r} note={(note_in or None)!r}"
        )

    elif action == "sell":
        if qty is None or qty <= 0:
            die("quantity is required and must be > 0 for sell")

        cur.execute("CALL sp_sell(%s,%s,%s)", (symbol, str(qty), (note_in or None)))

        if price_dec is None:
            die("price could not be resolved for sell action")

        insert_txn("SELL", qty, price_dec, currency, note_in, broker_id)

        print(
            f"Sell OK -> {symbol} qty={qty} price={price_dec} cc={currency} "
            f"broker={(broker_in or None)!r} note={(note_in or None)!r}"
        )

    elif action == "add_watch":
        cur.execute(
            "CALL sp_add_watch(%s,%s,%s,%s,%s)",
            (symbol, currency, (note_in or None), long_name, (str(price_dec) if price_dec is not None else None)),
        )

        # Resolve ticker_id for inserts
        cur.execute("SELECT id FROM tickers WHERE symbol=%s", (symbol,))
        r = cur.fetchone()
        if not r:
            die(f"after sp_add_watch, ticker not found in tickers for {symbol}")
        ticker_id = r[0]

        # Backfill history immediately (WATCHLIST phase)
        backfill_watch_history(cur, ticker_id, symbol)

        print(
            f"Watch OK -> {symbol} cc={currency} name={long_name!r} "
            f"initial_price={(price_dec if price_dec is not None else None)!r} note={(note_in or None)!r}"
        )

    elif action == "deactivate_watch":
        cur.execute("CALL sp_deactivate_watch(%s)", (symbol,))
        print(f"Watch deactivated -> {symbol}")

    else:
        die(f"unsupported action: {action}")

    # Feedback (quick visibility)
    cur.execute(
        """
        SELECT t.id, t.symbol, t.currency, t.name
        FROM tickers t
        WHERE t.symbol=%s
        """,
        (symbol,),
    )
    print("Ticker ->", cur.fetchall())

    cur.execute(
        """
        SELECT w.ticker_id, w.active, w.note, w.initial_price, w.initial_price_time
        FROM watchlist w
        JOIN tickers t ON t.id=w.ticker_id
        WHERE t.symbol=%s
        """,
        (symbol,),
    )
    print("Watchlist ->", cur.fetchall())

    cur.execute(
        """
        SELECT ps.as_of_date, ps.price, ps.price_source, ps.phase
        FROM price_snapshots ps
        JOIN tickers t ON t.id=ps.ticker_id
        WHERE t.symbol=%s
        ORDER BY ps.as_of_date DESC
        LIMIT 5
        """,
        (symbol,),
    )
    print("Latest snapshots ->", cur.fetchall())

if __name__ == "__main__":
    main()
