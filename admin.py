import os
import sys
import decimal
import time
import random
import datetime as dt
from typing import Dict, List, Optional, Tuple

import pymysql
import yfinance as yf

SNAP_MINUTE_OFFSET = 7


def die(msg: str) -> None:
    print(f"::error::{msg}")
    sys.exit(1)


def dec_or_none(s: str):
    try:
        return decimal.Decimal(s)
    except Exception:
        return None


def is_rate_limit_error(e: Exception) -> bool:
    msg = str(e).lower()
    return (
        ("ratelimit" in msg)
        or ("rate limit" in msg)
        or ("too many requests" in msg)
        or ("http 429" in msg)
        or ("status code 429" in msg)
    )


def yf_history_with_retry(
    tk: yf.Ticker,
    *,
    start: dt.date,
    end: dt.date,
    interval: str,
    max_attempts: int = 6,
):
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
            print(
                f"::warning::yfinance history interval={interval} attempt "
                f"{attempt}/{max_attempts} failed: {e}"
            )
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


def bucket_ts(ts: dt.datetime, interval: str) -> dt.datetime:
    """
    Rejilla canónica:
      - 1h descargado -> grid 3h a minuto :07
      - 1d -> 00:07
      - 1wk -> Monday 00:07
    """
    if interval == "1h":
        h = ts.hour - (ts.hour % 3)
        return ts.replace(hour=h, minute=SNAP_MINUTE_OFFSET, second=0, microsecond=0)
    if interval == "1d":
        return dt.datetime.combine(ts.date(), dt.time(hour=0, minute=SNAP_MINUTE_OFFSET))
    if interval == "1wk":
        monday = ts.date() - dt.timedelta(days=ts.date().weekday())
        return dt.datetime.combine(monday, dt.time(hour=0, minute=SNAP_MINUTE_OFFSET))
    return ts


def build_grid(
    start_dt: dt.datetime,
    end_dt_exclusive: dt.datetime,
    interval: str,
) -> List[dt.datetime]:
    grid: List[dt.datetime] = []

    if interval == "1wk":
        monday = start_dt.date() - dt.timedelta(days=start_dt.date().weekday())
        cur = dt.datetime.combine(monday, dt.time(hour=0, minute=SNAP_MINUTE_OFFSET))
        step = dt.timedelta(days=7)
    elif interval == "1d":
        cur = dt.datetime.combine(start_dt.date(), dt.time(hour=0, minute=SNAP_MINUTE_OFFSET))
        step = dt.timedelta(days=1)
    else:
        cur = start_dt.replace(minute=SNAP_MINUTE_OFFSET, second=0, microsecond=0)
        cur = cur.replace(hour=cur.hour - (cur.hour % 3))
        if cur < start_dt:
            cur += dt.timedelta(hours=3)
        step = dt.timedelta(hours=3)

    while cur < end_dt_exclusive:
        if cur >= start_dt:
            grid.append(cur)
        cur += step

    return grid


def fetch_last_snapshot_before(
    cur,
    ticker_id: int,
    phase: str,
    before_ts: dt.datetime,
) -> Optional[Tuple[dt.datetime, float]]:
    cur.execute(
        """
        SELECT as_of_date, price
        FROM price_snapshots
        WHERE ticker_id = %s
          AND phase = %s
          AND as_of_date < %s
        ORDER BY as_of_date DESC
        LIMIT 1
        """,
        (ticker_id, phase, before_ts),
    )
    row = cur.fetchone()
    if not row:
        return None
    return (row[0], float(row[1]))


def ffill_on_grid(
    grid: List[dt.datetime],
    known: Dict[dt.datetime, float],
    prior_price: Optional[float],
) -> Tuple[List[Tuple[dt.datetime, float]], List[Tuple[dt.datetime, float]]]:
    real_rows: List[Tuple[dt.datetime, float]] = []
    ffill_rows: List[Tuple[dt.datetime, float]] = []
    last = prior_price

    for g in grid:
        if g in known:
            last = known[g]
            real_rows.append((g, last))
        elif last is not None:
            ffill_rows.append((g, last))

    return real_rows, ffill_rows


def upsert_snapshots_phase(
    cur,
    ticker_id: int,
    phase: str,
    rows: List[Tuple[dt.datetime, float]],
    source: str,
    overwrite_existing: bool,
) -> int:
    if not rows:
        return 0

    if overwrite_existing:
        sql = """
            INSERT INTO price_snapshots (ticker_id, as_of_date, price, price_source, phase)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
              price = VALUES(price),
              price_source = VALUES(price_source)
        """
    else:
        sql = """
            INSERT INTO price_snapshots (ticker_id, as_of_date, price, price_source, phase)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
              ticker_id = ticker_id
        """

    cur.executemany(
        sql,
        [(ticker_id, ts, float(px), source, phase) for (ts, px) in rows],
    )
    return len(rows)


def update_ticker_profile(
    cur,
    symbol: str,
    currency: Optional[str],
    info: Optional[dict],
    name_hint: Optional[str],
) -> None:
    if not info:
        info = {}

    payload = {
        "name": info.get("longName") or info.get("shortName") or name_hint,
        "currency": currency,
        "exchange": info.get("exchange"),
        "long_business_summary": info.get("longBusinessSummary"),
        "industry": info.get("industry"),
        "industry_disp": info.get("industryDisp") or info.get("industry"),
        "sector": info.get("sector"),
        "sector_disp": info.get("sectorDisp") or info.get("sector"),
        "market": info.get("market"),
        "short_name": info.get("shortName"),
        "website": info.get("website"),
        "quote_type": info.get("quoteType"),
    }

    cur.execute(
        """
        UPDATE tickers
        SET
          name = COALESCE(%s, name),
          currency = COALESCE(%s, currency),
          exchange = COALESCE(%s, exchange),
          long_business_summary = COALESCE(%s, long_business_summary),
          industry = COALESCE(%s, industry),
          industry_disp = COALESCE(%s, industry_disp),
          sector = COALESCE(%s, sector),
          sector_disp = COALESCE(%s, sector_disp),
          market = COALESCE(%s, market),
          short_name = COALESCE(%s, short_name),
          website = COALESCE(%s, website),
          quote_type = COALESCE(%s, quote_type)
        WHERE symbol = %s
        """,
        (
            payload["name"],
            payload["currency"],
            payload["exchange"],
            payload["long_business_summary"],
            payload["industry"],
            payload["industry_disp"],
            payload["sector"],
            payload["sector_disp"],
            payload["market"],
            payload["short_name"],
            payload["website"],
            payload["quote_type"],
            symbol,
        ),
    )


def backfill_watch_history(cur, ticker_id: int, symbol: str):
    enable = (os.getenv("BACKFILL_WATCH_HISTORY", "1") == "1")
    if not enable:
        print("::notice::BACKFILL_WATCH_HISTORY=0, skipping history backfill")
        return

    tk = yf.Ticker(symbol)
    end_inclusive = dt.datetime.now(dt.timezone.utc).date()
    end_excl = end_inclusive + dt.timedelta(days=1)

    # grueso -> fino; los más finos sobreescriben buckets coincidentes
    windows = [
        ("1wk", 365),
        ("1d", 60),
        ("1h", 7),
    ]

    total = 0

    for interval, days in windows:
        start = end_excl - dt.timedelta(days=days)
        start_dt = dt.datetime.combine(start, dt.time.min)
        end_dt = dt.datetime.combine(end_excl, dt.time.min)

        df = yf_history_with_retry(tk, start=start, end=end_excl, interval=interval)
        if df is None or getattr(df, "empty", True):
            print(f"::notice::no history for {symbol} interval={interval}")
            continue

        closes = df.get("Close")
        if closes is None:
            print(f"::warning::unexpected history format for {symbol} interval={interval}")
            continue

        known: Dict[dt.datetime, float] = {}
        for idx, px in closes.items():
            try:
                ts = bucket_ts(to_naive_datetime(idx), interval)
                known[ts] = float(px)
            except Exception:
                continue

        prior = fetch_last_snapshot_before(cur, ticker_id, "WATCHLIST", start_dt)
        prior_price = prior[1] if prior else None

        grid = build_grid(start_dt, end_dt, interval)
        real_rows, ffill_rows = ffill_on_grid(grid, known, prior_price)

        inserted_real = upsert_snapshots_phase(
            cur,
            ticker_id,
            "WATCHLIST",
            real_rows,
            f"yfinance_history:{interval}",
            overwrite_existing=True,
        )
        inserted_ffill = upsert_snapshots_phase(
            cur,
            ticker_id,
            "WATCHLIST",
            ffill_rows,
            f"ffill_grid:{'3h' if interval == '1h' else interval}",
            overwrite_existing=False,
        )

        total += inserted_real + inserted_ffill
        print(
            f"::notice::{symbol} interval={interval} grid={len(grid)} "
            f"real={len(real_rows)} ffill={len(ffill_rows)} inserted={inserted_real + inserted_ffill}"
        )

    print(f"::notice::watch history inserted rows={total} for {symbol}")


def main():
    action = (os.getenv("ACTION") or "").strip().lower()
    symbol = (os.getenv("SYMBOL") or "").strip()
    qty_in = (os.getenv("QTY_IN") or "").strip()
    price_in = (os.getenv("PRICE_IN") or "").strip()
    curr_in = (os.getenv("CURR_IN") or "").strip()
    note_in = (os.getenv("NOTE_IN") or "").strip()
    broker_in = (os.getenv("BROKER_IN") or "").strip()

    if not symbol:
        die("SYMBOL is required")

    tk = yf.Ticker(symbol)

    currency = curr_in or None
    info_full = None

    if not currency:
        fi = getattr(tk, "fast_info", None)
        if isinstance(fi, dict):
            currency = fi.get("currency")
        elif fi is not None:
            currency = getattr(fi, "currency", None)

    try:
        info_full = tk.get_info()
    except Exception as e:
        print(f"::warning::get_info failed for {symbol}: {e}")
        info_full = {}

    if not currency:
        currency = (info_full or {}).get("currency")

    if not currency:
        currency = "USD"
        print(f"::warning::currency not found for {symbol}, defaulting to USD")

    def resolve_last_price():
        fi = getattr(tk, "fast_info", None)
        candidates = []
        if isinstance(fi, dict):
            candidates.extend([fi.get("lastPrice"), fi.get("last_price"), fi.get("regularMarketPrice")])
        elif fi is not None:
            candidates.extend([
                getattr(fi, "last_price", None),
                getattr(fi, "lastPrice", None),
                getattr(fi, "regular_market_price", None),
            ])

        for p in candidates:
            try:
                if p is not None:
                    return float(p)
            except Exception:
                pass

        try:
            hist = tk.history(period="5d", interval="1d", auto_adjust=False)
            if hist is not None and not hist.empty:
                return float(hist["Close"].dropna().iloc[-1])
        except Exception:
            pass

        return None

    last_price = resolve_last_price()
    if price_in:
        try:
            last_price = float(decimal.Decimal(price_in))
        except Exception:
            die("price must be a number if provided")

    long_name = None
    if isinstance(info_full, dict):
        long_name = info_full.get("longName") or info_full.get("shortName")

    qty = dec_or_none(qty_in) if qty_in else None
    price_dec = decimal.Decimal(str(last_price)) if last_price is not None else None

    if action in ("buy", "sell") and (qty is None or qty <= 0):
        die("quantity must be > 0 for buy/sell")
    if action == "buy" and price_dec is None:
        die("price could not be resolved for buy action")

    cn = pymysql.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        database=os.getenv("DB_NAME"),
        port=int(os.getenv("DB_PORT") or "3306"),
        autocommit=False,
        connect_timeout=10,
    )
    cur = cn.cursor()

    def resolve_broker_id(name: str):
        if not name:
            return None
        cur.execute("SELECT id FROM brokers WHERE name = %s", (name,))
        row = cur.fetchone()
        if not row:
            die(f"broker not found: {name}")
        return row[0]

    broker_id = None
    if action in ("buy", "sell"):
        broker_id = resolve_broker_id(broker_in)

    def get_ticker_id() -> int:
        cur.execute("SELECT id FROM tickers WHERE symbol = %s", (symbol,))
        row = cur.fetchone()
        if not row:
            die(f"could not resolve ticker_id for {symbol}")
        return row[0]

    def insert_txn(txn_type, qty, price_dec, currency, note, broker_id):
        ticker_id = get_ticker_id()
        cur.execute(
            """
            INSERT INTO transactions
              (ticker_id, txn_type, quantity, price, fees, currency, note, source, broker_id)
            VALUES
              (%s, %s, %s, %s, NULL, %s, %s, 'admin', %s)
            """,
            (ticker_id, txn_type, str(qty), str(price_dec), currency, (note or None), broker_id),
        )

    try:
        if action == "buy":
            cur.execute(
                "CALL sp_buy(%s,%s,%s,%s,%s,%s)",
                (symbol, currency, str(qty), str(price_dec), long_name, (note_in or None)),
            )
            update_ticker_profile(cur, symbol, currency, info_full, long_name)
            insert_txn("BUY", qty, price_dec, currency, note_in, broker_id)
            print(f"Buy OK -> {symbol}")

        elif action == "sell":
            if price_dec is None:
                die("price could not be resolved for sell action")

            update_ticker_profile(cur, symbol, currency, info_full, long_name)
            insert_txn("SELL", qty, price_dec, currency, note_in, broker_id)
            print(f"Sell OK -> {symbol}")

        elif action == "add_watch":
            cur.execute(
                "CALL sp_add_watch(%s,%s,%s,%s,%s)",
                (symbol, currency, (note_in or None), long_name, (str(price_dec) if price_dec is not None else None)),
            )
            update_ticker_profile(cur, symbol, currency, info_full, long_name)
            ticker_id = get_ticker_id()
            backfill_watch_history(cur, ticker_id, symbol)
            print(f"Watch OK -> {symbol}")

        elif action == "deactivate_watch":
            cur.execute(
                """
                UPDATE watchlist w
                JOIN tickers t ON t.id = w.ticker_id
                SET w.active = 0
                WHERE t.symbol = %s
                """,
                (symbol,),
            )
            print(f"Deactivate watch OK -> {symbol}")

        else:
            die(f"unsupported action: {action}")

        cn.commit()
    except Exception:
        cn.rollback()
        raise
    finally:
        cur.close()
        cn.close()


if __name__ == "__main__":
    main()
