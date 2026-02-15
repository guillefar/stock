#!/usr/bin/env python3
import argparse
import os
import time
import datetime as dt
from typing import Any, Dict, Optional, Sequence, Tuple, List

import pymysql
import pandas as pd
import yfinance as yf



# Canonical minute offset to align snapshots (matches collector)
SNAP_MINUTE_OFFSET = 7

# -------------------------
# Env + DB helpers
# -------------------------
def load_env_file(path: str) -> None:
    """Minimal .env loader (KEY=VALUE). Ignores comments and blank lines."""
    if not path:
        return
    with open(path, "r", encoding="utf-8") as f:
        for raw in f.readlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            k = k.strip()
            v = v.strip().strip('"').strip("'")
            if k and k not in os.environ:
                os.environ[k] = v


def env(name: str, default: Optional[str] = None) -> str:
    v = os.getenv(name, default)
    if v is None or v == "":
        raise SystemExit(f"Missing required env var: {name}")
    return v


def db_connect():
    return pymysql.connect(
        host=env("DB_HOST"),
        user=env("DB_USER"),
        password=env("DB_PASS"),
        database=env("DB_NAME"),
        port=int(os.getenv("DB_PORT") or "3306"),
        autocommit=True,
        connect_timeout=10,
        cursorclass=pymysql.cursors.DictCursor,
    )


def pick(*vals):
    """Return first non-empty (not None, not '')."""
    for v in vals:
        if v is None:
            continue
        if isinstance(v, str) and v.strip() == "":
            continue
        return v
    return None


def parse_ymd(s: str) -> dt.date:
    return dt.datetime.strptime(s, "%Y-%m-%d").date()


def parse_ymd_hms(s: str) -> dt.datetime:
    return dt.datetime.strptime(s, "%Y-%m-%d %H:%M:%S")


def to_naive_datetime(x) -> dt.datetime:
    """Convert pandas Timestamp / datetime to naive datetime (drop tz)."""
    if isinstance(x, pd.Timestamp):
        x = x.to_pydatetime()
    if isinstance(x, dt.datetime):
        if x.tzinfo is not None:
            return x.astimezone(dt.timezone.utc).replace(tzinfo=None)
        return x.replace(tzinfo=None)
    raise TypeError(f"Unsupported datetime type: {type(x)}")


def existing_columns(cn, table_name: str) -> set:
    """Return existing column names for table_name; alias ensures stable dict key."""
    with cn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name AS col
            FROM information_schema.columns
            WHERE table_schema = DATABASE()
              AND table_name = %s
            """,
            (table_name,),
        )
        return {r["col"] for r in cur.fetchall()}


# -------------------------
# Schema ensure (idempotent)
# -------------------------
def ensure_tickers_columns(cn) -> None:
    wanted = [
        ("long_business_summary", "TEXT NULL"),
        ("industry", "VARCHAR(128) NULL"),
        ("industry_disp", "VARCHAR(128) NULL"),
        ("sector", "VARCHAR(128) NULL"),
        ("sector_disp", "VARCHAR(128) NULL"),
        ("market", "VARCHAR(32) NULL"),
        ("short_name", "VARCHAR(128) NULL"),
        ("website", "VARCHAR(255) NULL"),
        ("quote_type", "VARCHAR(32) NULL"),
    ]
    existing = existing_columns(cn, "tickers")
    with cn.cursor() as cur:
        missing = [(c, ddl) for (c, ddl) in wanted if c not in existing]
        for col, ddl in missing:
            cur.execute(f"ALTER TABLE tickers ADD COLUMN {col} {ddl}")
            print(f"OK: added tickers.{col}")
        if not missing:
            print("OK: tickers already has required profile columns")


def ensure_watchlist_initial_price_columns(cn) -> None:
    wanted = [
        ("initial_price", "DECIMAL(18,6) NULL"),
        ("initial_price_time", "TIMESTAMP NULL"),
        ("initial_price_source", "VARCHAR(32) NULL"),
    ]
    existing = existing_columns(cn, "watchlist")
    with cn.cursor() as cur:
        missing = [(c, ddl) for (c, ddl) in wanted if c not in existing]
        for col, ddl in missing:
            cur.execute(f"ALTER TABLE watchlist ADD COLUMN {col} {ddl}")
            print(f"OK: added watchlist.{col}")
        if not missing:
            print("OK: watchlist already has initial_price columns")


def ensure_brokers(cn, broker_names: Sequence[str]):
    with cn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS brokers (
              id INT NOT NULL AUTO_INCREMENT,
              name VARCHAR(64) NOT NULL,
              created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
              PRIMARY KEY (id),
              UNIQUE KEY uq_brokers_name (name)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
        )
        for name in broker_names:
            cur.execute(
                "INSERT INTO brokers(name) VALUES (%s) ON DUPLICATE KEY UPDATE name=VALUES(name)",
                (name,),
            )
    print(f"OK: ensured brokers: {', '.join(broker_names)}")


# -------------------------
# yfinance helpers
# -------------------------
def fetch_yf_info(symbol: str) -> Dict[str, Any]:
    tk = yf.Ticker(symbol)
    try:
        return tk.get_info() or {}
    except Exception:
        return {}


def update_ticker_profile(cn, symbol: str, info: Dict[str, Any], dry_run: bool):
    industry = info.get("industry")
    industry_disp = pick(info.get("industryDisp"), industry)

    sector = info.get("sector")
    sector_disp = pick(info.get("sectorDisp"), sector)

    payload = {
        "long_business_summary": info.get("longBusinessSummary"),
        "industry": industry,
        "industry_disp": industry_disp,
        "sector": sector,
        "sector_disp": sector_disp,
        "market": info.get("market"),
        "short_name": info.get("shortName"),
        "website": info.get("website"),
        "quote_type": info.get("quoteType"),
        "name": pick(info.get("longName"), info.get("shortName")),
    }

    sql = """
    UPDATE tickers
    SET
      name = COALESCE(%s, name),
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
    """

    params = (
        payload["name"],
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
    )

    if dry_run:
        shown = ", ".join(f"{k}={repr(v)[:80]}" for k, v in payload.items() if v is not None)
        print(f"[DRY] profile {symbol} => {shown}")
        return

    with cn.cursor() as cur:
        cur.execute(sql, params)


def update_ticker_currency(cn, symbol: str, info: Dict[str, Any], dry_run: bool):
    currency = info.get("currency")
    if not currency:
        return
    if dry_run:
        print(f"[DRY] currency {symbol} => {currency}")
        return
    with cn.cursor() as cur:
        cur.execute("UPDATE tickers SET currency=%s WHERE symbol=%s", (currency, symbol))


def yf_history(symbol: str, start: dt.date, end: dt.date, interval: str) -> pd.DataFrame:
    """Download history in [start, end) date window."""
    tk = yf.Ticker(symbol)
    return tk.history(
        start=start.strftime("%Y-%m-%d"),
        end=end.strftime("%Y-%m-%d"),
        interval=interval,
        auto_adjust=False,
    )


def yf_close_on_date(symbol: str, day: dt.date) -> Optional[float]:
    """Get daily close for a specific calendar day (best effort)."""
    tk = yf.Ticker(symbol)
    start = day
    end = day + dt.timedelta(days=1)
    try:
        df = tk.history(start=start.strftime("%Y-%m-%d"), end=end.strftime("%Y-%m-%d"), interval="1d")
        if df is None or df.empty:
            return None
        return float(df["Close"].dropna().iloc[-1])
    except Exception:
        return None


# -------------------------
# Snapshot helpers
# -------------------------
def bucket_ts(ts: dt.datetime, interval: str, minute_offset: int = SNAP_MINUTE_OFFSET) -> dt.datetime:
    """
    Bucket timestamps onto a canonical grid (UTC naive) to align across symbols for Grafana.

    We intentionally map intraday history onto the production collector cadence:
      - interval '1h' (yfinance fetch) -> 3-hour buckets at HH where HH % 3 == 0, minute=:07
      - interval '1d' -> 00:07
      - interval '1wk' -> Monday 00:07
    """
    # Ensure naive datetime in UTC (caller should already do this)
    if interval == "1h":
        h = ts.hour - (ts.hour % 3)
        return ts.replace(hour=h, minute=minute_offset, second=0, microsecond=0)
    if interval == "1d":
        return dt.datetime.combine(ts.date(), dt.time(hour=0, minute=minute_offset))
    if interval == "1wk":
        d = ts.date()
        monday = d - dt.timedelta(days=d.weekday())
        return dt.datetime.combine(monday, dt.time(hour=0, minute=minute_offset))
    return ts

def fetch_last_snapshot_before(
    cn,
    ticker_id: int,
    phase: str,
    before_ts: dt.datetime,
) -> Optional[Tuple[dt.datetime, float]]:
    """Return the latest snapshot strictly before before_ts for (ticker_id, phase)."""
    with cn.cursor() as cur:
        cur.execute(
            """
            SELECT as_of_date, price
            FROM price_snapshots
            WHERE ticker_id = %s AND phase = %s AND as_of_date < %s
            ORDER BY as_of_date DESC
            LIMIT 1
            """,
            (ticker_id, phase, before_ts),
        )
        row = cur.fetchone()
    if not row:
        return None
    # DictCursor returns a dict; be tolerant if driver returns tuples.
    if isinstance(row, dict):
        return (row["as_of_date"], float(row["price"]))
    return (row[0], float(row[1]))



def build_grid(
    start_dt: dt.datetime,
    end_dt_exclusive: dt.datetime,
    interval: str,
    minute_offset: int = SNAP_MINUTE_OFFSET,
) -> List[dt.datetime]:
    """Build canonical grid timestamps >= start_dt and < end_dt_exclusive."""
    grid: List[dt.datetime] = []

    if interval == "1d":
        cur = dt.datetime.combine(start_dt.date(), dt.time(hour=0, minute=minute_offset))
        step = dt.timedelta(days=1)
    elif interval == "1wk":
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


def ffill_on_grid(
    grid: List[dt.datetime],
    known: Dict[dt.datetime, float],
    prior_price: Optional[float],
) -> Tuple[List[Tuple[dt.datetime, float]], List[Tuple[dt.datetime, float]]]:
    """
    Forward-fill prices across the grid:
      - returns (real_rows, ffill_rows)
      - real_rows are points that existed in 'known'
      - ffill_rows are synthetic points filled with the last seen price (within the same phase)
    """
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

def insert_snapshots_phase(
    cn,
    ticker_id: int,
    phase: str,
    rows: List[Tuple[dt.datetime, float]],
    source: str,
    dry_run: bool,
) -> int:
    if not rows:
        return 0
    if dry_run:
        print(f"[DRY] would insert {len(rows)} {phase} snapshots for ticker_id={ticker_id} source={source}")
        return len(rows)

    with cn.cursor() as cur:
        cur.executemany(
            """
            INSERT IGNORE INTO price_snapshots (ticker_id, as_of_date, price, price_source, phase)
            VALUES (%s, %s, %s, %s, %s)
            """,
            [(ticker_id, ts, float(px), source, phase) for (ts, px) in rows],
        )
    return len(rows)


def delete_snapshots_in_range(cn, ticker_id: int, phase: str, start_dt: dt.datetime, end_dt: dt.datetime, dry_run: bool):
    if dry_run:
        print(f"[DRY] would delete snapshots ticker_id={ticker_id} phase={phase} range=[{start_dt},{end_dt})")
        return
    with cn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM price_snapshots
            WHERE ticker_id=%s AND phase=%s
              AND as_of_date >= %s AND as_of_date < %s
            """,
            (ticker_id, phase, start_dt, end_dt),
        )


def ensure_ticker_exists(cn, symbol: str, dry_run: bool) -> int:
    """Ensure tickers row exists (best effort) and return ticker_id."""
    with cn.cursor() as cur:
        cur.execute("SELECT id FROM tickers WHERE symbol=%s", (symbol,))
        r = cur.fetchone()
        if r:
            return int(r["id"])

    info = fetch_yf_info(symbol)
    currency = info.get("currency") or "USD"
    name = pick(info.get("longName"), info.get("shortName"), symbol)

    if dry_run:
        print(f"[DRY] would insert tickers row symbol={symbol} currency={currency} name={name!r}")
    else:
        with cn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO tickers(symbol, currency, name)
                VALUES (%s,%s,%s)
                ON DUPLICATE KEY UPDATE
                  currency = VALUES(currency),
                  name = COALESCE(VALUES(name), name)
                """,
                (symbol, currency, name),
            )

    with cn.cursor() as cur:
        cur.execute("SELECT id FROM tickers WHERE symbol=%s", (symbol,))
        r = cur.fetchone()
        if not r:
            raise RuntimeError(f"Could not ensure tickers row for {symbol}")
        return int(r["id"])


# -------------------------
# NEW: Tiered backfill for a specific symbol
# -------------------------
def backfill_symbol_tiered(
    cn,
    symbol: str,
    end_date_inclusive: dt.date,
    phase: str,
    overwrite: bool,
    do_bucket: bool,
    sleep_s: float,
    dry_run: bool,
):
    """
    Tiered history backfill for one symbol ending at end_date_inclusive:
      - 1h (fetched) -> 3-hour grid for last 7 days
      - 1d for last 60 days
      - 1wk for last 365 days

    If do_bucket=True:
      - timestamps are bucketed to the canonical grid (minute :07)
      - missing grid points are forward-filled (within the same phase) to produce continuous Grafana lines
    """
    if phase not in ("WATCHLIST", "HOLDING"):
        raise SystemExit("history-phase must be WATCHLIST or HOLDING")

    ticker_id = ensure_ticker_exists(cn, symbol, dry_run=dry_run)

    end_excl = end_date_inclusive + dt.timedelta(days=1)

    windows: List[Tuple[str, int]] = [
        ("1wk", 365),
        ("1d", 60),
        ("1h", 7),
    ]

    # Global delete window if overwrite
    global_start = end_excl - dt.timedelta(days=max(d for _, d in windows))
    global_start_dt = dt.datetime.combine(global_start, dt.time.min)
    global_end_dt = dt.datetime.combine(end_excl, dt.time.min)

    if overwrite:
        delete_snapshots_in_range(cn, ticker_id, phase, global_start_dt, global_end_dt, dry_run=dry_run)

    total_inserted = 0

    for interval, days in windows:
        start = end_excl - dt.timedelta(days=days)
        start_dt = dt.datetime.combine(start, dt.time.min)
        end_dt = dt.datetime.combine(end_excl, dt.time.min)

        df = yf_history(symbol, start=start, end=end_excl, interval=interval)
        if df is None or df.empty:
            print(f"[info] no data for {symbol} interval={interval}")
            continue

        closes = df["Close"].dropna()

        known: Dict[dt.datetime, float] = {}
        for ts, px in closes.items():
            dts = to_naive_datetime(ts)
            if do_bucket:
                dts = bucket_ts(dts, interval)
            known[dts] = float(px)

        if not do_bucket:
            rows = sorted([(ts, px) for ts, px in known.items()], key=lambda x: x[0])
            inserted = insert_snapshots_phase(
                cn,
                ticker_id=ticker_id,
                phase=phase,
                rows=rows,
                source=f"yfinance_history:{interval}",
                dry_run=dry_run,
            )
            total_inserted += inserted
            print(f"[summary] {symbol} interval={interval} rows={len(rows)} inserted={inserted}")
            if sleep_s:
                time.sleep(sleep_s)
            continue

        prior = None if dry_run else fetch_last_snapshot_before(cn, ticker_id, phase, start_dt)
        prior_price = prior[1] if prior else None

        grid = build_grid(start_dt, end_dt, interval)
        real_rows, ffill_rows = ffill_on_grid(grid, known, prior_price)

        inserted_real = insert_snapshots_phase(
            cn,
            ticker_id=ticker_id,
            phase=phase,
            rows=real_rows,
            source=f"yfinance_history:{interval}",
            dry_run=dry_run,
        )
        inserted_ffill = insert_snapshots_phase(
            cn,
            ticker_id=ticker_id,
            phase=phase,
            rows=ffill_rows,
            source=f"ffill_grid:{'3h' if interval=='1h' else interval}",
            dry_run=dry_run,
        )

        total_inserted += (inserted_real + inserted_ffill)
        print(
            f"[summary] {symbol} interval={interval} grid={len(grid)} "
            f"real={len(real_rows)} ffill={len(ffill_rows)} inserted={inserted_real + inserted_ffill}"
        )

        if sleep_s:
            time.sleep(sleep_s)

    print(f"[summary] backfill-symbol-tiered symbol={symbol} phase={phase} inserted_total={total_inserted}")

# -------------------------
# Watchlist utility (unchanged)
# -------------------------
def get_active_watchlist_symbols(cn, symbols_csv: Optional[str]) -> List[Tuple[int, str]]:
    filt = None
    if symbols_csv:
        filt = [s.strip() for s in symbols_csv.split(",") if s.strip()]

    with cn.cursor() as cur:
        if filt:
            qmarks = ",".join(["%s"] * len(filt))
            cur.execute(
                f"""
                SELECT t.id AS ticker_id, t.symbol
                FROM watchlist w
                JOIN tickers t ON t.id = w.ticker_id
                WHERE w.active=1 AND t.symbol IN ({qmarks})
                ORDER BY t.symbol
                """,
                tuple(filt),
            )
        else:
            cur.execute(
                """
                SELECT t.id AS ticker_id, t.symbol
                FROM watchlist w
                JOIN tickers t ON t.id = w.ticker_id
                WHERE w.active=1
                ORDER BY t.symbol
                """
            )
        return [(r["ticker_id"], r["symbol"]) for r in cur.fetchall()]


def build_tiered_backfill_windows(end_date: dt.date) -> List[Tuple[dt.date, dt.date, str, str]]:
    end_exclusive = end_date + dt.timedelta(days=1)
    return [
        (end_exclusive - dt.timedelta(days=7), end_exclusive, "1h", "tier_1h_7d"),
        (end_exclusive - dt.timedelta(days=60), end_exclusive, "1d", "tier_1d_60d"),
        (end_exclusive - dt.timedelta(days=365), end_exclusive, "1wk", "tier_1wk_1y"),
    ]


def backfill_watchlist_history(
    cn,
    symbols_csv: Optional[str],
    end_date: dt.date,
    overwrite: bool,
    do_bucket: bool,
    sleep_s: float,
    dry_run: bool,
):
    pairs = get_active_watchlist_symbols(cn, symbols_csv)
    print(f"Active watchlist symbols: {len(pairs)}")

    # Tier windows (inclusive end_date -> end_excl)
    end_excl = end_date + dt.timedelta(days=1)
    windows: List[Tuple[str, int]] = [
        ("1wk", 365),
        ("1d", 60),
        ("1h", 7),
    ]

    global_start = end_excl - dt.timedelta(days=max(d for _, d in windows))
    global_start_dt = dt.datetime.combine(global_start, dt.time.min)
    global_end_dt = dt.datetime.combine(end_excl, dt.time.min)

    total_inserted = 0

    for i, (ticker_id, sym) in enumerate(pairs, 1):
        print(f"[{i}/{len(pairs)}] backfill {sym}")

        if overwrite:
            delete_snapshots_in_range(cn, ticker_id, "WATCHLIST", global_start_dt, global_end_dt, dry_run=dry_run)

        inserted_for_sym = 0

        for interval, days in windows:
            start = end_excl - dt.timedelta(days=days)
            start_dt = dt.datetime.combine(start, dt.time.min)
            end_dt = dt.datetime.combine(end_excl, dt.time.min)

            try:
                df = yf_history(sym, start=start, end=end_excl, interval=interval)
            except Exception as e:
                print(f"[warn] yfinance history failed {sym} interval={interval}: {e}")
                df = None

            if df is None or df.empty:
                print(f"[info] no data for {sym} interval={interval}")
                continue

            closes = df["Close"].dropna()

            known: Dict[dt.datetime, float] = {}
            for ts, px in closes.items():
                dts = to_naive_datetime(ts)
                if do_bucket:
                    dts = bucket_ts(dts, interval)
                known[dts] = float(px)

            if not do_bucket:
                rows = sorted([(ts, px) for ts, px in known.items()], key=lambda x: x[0])
                inserted_for_sym += insert_snapshots_phase(
                    cn, ticker_id, "WATCHLIST", rows, source=f"yfinance_history:{interval}", dry_run=dry_run
                )
                print(f"[info] {sym} interval={interval} rows={len(rows)} inserted={len(rows)}")
                if sleep_s:
                    time.sleep(sleep_s)
                continue

            prior = None if dry_run else fetch_last_snapshot_before(cn, ticker_id, "WATCHLIST", start_dt)
            prior_price = prior[1] if prior else None

            grid = build_grid(start_dt, end_dt, interval)
            real_rows, ffill_rows = ffill_on_grid(grid, known, prior_price)

            inserted_for_sym += insert_snapshots_phase(
                cn, ticker_id, "WATCHLIST", real_rows, source=f"yfinance_history:{interval}", dry_run=dry_run
            )
            inserted_for_sym += insert_snapshots_phase(
                cn,
                ticker_id,
                "WATCHLIST",
                ffill_rows,
                source=f"ffill_grid:{'3h' if interval=='1h' else interval}",
                dry_run=dry_run,
            )

            print(
                f"[info] {sym} interval={interval} grid={len(grid)} real={len(real_rows)} ffill={len(ffill_rows)}"
            )

            if sleep_s:
                time.sleep(sleep_s)

        total_inserted += inserted_for_sym
        print(f"[ok] {sym} inserted={inserted_for_sym}")

    print(f"[summary] total_inserted={total_inserted} overwrite={overwrite} end_date={end_date} bucket={do_bucket}")

def set_watchlist_initial_price(cn, symbols_csv: Optional[str], price_date: dt.date, sleep_s: float, dry_run: bool):
    ensure_watchlist_initial_price_columns(cn)

    pairs = get_active_watchlist_symbols(cn, symbols_csv)
    print(f"Setting initial_price for active watchlist symbols: {len(pairs)} using close on {price_date}")

    for i, (ticker_id, sym) in enumerate(pairs, 1):
        px = yf_close_on_date(sym, price_date)
        if px is None:
            print(f"[warn] {sym}: could not fetch close for {price_date}, leaving initial_price unchanged")
            continue

        if dry_run:
            print(f"[DRY] {sym}: set initial_price={px}")
        else:
            with cn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE watchlist
                    SET initial_price=%s,
                        initial_price_time=%s,
                        initial_price_source='yfinance_history'
                    WHERE ticker_id=%s AND active=1
                    """,
                    (float(px), dt.datetime.combine(price_date, dt.time.min), ticker_id),
                )
        if sleep_s:
            time.sleep(sleep_s)

    print("[ok] initial_price update done")


# -------------------------
# Main CLI
# -------------------------
def main():
    ap = argparse.ArgumentParser(description="Local maintenance for portfolio DB")
    ap.add_argument("--env-file", help="Path to env file with DB_* keys (KEY=VALUE lines)")
    ap.add_argument("--symbols", help="Comma-separated symbols to restrict some operations")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--sleep", type=float, default=0.0)

    ap.add_argument("--ensure-brokers", action="store_true")
    ap.add_argument("--refresh-tickers", action="store_true")
    ap.add_argument("--update-currency", action="store_true")

    ap.add_argument("--backfill-watchlist-history", action="store_true")
    ap.add_argument("--watchlist-end-date", default="2026-02-11")
    ap.add_argument("--overwrite-watchlist", action="store_true")

    ap.add_argument("--set-watchlist-initial-price", action="store_true")
    ap.add_argument("--initial-price-date", default="2026-02-12")

    # NEW: tiered history for one symbol
    ap.add_argument("--backfill-symbol-tiered", action="store_true", help="Tiered: 1h/7d + 1d/60d + 1wk/365d for one symbol")
    ap.add_argument("--history-symbol", help="Symbol to backfill (e.g. LYM9.F)")
    ap.add_argument("--history-end-date", help="End date YYYY-MM-DD (inclusive). Default: today")
    ap.add_argument("--history-phase", default="WATCHLIST", help="WATCHLIST or HOLDING (default WATCHLIST)")
    ap.add_argument("--history-overwrite", action="store_true", help="Delete existing snapshots in tier window before inserting")
    ap.add_argument("--history-bucket", action="store_true", help="Bucket timestamps to shared grid (recommended)")

    args = ap.parse_args()

    if args.env_file:
        load_env_file(args.env_file)

    cn = db_connect()

    if args.ensure_brokers:
        ensure_brokers(cn, ["HeyTrade", "MyInvestor", "BBVA"])

    if args.refresh_tickers:
        ensure_tickers_columns(cn)
        with cn.cursor() as cur:
            if args.symbols:
                wanted = [s.strip() for s in args.symbols.split(",") if s.strip()]
                qmarks = ",".join(["%s"] * len(wanted))
                cur.execute(f"SELECT symbol FROM tickers WHERE symbol IN ({qmarks}) ORDER BY symbol", tuple(wanted))
            else:
                cur.execute("SELECT symbol FROM tickers ORDER BY symbol")
            syms = [r["symbol"] for r in cur.fetchall()]

        print(f"Refreshing ticker profiles: {len(syms)} symbols")
        for i, sym in enumerate(syms, 1):
            info = fetch_yf_info(sym)
            update_ticker_profile(cn, sym, info, args.dry_run)
            if args.sleep:
                time.sleep(args.sleep)
            if i % 25 == 0:
                print(f"... {i}/{len(syms)} done")
        print("[ok] refresh-tickers complete")

    if args.update_currency:
        with cn.cursor() as cur:
            if args.symbols:
                wanted = [s.strip() for s in args.symbols.split(",") if s.strip()]
                qmarks = ",".join(["%s"] * len(wanted))
                cur.execute(f"SELECT symbol FROM tickers WHERE symbol IN ({qmarks}) ORDER BY symbol", tuple(wanted))
            else:
                cur.execute("SELECT symbol FROM tickers ORDER BY symbol")
            syms = [r["symbol"] for r in cur.fetchall()]

        print(f"Updating tickers.currency: {len(syms)} symbols")
        for i, sym in enumerate(syms, 1):
            info = fetch_yf_info(sym)
            update_ticker_currency(cn, sym, info, args.dry_run)
            if args.sleep:
                time.sleep(args.sleep)
            if i % 25 == 0:
                print(f"... {i}/{len(syms)} done")
        print("[ok] update-currency complete")

    if args.backfill_watchlist_history:
        end_date = parse_ymd(args.watchlist_end_date)
        backfill_watchlist_history(
            cn=cn,
            symbols_csv=args.symbols,
            end_date=end_date,
            overwrite=args.overwrite_watchlist,
            do_bucket=args.history_bucket,
            sleep_s=args.sleep,
            dry_run=args.dry_run,
        )

    if args.set_watchlist_initial_price:
        price_date = parse_ymd(args.initial_price_date)
        set_watchlist_initial_price(
            cn=cn,
            symbols_csv=args.symbols,
            price_date=price_date,
            sleep_s=args.sleep,
            dry_run=args.dry_run,
        )

    if args.backfill_symbol_tiered:
        if not args.history_symbol:
            raise SystemExit("--backfill-symbol-tiered requires --history-symbol")
        end_date = parse_ymd(args.history_end_date) if args.history_end_date else dt.datetime.utcnow().date()
        backfill_symbol_tiered(
            cn=cn,
            symbol=args.history_symbol.strip(),
            end_date_inclusive=end_date,
            phase=args.history_phase.strip().upper(),
            overwrite=args.history_overwrite,
            do_bucket=args.history_bucket,
            sleep_s=args.sleep,
            dry_run=args.dry_run,
        )

    if (
        not args.ensure_brokers
        and not args.refresh_tickers
        and not args.update_currency
        and not args.backfill_watchlist_history
        and not args.set_watchlist_initial_price
        and not args.backfill_symbol_tiered
    ):
        ap.print_help()
        return 2

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
