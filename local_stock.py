#!/usr/bin/env python3
import argparse
import os
import time
import datetime as dt
from typing import Any, Dict, Optional, Sequence, Tuple, List

import pymysql
import pandas as pd
import yfinance as yf


# -------------------------
# Env + DB helpers
# -------------------------
def load_env_file(path: str) -> None:
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
    if isinstance(x, pd.Timestamp):
        x = x.to_pydatetime()
    if isinstance(x, dt.datetime):
        if x.tzinfo is not None:
            return x.astimezone(dt.timezone.utc).replace(tzinfo=None)
        return x.replace(tzinfo=None)
    raise TypeError(f"Unsupported datetime type: {type(x)}")


def existing_columns(cn, table_name: str) -> set:
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
# Schema ensure
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
    tk = yf.Ticker(symbol)
    return tk.history(
        start=start.strftime("%Y-%m-%d"),
        end=end.strftime("%Y-%m-%d"),
        interval=interval,
        auto_adjust=False,
    )


def yf_close_on_date(symbol: str, day: dt.date) -> Optional[float]:
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
def bucket_to_07(ts: dt.datetime, interval: str) -> dt.datetime:
    """Canonical bucket aligned to :07."""
    if interval == "1h":
        return ts.replace(minute=7, second=0, microsecond=0)
    if interval == "1d":
        return dt.datetime.combine(ts.date(), dt.time(hour=0, minute=7))
    if interval == "1wk":
        d = ts.date()
        monday = d - dt.timedelta(days=d.weekday())
        return dt.datetime.combine(monday, dt.time(hour=0, minute=7))
    return ts


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
            VALUES (%s,%s,%s,%s,%s)
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
# Tiered backfill for a specific symbol
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
    if phase not in ("WATCHLIST", "HOLDING"):
        raise SystemExit("history-phase must be WATCHLIST or HOLDING")

    ticker_id = ensure_ticker_exists(cn, symbol, dry_run=dry_run)
    end_excl = end_date_inclusive + dt.timedelta(days=1)

    windows = [("1h", 7), ("1d", 60), ("1wk", 365)]
    global_start = end_excl - dt.timedelta(days=365)
    start_dt = dt.datetime.combine(global_start, dt.time.min)
    end_dt = dt.datetime.combine(end_excl, dt.time.min)

    if overwrite:
        delete_snapshots_in_range(cn, ticker_id, phase, start_dt, end_dt, dry_run=dry_run)

    dedup: Dict[dt.datetime, float] = {}

    for interval, days in windows:
        start = end_excl - dt.timedelta(days=days)
        df = yf_history(symbol, start=start, end=end_excl, interval=interval)
        if df is None or df.empty:
            print(f"[info] no data for {symbol} interval={interval}")
            continue
        closes = df["Close"].dropna()
        for ts, px in closes.items():
            dts = to_naive_datetime(ts)
            if do_bucket:
                dts = bucket_to_07(dts, interval)
            dedup[dts] = float(px)
        print(f"[ok] fetched {symbol} interval={interval} rows={len(closes)}")
        if sleep_s:
            time.sleep(sleep_s)

    rows = sorted([(ts, px) for ts, px in dedup.items()], key=lambda x: x[0])
    inserted = insert_snapshots_phase(
        cn, ticker_id=ticker_id, phase=phase, rows=rows, source="yfinance_history:tiered", dry_run=dry_run
    )
    print(f"[summary] backfill-symbol-tiered symbol={symbol} phase={phase} inserted_total={inserted}")


# -------------------------
# Active watchlist helpers
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
                JOIN tickers t ON t.id=w.ticker_id
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
                JOIN tickers t ON t.id=w.ticker_id
                WHERE w.active=1
                ORDER BY t.symbol
                """
            )
        return [(r["ticker_id"], r["symbol"]) for r in cur.fetchall()]


# -------------------------
# Set watchlist initial price
# -------------------------
def set_watchlist_initial_price(cn, symbols_csv: Optional[str], price_date: dt.date, sleep_s: float, dry_run: bool):
    ensure_watchlist_initial_price_columns(cn)
    pairs = get_active_watchlist_symbols(cn, symbols_csv)
    print(f"Setting initial_price for active watchlist symbols: {len(pairs)} using close on {price_date}")
    for ticker_id, sym in pairs:
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
# NEW: forward-fill shared grid for watchlist
# -------------------------
def gen_daily_grid(start_date: dt.date, end_date: dt.date) -> List[dt.datetime]:
    out = []
    d = start_date
    while d <= end_date:
        out.append(dt.datetime.combine(d, dt.time(hour=0, minute=7)))
        d += dt.timedelta(days=1)
    return out


def gen_hourly_grid(start_dt: dt.datetime, end_dt: dt.datetime) -> List[dt.datetime]:
    out = []
    cur = start_dt
    while cur <= end_dt:
        out.append(cur.replace(minute=7, second=0, microsecond=0))
        cur += dt.timedelta(hours=1)
    return out


def fetch_existing_prices(cn, ticker_id: int, start_dt: dt.datetime, end_dt: dt.datetime) -> Dict[dt.datetime, float]:
    with cn.cursor() as cur:
        cur.execute(
            """
            SELECT as_of_date, price
            FROM price_snapshots
            WHERE ticker_id=%s AND phase='WATCHLIST'
              AND as_of_date >= %s AND as_of_date <= %s
            ORDER BY as_of_date
            """,
            (ticker_id, start_dt, end_dt),
        )
        rows = cur.fetchall()
    return {r["as_of_date"]: float(r["price"]) for r in rows}


def fetch_last_before(cn, ticker_id: int, ts: dt.datetime) -> Optional[float]:
    with cn.cursor() as cur:
        cur.execute(
            """
            SELECT price
            FROM price_snapshots
            WHERE ticker_id=%s AND phase='WATCHLIST' AND as_of_date < %s
            ORDER BY as_of_date DESC
            LIMIT 1
            """,
            (ticker_id, ts),
        )
        r = cur.fetchone()
    return None if not r else float(r["price"])


def ffill_watchlist_grid(
    cn,
    symbols_csv: Optional[str],
    daily_start: dt.date,
    daily_end: dt.date,
    hourly_start: dt.datetime,
    hourly_end: dt.datetime,
    sleep_s: float,
    dry_run: bool,
):
    pairs = get_active_watchlist_symbols(cn, symbols_csv)
    print(f"Forward-filling WATCHLIST grid for {len(pairs)} tickers")

    daily_grid = gen_daily_grid(daily_start, daily_end)
    hourly_grid = gen_hourly_grid(hourly_start, hourly_end)

    total_inserted = 0

    for i, (ticker_id, sym) in enumerate(pairs, 1):
        print(f"[{i}/{len(pairs)}] ffill {sym}")

        # Daily
        if daily_grid:
            start_dt = daily_grid[0]
            end_dt = daily_grid[-1]
            existing = fetch_existing_prices(cn, ticker_id, start_dt, end_dt)
            last_price = fetch_last_before(cn, ticker_id, start_dt)
            to_insert = []
            for ts in daily_grid:
                if ts in existing:
                    last_price = existing[ts]
                elif last_price is not None:
                    to_insert.append((ts, last_price))
            total_inserted += insert_snapshots_phase(
                cn, ticker_id, "WATCHLIST", to_insert, source="ffill_grid:1d", dry_run=dry_run
            )

        # Hourly
        if hourly_grid:
            start_dt = hourly_grid[0]
            end_dt = hourly_grid[-1]
            existing = fetch_existing_prices(cn, ticker_id, start_dt, end_dt)
            last_price = fetch_last_before(cn, ticker_id, start_dt)
            to_insert = []
            for ts in hourly_grid:
                if ts in existing:
                    last_price = existing[ts]
                elif last_price is not None:
                    to_insert.append((ts, last_price))
            total_inserted += insert_snapshots_phase(
                cn, ticker_id, "WATCHLIST", to_insert, source="ffill_grid:1h", dry_run=dry_run
            )

        if sleep_s:
            time.sleep(sleep_s)

    print(f"[summary] ffill total_inserted={total_inserted}")


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

    ap.add_argument("--set-watchlist-initial-price", action="store_true")
    ap.add_argument("--initial-price-date", default="2026-02-12")

    ap.add_argument("--backfill-symbol-tiered", action="store_true")
    ap.add_argument("--history-symbol")
    ap.add_argument("--history-end-date")
    ap.add_argument("--history-phase", default="WATCHLIST")
    ap.add_argument("--history-overwrite", action="store_true")
    ap.add_argument("--history-bucket", action="store_true")

    # NEW
    ap.add_argument("--ffill-watchlist-grid", action="store_true")
    ap.add_argument("--ffill-daily-start", default="2025-12-08")
    ap.add_argument("--ffill-daily-end", default="2026-02-04")
    ap.add_argument("--ffill-hourly-start", default="2026-02-05 00:07:00")
    ap.add_argument("--ffill-hourly-end", default="2026-02-22 23:07:00")

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
        for sym in syms:
            info = fetch_yf_info(sym)
            update_ticker_profile(cn, sym, info, args.dry_run)
            if args.sleep:
                time.sleep(args.sleep)

    if args.update_currency:
        with cn.cursor() as cur:
            if args.symbols:
                wanted = [s.strip() for s in args.symbols.split(",") if s.strip()]
                qmarks = ",".join(["%s"] * len(wanted))
                cur.execute(f"SELECT symbol FROM tickers WHERE symbol IN ({qmarks}) ORDER BY symbol", tuple(wanted))
            else:
                cur.execute("SELECT symbol FROM tickers ORDER BY symbol")
            syms = [r["symbol"] for r in cur.fetchall()]
        for sym in syms:
            info = fetch_yf_info(sym)
            update_ticker_currency(cn, sym, info, args.dry_run)
            if args.sleep:
                time.sleep(args.sleep)

    if args.set_watchlist_initial_price:
        set_watchlist_initial_price(
            cn,
            symbols_csv=args.symbols,
            price_date=parse_ymd(args.initial_price_date),
            sleep_s=args.sleep,
            dry_run=args.dry_run,
        )

    if args.backfill_symbol_tiered:
        if not args.history_symbol:
            raise SystemExit("--backfill-symbol-tiered requires --history-symbol")
        end_date = parse_ymd(args.history_end_date) if args.history_end_date else dt.datetime.now(dt.timezone.utc).date()
        backfill_symbol_tiered(
            cn,
            symbol=args.history_symbol.strip(),
            end_date_inclusive=end_date,
            phase=args.history_phase.strip().upper(),
            overwrite=args.history_overwrite,
            do_bucket=args.history_bucket,
            sleep_s=args.sleep,
            dry_run=args.dry_run,
        )

    if args.ffill_watchlist_grid:
        ffill_watchlist_grid(
            cn,
            symbols_csv=args.symbols,
            daily_start=parse_ymd(args.ffill_daily_start),
            daily_end=parse_ymd(args.ffill_daily_end),
            hourly_start=parse_ymd_hms(args.ffill_hourly_start),
            hourly_end=parse_ymd_hms(args.ffill_hourly_end),
            sleep_s=args.sleep,
            dry_run=args.dry_run,
        )

    if (
        not args.ensure_brokers
        and not args.refresh_tickers
        and not args.update_currency
        and not args.set_watchlist_initial_price
        and not args.backfill_symbol_tiered
        and not args.ffill_watchlist_grid
    ):
        ap.print_help()
        return 2

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
