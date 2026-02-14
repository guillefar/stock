#!/usr/bin/env python3
import argparse
import os
import time
import datetime as dt
from typing import Any, Dict, Optional, Sequence, Tuple, List

import pymysql
import pandas as pd
import yfinance as yf


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


# -------------------------
# Robust schema inspection
# -------------------------
def existing_columns(cn, table_name: str) -> set:
    """
    Return existing column names for table_name, robust to cursor key casing.
    We alias to 'col' so our dict key is stable.
    """
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
# Watchlist operations
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


def delete_watchlist_snapshots_in_range(cn, ticker_id: int, start_dt: dt.datetime, end_dt: dt.datetime):
    with cn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM price_snapshots
            WHERE ticker_id=%s
              AND phase='WATCHLIST'
              AND as_of_date >= %s
              AND as_of_date < %s
            """,
            (ticker_id, start_dt, end_dt),
        )


def insert_watchlist_snapshots(cn, ticker_id: int, rows: List[Tuple[dt.datetime, float]], source: str, dry_run: bool):
    if not rows:
        return 0
    if dry_run:
        print(f"[DRY] would insert {len(rows)} WATCHLIST snapshots for ticker_id={ticker_id} source={source}")
        return len(rows)

    with cn.cursor() as cur:
        cur.executemany(
            """
            INSERT IGNORE INTO price_snapshots (ticker_id, as_of_date, price, price_source, phase)
            VALUES (%s, %s, %s, %s, 'WATCHLIST')
            """,
            [(ticker_id, ts, float(px), source) for (ts, px) in rows],
        )
    return len(rows)


def build_tiered_backfill_windows(end_date: dt.date) -> List[Tuple[dt.date, dt.date, str, str]]:
    end_exclusive = end_date + dt.timedelta(days=1)
    return [
        (end_exclusive - dt.timedelta(days=7), end_exclusive, "1h", "tier_1h_7d"),
        (end_exclusive - dt.timedelta(days=60), end_exclusive, "1d", "tier_1d_60d"),
        (end_exclusive - dt.timedelta(days=365), end_exclusive, "1wk", "tier_1wk_1y"),
    ]


def backfill_watchlist_history(cn, symbols_csv: Optional[str], end_date: dt.date, overwrite: bool, sleep_s: float, dry_run: bool):
    pairs = get_active_watchlist_symbols(cn, symbols_csv)
    print(f"Active watchlist symbols: {len(pairs)}")

    windows = build_tiered_backfill_windows(end_date)
    global_start = min(w[0] for w in windows)
    global_end_excl = max(w[1] for w in windows)
    global_start_dt = dt.datetime.combine(global_start, dt.time.min)
    global_end_dt = dt.datetime.combine(global_end_excl, dt.time.min)

    total_inserted = 0

    for i, (ticker_id, sym) in enumerate(pairs, 1):
        print(f"[{i}/{len(pairs)}] backfill {sym}")

        if overwrite:
            if dry_run:
                print(f"[DRY] would delete WATCHLIST snapshots for {sym} in [{global_start_dt}, {global_end_dt})")
            else:
                delete_watchlist_snapshots_in_range(cn, ticker_id, global_start_dt, global_end_dt)

        all_rows: List[Tuple[dt.datetime, float, str]] = []

        for (start, end, interval, label) in windows:
            try:
                df = yf_history(sym, start, end, interval=interval)
            except Exception as e:
                print(f"[warn] yfinance history failed {sym} interval={interval}: {e}")
                df = None

            if df is None or df.empty:
                print(f"[info] no data for {sym} interval={interval}")
                continue

            closes = df["Close"].dropna()
            for ts, px in closes.items():
                all_rows.append((to_naive_datetime(ts), float(px), label))

            print(f"[info] {sym} interval={interval} rows={len(closes)}")
            if sleep_s:
                time.sleep(sleep_s)

        dedup: Dict[dt.datetime, Tuple[float, str]] = {}
        for ts, px, label in all_rows:
            dedup[ts] = (px, label)

        final = sorted([(ts, v[0], v[1]) for ts, v in dedup.items()], key=lambda x: x[0])

        inserted_for_sym = 0
        by_label: Dict[str, List[Tuple[dt.datetime, float]]] = {}
        for ts, px, label in final:
            by_label.setdefault(label, []).append((ts, px))

        for label, rows in by_label.items():
            inserted_for_sym += insert_watchlist_snapshots(
                cn, ticker_id, rows, source=f"yfinance_history:{label}", dry_run=dry_run
            )

        total_inserted += inserted_for_sym
        print(f"[ok] {sym} inserted={inserted_for_sym}")

    print(f"[summary] total_inserted={total_inserted} overwrite={overwrite} end_date={end_date}")


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


# (Rest of the file — including ffill-watchlist-grid and CLI — should remain
# exactly as you currently have it, unchanged.)

def main():
    ap = argparse.ArgumentParser(description="Local maintenance for portfolio DB")
    ap.add_argument("--env-file")
    ap.add_argument("--symbols")
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

    if (
        not args.ensure_brokers
        and not args.refresh_tickers
        and not args.update_currency
        and not args.backfill_watchlist_history
        and not args.set_watchlist_initial_price
    ):
        ap.print_help()
        return 2

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
