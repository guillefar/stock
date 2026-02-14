#!/usr/bin/env python3
import argparse
import os
import time
from typing import Any, Dict, Optional, Sequence

import pymysql
import yfinance as yf


def load_env_file(path: str) -> None:
    """
    Minimal .env loader (KEY=VALUE). Ignores comments and blank lines.
    Does not require python-dotenv.
    """
    if not path:
        return
    with open(path, "r", encoding="utf-8") as f:
        for raw in f.readlines():
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            k, v = line.split("=", 1)
            k = k.strip()
            v = v.strip().strip('"').strip("'")
            # Only set if not already set; env vars can override after
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


def ensure_tickers_columns(cn) -> None:
    """Idempotently add missing columns used by refresh-tickers."""
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

    with cn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = DATABASE()
              AND table_name = 'tickers'
            """
        )
        existing = {r["column_name"] for r in cur.fetchall()}

        missing = [(c, ddl) for (c, ddl) in wanted if c not in existing]
        if not missing:
            print("OK: tickers already has required profile columns")
            return

        # Add one by one to keep it easy to recover if a single ADD fails.
        for col, ddl in missing:
            sql = f"ALTER TABLE tickers ADD COLUMN {col} {ddl}"
            cur.execute(sql)
            print(f"OK: added tickers.{col}")


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


def list_symbols(cn, symbols_csv: Optional[str]) -> Sequence[str]:
    if symbols_csv:
        return [s.strip() for s in symbols_csv.split(",") if s.strip()]
    with cn.cursor() as cur:
        cur.execute("SELECT symbol FROM tickers ORDER BY symbol")
        return [r["symbol"] for r in cur.fetchall()]


def fetch_yf_info(symbol: str) -> Dict[str, Any]:
    tk = yf.Ticker(symbol)
    try:
        return tk.get_info() or {}
    except Exception:
        return {}


def update_ticker_info(cn, symbol: str, info: Dict[str, Any], dry_run: bool):
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
        # keep existing name behavior: prefer longName then shortName
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
        print(f"[DRY] {symbol} => {shown}")
        return

    with cn.cursor() as cur:
        cur.execute(sql, params)


def main():
    ap = argparse.ArgumentParser(description="Local maintenance for portfolio DB (tickers, brokers, etc.)")
    ap.add_argument("--env-file", help="Path to env file with DB_* keys (KEY=VALUE lines)")
    ap.add_argument("--symbols", help="Comma-separated symbols to process (default: all tickers)")
    ap.add_argument("--dry-run", action="store_true", help="Do not write; print intended updates")
    ap.add_argument("--sleep", type=float, default=0.0, help="Seconds to sleep between yfinance calls")
    ap.add_argument("--ensure-schema", action="store_true", help="Add missing tickers profile columns if needed")
    ap.add_argument("--ensure-brokers", action="store_true", help="Create brokers table + insert known brokers")
    ap.add_argument("--refresh-tickers", action="store_true", help="Fetch yfinance info and update tickers columns")
    args = ap.parse_args()

    # Load file first, then allow actual env vars to override by being already set.
    if args.env_file:
        load_env_file(args.env_file)

    cn = db_connect()

    if args.ensure_schema:
        ensure_tickers_columns(cn)

    if args.ensure_brokers:
        ensure_brokers(cn, ["HeyTrade", "MyInvestor", "BBVA"])

    if args.refresh_tickers:
        # Safety: ensure schema so refresh doesn't fail on missing columns
        ensure_tickers_columns(cn)

        syms = list_symbols(cn, args.symbols)
        print(f"Processing {len(syms)} symbols")
        for i, sym in enumerate(syms, 1):
            info = fetch_yf_info(sym)
            update_ticker_info(cn, sym, info, args.dry_run)
            if args.sleep:
                time.sleep(args.sleep)
            if i % 25 == 0:
                print(f"... {i}/{len(syms)} done")
        print("OK: refresh complete")

    if (not args.ensure_schema) and (not args.ensure_brokers) and (not args.refresh_tickers):
        ap.print_help()
        return 2

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
