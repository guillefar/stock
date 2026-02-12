import os, sys, decimal
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

def main():
    action   = (os.getenv("ACTION") or "").strip().lower()
    symbol   = (os.getenv("SYMBOL") or "").strip()
    qty_in   = (os.getenv("QTY_IN") or "").strip()
    price_in = (os.getenv("PRICE_IN") or "").strip()
    curr_in  = (os.getenv("CURR_IN") or "").strip()
    note_in  = (os.getenv("NOTE_IN") or "").strip()

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

    # price resolver (buy/sell if price blank)
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

    def insert_txn(txn_type, qty, price_dec, currency, note):
        cur.execute("SELECT id FROM tickers WHERE symbol=%s", (symbol,))
        r = cur.fetchone()
        if not r:
            die(f"could not resolve ticker_id for {symbol}")
        ticker_id = r[0]
        cur.execute(
            "INSERT INTO transactions (ticker_id, txn_type, quantity, price, fees, currency, note, source) "
            "VALUES (%s,%s,%s,%s,NULL,%s,%s,'admin')",
            (ticker_id, txn_type, str(qty), str(price_dec), currency, (note or None)),
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
        insert_txn("BUY", qty, price_dec, currency, note_in)
        print(f"Buy OK -> {symbol} qty={qty} price={price_dec} cc={currency} name={long_name!r} note={(note_in or None)!r}")

    elif action == "sell":
        if qty is None or qty <= 0:
            die("quantity is required and must be > 0 for sell")
        cur.execute("CALL sp_sell(%s,%s,%s)", (symbol, str(qty), (note_in or None)))
        if price_dec is None:
            die("price could not be resolved for sell action")
        insert_txn("SELL", qty, price_dec, currency, note_in)
        print(f"Sell OK -> {symbol} qty={qty} price={price_dec} cc={currency} note={(note_in or None)!r}")

    elif action == "add_watch":
        cur.execute(
            "CALL sp_add_watch(%s,%s,%s,%s)",
            (symbol, currency, (note_in or None), long_name),
        )
        print(f"Watch OK -> {symbol} cc={currency} name={long_name!r} note={(note_in or None)!r}")

    elif action == "deactivate_watch":
        cur.execute("CALL sp_deactivate_watch(%s)", (symbol,))
        print(f"Watch deactivated -> {symbol}")

    else:
        die(f"unsupported action: {action}")

    # Feedback
    cur.execute(
        """
        SELECT t.symbol, t.currency, t.name, h.quantity, h.avg_cost, h.note
        FROM tickers t
        LEFT JOIN holdings h ON h.ticker_id = t.id
        WHERE t.symbol=%s
        """,
        (symbol,),
    )
    print("Row ->", cur.fetchall())

if __name__ == "__main__":
    main()
