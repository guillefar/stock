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
    action    = (os.getenv("ACTION") or "").strip().lower()
    symbol    = (os.getenv("SYMBOL") or "").strip()
    qty_in    = (os.getenv("QTY_IN") or "").strip()
    price_in  = (os.getenv("PRICE_IN") or "").strip()
    curr_in   = (os.getenv("CURR_IN") or "").strip()
    note_in   = (os.getenv("NOTE_IN") or "").strip()
    broker_in = (os.getenv("BROKER_IN") or "").strip()  # NEW

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

    # Determine last_price:
    # - If PRICE_IN provided, always use it
    # - Otherwise try resolve via yfinance
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
