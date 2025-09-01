"""
ETL pipeline for daily foreign-exchange (FX) rates.

Fetches EUR-based rates from exchangerate.host API (Apilayer, requires API key),
stores them in a SQLite database, and logs execution details.
Intended to be scheduled via Windows Task Scheduler using a .cmd wrapper.
"""

import logging
import sqlite3
import datetime as dt
from pathlib import Path
import requests
import os
from dotenv import load_dotenv


# Load API key from .env
load_dotenv()
API_KEY = os.getenv("FX_API_KEY")


# File paths
DB_PATH: Path = Path("fx.db")
LOG_PATH: Path = Path("fx.log")


# API configuration
API_URL: str = "http://api.exchangerate.host/live"
API_PARAMS = {"access_key": API_KEY, "source": "EUR"}
HTTP_TIMEOUT: int = 15  # seconds


# Logging configuration
logging.basicConfig(
    filename=LOG_PATH,
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    encoding="utf-8",
)


def init_db():
    """Create the fx_rates table if it doesn't already exist."""
    with sqlite3.connect(DB_PATH) as con:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS fx_rates (
                date   TEXT NOT NULL,
                base   TEXT NOT NULL,
                symbol TEXT NOT NULL,
                rate   REAL NOT NULL,
                PRIMARY KEY (date, symbol)
            )
            """
        )
    logging.info("Database setup complete.")


def fetch_rates():
    """Fetch FX rates from API and return parsed JSON dict."""
    if not API_KEY:
        raise RuntimeError("Missing FX_API_KEY. Please check your .env file.")

    resp = requests.get(API_URL, params=API_PARAMS, timeout=HTTP_TIMEOUT)
    resp.raise_for_status()
    data = resp.json()

    # Ensure response has required fields
    if "quotes" not in data or "source" not in data:
        raise ValueError(f"Unexpected API response: {data}")

    logging.info("Successfully fetched FX rates (source=%s)", data["source"])
    return data


def save_rates(base, quotes, date_str):
    """Insert FX rates into SQLite database."""
    rows = []
    for pair, rate in quotes.items():
        # Get target currency (EURUSD → USD)
        symbol = pair.replace(base, "")
        rows.append((date_str, base, symbol, float(rate)))
        
   # Insert rows, ignore duplicates
    with sqlite3.connect(DB_PATH) as con:
        con.executemany(
            "INSERT OR IGNORE INTO fx_rates(date, base, symbol, rate) VALUES (?, ?, ?, ?)",
            rows,
        )

    logging.info("Inserted %d FX rates for %s.", len(rows), date_str)
    return len(rows)


def main():
    """Run the ETL pipeline end-to-end."""
    logging.info("FX ETL STARTED")
    try:
        init_db()
        data = fetch_rates()
        base = data.get("source", "EUR")        # API field for base currency
        date_str = dt.date.today().isoformat()  # Use today's date
        quotes = data["quotes"]

        n = save_rates(base, quotes, date_str)
        logging.info("Saved %d rates for %s (base=%s)", n, date_str, base)

        print("FX ETL completed successfully")  # Success signal for Task Scheduler

    except Exception as e:
        logging.exception("FX ETL FAILED: %s", e)
        print("FX ETL failed — check logs")

    finally:
        logging.info("FX ETL FINISHED")


if __name__ == "__main__":
    main()
