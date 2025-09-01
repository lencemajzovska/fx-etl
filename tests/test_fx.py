"""
Automated tests for fx.py (Foreign Exchange ETL).
"""

import sqlite3
import pytest
import fx


@pytest.fixture(autouse=True)
def clean_db(tmp_path, monkeypatch):
    """
    Use a unique temporary database file per test.
    This avoids file-locking issues on Windows and ensures a clean DB for each test.
    """
    test_db = tmp_path / "test_fx.db"
    # Patch the DB_PATH in fx.py so all functions use the temporary test DB
    monkeypatch.setattr(fx, "DB_PATH", test_db)
    yield
    # No explicit cleanup needed – pytest deletes tmp_path automatically


def test_init_db_creates_table():
    """Verify that init_db() creates the fx_rates table."""
    fx.init_db()

    # Verify the table exists in the database
    with sqlite3.connect(fx.DB_PATH) as con:
        cur = con.cursor()
        cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='fx_rates'"
        )
        assert cur.fetchone() is not None


def test_save_rates_inserts_data():
    """Verify that save_rates() inserts rows into the table."""
    fx.init_db()
    dummy_quotes = {"EURUSD": 1.1, "EURSEK": 11.2}

    # Insert two rows (USD and SEK)
    fx.save_rates("EUR", dummy_quotes, "2025-09-01")

    # Verify two rows were inserted
    with sqlite3.connect(fx.DB_PATH) as con:
        cur = con.cursor()
        cur.execute("SELECT COUNT(*) FROM fx_rates")
        count = cur.fetchone()[0]
        assert count == 2


def test_save_rates_ignores_duplicates():
    """
    Verify that duplicate rows are ignored.
    Two different symbols (USD, SEK) should only be stored once each.
    """
    fx.init_db()
    dummy_quotes = {"EURUSD": 1.1, "EURSEK": 11.2}

    # Insert the same row twice
    fx.save_rates("EUR", dummy_quotes, "2025-09-01")
    fx.save_rates("EUR", dummy_quotes, "2025-09-01")

    # Expect 2 unique rows total (USD, SEK)
    with sqlite3.connect(fx.DB_PATH) as con:
        cur = con.cursor()
        cur.execute("SELECT COUNT(*) FROM fx_rates")
        count = cur.fetchone()[0]
        assert count == 2


def test_fetch_rates_monkeypatch(monkeypatch):
    """
    Verify that fetch_rates() can be monkeypatched to avoid real API calls.
    This allows us to test the logic without hitting the external API.
    """
    
    # Define mock version of fetch_rates (replaces the real API call)
    def mock_fetch():
        return {
            "source": "EUR",
            "quotes": {"EURUSD": 1.2, "EURSEK": 11.3},
        }

    # Replace the real fetch_rates with the mock
    monkeypatch.setattr(fx, "fetch_rates", mock_fetch)

    # Verify the mock data is returned instead of API data
    data = fx.fetch_rates()
    assert data["quotes"]["EURUSD"] == 1.2
    assert data["source"] == "EUR"
