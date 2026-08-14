"""Shared DB shape for both chambers: tables, the `trades` view, amount parsing.

Lives in its own module because two scrapers write the same view -- if each
owned a copy of the DDL, whichever ran last would silently redefine `trades`.
"""

import re

DB_PATH = "congress_trades.duckdb"

# Both chambers land in the same shape so the view is one SELECT body twice.
# House has no ticker column on the form (the symbol is inside asset_name), so
# `ticker` is always NULL there and entities.py fills `ticker_guess`.
COLUMNS = """first_name VARCHAR, last_name VARCHAR, office VARCHAR,
    filed VARCHAR, link VARCHAR, tx_date VARCHAR, owner VARCHAR,
    ticker VARCHAR, asset_name VARCHAR, asset_type VARCHAR,
    tx_type VARCHAR, amount_raw VARCHAR,
    amount_low BIGINT, amount_high BIGINT,
    ticker_guess VARCHAR, ticker_guess_how VARCHAR"""

TABLES = ("senate_trades", "house_trades")


def _select(table, chamber):
    return rf"""SELECT '{chamber}' AS chamber, last_name,
            -- Senate Exchange rows pack both legs into one cell ("--  AMCR" =
            -- gave up an untickered holding, received AMCR). Take the trailing
            -- symbol, i.e. what they hold afterwards. Plain tickers pass
            -- through; House rows have no ticker cell and fall to the guess.
            coalesce(
                nullif(nullif(regexp_extract(trim(ticker), '([A-Z.\-]+)$', 1),
                              '--'), ''),
                ticker_guess) AS tkr,
            asset_name, tx_type, amount_low, amount_high,
            try_strptime(tx_date, '%m/%d/%Y')::DATE AS txn_date,
            try_strptime(filed, '%m/%d/%Y')::DATE AS filed_date,
            date_diff('day', try_strptime(tx_date, '%m/%d/%Y')::DATE,
                             try_strptime(filed, '%m/%d/%Y')::DATE) AS lag_days,
            -- secondary columns: filtering, auditing, provenance
            asset_type, owner,
            ticker IS NULL AND ticker_guess IS NOT NULL AS tkr_recovered,
            first_name, office, link
        FROM {table}"""


def ensure_schema(con):
    """Create both trade tables and the `trades` view. Idempotent.

    The view is what you query: it coalesces recovered tickers into `tkr`,
    turns MM/DD/YYYY strings into real DATEs so ORDER BY sorts chronologically
    instead of lexically, and unions both chambers behind a `chamber` column.
    """
    for t in TABLES:
        con.execute(f"CREATE TABLE IF NOT EXISTS {t} ({COLUMNS})")
    con.execute("CREATE OR REPLACE VIEW trades AS "
                + _select("senate_trades", "S")
                + " UNION ALL "
                + _select("house_trades", "H"))


def parse_amount(text):
    """'$1,001 - $15,000' -> (1001, 15000). Open-ended high returns None."""
    nums = [int(n.replace(",", "")) for n in re.findall(r"[\d,]+", text or "")]
    if not nums:
        return None, None
    return nums[0], (nums[1] if len(nums) > 1 else None)
