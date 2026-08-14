"""Query the scraped congress trades.

    py q.py                        # summary by senator
    py q.py --types                # what asset types exist, and how many
    py q.py --type Stock           # trades of one asset type
    py q.py --type "Municipal Security" --limit 100
    py q.py "SELECT ..."           # arbitrary SQL

Query the `trades` view, not the raw `senate_trades` table -- it has the
recovered tickers coalesced in and real DATE columns. Raw table is still there
if you want the untouched scraped values.

Exists because PowerShell mangles quotes and '%' in `py -c "..."` one-liners.
"""

import sys

import duckdb
import pandas as pd

DB_PATH = "congress_trades.duckdb"

SUMMARY = """
SELECT last_name, count(*) AS txns, count(DISTINCT tkr) AS tickers,
       min(txn_date) AS earliest, max(txn_date) AS latest
FROM trades GROUP BY last_name ORDER BY txns DESC
"""

TYPES = """
SELECT asset_type, count(*) AS txns,
       count(tkr) AS with_ticker,
       count(DISTINCT last_name) AS senators,
       sum(amount_low) AS min_dollars
FROM trades GROUP BY asset_type ORDER BY txns DESC
"""

BY_TYPE = """
SELECT txn_date, last_name, tkr, asset_name, tx_type, amount_low, amount_high
FROM trades WHERE asset_type = ? ORDER BY txn_date DESC LIMIT {limit}
"""


def arg(flag, default=None):
    """Value following `flag` in argv, or default."""
    return sys.argv[sys.argv.index(flag) + 1] if flag in sys.argv else default


def main():
    pd.set_option("display.width", 200, "display.max_columns", 50,
                  "display.max_colwidth", 45)
    con = duckdb.connect(DB_PATH, read_only=True)

    if "--types" in sys.argv:
        sql, params = TYPES, []
    elif "--type" in sys.argv:
        want = arg("--type")
        known = [r[0] for r in con.execute(
            "SELECT DISTINCT asset_type FROM trades WHERE asset_type IS NOT NULL"
        ).fetchall()]
        # case-insensitive match so --type stock works
        hit = next((k for k in known if k.lower() == want.lower()), None)
        if not hit:
            sys.exit(f"unknown asset type {want!r}\nknown: {', '.join(sorted(known))}")
        sql = BY_TYPE.format(limit=int(arg("--limit", 50)))
        params = [hit]
    elif len(sys.argv) > 1 and not sys.argv[1].startswith("--"):
        sql, params = sys.argv[1], []
    else:
        sql, params = SUMMARY, []

    print(con.execute(sql, params).df().to_string())


if __name__ == "__main__":
    main()
