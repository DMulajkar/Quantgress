"""Query the scraped congress trades.

    py q.py                        # summary by chamber + member
    py q.py --types                # what asset types exist, and how many
    py q.py --type Stock           # trades of one asset type
    py q.py --tickered             # only rows that have a ticker
    py q.py --type Stock --tickered --limit 100    # flags combine
    py q.py "SELECT ..."           # arbitrary SQL

--tickered hides rows without a ticker; it filters the view, it does not
delete anything. Bonds, munis and private holdings have no ticker to have,
so this is about 100 of 1,022 rows -- drop the flag to see them again.

Query the `trades` view, not the raw `senate_trades` / `house_trades` tables --
it has the recovered tickers coalesced in, real DATE columns, and both chambers.
The raw tables are still there if you want the untouched scraped values.

Exists because PowerShell mangles quotes and '%' in `py -c "..."` one-liners.
"""

import sys

import duckdb
import pandas as pd

# Windows' console codepage can't display some characters DocumentCloud's OCR
# left behind in Phase 17's data (stray U+FFFD replacement chars etc.) --
# without this, printing them crashes the whole query instead of just
# showing a substitute character.
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

DB_PATH = "congress_trades.duckdb"

# grouped by chamber too: last_name is not unique across the House and Senate
SUMMARY = """
SELECT chamber, last_name, count(*) AS txns, count(DISTINCT tkr) AS tickers,
       min(txn_date) AS first_trade, max(txn_date) AS last_trade,
       min(filed_date) AS first_filing, max(filed_date) AS last_filing
FROM trades GROUP BY chamber, last_name ORDER BY txns DESC
"""

TYPES = """
SELECT asset_type, count(*) AS txns,
       count(tkr) AS with_ticker,
       count(DISTINCT last_name) AS senators,
       sum(amount_low) AS min_dollars
FROM trades GROUP BY asset_type ORDER BY txns DESC
"""

LISTING = """
SELECT last_name, tkr, asset_name, tx_type, amount_low, amount_high,
       txn_date, filed_date, lag_days
FROM trades WHERE 1=1 {type_filter} {tickered}
ORDER BY txn_date DESC LIMIT {limit}
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
    elif "--type" in sys.argv or "--tickered" in sys.argv:
        params = []
        type_filter = ""
        if "--type" in sys.argv:
            want = arg("--type")
            known = [r[0] for r in con.execute(
                "SELECT DISTINCT asset_type FROM trades WHERE asset_type IS NOT NULL"
            ).fetchall()]
            # case-insensitive match so --type stock works
            hit = next((k for k in known if k.lower() == want.lower()), None)
            if not hit:
                sys.exit(f"unknown asset type {want!r}\n"
                         f"known: {', '.join(sorted(known))}")
            type_filter, params = "AND asset_type = ?", [hit]
        sql = LISTING.format(
            type_filter=type_filter,
            tickered="AND tkr IS NOT NULL" if "--tickered" in sys.argv else "",
            limit=int(arg("--limit", 50)),
        )
    elif len(sys.argv) > 1 and not sys.argv[1].startswith("--"):
        sql, params = sys.argv[1], []
    else:
        sql, params = SUMMARY, []

    print(con.execute(sql, params).df().to_string())


if __name__ == "__main__":
    main()
