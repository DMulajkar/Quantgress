"""Run a SQL query against the scraped congress trades DB.

    py q.py "SELECT * FROM senate_trades LIMIT 5"
    py q.py                                        # no args -> quick summary

Exists because PowerShell mangles quotes and '%' in `py -c "..."` one-liners.
"""

import sys

import duckdb
import pandas as pd

SUMMARY = """
SELECT last_name, count(*) AS txns,
       min(strptime(tx_date, '%m/%d/%Y')) AS earliest,
       max(strptime(tx_date, '%m/%d/%Y')) AS latest
FROM senate_trades GROUP BY last_name ORDER BY txns DESC
"""

sql = sys.argv[1] if len(sys.argv) > 1 else SUMMARY
pd.set_option("display.width", 200, "display.max_columns", 50,
              "display.max_colwidth", 40)
print(duckdb.connect("congress_trades.duckdb", read_only=True).execute(sql).df())
