"""Fill in missing tickers. Most filings already put the ticker in the asset name.

eFD leaves the ticker column blank on many rows, but the free-text asset_name
usually ends with the symbol in parentheses:

    "Roper Technologies, Inc. - Common Stock (ROP)"  -> ROP

So this is an extraction, not a guess. An earlier version fuzzy-matched names
against SEC's company list instead and was wrong in dangerous ways -- it mapped
"ABB Ltd. (ABBNY)" to ABLZF and "Everpure, Inc. Class A (PSTG)" to P, i.e. a
different security. Reading the parentheses is both simpler and correct.

Guesses go in NEW columns (ticker_guess / ticker_guess_how); the scraped `ticker`
is never overwritten. Query `coalesce(ticker, ticker_guess)` to use both.

    py resolve_tickers.py            # extract and write
    py resolve_tickers.py --dry      # show what it would do, write nothing
    py resolve_tickers.py --selftest # offline checks
"""

import re
import sys

import duckdb

DB_PATH = "congress_trades.duckdb"

# Two layouts appear in the wild:
#   trailing parens -- "Roper Technologies, Inc. - Common Stock (ROP)"
#   leading prefix  -- "ACN - Accenture plc Class A Ordinary Shares (Ireland)"
PAREN_RE = re.compile(r"\(([A-Z][A-Z.\-]{0,5})\)\s*$")
PREFIX_RE = re.compile(r"^([A-Z][A-Z.\-]{0,5})\s+-\s+")

# Parentheticals that are shaped like tickers but aren't.
NOT_TICKERS = {"ADR", "ADS", "ETF", "REIT", "LLC", "LP", "INC", "THE", "USA", "NEW"}


def extract(asset_name):
    """Return the ticker embedded in asset_name, or None."""
    s = (asset_name or "").strip()
    m = PAREN_RE.search(s) or PREFIX_RE.match(s)
    if not m:
        return None
    tk = m.group(1)
    return None if tk in NOT_TICKERS else tk


def main(dry=False):
    con = duckdb.connect(DB_PATH)
    for col in ("ticker_guess VARCHAR", "ticker_guess_how VARCHAR"):
        con.execute(f"ALTER TABLE senate_trades ADD COLUMN IF NOT EXISTS {col}")

    todo = [r[0] for r in con.execute(
        """SELECT DISTINCT asset_name FROM senate_trades
           WHERE ticker IS NULL AND asset_type = 'Stock'"""
    ).fetchall()]

    hits = [(n, tk) for n in todo if (tk := extract(n))]
    misses = [n for n in todo if not extract(n)]

    for name, tk in hits[:15]:
        print(f"  {tk:6} <- {name[:65]}")
    print(f"\nextracted {len(hits)} of {len(todo)} names")
    if misses:
        print(f"\n{len(misses)} with no ticker in the name (left NULL, inspect by hand):")
        for n in misses:
            print(f"  - {n[:75]}")

    if dry:
        print("\n(dry run, nothing written)")
        return
    if hits:
        con.executemany(
            """UPDATE senate_trades SET ticker_guess = ?, ticker_guess_how = 'paren'
               WHERE asset_name = ? AND ticker IS NULL""",
            [(tk, n) for n, tk in hits],
        )
        rows, names = con.execute(
            """SELECT count(*), count(DISTINCT asset_name) FROM senate_trades
               WHERE ticker_guess IS NOT NULL"""
        ).fetchone()
        print(f"\n{rows} rows ({names} names) now carry a ticker_guess")


def selftest():
    assert extract("Roper Technologies, Inc. - Common Stock (ROP)") == "ROP"
    assert extract("Loews Corporation (L)") == "L"
    assert extract("Bank of New York Mellon Corp (BK)") == "BK"
    assert extract("Everpure, Inc. Class A (PSTG)") == "PSTG"
    assert extract("Seven & I Holdings Co Ltd ADR (SVNDY)") == "SVNDY"
    assert extract("ACN - Accenture plc Class A Ordinary Shares (Ireland)") == "ACN"
    assert extract("SCHP - Schwab U.S. TIPS ETF") == "SCHP"
    # only a trailing paren counts -- a mid-string aside is not the ticker
    assert extract("Ansett Aerospace Holdings LLC (Melbourne, Australia)") is None
    # a company name that merely contains a dash must not read as a prefix
    assert extract("Rolls-Royce Holdings plc Sponsored ADR") is None
    assert extract("Qualcomm Inc") is None
    assert extract("Some Fund ADR (ADR)") is None, "'ADR' is not a ticker"
    assert extract("") is None and extract(None) is None
    print("selftest ok")


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
    else:
        main(dry="--dry" in sys.argv)
