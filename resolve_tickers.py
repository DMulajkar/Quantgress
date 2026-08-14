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
from collections import Counter

import duckdb

from schema import DB_PATH, TABLES, ensure_schema

# Types whose rows genuinely have a symbol. Bonds, munis, real property and
# private LLCs do not, so a NULL ticker there is correct, not a parse failure.
# Chambers word these differently: Senate 'Stock', House 'Stocks (including
# ADRs)' / 'Exchange Traded Funds (ETF)'. Pre-2018 House filings print no asset
# code at all, so NULL has to be allowed through or none of them resolve --
# safe, because extraction reads a symbol the filer typed rather than guessing
# one, and a CUSIP or a parenthetical aside does not match as a ticker.
TICKERED = ("(asset_type IS NULL OR asset_type ILIKE 'stock%'"
            " OR asset_type ILIKE 'exchange traded%')")

# Two layouts appear in the wild:
#   trailing parens -- "Roper Technologies, Inc. - Common Stock (ROP)"
#   leading prefix  -- "ACN - Accenture plc Class A Ordinary Shares (Ireland)"
PAREN_RE = re.compile(r"\(([A-Z][A-Z.\-]{0,5})\)\s*$")
PREFIX_RE = re.compile(r"^([A-Z][A-Z.\-]{0,5})\s+-\s+")

# Pre-2018 House PDFs set the form in small caps and pdfplumber emits those
# glyphs as lowercase, so a ticker arrives as "RoP", "aaPl" or "DIs". Two font
# variants exist -- one corrupts only a/b/d/g/o/u, the other every letter -- so
# there is no tight letter rule to lean on, and the blocklist below is what
# does the work instead.
#
# The failure mode here is not the one that killed Phase 3's fuzzy matching.
# That mapped a real name to a *different real security* (ABBNY -> ABLZF), which
# is invisible in a query. Worst case here is a non-word like (Sold) becoming
# "SOLD", which matches no security and shows up as obvious garbage. Every one
# is tagged ticker_guess_how='smallcaps', so the whole class is auditable and
# reversible with one UPDATE. Single characters are refused: "(a)" is far more
# likely a footnote mark than Agilent.
SMALLCAPS_RE = re.compile(r"\(([A-Za-z][A-Za-z.\-]{1,5})\)\s*$")

# Parentheticals that are shaped like tickers but aren't.
NOT_TICKERS = {"ADR", "ADS", "ETF", "REIT", "LLC", "LP", "INC", "THE", "USA", "NEW",
               "SOLD", "OWNER", "CLASS", "FUND", "TRUST", "BOND", "NOTE", "PLC",
               "CORP", "LTD", "COMMON", "JOINT", "YES", "NO", "IPO"}


def extract(asset_name):
    """Return (ticker, how) embedded in asset_name, or None."""
    s = (asset_name or "").strip()
    m = PAREN_RE.search(s) or PREFIX_RE.match(s)
    how = "paren"
    if not m:
        m = SMALLCAPS_RE.search(s)
        if not m:
            return None
        how = "smallcaps"
    tk = m.group(1).upper()
    return None if tk in NOT_TICKERS else (tk, how)


def resolve(con, table, dry):
    todo = con.execute(
        f"""SELECT asset_name, count(*) AS n FROM {table}
            WHERE ticker IS NULL AND ticker_guess IS NULL AND {TICKERED}
            GROUP BY 1 ORDER BY n DESC, 1"""
    ).fetchall()
    print(f"\n=== {table} " + "=" * (46 - len(table)))
    if not todo:
        print("  nothing left to resolve")
        return

    hits = [(n, rows, *e) for n, rows in todo if (e := extract(n))]
    misses = [(n, rows) for n, rows in todo if not extract(n)]
    by_how = Counter(how for *_, how in hits)

    print(f"  {len(todo)} unresolved names / {sum(r for _, r in todo)} rows\n")
    print(f"  RESOLVED   {len(hits)} names / {sum(h[1] for h in hits)} rows"
          + f"   ({', '.join(f'{k} {v}' for k, v in by_how.most_common())})")
    for name, rows, tk, how in hits[:12]:
        print(f"    {tk:<7} {how:<10} x{rows:<4} {name[:58]}")
    if len(hits) > 12:
        print(f"    ... and {len(hits) - 12} more")

    if misses:
        print(f"\n  NO TICKER IN THE NAME   {len(misses)} names /"
              f" {sum(r for _, r in misses)} rows, left NULL")
        for name, rows in misses[:20]:
            print(f"    {'':<7} {'':<10} x{rows:<4} {name[:58]}")
        if len(misses) > 20:
            print(f"    ... and {len(misses) - 20} more")

    if dry or not hits:
        return
    con.executemany(
        f"""UPDATE {table} SET ticker_guess = ?, ticker_guess_how = ?
            WHERE asset_name = ? AND ticker IS NULL""",
        [(tk, how, n) for n, _, tk, how in hits],
    )
    rows, names = con.execute(
        f"""SELECT count(*), count(DISTINCT asset_name) FROM {table}
            WHERE ticker_guess IS NOT NULL"""
    ).fetchone()
    print(f"\n  WROTE      {table} now has {rows} rows ({names} names) with a ticker_guess")


def main(dry=False):
    con = duckdb.connect(DB_PATH)
    ensure_schema(con)
    for table in TABLES:
        # upgrades a DB created before these columns existed
        for col in ("ticker_guess VARCHAR", "ticker_guess_how VARCHAR"):
            con.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {col}")
        resolve(con, table, dry)
    if dry:
        print("\n(dry run, nothing written)")


def selftest():
    assert extract("Roper Technologies, Inc. - Common Stock (ROP)") == ("ROP", "paren")
    assert extract("Loews Corporation (L)") == ("L", "paren")
    assert extract("Bank of New York Mellon Corp (BK)") == ("BK", "paren")
    assert extract("Everpure, Inc. Class A (PSTG)") == ("PSTG", "paren")
    assert extract("Seven & I Holdings Co Ltd ADR (SVNDY)") == ("SVNDY", "paren")
    assert extract("ACN - Accenture plc Class A Ordinary Shares (Ireland)") == ("ACN", "paren")
    assert extract("SCHP - Schwab U.S. TIPS ETF") == ("SCHP", "paren")

    # pre-2018 House small caps, both font variants
    assert extract("Roper Technologies, Inc. (RoP)") == ("ROP", "smallcaps")
    assert extract("Vanguard Mega Cap growth ETF (MgK)") == ("MGK", "smallcaps")
    assert extract("sP apple Inc. (aaPl)") == ("AAPL", "smallcaps")
    assert extract("Cliffs Natural Resources Inc (ClF)") == ("CLF", "smallcaps")
    assert extract("sP Walt Disney Company (DIs)") == ("DIS", "smallcaps")
    # the blocklist is what keeps ordinary trailing parentheticals out
    for word in ("(The)", "(New)", "(Sold)", "(Owner)", "(Class)", "(Trust)"):
        assert extract(f"Something Inc. {word}") is None, word
    assert extract("Some Fund (a)") is None, "one letter is a footnote mark, not a ticker"
    assert extract("Ansett Aerospace Holdings LLC (Melbourne, Australia)") is None
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
