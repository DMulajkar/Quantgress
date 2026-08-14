"""Entity resolution across every Quantgress dataset -- one engine, per-source
adapters. Generalizes resolve_tickers.py (Phase 3) now that Phase 6 (lobbying)
and Phase 7 (gov contracts) proved the problem recurs with a different key per
dataset: asset_name for congress trades, client_name for lobbying, recipient
name for contracts, and it won't stop there (Phase 9+).

Two resolution strategies, ranked by trust exactly like Phase 3 taught:
  1. "extract" -- the ticker is already embedded in the text (congress trades'
     asset_name has it in parens). Just read it out. This is resolve_tickers.py's
     original logic, moved here unchanged.
  2. "sec_name" -- normalize a free-text company name and look it up against
     SEC's own public company list, exact match only after normalization. No
     fuzzy matching -- that's what mapped "ABB Ltd." to the wrong security
     (ABLZF instead of ABBNY) in Phase 3's first, deleted version. An
     ambiguous normalized name (two real tickers collapsing to one string) is
     dropped rather than guessed at, same caution.

Every source writes to its own `<col>_guess` / `<col>_guess_how` columns,
never overwriting a real scraped value -- same safety property as Phase 3.

    py entities.py            # resolve every registered source, write guesses
    py entities.py --dry      # preview, write nothing
    py entities.py --selftest # offline checks, no network
"""

import re
import sys
from collections import Counter

import duckdb
import requests

from schema import DB_PATH, ensure_schema

# Types whose rows genuinely have a symbol. Bonds, munis, real property and
# private LLCs do not, so a NULL ticker there is correct, not a parse failure.
CONGRESS_TICKERED = ("(asset_type IS NULL OR asset_type ILIKE 'stock%'"
                      " OR asset_type ILIKE 'exchange traded%')")

# Two layouts appear in the wild:
#   trailing parens -- "Roper Technologies, Inc. - Common Stock (ROP)"
#   leading prefix  -- "ACN - Accenture plc Class A Ordinary Shares (Ireland)"
PAREN_RE = re.compile(r"\(([A-Z][A-Z.\-]{0,5})\)\s*$")
PREFIX_RE = re.compile(r"^([A-Z][A-Z.\-]{0,5})\s+-\s+")
# Pre-2018 House PDFs render small caps as lowercase glyphs, so a ticker
# arrives as "RoP" or "aaPl" -- see resolve_tickers.py's original note.
SMALLCAPS_RE = re.compile(r"\(([A-Za-z][A-Za-z.\-]{1,5})\)\s*$")
NOT_TICKERS = {"ADR", "ADS", "ETF", "REIT", "LLC", "LP", "INC", "THE", "USA", "NEW",
               "SOLD", "OWNER", "CLASS", "FUND", "TRUST", "BOND", "NOTE", "PLC",
               "CORP", "LTD", "COMMON", "JOINT", "YES", "NO", "IPO"}


def extract_congress(asset_name):
    """Return (ticker, how) embedded in a congress trade's asset_name, or None."""
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


# Legal-form words that make two spellings of the same company look different
# ("Lockheed Martin Corp" vs "Lockheed Martin Corporation"). Stripped before
# comparing, never used to infer a match on their own.
_LEGAL_SUFFIX_RE = re.compile(
    r"\b(INCORPORATED|CORPORATION|COMPANY|LIMITED|HOLDINGS?|GROUP|TRUST|"
    r"INC|CORP|CO|LLC|LTD|LP|LLP|PLC)\b\.?")


def normalize_name(name):
    s = (name or "").upper()
    s = re.sub(r"[.,&']", "", s)
    s = _LEGAL_SUFFIX_RE.sub("", s)
    return re.sub(r"\s+", " ", s).strip()


def load_sec_names(data):
    """{normalized company name: ticker} from a parsed company_tickers.json
    dict. Pure function of the data -- the network fetch is a separate,
    thin wrapper below so this half is testable offline."""
    names = [(normalize_name(v["title"]), v["ticker"]) for v in data.values()]
    counts = Counter(n for n, _ in names)
    return {n: tk for n, tk in names if counts[n] == 1}


def fetch_sec_data(session):
    r = session.get("https://www.sec.gov/files/company_tickers.json", timeout=30)
    r.raise_for_status()
    return r.json()


def make_sec_resolver(lookup):
    def resolver(name):
        tk = lookup.get(normalize_name(name))
        return None if not tk else (tk, "sec_name_exact")
    return resolver


def _session():
    s = requests.Session()
    # SEC 403s on a generic UA -- unlike LDA/USAspending, it insists on a
    # contact email in the string specifically, confirmed live (a UA without
    # one, "quantgress/0.1 (personal research)", 403'd where this doesn't).
    s.headers["User-Agent"] = "quantgress/0.1 (mmulajkar@gmail.com)"
    return s


# One row per resolvable (table, column). Congress trades reuse Phase 3's
# extraction unchanged; lobbying and gov contracts are new sec_name adapters.
# real_col is the scraped column a guess must not clobber -- None where the
# source never had one to begin with (lobbying/contracts have no ticker field
# at all, only a free-text name).
SOURCES = [
    dict(table="senate_trades", name_col="asset_name", real_col="ticker",
         guess_col="ticker_guess", how_col="ticker_guess_how",
         extra_where=CONGRESS_TICKERED, kind="extract"),
    dict(table="house_trades", name_col="asset_name", real_col="ticker",
         guess_col="ticker_guess", how_col="ticker_guess_how",
         extra_where=CONGRESS_TICKERED, kind="extract"),
    dict(table="lobbying_filings", name_col="client_name", real_col=None,
         guess_col="client_ticker_guess", how_col="client_ticker_guess_how",
         extra_where=None, kind="sec_name"),
    dict(table="gov_contracts", name_col="recipient_name", real_col=None,
         guess_col="recipient_ticker_guess", how_col="recipient_ticker_guess_how",
         extra_where=None, kind="sec_name"),
    # Phase 10 (13F): INFOTABLE has no ticker/symbol field at all, only free-text
    # issuer_name -- same shape as lobbying/contracts, not Phase 9's free lookup.
    dict(table="f13_holdings", name_col="issuer_name", real_col=None,
         guess_col="issuer_ticker_guess", how_col="issuer_ticker_guess_how",
         extra_where=None, kind="sec_name"),
    # Phase 12: assignee_name is a raw company name off the grant record,
    # same shape as lobbying/contracts -- no ticker field to begin with.
    dict(table="patents", name_col="assignee_name", real_col=None,
         guess_col="assignee_ticker_guess", how_col="assignee_ticker_guess_how",
         extra_where=None, kind="sec_name"),
    # Phase 13: contributor_name is a committee/PAC name ("ACME WIDGET CORP
    # PAC"), not the bare company name SEC's list has -- expect a lower hit
    # rate than Phase 6/7/12 until normalize_name learns to strip PAC-style
    # suffixes too. Still safe: an unmatched name is dropped, never guessed.
    dict(table="corporate_donations", name_col="contributor_name", real_col=None,
         guess_col="contributor_ticker_guess", how_col="contributor_ticker_guess_how",
         extra_where=None, kind="sec_name"),
]


def resolve(con, src, resolver, dry):
    table, name_col = src["table"], src["name_col"]
    guess_col, how_col = src["guess_col"], src["how_col"]
    where = [f"{guess_col} IS NULL", f"{name_col} IS NOT NULL"]
    if src["real_col"]:
        where.append(f"{src['real_col']} IS NULL")
    if src["extra_where"]:
        where.append(src["extra_where"])
    todo = con.execute(
        f"""SELECT {name_col}, count(*) AS n FROM {table}
            WHERE {' AND '.join(where)} GROUP BY 1 ORDER BY n DESC, 1"""
    ).fetchall()

    label = f"{table}.{name_col}"
    print(f"\n=== {label} " + "=" * max(1, 46 - len(label)))
    if not todo:
        print("  nothing left to resolve")
        return

    hits = [(n, rows, *e) for n, rows in todo if (e := resolver(n))]
    misses = [(n, rows) for n, rows in todo if not resolver(n)]
    by_how = Counter(how for *_, how in hits)

    print(f"  {len(todo)} unresolved names / {sum(r for _, r in todo)} rows\n")
    print(f"  RESOLVED   {len(hits)} names / {sum(h[1] for h in hits)} rows"
          + f"   ({', '.join(f'{k} {v}' for k, v in by_how.most_common())})")
    for name, rows, tk, how in hits[:12]:
        print(f"    {tk:<7} {how:<14} x{rows:<4} {name[:54]}")
    if len(hits) > 12:
        print(f"    ... and {len(hits) - 12} more")

    if misses:
        print(f"\n  NO MATCH   {len(misses)} names / {sum(r for _, r in misses)} rows, left NULL")
        for name, rows in misses[:20]:
            print(f"    {'':<7} {'':<14} x{rows:<4} {name[:54]}")
        if len(misses) > 20:
            print(f"    ... and {len(misses) - 20} more")

    if dry or not hits:
        return
    con.executemany(
        f"""UPDATE {table} SET {guess_col} = ?, {how_col} = ?
            WHERE {name_col} = ? AND {guess_col} IS NULL""",
        [(tk, how, n) for n, _, tk, how in hits],
    )
    rows, names = con.execute(
        f"""SELECT count(*), count(DISTINCT {name_col}) FROM {table}
            WHERE {guess_col} IS NOT NULL"""
    ).fetchone()
    print(f"\n  WROTE      {table} now has {rows} rows ({names} names) with a {guess_col}")


def main(dry=False):
    con = duckdb.connect(DB_PATH)
    ensure_schema(con)  # congress tables + trades view

    resolvers = {"extract": extract_congress}
    if any(s["kind"] == "sec_name" for s in SOURCES):
        resolvers["sec_name"] = make_sec_resolver(load_sec_names(fetch_sec_data(_session())))

    for src in SOURCES:
        for col in (src["guess_col"], src["how_col"]):
            con.execute(f"ALTER TABLE {src['table']} ADD COLUMN IF NOT EXISTS {col} VARCHAR")
        resolve(con, src, resolvers[src["kind"]], dry)
    if dry:
        print("\n(dry run, nothing written)")


def selftest():
    # congress extraction -- unchanged from resolve_tickers.py
    assert extract_congress("Roper Technologies, Inc. - Common Stock (ROP)") == ("ROP", "paren")
    assert extract_congress("Loews Corporation (L)") == ("L", "paren")
    assert extract_congress("Bank of New York Mellon Corp (BK)") == ("BK", "paren")
    assert extract_congress("Everpure, Inc. Class A (PSTG)") == ("PSTG", "paren")
    assert extract_congress("Seven & I Holdings Co Ltd ADR (SVNDY)") == ("SVNDY", "paren")
    assert extract_congress("ACN - Accenture plc Class A Ordinary Shares (Ireland)") == ("ACN", "paren")
    assert extract_congress("SCHP - Schwab U.S. TIPS ETF") == ("SCHP", "paren")
    assert extract_congress("Roper Technologies, Inc. (RoP)") == ("ROP", "smallcaps")
    assert extract_congress("Vanguard Mega Cap growth ETF (MgK)") == ("MGK", "smallcaps")
    assert extract_congress("sP apple Inc. (aaPl)") == ("AAPL", "smallcaps")
    for word in ("(The)", "(New)", "(Sold)", "(Owner)", "(Class)", "(Trust)"):
        assert extract_congress(f"Something Inc. {word}") is None, word
    assert extract_congress("Some Fund (a)") is None, "one letter is a footnote mark, not a ticker"
    assert extract_congress("Ansett Aerospace Holdings LLC (Melbourne, Australia)") is None
    assert extract_congress("Rolls-Royce Holdings plc Sponsored ADR") is None
    assert extract_congress("Qualcomm Inc") is None
    assert extract_congress("Some Fund ADR (ADR)") is None, "'ADR' is not a ticker"
    assert extract_congress("") is None and extract_congress(None) is None

    # sec_name normalization + exact matching, offline
    assert normalize_name("Apple Inc.") == "APPLE"
    assert normalize_name("Lockheed Martin Corporation") == "LOCKHEED MARTIN"
    assert normalize_name("LOCKHEED MARTIN CORP") == "LOCKHEED MARTIN"

    sample_sec = {
        "0": {"cik_str": 1, "ticker": "LMT", "title": "Lockheed Martin Corp"},
        "1": {"cik_str": 2, "ticker": "AAPL", "title": "Apple Inc."},
        # two different real companies collapsing to the same normalized
        # name -- ambiguous, must be dropped rather than guessed at
        "2": {"cik_str": 3, "ticker": "FOO", "title": "Ambiguous Co"},
        "3": {"cik_str": 4, "ticker": "BAR", "title": "Ambiguous Co."},
    }
    lookup = load_sec_names(sample_sec)
    assert lookup["LOCKHEED MARTIN"] == "LMT"
    assert lookup["APPLE"] == "AAPL"
    assert "AMBIGUOUS" not in lookup, "colliding normalized names must be dropped, not guessed"

    resolver = make_sec_resolver(lookup)
    assert resolver("Lockheed Martin Corp") == ("LMT", "sec_name_exact")
    assert resolver("LOCKHEED MARTIN CORPORATION") == ("LMT", "sec_name_exact")
    assert resolver("Totally Unknown Company LLC") is None
    assert resolver("Ambiguous Co") is None

    print("selftest ok")


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
    else:
        main(dry="--dry" in sys.argv)
