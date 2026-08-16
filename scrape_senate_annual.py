"""Phase 18 of Quantgress: Senate Annual Financial Disclosure reports --
Schedule A (every asset: bank accounts, real estate, retirement accounts,
mutual funds, stocks -- not just disclosed trades) and Schedule D
(liabilities: mortgages, loans, lines of credit).

Scoped to the Senate only, per the human's own framing of the ask ("a more
accurate net worth tracker for the senators"). House's equivalent report is
a PDF (same parsing-difficulty class as Phase 2's House PTRs) -- not
requested here, not built.

Same efdsearch.senate.gov site and session handshake scrape_senate.py
already built (reused via import, not copy-pasted). One filing type over:
an Annual Report page renders as clean, pre-labeled HTML tables (Asset /
Asset Type / Owner / Value / Income Type / Income for Schedule A; Incurred /
Debtor / Type / Amount / Creditor for Schedule D) -- no OCR, no PDF
line-wrapping pain. Confirmed live against a real filing (Sen. Katie Britt's
CY2025 report) before writing the parser.

There is no dedicated "Annual Report" report_types filter confirmed live --
eFD's search form only exposes report_types as an opaque numeric list, and
report_types=[] mixes PTRs, Candidate Reports, and Annual Reports in one
feed. Filtered client-side instead on the office string ("(Senator)", not
"(Candidate)"/"(Former Senator)") and the report link's own label text
("Annual Report for CY ...") -- deterministic, no guessed magic number.

Usage:
    py scrape_senate_annual.py --selftest        # offline checks, no network
    py scrape_senate_annual.py --limit 20        # bounded run, 20 filings
    py scrape_senate_annual.py                   # every sitting senator's
                                                  # most recent Annual Report
    py scrape_senate_annual.py --since-year 2024 # widen the submission window

Default window is the current calendar year: every senator must file their
Annual Report (covering the prior year) by May 15, so a Jan-1-to-today
window is guaranteed to catch each sitting senator's current filing without
walking multiple years of PTR noise to find it.

Re-running skips report_ids already stored, same resume pattern as every
other scraper here.
"""

import datetime
import io
import re
import sys
import time

import duckdb
import pandas as pd
from bs4 import BeautifulSoup

from entities import extract_congress
from schema import DB_PATH, parse_amount
from scrape_senate import ROOT, REPORTS, SEARCH, _session

ANNUAL_LABEL_RE = re.compile(r"^Annual Report for CY (\d{4})")
NONE_OR_LESS_RE = re.compile(r"None \(or less than \$([\d,]+)\)")
RATE_LIMIT_SECS = 2  # same courtesy delay as scrape_senate.py, same host


def _parse_value(text):
    """Same bracket parsing as schema.parse_amount, plus one phrasing that's
    unique to Annual Report Value/Income cells and not used anywhere in the
    PTR amount_raw text schema.parse_amount was built for: "None (or less
    than $1,001)" means the value is BELOW that number, not a floor at it --
    parse_amount alone would misread the embedded "$1,001" as a floor,
    the opposite of what the phrase says. Handled here, not in
    schema.parse_amount, so Phase 1-3's tested PTR parsing is untouched."""
    m = NONE_OR_LESS_RE.search(text)
    if m:
        return 0, int(m.group(1).replace(",", "")) - 1
    return parse_amount(text)

ASSETS_TABLE = "fd_assets"
LIABILITIES_TABLE = "fd_liabilities"
ASSET_COLS = {"Asset", "Asset Type", "Owner", "Value"}
LIABILITY_COLS = {"Debtor", "Creditor", "Amount"}


def ensure_tables(con):
    con.execute(f"""CREATE TABLE IF NOT EXISTS {ASSETS_TABLE} (
        report_id VARCHAR, line_num INTEGER, chamber VARCHAR,
        last_name VARCHAR, first_name VARCHAR,
        filing_label VARCHAR, filing_year INTEGER, filed_date VARCHAR,
        asset_name VARCHAR, asset_type VARCHAR, owner VARCHAR,
        value_low BIGINT, value_high BIGINT, value_raw VARCHAR,
        ticker VARCHAR, ticker_how VARCHAR,
        PRIMARY KEY (report_id, line_num))""")
    con.execute(f"""CREATE TABLE IF NOT EXISTS {LIABILITIES_TABLE} (
        report_id VARCHAR, line_num INTEGER, chamber VARCHAR,
        last_name VARCHAR, first_name VARCHAR,
        filing_label VARCHAR, filing_year INTEGER, filed_date VARCHAR,
        creditor VARCHAR, liability_type VARCHAR, owner VARCHAR,
        value_low BIGINT, value_high BIGINT, value_raw VARCHAR,
        PRIMARY KEY (report_id, line_num))""")


def _find_table(tables, required_cols):
    """First table whose columns are a superset of required_cols -- lets
    Schedule A/D be picked by shape, not position, since a filer with fewer
    disclosed schedules renders fewer tables and shifts everyone after it."""
    for t in tables:
        if required_cols <= {str(c).strip() for c in t.columns}:
            return t
    return None


def parse_assets(html):
    """Schedule A rows -> list of dicts. Container/summary rows (Value ==
    '--', e.g. an account name with sub-holdings listed under it) are
    skipped -- they hold no value of their own, and counting them would
    double the value already counted in their child rows."""
    t = _find_table(pd.read_html(io.StringIO(html)), ASSET_COLS)
    if t is None:
        return []
    out = []
    for _, row in t.iterrows():
        value = str(row.get("Value", "")).strip()
        if value in ("--", "nan", ""):
            continue
        low, high = _parse_value(value)
        name = str(row.get("Asset", "")).strip()
        ticker, how = extract_congress(name) or (None, None)
        out.append({"asset_name": name, "asset_type": str(row.get("Asset Type", "")).strip(),
                     "owner": str(row.get("Owner", "")).strip(),
                     "value_low": low, "value_high": high, "value_raw": value,
                     "ticker": ticker, "ticker_how": how})
    return out


def parse_liabilities(html):
    t = _find_table(pd.read_html(io.StringIO(html)), LIABILITY_COLS)
    if t is None:
        return []
    out = []
    for _, row in t.iterrows():
        amount = str(row.get("Amount", "")).strip()
        low, high = _parse_value(amount)
        out.append({"creditor": str(row.get("Creditor", "")).strip(),
                     "liability_type": str(row.get("Type", "")).strip(),
                     "owner": str(row.get("Debtor", "")).strip(),
                     "value_low": low, "value_high": high, "value_raw": amount})
    return out


def list_senator_annual_reports(s, since_year, limit=None):
    """Yield (first, last, report_id, label, filing_year, filed) for every
    sitting senator's Annual Report filed since Jan 1 of since_year --
    originals and amendments alike; a later filed_date for the same senator
    is picked up as "most recent" downstream, no special-casing here."""
    offset = 0
    seen = 0
    start_date = f"01/01/{since_year} 00:00:00"
    while True:
        time.sleep(RATE_LIMIT_SECS)
        rows = s.post(
            REPORTS,
            data={"start": str(offset), "length": "100",
                  "report_types": "[]", "filer_types": "[]",
                  "submitted_start_date": start_date, "submitted_end_date": "",
                  "candidate_state": "", "senator_state": "", "office_id": "",
                  "first_name": "", "last_name": "", "csrfmiddlewaretoken": s.csrf},
            headers={"Referer": SEARCH},
        ).json()["data"]
        if not rows:
            return
        for first, last, office, report_html, filed in rows:
            if "(Senator)" not in office:
                continue
            a = BeautifulSoup(report_html, "html.parser").a
            m = ANNUAL_LABEL_RE.match(a.text.strip())
            if not m:
                continue
            report_id = a["href"].rstrip("/").rsplit("/", 1)[-1]
            yield first.strip(), last.strip(), report_id, a.text.strip(), int(m.group(1)), filed.strip()
            seen += 1
            if limit is not None and seen >= limit:
                return
        offset += 100


def main(since_year=None, limit=None):
    since_year = since_year or datetime.date.today().year
    # ponytail: con.close() in a finally from day one -- Phase 10/11 already
    # found what skipping this costs (WAL-only data lost on a killed run).
    con = duckdb.connect(DB_PATH)
    try:
        ensure_tables(con)
        done = {r[0] for r in con.execute(f"SELECT DISTINCT report_id FROM {ASSETS_TABLE}").fetchall()}

        s = _session()
        fetched = skipped = 0
        for first, last, report_id, label, filing_year, filed in list_senator_annual_reports(s, since_year, limit):
            if report_id in done:
                skipped += 1
                continue
            time.sleep(RATE_LIMIT_SECS)
            r = s.get(f"{ROOT}/search/view/annual/{report_id}/")
            assets = parse_assets(r.text)
            liabilities = parse_liabilities(r.text)
            for i, a in enumerate(assets):
                con.execute(f"INSERT INTO {ASSETS_TABLE} VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    [report_id, i, "S", last, first, label, filing_year, filed,
                     a["asset_name"], a["asset_type"], a["owner"],
                     a["value_low"], a["value_high"], a["value_raw"], a["ticker"], a["ticker_how"]])
            for i, l in enumerate(liabilities):
                con.execute(f"INSERT INTO {LIABILITIES_TABLE} VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    [report_id, i, "S", last, first, label, filing_year, filed,
                     l["creditor"], l["liability_type"], l["owner"],
                     l["value_low"], l["value_high"], l["value_raw"]])
            fetched += 1
            print(f"  {last}, {first} -- {label}: {len(assets)} assets, {len(liabilities)} liabilities")
        print(f"\n{fetched} annual reports fetched, {skipped} already stored")
    finally:
        con.close()


def selftest():
    # table selection picks the right table by column shape, even when a
    # decoy (transactions-shaped) table sits in front of it and the filer's
    # own row order shifts which table index things land on
    fixture = """
    <table><tr><th>Owner</th><th>Ticker</th><th>Asset Name</th></tr>
    <tr><td>Self</td><td>AAPL</td><td>Apple Inc.</td></tr></table>
    <table><tr><th>Asset</th><th>Asset Type</th><th>Owner</th><th>Value</th><th>Income Type</th><th>Income</th></tr>
    <tr><td>Fidelity IRA</td><td>Retirement PlansIRA</td><td>Self</td><td>--</td><td></td><td></td></tr>
    <tr><td>AAPL - Apple Inc. - Common Stock</td><td>Corporate SecuritiesStock</td><td>Self</td><td>$100,001 - $250,000</td><td></td><td>None</td></tr>
    <tr><td>Checking Account</td><td>Bank Accounts</td><td>Spouse</td><td>$1,001 - $15,000</td><td></td><td>None</td></tr>
    </table>
    <table><tr><th>Incurred</th><th>Debtor</th><th>Type</th><th>Amount</th><th>Creditor</th></tr>
    <tr><td>2020</td><td>Joint</td><td>Mortgage</td><td>$250,001 - $500,000</td><td>Regions Mortgage</td></tr>
    </table>
    """
    assets = parse_assets(fixture)
    assert len(assets) == 2  # the "--" IRA container row is dropped
    assert assets[0]["asset_name"] == "AAPL - Apple Inc. - Common Stock"
    assert assets[0]["ticker"] == "AAPL" and assets[0]["ticker_how"] == "paren"
    assert assets[0]["value_low"] == 100001 and assets[0]["value_high"] == 250000
    assert assets[1]["owner"] == "Spouse" and assets[1]["ticker"] is None

    # "None (or less than $1,001)" means below that number, not a floor at
    # it -- confirmed live in a real filing (Sen. Britt's CY2025 report,
    # a Block Inc. holding), the opposite reading from a plain parse_amount
    assert _parse_value("None (or less than $1,001)") == (0, 1000)
    assert _parse_value("None (or less than $201)") == (0, 200)  # same phrasing, income brackets
    assert _parse_value("$100,001 - $250,000") == (100001, 250000)  # ordinary bracket still works

    liabilities = parse_liabilities(fixture)
    assert len(liabilities) == 1
    assert liabilities[0]["creditor"] == "Regions Mortgage"
    assert liabilities[0]["value_low"] == 250001 and liabilities[0]["value_high"] == 500000

    # a page with neither schedule (e.g. "None disclosed") returns [], not a crash
    assert parse_assets("<table><tr><th>Ticker</th></tr></table>") == []
    assert parse_liabilities("<table><tr><th>Ticker</th></tr></table>") == []

    # label matching: real annual reports and their amendments match;
    # candidate reports and PTRs (same site, same /search/view/annual/ URL
    # prefix for candidates) do not
    assert ANNUAL_LABEL_RE.match("Annual Report for CY 2025").group(1) == "2025"
    assert ANNUAL_LABEL_RE.match("Annual Report for CY 2025 (Amendment 1)").group(1) == "2025"
    assert ANNUAL_LABEL_RE.match("Candidate Report") is None
    assert ANNUAL_LABEL_RE.match("Periodic Transaction Report for 08/14/2026") is None

    print("selftest ok")


def arg(flag, default=None):
    return sys.argv[sys.argv.index(flag) + 1] if flag in sys.argv else default


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
    else:
        y = arg("--since-year")
        n = arg("--limit")
        main(since_year=int(y) if y else None, limit=int(n) if n else None)
