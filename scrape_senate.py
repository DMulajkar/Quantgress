"""Scrape Senate periodic transaction reports (PTRs) from efdsearch.senate.gov into DuckDB.

Phase 1 of the Congress Trades project: Senate only. Senate PTRs render as HTML
tables, so no OCR is needed -- House PTRs are scanned PDFs and are a later phase.

Usage:
    py scrape_senate.py            # scrape everything into congress_trades.duckdb
    py scrape_senate.py --limit 20 # stop after 20 filings (full run takes hours)
    py scrape_senate.py --selftest # run parser checks, no network

Re-running skips filings recorded in `senate_filings`, so an interrupted run
resumes -- including filings that legitimately produced zero rows, which a
senate_trades-only check would re-fetch forever.
"""

import io
import re
import sys
import time

import duckdb
import pandas as pd
import requests
from bs4 import BeautifulSoup

from schema import DB_PATH, ensure_schema, parse_amount

ROOT = "https://efdsearch.senate.gov"
LANDING = f"{ROOT}/search/home/"
SEARCH = f"{ROOT}/search/"
REPORTS = f"{ROOT}/search/report/data/"

START_DATE = "01/01/2012 00:00:00"
BATCH = 100
RATE_LIMIT_SECS = 2  # be polite to a .gov host

# ponytail: three constants beat a config.yaml here; add one if this grows knobs


def _session():
    """Return a session that has accepted the eFD prohibition agreement."""
    s = requests.Session()
    s.headers["User-Agent"] = "congress-trades/0.1 (personal research)"
    r = s.get(LANDING)
    token = BeautifulSoup(r.text, "html.parser").find(
        attrs={"name": "csrfmiddlewaretoken"}
    )["value"]
    s.post(
        LANDING,
        data={"csrfmiddlewaretoken": token, "prohibition_agreement": "1"},
        headers={"Referer": LANDING},
    )
    s.csrf = s.cookies.get("csrftoken") or s.cookies["csrf"]
    return s


def _bounced(r):
    """True if eFD sent us back to the agreement page (session expired).

    The old check was `r.url == LANDING`, exact equality. Django appends
    `?next=/search/view/ptr/...` on an auth bounce, so an expiry mid-run slipped
    through and the tableless agreement page went straight into the parser.
    """
    return r.url.startswith(LANDING) or "prohibition_agreement" in r.text


def list_ptrs(s):
    """Yield (first, last, office, link, filed) for every PTR since START_DATE."""
    offset = 0
    while True:
        time.sleep(RATE_LIMIT_SECS)
        # A full backfill spreads ~25 of these POSTs over more than an hour, so
        # this generator outlives its session just like the fetch loop does.
        # It holds its own `s`, so main() re-handshaking cannot help it.
        for attempt in (1, 2):
            resp = s.post(
                REPORTS,
                data={
                    "start": str(offset),
                    "length": str(BATCH),
                    "report_types": "[11]",  # 11 = periodic transaction report
                    "filer_types": "[]",
                    "submitted_start_date": START_DATE,
                    "submitted_end_date": "",
                    "candidate_state": "",
                    "senator_state": "",
                    "office_id": "",
                    "first_name": "",
                    "last_name": "",
                    "csrfmiddlewaretoken": s.csrf,
                },
                headers={"Referer": SEARCH},
            )
            try:
                rows = resp.json()["data"]
                break
            except Exception:
                if attempt == 2:
                    raise
                s = _session()
        if not rows:
            return
        for first, last, office, report_html, filed in rows:
            href = BeautifulSoup(report_html, "html.parser").a["href"]
            yield first.strip(), last.strip(), office.strip(), href, filed
        offset += BATCH


def parse_ptr(html):
    """Return (rows, status) for a PTR page, mirroring house.parse_pdf.

    status is 'ok' (rows found), 'empty' (a table, but no transactions) or
    'no_table' (the page has no table at all -- not a filing page).

    flavor="lxml" is pinned deliberately. Left to itself pandas tries lxml then
    bs4, and the bs4 branch imports html5lib *outside* its try -- so a plain
    "No tables found" ValueError got replaced by an ImportError for a package
    this project never asked for, hiding the real failure.
    """
    try:
        tables = pd.read_html(io.StringIO(html), flavor="lxml")
    except ValueError:
        return [], "no_table"
    if not tables:
        return [], "no_table"
    df = tables[0]
    df.columns = [str(c).strip().lower() for c in df.columns]

    def col(*names):
        for n in names:
            if n in df.columns:
                return df[n]
        return pd.Series([None] * len(df))

    out = pd.DataFrame(
        {
            "tx_date": col("transaction date"),
            "owner": col("owner"),
            "ticker": col("ticker"),
            "asset_name": col("asset name"),
            "asset_type": col("asset type"),
            "tx_type": col("type"),
            "amount_raw": col("amount"),
        }
    )
    if out.empty:
        # A table with headers but no data rows. Guarded because the assignment
        # below broadcasts a list of 2-tuples into two columns, and an empty
        # list raises "Columns must be same length as key" -- which would take
        # down the whole run the same way the missing no_table guard did.
        return [], "empty"
    out[["amount_low", "amount_high"]] = [
        parse_amount(a) for a in out["amount_raw"]
    ]
    # eFD writes "--" for a missing ticker; normalize that and NaN to None so
    # DuckDB stores real NULLs instead of the string "--" or a float nan
    text = ["tx_date", "owner", "ticker", "asset_name", "asset_type",
            "tx_type", "amount_raw"]
    out[text] = out[text].replace("--", None).astype(object)
    out[text] = out[text].where(pd.notna(out[text]), None)
    rows = out.to_dict("records")
    return rows, "ok" if rows else "empty"


def _ensure_filings(con):
    """Create senate_filings and adopt filings stored before it existed.

    Mirrors house_filings. Without the backfill the first run after this change
    would re-fetch all ~235 filings already in senate_trades, since the resume
    key moved from that table to this one.
    """
    con.execute("""CREATE TABLE IF NOT EXISTS senate_filings (
        link VARCHAR PRIMARY KEY, last_name VARCHAR, first_name VARCHAR,
        office VARCHAR, filed VARCHAR, status VARCHAR, n_rows INTEGER,
        err VARCHAR)""")
    con.execute("""
        INSERT INTO senate_filings
            (link, last_name, first_name, office, filed, status, n_rows)
        SELECT link, min(last_name), min(first_name), min(office), min(filed),
               'ok', count(*)
        FROM senate_trades WHERE link IS NOT NULL GROUP BY link
        ON CONFLICT (link) DO NOTHING""")


def main(limit=None):
    con = duckdb.connect(DB_PATH)
    ensure_schema(con)  # tables + the `trades` view, shared with scrape_house
    _ensure_filings(con)
    # 'error' and 'no_table' are deliberately absent: both can be transient
    # (a dropped connection, an expiry that outlived the retry), and re-fetching
    # one costs 2s while silently dropping a real filing costs it forever.
    done = {r[0] for r in con.execute(
        "SELECT link FROM senate_filings WHERE status IN ('ok','paper','empty')"
    ).fetchall()}

    s = _session()
    seen = {"ok": 0, "paper": 0, "empty": 0, "no_table": 0, "error": 0}
    for first, last, office, link, filed in list_ptrs(s):
        if limit is not None and sum(seen.values()) >= limit:
            break
        if link in done:
            continue

        rows, err = [], None
        if "/search/view/paper/" in link:
            status = "paper"  # never electronic; no table to parse, so don't fetch
        else:
            time.sleep(RATE_LIMIT_SECS)
            try:
                r = s.get(ROOT + link, timeout=60)
                if _bounced(r):  # session expired mid-run
                    s = _session()
                    r = s.get(ROOT + link, timeout=60)
                r.raise_for_status()
                rows, status = parse_ptr(r.text)
            except Exception as e:
                status, err = "error", f"{type(e).__name__}: {e}"

        if rows:
            df = pd.DataFrame(rows).assign(
                first_name=first, last_name=last, office=office, filed=filed,
                link=link
            )
            con.execute(
                "INSERT INTO senate_trades (first_name, last_name, office, filed,"
                " link, tx_date, owner, ticker, asset_name, asset_type, tx_type,"
                " amount_raw, amount_low, amount_high)"
                " SELECT first_name, last_name, office, filed, link, tx_date, owner,"
                " ticker, asset_name, asset_type, tx_type, amount_raw, amount_low,"
                " amount_high FROM df"
            )
        # Recorded whatever happened -- this row is the resume key, so a filing
        # that yielded nothing still counts as attempted and is not re-fetched.
        con.execute(
            "INSERT OR REPLACE INTO senate_filings (link, last_name, first_name,"
            " office, filed, status, n_rows, err) VALUES (?,?,?,?,?,?,?,?)",
            [link, last, first, office, filed, status, len(rows), err]
        )
        seen[status] += 1
        print(f"{last}, {first} — {len(rows)} txns"
              + ("" if status == "ok" else f" [{status}]")
              + (f" {err}" if err else ""))

    total = con.execute("SELECT count(*) FROM senate_trades").fetchone()[0]
    print(f"\n{total} transactions in {DB_PATH}")
    print("this run: " + ", ".join(f"{k}={v}" for k, v in seen.items()))
    stuck = con.execute(
        "SELECT status, count(*) FROM senate_filings"
        " WHERE status IN ('empty','no_table','error') GROUP BY 1"
    ).fetchall()
    if stuck:
        print("needs a look: " + ", ".join(f"{k}={v}" for k, v in stuck)
              + "  (py q.py \"SELECT * FROM senate_filings WHERE status <> 'ok'\")")


def selftest():
    assert parse_amount("$1,001 - $15,000") == (1001, 15000)
    assert parse_amount("$50,000,001 -") == (50000001, None)
    assert parse_amount("") == (None, None)
    assert parse_amount(None) == (None, None)

    html = """<table><thead><tr><th>#</th><th>Transaction Date</th>
      <th>Owner</th><th>Ticker</th><th>Asset Name</th><th>Asset Type</th>
      <th>Type</th><th>Amount</th></tr></thead><tbody>
      <tr><td>1</td><td>02/18/2026</td><td>Spouse</td><td>MSFT</td>
        <td>Microsoft Corp</td><td>Stock</td><td>Purchase</td>
        <td>$1,001 - $15,000</td></tr>
      <tr><td>2</td><td>02/19/2026</td><td>Self</td><td>--</td>
        <td>Some Muni Bond</td><td>Corporate Bond</td><td>Sale</td>
        <td>$15,001 - $50,000</td></tr></tbody></table>"""
    rows, status = parse_ptr(html)
    assert status == "ok", status
    assert len(rows) == 2, rows
    assert rows[0]["ticker"] == "MSFT"
    assert rows[0]["amount_low"] == 1001 and rows[0]["amount_high"] == 15000
    assert rows[1]["ticker"] is None, "'--' should normalize to None"
    assert rows[1]["tx_type"] == "Sale"

    # The bug this file was fixed for: the eFD agreement page has no table, and
    # pandas raised rather than returning [], so the old `if not tables` guard
    # was unreachable and the ValueError surfaced as a missing-html5lib import.
    agreement = ("<html><body><form>"
                 "<input name='prohibition_agreement' value='1'>"
                 "</form></body></html>")
    rows, status = parse_ptr(agreement)
    assert (rows, status) == ([], "no_table"), (rows, status)

    # A well-formed table with a header but no data rows is a different thing
    # and must not be confused with the above -- it is a filing, just an empty one.
    header_only = ("<table><thead><tr><th>Transaction Date</th><th>Ticker</th>"
                   "</tr></thead><tbody></tbody></table>")
    rows, status = parse_ptr(header_only)
    assert (rows, status) == ([], "empty"), (rows, status)

    print("selftest ok")


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
    else:
        n = sys.argv.index("--limit") + 1 if "--limit" in sys.argv else 0
        main(int(sys.argv[n]) if n else None)
